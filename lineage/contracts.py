from pathlib import Path
from typing import List

import yaml
from psycopg2.extras import Json

from config import CONTRACT_DIR
from models import DataContract, LineageViolationError


def load_contracts(directory: Path = CONTRACT_DIR) -> List[DataContract]:
    contracts = []

    for path in sorted(directory.glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        contracts.append(DataContract(**data))

    return contracts


def validate_and_register_contract(conn, contract: DataContract) -> dict:
    declared_sources = {
        source["table"]
        for source in contract.lineage.upstream_sources
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT source_table
            FROM lineage.table_lineage
            WHERE target_table = %s
            """,
            (contract.dataset_name,),
        )

        actual_sources = {row["source_table"] for row in cur.fetchall()}

    undocumented = actual_sources - declared_sources
    missing_actual = declared_sources - actual_sources

    if undocumented:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lineage.contract_violations (
                    dataset_name,
                    version,
                    violation_type,
                    details
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    contract.dataset_name,
                    contract.version,
                    "UNDOCUMENTED_DEPENDENCY",
                    Json({"undocumented_sources": sorted(undocumented)}),
                ),
            )
        conn.commit()

        raise LineageViolationError(
            f"Undocumented dependencies detected for {contract.dataset_name}: "
            f"{sorted(undocumented)}"
        )

    fingerprint = contract.generate_fingerprint()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lineage.data_contracts (
                dataset_name,
                version,
                schema_definition,
                lineage,
                sla,
                owner,
                fingerprint,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dataset_name, version) DO UPDATE SET
                schema_definition = EXCLUDED.schema_definition,
                lineage = EXCLUDED.lineage,
                sla = EXCLUDED.sla,
                owner = EXCLUDED.owner,
                fingerprint = EXCLUDED.fingerprint
            """,
            (
                contract.dataset_name,
                contract.version,
                Json(contract.schema_definition),
                Json(contract.lineage.model_dump(mode="json")),
                Json(contract.sla),
                contract.owner,
                fingerprint,
                contract.created_at,
            ),
        )

    conn.commit()

    return {
        "registered": contract.dataset_name,
        "version": contract.version,
        "fingerprint": fingerprint,
        "warnings_declared_but_not_observed": sorted(missing_actual),
    }