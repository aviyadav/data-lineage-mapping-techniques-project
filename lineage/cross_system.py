import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg2.extras import Json


class CrossSystemLineageEmitter:
    def __init__(self, conn):
        self.conn = conn
        self.namespace = "production.data-platform"
        self.producer = "cross-system-emitter/v1.0"

    def _emit_event(
        self,
        event_type: str,
        job_name: str,
        inputs: List[Dict[str, str]],
        outputs: List[Dict[str, str]],
        facets: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
    ) -> str:
        event_id = str(uuid.uuid4())
        run_id = run_id or str(uuid.uuid4())

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lineage.lineage_events (
                    event_id,
                    run_id,
                    event_time,
                    event_type,
                    job_namespace,
                    job_name,
                    producer,
                    inputs,
                    outputs,
                    facets
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event_id,
                    run_id,
                    datetime.now(timezone.utc),
                    event_type,
                    self.namespace,
                    job_name,
                    self.producer,
                    Json(inputs),
                    Json(outputs),
                    Json(facets or {}),
                ),
            )

        self.conn.commit()
        return run_id

    def _insert_table_lineage_edge(
        self,
        source_table: str,
        target_table: str,
        transformation_type: str,
        source_file: str,
        run_id: Optional[str] = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lineage.table_lineage (
                    source_table,
                    target_table,
                    transformation_type,
                    confidence,
                    source_file,
                    run_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    source_table,
                    target_table,
                    transformation_type,
                    0.90,
                    source_file,
                    run_id,
                ),
            )
        self.conn.commit()

    def emit_ingestion_event(
        self,
        source_bucket: str,
        source_key: str,
        target_schema_table: str,
        row_count: int,
        extraction_method: str = "rustfs-postgres-copy",
    ) -> str:
        source_dataset_id = f"rustfs://{source_bucket}/{source_key}"

        inputs = [
            {
                "namespace": f"rustfs.{source_bucket}",
                "name": source_key,
                "id": source_dataset_id,
            }
        ]

        outputs = [
            {
                "namespace": "postgres",
                "name": target_schema_table,
                "id": target_schema_table,
            }
        ]

        facets = {
            "row_count": row_count,
            "extraction_method": extraction_method,
        }

        run_id = self._emit_event(
            event_type="COMPLETE",
            job_name=f"{extraction_method}.{source_key.replace('/', '_')}",
            inputs=inputs,
            outputs=outputs,
            facets=facets,
        )

        self._insert_table_lineage_edge(
            source_table=source_dataset_id,
            target_table=target_schema_table,
            transformation_type="INGESTION",
            source_file="cross-system-ingestion",
            run_id=run_id,
        )

        return run_id

    def emit_transformation_event(
        self,
        input_tables: List[str],
        output_table: str,
        sql_query: str,
        transformation_tool: str = "postgres-sql",
    ) -> str:
        inputs = [
            {
                "namespace": "postgres",
                "name": table,
                "id": table,
            }
            for table in input_tables
        ]

        outputs = [
            {
                "namespace": "postgres",
                "name": output_table,
                "id": output_table,
            }
        ]

        sql_hash = hashlib.sha256(sql_query.encode("utf-8")).hexdigest()[:16]

        facets = {
            "sql_hash": sql_hash,
            "dialect": "postgres",
            "transformation_tool": transformation_tool,
        }

        return self._emit_event(
            event_type="COMPLETE",
            job_name=f"{transformation_tool}.{output_table.replace('.', '_')}_transform",
            inputs=inputs,
            outputs=outputs,
            facets=facets,
        )

    def emit_bi_consumption_event(
        self,
        warehouse_table: str,
        dashboard_name: str,
        bi_tool: str = "bi-dashboard",
    ) -> str:
        bi_dataset_id = f"{bi_tool}.{dashboard_name}"

        inputs = [
            {
                "namespace": "postgres",
                "name": warehouse_table,
                "id": warehouse_table,
            }
        ]

        outputs = [
            {
                "namespace": bi_tool,
                "name": dashboard_name,
                "id": bi_dataset_id,
            }
        ]

        run_id = self._emit_event(
            event_type="COMPLETE",
            job_name=f"{bi_tool}.{dashboard_name}_consume",
            inputs=inputs,
            outputs=outputs,
            facets={"bi_tool": bi_tool},
        )

        self._insert_table_lineage_edge(
            source_table=warehouse_table,
            target_table=bi_dataset_id,
            transformation_type="BI_CONSUMPTION",
            source_file="cross-system-bi",
            run_id=run_id,
        )

        return run_id