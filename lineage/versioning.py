import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

from psycopg2.extras import Json

from config import GIT_COMMIT_SHA, PIPELINE_VERSION


APP_SCHEMAS = {"raw", "staging", "marts"}


def _compute_schema_versions(conn, nodes: List[str]) -> Dict[str, str]:
    schema_versions = {}

    with conn.cursor() as cur:
        for node in nodes:
            if "." not in node:
                continue

            schema, table = node.split(".", 1)

            if schema not in APP_SCHEMAS:
                continue

            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )

            columns = [dict(row) for row in cur.fetchall()]

            if columns:
                digest = hashlib.md5(
                    json.dumps(columns, sort_keys=True).encode("utf-8")
                ).hexdigest()
                schema_versions[node] = digest

    return schema_versions


def capture_snapshot(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT source_table, target_table
            FROM lineage.table_lineage
            ORDER BY source_table, target_table
            """
        )

        edges = [
            [row["source_table"], row["target_table"]]
            for row in cur.fetchall()
        ]

    nodes = sorted(
        {source for source, _ in edges} | {target for _, target in edges}
    )

    schema_versions = _compute_schema_versions(conn, nodes)

    graph = {
        "nodes": nodes,
        "edges": edges,
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lineage.lineage_versions (
                snapshot_time,
                commit_sha,
                pipeline_version,
                graph,
                schema_versions
            )
            VALUES (%s, %s, %s, %s, %s)
            RETURNING version_id
            """,
            (
                datetime.now(timezone.utc),
                GIT_COMMIT_SHA,
                PIPELINE_VERSION,
                Json(graph),
                Json(schema_versions),
            ),
        )

        version_id = cur.fetchone()["version_id"]

    conn.commit()
    return version_id


def query_lineage_at_time(conn, target_time: datetime) -> Optional[Dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                version_id,
                snapshot_time,
                commit_sha,
                pipeline_version,
                graph,
                schema_versions
            FROM lineage.lineage_versions
            WHERE snapshot_time <= %s
            ORDER BY snapshot_time DESC
            LIMIT 1
            """,
            (target_time,),
        )

        row = cur.fetchone()
        return dict(row) if row else None


def diff_versions(conn, time_a: datetime, time_b: datetime) -> Dict:
    v1 = query_lineage_at_time(conn, time_a)
    v2 = query_lineage_at_time(conn, time_b)

    if not v1 or not v2:
        return {"error": "One or both lineage versions not found"}

    edges_a = {tuple(edge) for edge in v1["graph"].get("edges", [])}
    edges_b = {tuple(edge) for edge in v2["graph"].get("edges", [])}

    added = sorted(edges_b - edges_a)
    removed = sorted(edges_a - edges_b)

    schema_a = v1.get("schema_versions") or {}
    schema_b = v2.get("schema_versions") or {}

    schema_changes = {}

    for table, hash_a in schema_a.items():
        hash_b = schema_b.get(table)
        if hash_a != hash_b:
            schema_changes[table] = {
                "before": hash_a,
                "after": hash_b,
            }

    for table, hash_b in schema_b.items():
        if table not in schema_a:
            schema_changes[table] = {
                "before": None,
                "after": hash_b,
            }

    return {
        "version_a": v1["version_id"],
        "version_b": v2["version_id"],
        "time_range": f"{time_a.isoformat()} to {time_b.isoformat()}",
        "added_dependencies": [list(edge) for edge in added],
        "removed_dependencies": [list(edge) for edge in removed],
        "schema_changes": schema_changes,
    }