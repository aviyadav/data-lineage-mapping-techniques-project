from pathlib import Path

from config import SQL_DIR
from lineage.extractors import parse_sql_file, persist_lineage_for_file


TRANSFORM_SQL_FILES = [
    SQL_DIR / "staging" / "stg_orders.sql",
    SQL_DIR / "marts" / "fct_revenue.sql",
]


def run_transformations(conn, emitter=None) -> list:
    results = []

    for path in TRANSFORM_SQL_FILES:
        sql_text = path.read_text(encoding="utf-8")

        with conn.cursor() as cur:
            cur.execute(sql_text)

        conn.commit()

        parsed = parse_sql_file(path)

        if parsed.get("target"):
            persist_lineage_for_file(conn, parsed)

            if emitter:
                emitter.emit_transformation_event(
                    input_tables=parsed["sources"],
                    output_table=parsed["target"],
                    sql_query=sql_text,
                    transformation_tool="postgres-sql",
                )

            results.append(
                {
                    "file": str(path),
                    "target": parsed["target"],
                    "sources": parsed["sources"],
                }
            )

            print(f"Transformed {parsed['target']} from {parsed['sources']}")

    return results