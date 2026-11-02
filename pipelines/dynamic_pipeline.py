from datetime import datetime

from psycopg2 import sql


def run_dynamic_segmentation(conn, tracker, date_param: str | None = None) -> str:
    date_param = date_param or datetime.now().strftime("%Y%m%d")
    output_table = f"marts.customer_segments_{date_param}"

    tracker.record_input("postgres.marts", "marts.revenue_summary")

    schema, table = output_table.split(".")

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE {}.{} AS
                SELECT
                    customer_id,
                    region,
                    total_revenue,
                    order_count,
                    CASE
                        WHEN total_revenue >= 500 THEN 'HIGH_VALUE'
                        ELSE 'STANDARD'
                    END AS segment
                FROM marts.revenue_summary
                """
            ).format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )

        cur.execute(
            sql.SQL("SELECT COUNT(*) AS cnt FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )

        row_count = cur.fetchone()["cnt"]

    tracker.record_output("postgres.marts", output_table, row_count=row_count)

    conn.commit()

    print(f"Created dynamic segmentation table: {output_table} ({row_count} rows)")

    return output_table