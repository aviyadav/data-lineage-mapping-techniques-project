import tempfile
from pathlib import Path

from psycopg2 import sql

from config import RUSTFS_BUCKET_RAW, SEED_RAW_DIR
from storage import download_file, ensure_bucket, upload_file


FILE_TO_TABLE = {
    "raw_customers.csv": "raw.customers",
    "raw_orders.csv": "raw.orders",
}

TABLE_COLUMNS = {
    "raw.customers": [
        "customer_id",
        "name",
        "email",
        "region",
        "signup_date",
    ],
    "raw.orders": [
        "order_id",
        "customer_id",
        "product_id",
        "amount",
        "order_timestamp",
        "status",
    ],
}


def upload_seed_files_to_rustfs() -> None:
    ensure_bucket(RUSTFS_BUCKET_RAW)

    for filename in FILE_TO_TABLE.keys():
        local_path = SEED_RAW_DIR / filename
        upload_file(local_path, RUSTFS_BUCKET_RAW, key=filename)
        print(f"Uploaded {local_path.name} to rustfs://{RUSTFS_BUCKET_RAW}/{filename}")


def _truncate_table(cur, table_name: str) -> None:
    schema, table = table_name.split(".")
    cur.execute(
        sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
    )


def _copy_csv_to_table(cur, table_name: str, csv_path: Path) -> None:
    schema, table = table_name.split(".")
    columns = TABLE_COLUMNS[table_name]

    column_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)

    copy_sql = sql.SQL(
        "COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        column_sql,
    )

    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)


def _row_count(cur, table_name: str) -> int:
    schema, table = table_name.split(".")
    cur.execute(
        sql.SQL("SELECT COUNT(*) AS cnt FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
    )
    return cur.fetchone()["cnt"]


def ingest_seed_files(conn, emitter=None) -> list:
    ensure_bucket(RUSTFS_BUCKET_RAW)

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for filename, table_name in FILE_TO_TABLE.items():
            local_file = tmp_path / filename
            download_file(RUSTFS_BUCKET_RAW, filename, local_file)

            with conn.cursor() as cur:
                _truncate_table(cur, table_name)
                _copy_csv_to_table(cur, table_name, local_file)
                count = _row_count(cur, table_name)

            conn.commit()

            if emitter:
                emitter.emit_ingestion_event(
                    source_bucket=RUSTFS_BUCKET_RAW,
                    source_key=filename,
                    target_schema_table=table_name,
                    row_count=count,
                )

            results.append(
                {
                    "file": filename,
                    "table": table_name,
                    "row_count": count,
                }
            )

            print(f"Ingested {filename} into {table_name}: {count} rows")

    return results