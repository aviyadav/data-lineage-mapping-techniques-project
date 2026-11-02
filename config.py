from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "lineage_demo")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


RUSTFS_ENDPOINT_URL = os.getenv("RUSTFS_ENDPOINT_URL", "http://localhost:9001")
RUSTFS_ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY", "minioadmin")
RUSTFS_SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY", "minioadmin")
RUSTFS_REGION = os.getenv("RUSTFS_REGION", "us-east-1")
RUSTFS_BUCKET_RAW = os.getenv("RUSTFS_BUCKET_RAW", "demo-raw")
RUSTFS_BUCKET_CURATED = os.getenv("RUSTFS_BUCKET_CURATED", "demo-curated")


PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "0.1.0")
GIT_COMMIT_SHA = os.getenv("GIT_COMMIT_SHA", "local-dev")

AUDIT_INTERVAL_DAYS = int(os.getenv("AUDIT_INTERVAL_DAYS", "90"))
MIN_LINEAGE_COVERAGE = float(os.getenv("MIN_LINEAGE_COVERAGE", "0.8"))


SQL_DIR = BASE_DIR / "sql"
SEED_RAW_DIR = BASE_DIR / "seed_data" / "raw"
CONTRACT_DIR = BASE_DIR / "contracts"