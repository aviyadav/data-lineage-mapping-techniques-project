# Data Lineage Mapping Project

This project demonstrates 10 practical data lineage mapping techniques using:

- PostgreSQL
- RustFS / S3-compatible object storage
- Python
- SQL transformation parsing
- Data contracts
- Runtime lineage capture
- Impact scoring
- Lineage-aware data quality
- Temporal lineage versioning
- Recursive CTE graph traversal
- Governance ownership and audits

## Prerequisites

- PostgreSQL running locally.
- RustFS or another S3-compatible storage running at `http://localhost:9001`.
- Python 3.10+.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env