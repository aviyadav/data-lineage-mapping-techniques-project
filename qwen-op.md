# A complete, realistic Python project that implements the 10 data lineage mapping techniques from the article.

It uses:
* PostgreSQL locally as the data warehouse and lineage metadata store.
* RustFS at http://localhost:9001/ as S3-compatible object storage for raw files.
* Python for pipelines, lineage extraction, contracts, impact scoring, quality propagation, versioning, graph traversal, and governance.
* A realistic e-commerce example:
* * Raw customer/order files in RustFS.
* * PostgreSQL raw/staging/marts layers.
* * Column-level lineage from SQL transformations.
* * Cross-system lineage: RustFS → PostgreSQL → BI dashboard.
* * Runtime lineage for dynamic pipelines.
* * Data contracts with lineage validation.
* * Impact scoring.
* * Lineage-aware data quality monitoring.
* * Versioned/temporal lineage snapshots.
* * Recursive CTE graph traversal in PostgreSQL.
* * Governance ownership, audits, change notifications, and deprecation impact assessment.

Create the PostgreSQL database first:

```
createdb lineage_demo

psql -U demouser -c "CREATE DATABASE lineage_demo;"

```

Add project dependency

```bash
uv add -r requirements.txt
```


raw_orders.csv
This intentionally contains:
A null customer_id.
A duplicate order_id.
A cancelled order.
Realistic revenue data.


### Run the generator:

```bash
uv run python scripts/generate_seed_data.py
```

This uses the defaults:

```text
customers = 100
orders    = 15000
seed      = 42
output    = seed_data/raw/
```

You can also run it explicitly:

```bash
uv run python scripts/generate_seed_data.py \
  --customers 100 \
  --orders 15000 \
  --seed 42
```

Example output:

```text
Wrote customers to: /path/to/data-lineage-project/seed_data/raw/raw_customers.csv
Wrote orders to:    /path/to/data-lineage-project/seed_data/raw/raw_orders.csv

Seed data summary
------------------------------------------------------------
Customers generated: 100
Orders generated:    15000
Orders with missing customer_id: 297
Cancelled orders:                1642
Negative amount orders:          72
Duplicate order_id values:       100
Rows involved in duplicates:     200
------------------------------------------------------------
```

### Upload the generated files to RustFS

If you want the generator to upload directly to RustFS:

```bash
uv run python scripts/generate_seed_data.py --upload
```

Or generate first, then use the existing CLI:

```bash
uv run python cli.py upload-seed
```

Then ingest into PostgreSQL:

```bash
uv run python cli.py ingest
```

Then run transformations and capture lineage:

```bash
uv run python cli.py transform
```

You can also rerun lineage parsing independently:

```bash
uv run python cli.py parse-lineage
```




### Run Full Demo

```bash
uv run python cli.py full-demo
```

### Individual Commands (techniques)

```bash
uv run python cli.py init-db
uv run python cli.py upload-seed
uv run python cli.py ingest
uv run python cli.py transform
uv run python cli.py emit-bi
uv run python cli.py parse-lineage
uv run python cli.py register-contracts
uv run python cli.py seed-governance
uv run python cli.py runtime-demo --date 20261103
uv run python cli.py score-impact
uv run python cli.py run-quality
uv run python cli.py snapshot-lineage
uv run python cli.py audit-governance
```

### Useful Analysis Commands

Simulate failure:

```bash
uv run python cli.py simulate-failure --table raw.orders
```

Notify downstream owners:

```bash
uv run python cli.py notify-change \
  --table raw.orders \
  --description "Added new mandatory column fraud_score"
```

Assess deprecation impact:

```bash
uv run python cli.py deprecate --table staging.orders
```

Diff lineage:

```bash
uv run python cli.py diff-lineage --days-back 7
```


Output - What is does

### Raw Data Ingested from RustFS

The project uploads:

```text
seed_data/raw/raw_customers.csv
seed_data/raw/raw_orders.csv
```

to:

```text
rustfs://demo-raw/raw_customers.csv
rustfs://demo-raw/raw_orders.csv
```

Then it loads them into:

```text
raw.customers
raw.orders
```

It emits cross-system lineage:


```text
rustfs://demo-raw/raw_orders.csv -> raw.orders
rustfs://demo-raw/raw_customers.csv -> raw.customers
```


### SQL Transformations Create Staging and Marts

The project executes:

```text
sql/staging/stg_orders.sql
sql/marts/fct_revenue.sql
```

Creating:

```text
staging.orders
marts.revenue_summary
```

Lineage captured:

```text
raw.orders -> staging.orders
staging.orders -> marts.revenue_summary
raw.customers -> marts.revenue_summary
```

BI lineage:

```text
marts.revenue_summary -> bi.revenue_dashboard_q3
```

### Column-Level Lineage

For marts.revenue_summary, the project extracts lineage such as:

```text
staging.orders.customer_id -> marts.revenue_summary.customer_id
raw.customers.region -> marts.revenue_summary.region
staging.orders.amount -> marts.revenue_summary.total_revenue [AGGREGATION]
staging.orders.order_timestamp -> marts.revenue_summary.last_order_at [AGGREGATION]
```

You can inspect it:

```sql
SELECT
    source_table,
    source_column,
    target_table,
    target_column,
    transformation_type,
    sensitive
FROM lineage.column_lineage
ORDER BY target_table, target_column;
```


### Data Contract Validation

```text
contracts/revenue_summary_contract.yml
```

declares upstream sources:

```text
staging.orders
raw.customers
```

The project compares declared lineage against observed lineage from lineage.table_lineage.
If an undocumented dependency appears, it raises:

```text
LineageViolationError
```


and stores a row in:

```text
lineage.contract_violations
```

### Runtime Lineage for Dynamic Pipelines

The dynamic pipeline creates a table like:

```text
marts.customer_segments_20261103
```

Runtime lineage captured:

```text
marts.revenue_summary -> marts.customer_segments_20261103
```

Stored in:

```text
lineage.runs
lineage.lineage_events
lineage.table_lineage
```

Inspect runtime runs:

```sql
SELECT
    run_id,
    job_name,
    status,
    inputs,
    outputs,
    metrics
FROM lineage.runs
ORDER BY start_time DESC;
```

### Impact Scoring

Run:

```bash
uv run python cli.py score-impact
```

The project calculates scores using:

```text
impact =
    downstream_count_score * 0.3
  + criticality_score * 0.5
  + recovery_score * 0.2
```

It stores results in:

```text
lineage.impact_scores
```

Example query:

```sql
SELECT
    table_name,
    impact_score,
    priority_tier,
    downstream_count,
    criticality_level
FROM lineage.impact_scores
ORDER BY impact_score DESC;
```

### Lineage-Aware Quality Failures

The seed data intentionally contains a duplicate order_id.

The quality check:

```text
staging.orders.order_id must be unique
```

fails.
Because lineage knows:

```text
staging.orders -> marts.revenue_summary -> bi.revenue_dashboard_q3
staging.orders -> marts.revenue_summary -> marts.customer_segments_...
```

the project marks downstream datasets as contaminated.

Inspect incidents:

```sql
SELECT
    source_table,
    check_name,
    severity,
    contaminated_downstream,
    propagation_path,
    created_at
FROM lineage.quality_incidents
ORDER BY created_at DESC;
```

Check whether a dashboard source is safe:

```bash
-- Use the Python helper:
uv run python cli.py run-quality
```

The output includes something like:

```text
Safety check for marts.revenue_summary:
{
  'safe': False,
  'reason': 'Upstream contamination detected',
  'table': 'marts.revenue_summary',
  'contaminated_by': 'staging.orders'
}
```

### Versioned Lineage Snapshots

```bash
uv run python cli.py snapshot-lineage
```

This stores a full graph snapshot in:
```text
lineage.lineage_versions
```

Example:

```sql
SELECT
    version_id,
    snapshot_time,
    commit_sha,
    pipeline_version,
    graph
FROM lineage.lineage_versions
ORDER BY snapshot_time DESC;
```

Diff lineage:
```bash
uv run python cli.py diff-lineage --days-back 7
```

### Recursive CTE Graph Traversal

Find downstream dependencies of ```raw.orders```:

```sql
WITH RECURSIVE descendants AS (
    SELECT target_table AS node, 1 AS hop
    FROM lineage.table_lineage
    WHERE source_table = 'raw.orders'

    UNION

    SELECT tl.target_table, d.hop + 1
    FROM lineage.table_lineage tl
    JOIN descendants d ON tl.source_table = d.node
    WHERE d.hop < 10
)
SELECT DISTINCT node, MIN(hop) AS hop
FROM descendants
GROUP BY node
ORDER BY hop;
```

Or use the CLI:

```bash
uv run python cli.py simulate-failure --table raw.orders
```

### Governance Audit

```bash
uv run python cli.py audit-governance
```

The seed governance data intentionally makes:

```text
raw.customers
```

stale and incomplete.

The dynamic segmentation table has no owner.

So the audit reports issues such as:

```text

- raw.customers: Stale lineage verification
- raw.customers: Incomplete lineage coverage
- marts.customer_segments_20261103: Missing ownership assignment

```

Inspect audit logs:

```sql
SELECT
    audit_time,
    issues_found,
    compliance_rate,
    details
FROM lineage.governance_audit_log
ORDER BY audit_time DESC;
```

## Suggested Next Extensions

To make this production-like, you could add:

1. Airflow/Dagster orchestration
    * Wrap ingest, transform, runtime-demo, quality, and snapshot-lineage as tasks.
2. OpenLineage emitter
    * Replace the PostgreSQL event table with actual OpenLineage events sent to Marquez.
3. Great Expectations or Soda integration
    * Replace the simple quality checks with a full expectation suite.
4. dbt integration
    * Use dbt docs generate and parse manifest.json for richer lineage.
5. Neo4j or Amazon Neptune
    * Export lineage.table_lineage into a native graph database for advanced graph algorithms.
6. UI lineage explorer
    * Build a FastAPI + React graph viewer using /api/lineage/downstream, /api/lineage/upstream, and /api/lineage/impact.
7. CI/CD contract enforcement
    * Run python cli.py register-contracts inside GitHub Actions/GitLab CI to block merges when undocumented dependencies are introduced.
