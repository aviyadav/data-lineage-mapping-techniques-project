# Data Lineage Technique-to-Code Mapping

This document maps the **10 Data Lineage Mapping Techniques** described in the article *"10 DATA LINEAGE MAPPING Techniques To Untangle Spaghetti Pipeline Dependencies"* by Rohan Dutt to their specific implementations in this Python/PostgreSQL/RustFS project.

## Overview of Implementation Stack

| Component | Technology | Role in Lineage |
| :--- | :--- | :--- |
| **Storage** | PostgreSQL | Metadata store for lineage graphs, contracts, and quality results. |
| **Object Store** | RustFS (S3-compatible) | Source of raw data files; lineage starts here. |
| **Parsing** | `sqlglot` | AST-based extraction of column/table lineage from SQL. |
| **Graphing** | `networkx` + Recursive CTEs | In-memory graph analysis and multi-hop traversal. |
| **Validation** | `pydantic` + YAML | Data contract enforcement and schema validation. |

---

## 1. Implement Column-Level Lineage Tracking

> **Goal:** Trace exactly which source column feeds which target column, including transformation types (e.g., `SUM`, `CAST`).

### Code Implementation
*   **File:** [`lineage/extractors.py`](./lineage/extractors.py)
*   **Key Functions:**
    *   `_extract_column_lineage()`: Parses SQL AST nodes to resolve source columns to target columns.
    *   `_classify_column()`: Detects if a column is transformed via `AGGREGATION`, `CAST`, `FUNCTION`, or `DIRECT_COPY`.
    *   `persist_lineage_for_file()`: Writes the granular edges to the database.
*   **Sensitive Flagging:** Columns like `email` or `customer_id` are automatically flagged as `sensitive=True` based on the `SENSITIVE_COLUMNS` set.

### Database Schema
*   **Table:** `lineage.column_lineage`
*   **Columns:** `source_table`, `source_column`, `target_table`, `target_column`, `transformation_type`, `confidence`, `sensitive`.

### Execution Command
```bash
# Runs during transformation
python cli.py transform

# Or re-parse existing SQL without running it
python cli.py parse-lineage
```

### Verification Query

```sql
SELECT 
    source_table, 
    source_column, 
    target_table, 
    target_column, 
    transformation_type 
FROM lineage.column_lineage 
WHERE target_table = 'marts.revenue_summary'; 
```

## 2. Build a Lineage-First Data Contract Strategy

> **Goal:** Goal: Define contracts with explicit upstream/downstream lineage expectations and validate them against actual runtime behavior.

### Code Implementation
*   **Models:** [`models.py`](./models.py) defines `DataContract` and `LineageMetadata` using Pydantic.
*   **Validator:** [`lineage/contracts.py`](./lineage/contracts.py) contains `validate_and_register_contract()`.
    *   It compares `declared_sources` (from YAML) against `actual_sources` (observed in lineage.table_lineage).
    *   Raises `LineageViolationError` if undocumented dependencies exist.
*   **Contract Definition:** [`contracts/revenue_summary_contract.yml`](./contracts/revenue_summary_contract.yml) declares expected sources (`staging.orders`, `raw.customers`) and sensitivity levels.

### Database Schema
*   **Table:** 
    * `lineage.data_contracts`: Stores active contracts and fingerprints.
    * `lineage.contract_violations`: Logs mismatches between declared and observed lineage.


### Execution Command
```bash
python cli.py register-contracts
```


### Verification Query

```sql
SELECT * FROM lineage.contract_violations ORDER BY created_at DESC;
```

## 3. Use Automated Lineage Parsers on Raw SQL

> **Goal:** Automatically extract dependency graphs by parsing SQL ASTs rather than manual documentation.

### Code Implementation
*   **Library:** Uses `sqlglot` for robust SQL parsing (handles CTEs, Joins, Subqueries).
*   **File:** [`lineage/extractors.py`](./lineage/extractors.py)
*   **Function:** `parse_sql_file(path: Path)`
    * Reads `.sql` files from `sql/staging/` and `sql/marts/`.
    * Identifies `CREATE TABLE ... AS SELECT` statements.
    * Extracts table-level and column-level dependencies automatically.

### Execution Command
```bash
python cli.py parse-lineage
```


### Verification

Check the console output for parsed targets and sources, or query lineage.table_lineage to see edges created solely by static analysis.


## 4. Map Cross-System Lineage Across Your Entire Data Ecosystem

> **Goal:** Track data flow across system boundaries (e.g., Object Storage → Database → BI Tool).

### Code Implementation
*   **File:** [`lineage/cross_system.py`](./lineage/cross_system.py)
*   **Class:** `CrossSystemLineageEmitter`
*   **Method:** 
    * `emit_ingestion_event()`: Creates lineage edge from `rustfs://bucket/file.csv` to `postgres.raw.table`.
    * `emit_transformation_event()`: Links staging tables to marts with SQL hash facets.
    * `emit_bi_consumption_event()`: Links `postgres.marts.table` to `bi.dashboard.name`.


### Database Schema

*   **Table:** `lineage.lineage_events` (Stores OpenLineage-style JSON events).
*   **Table:** `lineage.table_lineage` (Stores the resulting graph edges, e.g., `rustfs://... -> raw.orders`).

### Execution Command

```bash
python cli.py ingest      # Emits RustFS -> Postgres
python cli.py transform   # Emits Staging -> Mart
python cli.py emit-bi     # Emits Mart -> Dashboard
```

### Verification Query

```sql
SELECT 
    source_table, 
    target_table, 
    transformation_type 
FROM lineage.table_lineage 
WHERE source_table LIKE 'rustfs://%';
```

## 5. Leverage Runtime Lineage Capture for Dynamic Pipelines

> **Goal:** Capture what actually happened during execution, especially for pipelines with dynamic table names.

### Code Implementation

*   **Tracker:** [`lineage/runtime.py`](./lineage/runtime.py) implements `RuntimeLineageTracker`.
    * Acts as a context manager to record inputs/outputs during execution.
*   **Dynamic Pipeline:** [`pipeline/dynamic_pipeline.py`](./pipeline/dynamic_pipeline.py)
    * Function `run_dynamic_segmentation()` creates a table name based on runtime parameters (e.g., `marts.customer_segments_20261103`).
    * Static parsers cannot predict this name; runtime tracker captures it.


### Database Schema

*   **Table:** 
    * `lineage.runs`: Stores run metadata (start/end time, status, job name).
    * `lineage.lineage_events`: Stores the specific datasets touched during that run.


### Execution Command

```bash
python cli.py runtime-demo --date 20261103
```

### Verification Query

```sql
SELECT 
    job_name, 
    status, 
    inputs, 
    outputs 
FROM lineage.runs 
ORDER BY start_time DESC LIMIT 5;
```

## 6. Create Dependency Impact Scoring for Prioritized Incident Response

> **Goal:** Quantify business risk (P0/P1/P2) based on downstream blast radius and criticality.

### Code Implementation

*   **File:** [`lineage/impact.py`](./lineage/impact.py)
*   **Logic**
    * Builds a directed graph using `networkx` from `lineage.table_lineage`.
    * Calculates score: `(Downstream Count * 0.3) + (Criticality * 0.5) + (Recovery Time * 0.2)`.
    * Assigns Priority Tier: P0 (Immediate), P1 (4 hours), P2 (24 hours).
*   **Simulation:** `simulate_failure(conn, failed_node)` identifies all affected descendants.

### Database Schema

*   **Table:** `lineage.impact_scores`
*   **Columns:** `table_name`, `impact_score`, `priority_tier`, `downstream_count`, `blast_radius` (JSONB).

### Execution Command

```bash
python cli.py score-impact
# OR simulate a failure
python cli.py simulate-failure --table raw.orders
```

### Verification Query

```sql
SELECT 
    table_name, 
    impact_score, 
    priority_tier, 
    downstream_count 
FROM lineage.impact_scores 
ORDER BY impact_score DESC;
```

## 7. Implement Lineage-Aware Data Quality Monitoring

> **Goal:** Propagate quality failures downstream so consumers know if their data is contaminated.

### Code Implementation

*   **File:** [`lineage/quality.py`](./lineage/quality.py)
*   **Logic:** 
    * Runs checks (Uniqueness, Not Null, etc.) defined in `QUALITY_CHECKS`.
    * If a check fails, it traverses the lineage graph to find all descendants.
    * Marks those descendants as "contaminated" in `lineage.quality_incidents`.
    * Provides `is_safe_to_query(table_name)` helper.

### Database Schema

*   **Table:**
    * `lineage.quality_checks`: Results of individual tests.
    * `lineage.quality_incidents`: Tracks propagation paths and affected downstream nodes.


### Execution Command

```bash
python cli.py run-quality
```

### Verification Query

```sql
SELECT 
    source_table, 
    check_name, 
    severity, 
    contaminated_downstream, 
    propagation_path 
FROM lineage.quality_incidents 
ORDER BY created_at DESC;
```

## 8. Build a Versioned Lineage Graph With Temporal Querying

> **Goal:** Save snapshots of the lineage graph over time to answer "What did the pipeline look like last week?"

### Code Implementation

*   **File:** [`lineage/versioning.py`](./lineage/versioning.py)
*   **Function:**
    * `capture_snapshot()`: Serializes the entire current graph (nodes + edges) into JSON and stores it with a timestamp and Git commit SHA.
    * `diff_versions(time_a, time_b)`: Compares two snapshots to show added/removed dependencies.

### Database Schema

*   **Table:** `lineage.lineage_versions`
*   **Columns:** `snapshot_time`, `commit_sha`, `pipeline_version`, `graph` (JSONB), `schema_versions` (JSONB).

### Execution Command

```bash
python cli.py snapshot-lineage
# OR compare versions
python cli.py diff-lineage --days-back 7
```

### Verification Query

```sql
SELECT 
    version_id, 
    snapshot_time, 
    commit_sha, 
    graph->>'nodes' AS node_count 
FROM lineage.lineage_versions 
ORDER BY snapshot_time DESC;
```



## 9. Apply Graph Database Techniques to Lineage Traversal

> **Goal:** Efficiently traverse multi-hop dependencies (upstream/downstream).

### Implementation Note

While the article suggests Neo4j, this project implements efficient traversal using PostgreSQL Recursive CTEs combined with NetworkX for in-memory algorithms. This avoids extra infrastructure while achieving similar results for moderate-scale datasets.

### Code Implementation

*   **File:** [`lineage/graph.py`](./lineage/graph.py)
*   **Functions:**
    *   `get_downstream(conn, table_name)`: Uses `WITH RECURSIVE` SQL to find all ancestors/descendants up to N hops.
    *   `get_upstream(conn, table_name)`: Reverse traversal.
    *   `find_common_ancestors(conn, table_a, table_b)`: Identifies shared upstream sources (single points of failure).
*   **Usage:** Imported by `lineage/impact.py` and `lineage/quality.py` for graph operations.

### Execution Command

```bash
python cli.py simulate-failure --table raw.orders
python cli.py run-quality
```

### Verification Query (Recursive CTE Example)

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
SELECT DISTINCT node, MIN(hop) AS min_hop
FROM descendants
GROUP BY node
ORDER BY min_hop;
```


## 10. Establish a Lineage Governance Cadence With Ownership Mapping

> **Goal:** Enforce ownership, audit stale lineage, and manage deprecations.

### Code Implementation

*   **File:** [`lineage/governance.py`](./lineage/governance.py)
*   **Features:**
    *   `seed_ownership()`: Maps datasets to owners/teams (simulating a governance catalog).
    *   `audit_lineage_completeness()`: Checks for missing owners or stale verification dates (>90 days).
    *   `notify_upstream_change()`: Simulates alerting downstream owners when an upstream table changes.
    *   `assess_deprecation_impact()`: Blocks deletion/deprecation if critical downstream consumers exist.

### Database Schema

*   **Tables:**
    *   `lineage.dataset_ownership`: Stores owner/team/steward info.
    *   `lineage.governance_audit_log`: History of compliance audits.

### Execution Command

```bash
python cli.py seed-governance
python cli.py audit-governance
python cli.py notify-change --table raw.orders --description "Schema change"
python cli.py deprecate --table staging.orders
```


### Verification Query

```sql
SELECT 
    dataset_name, 
    owner_email, 
    criticality, 
    last_verified 
FROM lineage.dataset_ownership;

SELECT * FROM lineage.governance_audit_log ORDER BY audit_time DESC;
```
