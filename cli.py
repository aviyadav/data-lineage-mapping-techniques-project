import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import CONTRACT_DIR, SEED_RAW_DIR, SQL_DIR
from db import get_conn
from lineage.contracts import load_contracts, validate_and_register_contract
from lineage.cross_system import CrossSystemLineageEmitter
from lineage.governance import (
    assess_deprecation_impact,
    audit_lineage_completeness,
    notify_upstream_change,
    seed_ownership,
)
from lineage.impact import (
    calculate_and_store_impact_scores,
    rank_impact,
    simulate_failure,
)
from lineage.quality import is_safe_to_query, run_quality_checks
from lineage.runtime import RuntimeLineageTracker
from lineage.versioning import capture_snapshot, diff_versions
from models import LineageViolationError
from pipelines.dynamic_pipeline import run_dynamic_segmentation
from pipelines.ingest import ingest_seed_files, upload_seed_files_to_rustfs
from pipelines.transform import run_transformations
from storage import ensure_bucket, upload_file


def cmd_init_db(args):
    conn = get_conn(autocommit=True)

    schema_files = [
        SQL_DIR / "01_app_schema.sql",
        SQL_DIR / "02_lineage_schema.sql",
    ]

    with conn.cursor() as cur:
        for path in schema_files:
            cur.execute(path.read_text(encoding="utf-8"))
            print(f"Applied {path.name}")

    conn.close()
    print("Database schema initialized.")


def cmd_upload_seed(args):
    ensure_bucket("demo-raw")

    for csv_file in sorted(SEED_RAW_DIR.glob("*.csv")):
        key = upload_file(csv_file, "demo-raw", key=csv_file.name)
        print(f"Uploaded {csv_file.name} to rustfs://demo-raw/{key}")


def cmd_ingest(args):
    conn = get_conn()
    emitter = CrossSystemLineageEmitter(conn)
    ingest_seed_files(conn, emitter)
    conn.close()


def cmd_transform(args):
    conn = get_conn()
    emitter = CrossSystemLineageEmitter(conn)
    run_transformations(conn, emitter)
    conn.close()


def cmd_emit_bi(args):
    conn = get_conn()
    emitter = CrossSystemLineageEmitter(conn)
    emitter.emit_bi_consumption_event(
        warehouse_table="marts.revenue_summary",
        dashboard_name="revenue_dashboard_q3",
        bi_tool="bi",
    )
    conn.close()
    print("Emitted BI consumption lineage: marts.revenue_summary -> bi.revenue_dashboard_q3")


def cmd_parse_lineage(args):
    conn = get_conn()

    from lineage.extractors import parse_sql_file, persist_lineage_for_file

    sql_files = []

    staging_dir = SQL_DIR / "staging"
    marts_dir = SQL_DIR / "marts"

    if staging_dir.exists():
        sql_files.extend(sorted(staging_dir.rglob("*.sql")))

    if marts_dir.exists():
        sql_files.extend(sorted(marts_dir.rglob("*.sql")))

    for path in sql_files:
        parsed = parse_sql_file(path)
        if parsed.get("target"):
            persist_lineage_for_file(conn, parsed)
            print(f"Parsed lineage for {parsed['target']} from {path.name}")

    conn.close()


def cmd_register_contracts(args):
    conn = get_conn()

    contracts = load_contracts(CONTRACT_DIR)

    for contract in contracts:
        try:
            result = validate_and_register_contract(conn, contract)
            print(f"Registered contract: {result['registered']} v{result['version']}")
            print(f"Fingerprint: {result['fingerprint']}")
            if result["warnings_declared_but_not_observed"]:
                print(
                    "Warnings: declared sources not observed in lineage: "
                    f"{result['warnings_declared_but_not_observed']}"
                )
        except LineageViolationError as exc:
            print(f"Contract violation: {exc}")

    conn.close()


def cmd_runtime_demo(args):
    conn = get_conn()
    tracker = RuntimeLineageTracker(conn)

    date_param = getattr(args, "date", None)

    with tracker.track_run(job_name="dynamic_customer_segmentation"):
        run_dynamic_segmentation(conn, tracker, date_param=date_param)

    conn.close()


def cmd_seed_governance(args):
    conn = get_conn()
    seed_ownership(conn)
    conn.close()
    print("Seeded dataset ownership metadata.")


def cmd_score_impact(args):
    conn = get_conn()

    calculate_and_store_impact_scores(conn)

    limit = getattr(args, "limit", 20)
    ranked = rank_impact(conn, limit=limit)

    print("\nTop impacted lineage nodes:")
    print("-" * 90)
    print(f"{'Table':40} {'Score':>8} {'Tier':>6} {'Downstream':>12} {'Criticality':>12}")
    print("-" * 90)

    for row in ranked:
        print(
            f"{row['table_name']:40} "
            f"{row['impact_score']:>8.2f} "
            f"{row['priority_tier']:>6} "
            f"{row['downstream_count']:>12} "
            f"{row['criticality_level']:>12}"
        )

    conn.close()


def cmd_simulate_failure(args):
    conn = get_conn()
    result = simulate_failure(conn, args.table)

    print("\nFailure simulation:")
    print(f"Failed node: {result.get('failed_node')}")
    print(f"Total affected: {result.get('total_affected')}")
    print(f"Critical affected: {result.get('critical_affected')}")
    print(f"Business impact: {result.get('business_impact')}")

    conn.close()


def cmd_run_quality(args):
    conn = get_conn()
    incidents = run_quality_checks(conn)

    print("\nQuality incidents:")
    if not incidents:
        print("No quality failures detected.")
    else:
        for incident in incidents:
            print(
                f"- {incident['source_table']} failed {incident['check_name']} "
                f"({incident['severity']}): {incident['message']}"
            )
            print(f"  Contaminated downstream: {incident['contaminated_downstream']}")
            if incident["propagation_path"]:
                print(f"  Propagation path: {' -> '.join(incident['propagation_path'])}")

    safety = is_safe_to_query(conn, "marts.revenue_summary")
    print("\nSafety check for marts.revenue_summary:")
    print(safety)

    conn.close()


def cmd_snapshot_lineage(args):
    conn = get_conn()
    version_id = capture_snapshot(conn)
    conn.close()
    print(f"Captured lineage snapshot version_id={version_id}")


def cmd_diff_lineage(args):
    conn = get_conn()

    time_b = datetime.now(timezone.utc)
    time_a = time_b - timedelta(days=args.days_back)

    result = diff_versions(conn, time_a, time_b)

    print("\nLineage diff:")
    print(result)

    conn.close()


def cmd_audit_governance(args):
    conn = get_conn()
    result = audit_lineage_completeness(conn)

    print("\nGovernance audit results:")
    print(f"Compliance rate: {result['compliance_rate']:.2%}")
    print(f"Issues found: {len(result['issues_found'])}")

    for issue in result["issues_found"]:
        owner = issue.get("owner") or "unassigned"
        print(f"- {issue['dataset']}: {issue['issue']} (owner: {owner})")

    conn.close()


def cmd_notify_change(args):
    conn = get_conn()
    notifications = notify_upstream_change(
        conn,
        upstream_table=args.table,
        change_description=args.description,
        change_author=args.author,
    )

    print(f"\nGenerated {len(notifications)} upstream-change notifications.")
    conn.close()


def cmd_deprecate(args):
    conn = get_conn()
    result = assess_deprecation_impact(conn, args.table)

    print("\nDeprecation impact assessment:")
    print(result)

    conn.close()


def cmd_full_demo(args):
    print("=== Initializing database ===")
    cmd_init_db(args)

    print("\n=== Uploading seed files to RustFS ===")
    cmd_upload_seed(args)

    print("\n=== Ingesting raw data ===")
    cmd_ingest(args)

    print("\n=== Running transformations ===")
    cmd_transform(args)

    print("\n=== Emitting BI lineage ===")
    cmd_emit_bi(args)

    print("\n=== Parsing SQL lineage ===")
    cmd_parse_lineage(args)

    print("\n=== Registering data contracts ===")
    cmd_register_contracts(args)

    print("\n=== Seeding governance ownership ===")
    cmd_seed_governance(args)

    print("\n=== Running dynamic runtime lineage demo ===")
    cmd_runtime_demo(args)

    print("\n=== Calculating impact scores ===")
    cmd_score_impact(args)

    print("\n=== Running lineage-aware quality checks ===")
    cmd_run_quality(args)

    print("\n=== Capturing versioned lineage snapshot ===")
    cmd_snapshot_lineage(args)

    print("\n=== Running governance audit ===")
    cmd_audit_governance(args)

    print("\nFull demo completed.")


def build_parser():
    parser = argparse.ArgumentParser(
        description="Data lineage mapping project using PostgreSQL and RustFS."
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "init-db",
        help="Initialize PostgreSQL schemas and lineage tables",
    ).set_defaults(func=cmd_init_db)

    subparsers.add_parser(
        "upload-seed",
        help="Upload seed CSV files to RustFS",
    ).set_defaults(func=cmd_upload_seed)

    subparsers.add_parser(
        "ingest",
        help="Ingest raw CSV files from RustFS into PostgreSQL",
    ).set_defaults(func=cmd_ingest)

    subparsers.add_parser(
        "transform",
        help="Run SQL transformations and persist lineage",
    ).set_defaults(func=cmd_transform)

    subparsers.add_parser(
        "emit-bi",
        help="Emit BI consumption lineage",
    ).set_defaults(func=cmd_emit_bi)

    subparsers.add_parser(
        "parse-lineage",
        help="Parse SQL files and persist lineage",
    ).set_defaults(func=cmd_parse_lineage)

    subparsers.add_parser(
        "register-contracts",
        help="Load and validate data contracts",
    ).set_defaults(func=cmd_register_contracts)

    runtime_parser = subparsers.add_parser(
        "runtime-demo",
        help="Run dynamic pipeline with runtime lineage capture",
    )
    runtime_parser.add_argument(
        "--date",
        default=None,
        help="Runtime date parameter, e.g. 20261103",
    )
    runtime_parser.set_defaults(func=cmd_runtime_demo)

    subparsers.add_parser(
        "seed-governance",
        help="Seed dataset ownership metadata",
    ).set_defaults(func=cmd_seed_governance)

    score_parser = subparsers.add_parser(
        "score-impact",
        help="Calculate and store impact scores",
    )
    score_parser.add_argument(
        "--limit",
        type=int,
        default=20,
    )
    score_parser.set_defaults(func=cmd_score_impact)

    sim_parser = subparsers.add_parser(
        "simulate-failure",
        help="Simulate failure of a lineage node",
    )
    sim_parser.add_argument("--table", required=True)
    sim_parser.set_defaults(func=cmd_simulate_failure)

    subparsers.add_parser(
        "run-quality",
        help="Run lineage-aware quality checks",
    ).set_defaults(func=cmd_run_quality)

    subparsers.add_parser(
        "snapshot-lineage",
        help="Capture versioned lineage snapshot",
    ).set_defaults(func=cmd_snapshot_lineage)

    diff_parser = subparsers.add_parser(
        "diff-lineage",
        help="Diff lineage between now and N days back",
    )
    diff_parser.add_argument(
        "--days-back",
        type=int,
        default=7,
    )
    diff_parser.set_defaults(func=cmd_diff_lineage)

    subparsers.add_parser(
        "audit-governance",
        help="Run lineage governance audit",
    ).set_defaults(func=cmd_audit_governance)

    notify_parser = subparsers.add_parser(
        "notify-change",
        help="Notify downstream owners of upstream change",
    )
    notify_parser.add_argument("--table", required=True)
    notify_parser.add_argument("--description", required=True)
    notify_parser.add_argument("--author", default="cli-user")
    notify_parser.set_defaults(func=cmd_notify_change)

    deprecate_parser = subparsers.add_parser(
        "deprecate",
        help="Assess deprecation impact",
    )
    deprecate_parser.add_argument("--table", required=True)
    deprecate_parser.set_defaults(func=cmd_deprecate)

    full_demo_parser = subparsers.add_parser(
        "full-demo",
        help="Run the complete end-to-end demo",
    )
    full_demo_parser.add_argument(
        "--date",
        default=None,
        help="Optional runtime date parameter for dynamic pipeline, e.g. 20261103",
    )
    full_demo_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit number of impact-score rows printed",
    )
    full_demo_parser.set_defaults(func=cmd_full_demo)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()