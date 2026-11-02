from datetime import datetime, timedelta, timezone
from typing import Dict, List

from psycopg2.extras import Json

from config import AUDIT_INTERVAL_DAYS, MIN_LINEAGE_COVERAGE
from lineage.graph import get_downstream


SEED_OWNERSHIP = [
    {
        "dataset_name": "raw.orders",
        "owner_email": "ingestion-team@company.com",
        "team": "Ingestion",
        "steward_email": "data-steward@company.com",
        "last_verified_days_ago": 10,
        "documentation_url": "https://wiki.company.com/raw/orders",
        "lineage_completeness": 0.95,
        "criticality": "critical",
        "recovery_hours": 40,
    },
    {
        "dataset_name": "raw.customers",
        "owner_email": "privacy-team@company.com",
        "team": "Privacy",
        "steward_email": "data-steward@company.com",
        "last_verified_days_ago": 200,
        "documentation_url": "https://wiki.company.com/raw/customers",
        "lineage_completeness": 0.75,
        "criticality": "critical",
        "recovery_hours": 24,
    },
    {
        "dataset_name": "staging.orders",
        "owner_email": "platform-team@company.com",
        "team": "Data Platform",
        "steward_email": "data-steward@company.com",
        "last_verified_days_ago": 7,
        "documentation_url": "https://wiki.company.com/staging/orders",
        "lineage_completeness": 0.98,
        "criticality": "high",
        "recovery_hours": 8,
    },
    {
        "dataset_name": "marts.revenue_summary",
        "owner_email": "finance-team@company.com",
        "team": "Finance Data",
        "steward_email": "data-governance@company.com",
        "last_verified_days_ago": 5,
        "documentation_url": "https://wiki.company.com/marts/revenue_summary",
        "lineage_completeness": 0.96,
        "criticality": "critical",
        "recovery_hours": 4,
    },
    {
        "dataset_name": "bi.revenue_dashboard_q3",
        "owner_email": "exec-team@company.com",
        "team": "Executive Reporting",
        "steward_email": "data-governance@company.com",
        "last_verified_days_ago": 2,
        "documentation_url": "https://bi.company.com/dashboards/revenue_q3",
        "lineage_completeness": 0.90,
        "criticality": "critical",
        "recovery_hours": 1,
    },
]


def seed_ownership(conn) -> None:
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        for item in SEED_OWNERSHIP:
            last_verified = now - timedelta(days=item["last_verified_days_ago"])

            cur.execute(
                """
                INSERT INTO lineage.dataset_ownership (
                    dataset_name,
                    owner_email,
                    team,
                    steward_email,
                    last_verified,
                    documentation_url,
                    lineage_completeness,
                    criticality,
                    recovery_hours
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset_name) DO UPDATE SET
                    owner_email = EXCLUDED.owner_email,
                    team = EXCLUDED.team,
                    steward_email = EXCLUDED.steward_email,
                    last_verified = EXCLUDED.last_verified,
                    documentation_url = EXCLUDED.documentation_url,
                    lineage_completeness = EXCLUDED.lineage_completeness,
                    criticality = EXCLUDED.criticality,
                    recovery_hours = EXCLUDED.recovery_hours
                """,
                (
                    item["dataset_name"],
                    item["owner_email"],
                    item["team"],
                    item["steward_email"],
                    last_verified,
                    item["documentation_url"],
                    item["lineage_completeness"],
                    item["criticality"],
                    item["recovery_hours"],
                ),
            )

    conn.commit()


def audit_lineage_completeness(conn) -> Dict:
    issues: List[Dict] = []
    current_time = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                dataset_name,
                owner_email,
                last_verified,
                lineage_completeness
            FROM lineage.dataset_ownership
            """
        )

        owned_datasets = cur.fetchall()

        for row in owned_datasets:
            days_since_verified = (current_time - row["last_verified"]).days

            if days_since_verified > AUDIT_INTERVAL_DAYS:
                issues.append(
                    {
                        "dataset": row["dataset_name"],
                        "issue": "Stale lineage verification",
                        "days_overdue": days_since_verified - AUDIT_INTERVAL_DAYS,
                        "owner": row["owner_email"],
                    }
                )

            if float(row["lineage_completeness"]) < MIN_LINEAGE_COVERAGE:
                issues.append(
                    {
                        "dataset": row["dataset_name"],
                        "issue": "Incomplete lineage coverage",
                        "coverage": float(row["lineage_completeness"]),
                        "owner": row["owner_email"],
                    }
                )

        cur.execute(
            """
            WITH nodes AS (
                SELECT source_table AS dataset
                FROM lineage.table_lineage

                UNION

                SELECT target_table AS dataset
                FROM lineage.table_lineage
            )
            SELECT dataset
            FROM nodes
            WHERE dataset ~ '^(raw|staging|marts|bi)\.'
              AND dataset NOT IN (
                  SELECT dataset_name
                  FROM lineage.dataset_ownership
              )
            ORDER BY dataset
            """
        )

        for row in cur.fetchall():
            issues.append(
                {
                    "dataset": row["dataset"],
                    "issue": "Missing ownership assignment",
                    "owner": None,
                }
            )

    total_audited = len(owned_datasets)
    compliance_rate = 1.0 if total_audited == 0 else max(
        0.0,
        1.0 - (len(issues) / float(total_audited + len(issues) or 1)),
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lineage.governance_audit_log (
                issues_found,
                details,
                compliance_rate
            )
            VALUES (%s, %s, %s)
            """,
            (
                len(issues),
                Json(issues),
                compliance_rate,
            ),
        )

    conn.commit()

    return {
        "total_audited": total_audited,
        "issues_found": issues,
        "compliance_rate": round(compliance_rate, 4),
    }


def notify_upstream_change(
    conn,
    upstream_table: str,
    change_description: str,
    change_author: str = "cli-user",
) -> List[Dict]:
    downstream_rows = get_downstream(conn, upstream_table, max_hops=10)
    downstream_datasets = [row["node"] for row in downstream_rows]

    if not downstream_datasets:
        return []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dataset_name, owner_email, criticality
            FROM lineage.dataset_ownership
            WHERE dataset_name = ANY(%s)
            """,
            (downstream_datasets,),
        )

        affected_owners = [dict(row) for row in cur.fetchall()]

    notifications = []

    for owner in affected_owners:
        notification = {
            "to": owner["owner_email"],
            "dataset": owner["dataset_name"],
            "subject": f"Lineage Alert: Upstream change to {upstream_table}",
            "body": (
                f"The table {upstream_table} has been modified by {change_author}.\n"
                f"Change: {change_description}\n\n"
                f"Your downstream dataset {owner['dataset_name']} may be affected.\n"
                f"Criticality: {owner['criticality']}\n"
                "Please verify data quality and lineage integrity."
            ),
        }

        notifications.append(notification)

        # Mock email sending.
        print(f"MOCK EMAIL to {notification['to']}: {notification['subject']}")

    return notifications


def assess_deprecation_impact(conn, table_name: str) -> Dict:
    downstream_rows = get_downstream(conn, table_name, max_hops=10)
    downstream_datasets = [row["node"] for row in downstream_rows]

    if not downstream_datasets:
        return {
            "approved": True,
            "reason": "No downstream dependencies detected",
            "downstream_count": 0,
            "notification_list": [],
            "grace_period_days": 0,
        }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dataset_name, owner_email, criticality
            FROM lineage.dataset_ownership
            WHERE dataset_name = ANY(%s)
            """,
            (downstream_datasets,),
        )

        downstream_owners = [dict(row) for row in cur.fetchall()]

    critical_consumers = [
        item
        for item in downstream_owners
        if item["criticality"] in ("critical", "high")
    ]

    if critical_consumers:
        return {
            "approved": False,
            "reason": "Critical or high-priority dependencies found",
            "blocking_consumers": critical_consumers,
            "required_action": "Migrate critical consumers before deprecation",
        }

    return {
        "approved": True,
        "downstream_count": len(downstream_datasets),
        "notification_list": [item["owner_email"] for item in downstream_owners],
        "grace_period_days": 30,
    }