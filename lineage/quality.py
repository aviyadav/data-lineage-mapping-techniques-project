from typing import Dict, List, Optional

import networkx as nx
from psycopg2 import sql
from psycopg2.extras import Json

from lineage.impact import load_lineage_graph


QUALITY_CHECKS = [
    {
        "table": "staging.orders",
        "column": "order_id",
        "type": "unique",
        "severity": "critical",
    },
    {
        "table": "staging.orders",
        "column": "customer_id",
        "type": "not_null",
        "severity": "high",
    },
    {
        "table": "staging.orders",
        "column": "amount",
        "type": "non_negative",
        "severity": "high",
    },
    {
        "table": "staging.orders",
        "column": "status",
        "type": "accepted_values",
        "values": ["PLACED", "SHIPPED", "DELIVERED"],
        "severity": "medium",
    },
    {
        "table": "marts.revenue_summary",
        "column": None,
        "type": "row_count_min",
        "min_rows": 1,
        "severity": "critical",
    },
]


def _identifier(table_name: str):
    parts = table_name.split(".")
    return sql.Identifier(*parts)


def _execute_check(conn, check: Dict) -> Dict:
    table = check["table"]
    column = check.get("column")
    check_type = check["type"]

    with conn.cursor() as cur:
        if check_type == "not_null":
            query = sql.SQL(
                "SELECT COUNT(*) AS violations FROM {} WHERE {} IS NULL"
            ).format(_identifier(table), sql.Identifier(column))

            cur.execute(query)
            violations = cur.fetchone()["violations"]
            passed = violations == 0
            message = f"{violations} null values found in {table}.{column}"

        elif check_type == "unique":
            query = sql.SQL(
                """
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT {column}) AS distinct_rows
                FROM {table}
                """
            ).format(
                table=_identifier(table),
                column=sql.Identifier(column),
            )

            cur.execute(query)
            row = cur.fetchone()
            duplicates = row["total_rows"] - row["distinct_rows"]
            passed = duplicates == 0
            message = f"{duplicates} duplicate values found in {table}.{column}"

        elif check_type == "non_negative":
            query = sql.SQL(
                "SELECT COUNT(*) AS violations FROM {} WHERE {} < 0"
            ).format(_identifier(table), sql.Identifier(column))

            cur.execute(query)
            violations = cur.fetchone()["violations"]
            passed = violations == 0
            message = f"{violations} negative values found in {table}.{column}"

        elif check_type == "accepted_values":
            values = check["values"]
            value_literals = sql.SQL(", ").join(sql.Literal(v) for v in values)

            query = sql.SQL(
                """
                SELECT COUNT(*) AS violations
                FROM {table}
                WHERE {column} NOT IN ({values})
                """
            ).format(
                table=_identifier(table),
                column=sql.Identifier(column),
                values=value_literals,
            )

            cur.execute(query)
            violations = cur.fetchone()["violations"]
            passed = violations == 0
            message = f"{violations} invalid values found in {table}.{column}"

        elif check_type == "row_count_min":
            query = sql.SQL("SELECT COUNT(*) AS row_count FROM {}").format(
                _identifier(table)
            )

            cur.execute(query)
            row_count = cur.fetchone()["row_count"]
            min_rows = check.get("min_rows", 1)
            passed = row_count >= min_rows
            message = f"Row count {row_count}, minimum required {min_rows}"

        else:
            raise ValueError(f"Unsupported check type: {check_type}")

    return {
        "table": table,
        "column": column,
        "check_type": check_type,
        "severity": check["severity"],
        "passed": passed,
        "message": message,
    }


def _find_propagation_path(
    graph: nx.DiGraph,
    source_table: str,
    descendants: List[str],
) -> List[str]:
    for target in descendants:
        criticality = graph.nodes[target].get("criticality", "medium")
        if criticality in ("critical", "high"):
            try:
                return nx.shortest_path(graph, source_table, target)
            except nx.NetworkXNoPath:
                continue
    return []


def run_quality_checks(conn) -> List[Dict]:
    graph = load_lineage_graph(conn)
    incidents = []

    for check in QUALITY_CHECKS:
        result = _execute_check(conn, check)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lineage.quality_checks (
                    table_name,
                    column_name,
                    check_type,
                    severity,
                    passed,
                    message
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    result["table"],
                    result["column"],
                    result["check_type"],
                    result["severity"],
                    result["passed"],
                    result["message"],
                ),
            )

        if not result["passed"]:
            source_table = result["table"]

            if source_table in graph:
                descendants = list(nx.descendants(graph, source_table))
            else:
                descendants = []

            propagation_path = _find_propagation_path(graph, source_table, descendants)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO lineage.quality_incidents (
                        source_table,
                        check_name,
                        severity,
                        contaminated_downstream,
                        propagation_path
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        source_table,
                        result["check_type"],
                        result["severity"],
                        Json(descendants),
                        Json(propagation_path),
                    ),
                )

            incident = {
                "source_table": source_table,
                "check_name": result["check_type"],
                "severity": result["severity"],
                "message": result["message"],
                "contaminated_downstream": descendants,
                "propagation_path": propagation_path,
            }

            incidents.append(incident)

    conn.commit()
    return incidents


def is_safe_to_query(conn, table_name: str) -> Dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM lineage.quality_checks
            WHERE table_name = %s
              AND passed = FALSE
            ORDER BY executed_at DESC
            LIMIT 1
            """,
            (table_name,),
        )

        if cur.fetchone():
            return {
                "safe": False,
                "reason": "Direct quality failure detected on this table",
                "table": table_name,
            }

        cur.execute(
            """
            SELECT source_table
            FROM lineage.quality_incidents
            WHERE contaminated_downstream @> jsonb_build_array(%s)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (table_name,),
        )

        row = cur.fetchone()
        if row:
            return {
                "safe": False,
                "reason": "Upstream contamination detected",
                "table": table_name,
                "contaminated_by": row["source_table"],
            }

    return {
        "safe": True,
        "reason": "No direct or upstream quality failures detected",
        "table": table_name,
    }