from typing import Dict, List

import networkx as nx
from psycopg2.extras import Json


CRITICALITY_SCORES = {
    "low": 1,
    "medium": 4,
    "high": 7,
    "critical": 10,
}


def load_lineage_graph(conn) -> nx.DiGraph:
    graph = nx.DiGraph()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT source_table, target_table
            FROM lineage.table_lineage
            """
        )

        for row in cur.fetchall():
            graph.add_edge(row["source_table"], row["target_table"])

        cur.execute(
            """
            SELECT dataset_name, criticality, recovery_hours
            FROM lineage.dataset_ownership
            """
        )

        for row in cur.fetchall():
            node = row["dataset_name"]
            if node not in graph:
                graph.add_node(node)

            graph.nodes[node]["criticality"] = row["criticality"] or "medium"
            graph.nodes[node]["recovery_hours"] = float(row["recovery_hours"] or 1.0)

    return graph


def calculate_impact_score(graph: nx.DiGraph, node: str) -> Dict:
    if node not in graph:
        return {"error": "Node not found"}

    descendants = nx.descendants(graph, node)
    downstream_count = len(descendants)

    criticality = graph.nodes[node].get("criticality", "medium")
    recovery_hours = float(graph.nodes[node].get("recovery_hours", 1.0))

    downstream_score = min(downstream_count, 10)
    recovery_score = min((recovery_hours / 40.0) * 10.0, 10.0)

    impact = (
        downstream_score * 0.3
        + CRITICALITY_SCORES.get(criticality, 4) * 0.5
        + recovery_score * 0.2
    )

    impact = min(impact, 10.0)

    if impact >= 8:
        tier = "P0"
        sla = "Immediate"
    elif impact >= 5:
        tier = "P1"
        sla = "4 hours"
    else:
        tier = "P2"
        sla = "24 hours"

    return {
        "table": node,
        "impact_score": round(impact, 2),
        "priority_tier": tier,
        "sla_response": sla,
        "downstream_count": downstream_count,
        "criticality_level": criticality,
        "blast_radius": list(descendants)[:20],
    }


def calculate_and_store_impact_scores(conn) -> List[Dict]:
    graph = load_lineage_graph(conn)
    results = []

    with conn.cursor() as cur:
        for node in graph.nodes():
            score = calculate_impact_score(graph, node)
            if "error" in score:
                continue

            cur.execute(
                """
                INSERT INTO lineage.impact_scores (
                    table_name,
                    impact_score,
                    priority_tier,
                    sla_response,
                    downstream_count,
                    criticality_level,
                    blast_radius,
                    calculated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (table_name) DO UPDATE SET
                    impact_score = EXCLUDED.impact_score,
                    priority_tier = EXCLUDED.priority_tier,
                    sla_response = EXCLUDED.sla_response,
                    downstream_count = EXCLUDED.downstream_count,
                    criticality_level = EXCLUDED.criticality_level,
                    blast_radius = EXCLUDED.blast_radius,
                    calculated_at = NOW()
                """,
                (
                    score["table"],
                    score["impact_score"],
                    score["priority_tier"],
                    score["sla_response"],
                    score["downstream_count"],
                    score["criticality_level"],
                    Json(score["blast_radius"]),
                ),
            )

            results.append(score)

    conn.commit()

    return sorted(results, key=lambda x: x["impact_score"], reverse=True)


def rank_impact(conn, limit: int = 20) -> List[Dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                table_name,
                impact_score,
                priority_tier,
                sla_response,
                downstream_count,
                criticality_level
            FROM lineage.impact_scores
            ORDER BY impact_score DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]


def simulate_failure(conn, failed_node: str) -> Dict:
    graph = load_lineage_graph(conn)

    if failed_node not in graph:
        return {"error": f"Node {failed_node} not found in lineage graph"}

    affected = nx.descendants(graph, failed_node)

    critical_affected = [
        node
        for node in affected
        if graph.nodes[node].get("criticality") in ("critical", "high")
    ]

    return {
        "failed_node": failed_node,
        "total_affected": len(affected),
        "critical_affected": len(critical_affected),
        "business_impact": "SEVERE" if critical_affected else "MODERATE",
        "affected_nodes": list(affected),
    }