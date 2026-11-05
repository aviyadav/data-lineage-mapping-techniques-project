import networkx as nx
from dataclasses import dataclass
from typing import Dict, List, Tuple
from enum import Enum
import pandas as pd

class CriticalityLevel(Enum):
    CRITICAL = 10
    HIGH = 7
    MEDIUM = 4
    LOW = 1
@dataclass
class LineageNode:
    table_name: str
    consumer_count: int
    criticality: CriticalityLevel
    recovery_complexity_hours: float
    owner: str
    last_updated: str
class ImpactScoringEngine:
    """
    Calculates business impact scores for lineage nodes using graph algorithms.
    Enables P0/P1/P2 prioritization for incident response.
    """

    def __init__(self):
        self.graph = nx.DiGraph()

    def build_lineage_graph(self, edges: List[Tuple[str, str]], node_metadata: Dict[str, Dict]):
        self.graph.add_edges_from(edges)

        for node, meta in node_metadata.items():
            if node in self.graph:
                self.graph.nodes[node].update(meta)

    def calculate_impact_score(self, node: str) -> Dict:
        """
        Calculate weighted impact score.
        Formula: (Downstream Count * 0.3) + (Criticality * 0.5) + (Recovery * 0.2)
        """
        if node not in self.graph:
            return {"error": "Node not found"}

        try:
            descendants = nx.descendants(self.graph, node)
            downstream_count = len(descendants)
        except nx.NetworkXError:
            downstream_count = 0

        attrs = self.graph.nodes[node]
        criticality = attrs.get('criticality', CriticalityLevel.LOW)
        recovery_hours = attrs.get('recovery_complexity_hours', 1.0)

        recovery_score = min(recovery_hours / 40 * 10, 10)

        impact = (downstream_count * 0.3) + (criticality.value * 0.5) + (recovery_score * 0.2)

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
            "criticality_level": criticality.name,
            "blast_radius": list(descendants)[:10]
        }

    def rank_all_nodes(self) -> pd.DataFrame:
        results = []

        for node in self.graph.nodes():
            score = self.calculate_impact_score(node)
            results.append(score)

        df = pd.DataFrame(results)
        return df.sort_values('impact_score', ascending=False)

    def simulate_failure(self, failed_node: str) -> Dict:
        affected = nx.descendants(self.graph, failed_node)

        critical_affected = [
            n for n in affected
            if self.graph.nodes[n].get('criticality') == CriticalityLevel.CRITICAL
        ]

        return {
            "failed_node": failed_node,
            "total_affected": len(affected),
            "critical_affected": len(critical_affected),
            "business_impact": "SEVERE" if critical_affected else "MODERATE",
            "affected_nodes": list(affected)
        }
# Production Usage
scorer = ImpactScoringEngine()
edges = [
    ("raw_orders", "stg_orders"),
    ("stg_orders", "fct_revenue"),
    ("fct_revenue", "exec_dashboard"),
    ("fct_revenue", "ml_customer_model")
]
metadata = {
    "raw_orders": {
        "criticality": CriticalityLevel.CRITICAL,
        "recovery_complexity_hours": 24,
        "owner": "ingestion-team"
    },
    "exec_dashboard": {
        "criticality": CriticalityLevel.CRITICAL,
        "recovery_complexity_hours": 1,
        "owner": "ceo-staff"
    }
}
scorer.build_lineage_graph(edges, metadata)
impact = scorer.calculate_impact_score("raw_orders")
print(f"raw_orders impact score: {impact['impact_score']} ({impact['priority_tier']})")
