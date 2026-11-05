from dataclasses import dataclass
from typing import List, Dict, Set
import networkx as nx
from datetime import datetime

@dataclass
class QualityAlert:
    severity: str
    message: str
    source_table: str
    contaminated_downstream: List[str]
    propagation_path: List[str]
    timestamp: datetime
class LineageAwareQualityMonitor:
    """
    Propagates data quality failures through lineage graph.
    Prevents bad data from poisoning downstream reports.
    """

    def __init__(self, lineage_graph: nx.DiGraph):
        self.graph = lineage_graph
        self.quality_state: Dict[str, str] = {}
        self.contaminated_cache: Set[str] = set()

    def report_quality_failure(self,
                               table_name: str,
                               check_name: str,
                               severity: str = "high") -> QualityAlert:
        """
        Report quality failure and calculate contaminated downstream.
        """
        self.quality_state[table_name] = "FAILED"

        if table_name in self.graph:
            descendants = nx.descendants(self.graph, table_name)
            self.contaminated_cache.update(descendants)

            for downstream in descendants:
                self.quality_state[downstream] = "CONTAMINATED"

        critical_paths = []
        for target in descendants:
            if self.graph.nodes[target].get('criticality') == 'high':
                try:
                    path = nx.shortest_path(self.graph, table_name, target)
                    critical_paths.append(path)
                except:
                    continue

        return QualityAlert(
            severity=severity,
            message=f"Quality check '{check_name}' failed on {table_name}",
            source_table=table_name,
            contaminated_downstream=list(descendants),
            propagation_path=critical_paths[0] if critical_paths else [],
            timestamp=datetime.utcnow()
        )

    def is_safe_to_query(self, table_name: str) -> Dict:
        """
        Check if table is safe to query or contaminated upstream.
        """
        if table_name in self.contaminated_cache:
            return {
                "safe": False,
                "reason": "Upstream contamination detected",
                "contaminated_by": self._find_contamination_source(table_name)
            }

        if self.quality_state.get(table_name) == "FAILED":
            return {
                "safe": False,
                "reason": "Direct quality failure on this table",
                "contaminated_by": [table_name]
            }

        return {"safe": True}

    def _find_contamination_source(self, table_name: str) -> List[str]:
        sources = []
        for ancestor in nx.ancestors(self.graph, table_name):
            if self.quality_state.get(ancestor) == "FAILED":
                sources.append(ancestor)
        return sources

    def get_quarantine_recommendation(self) -> Dict[str, List[str]]:
        failed_sources = [
            node for node, state in self.quality_state.items()
            if state == "FAILED"
        ]

        quarantine_targets = []
        for source in failed_sources:
            children = list(self.graph.successors(source))
            quarantine_targets.extend(children)

        return {
            "failed_sources": failed_sources,
            "immediate_quarantine": list(set(quarantine_targets)),
            "total_contaminated": list(self.contaminated_cache)
        }
class QualityIntegration:
    def __init__(self, monitor: LineageAwareQualityMonitor):
        self.monitor = monitor

    def great_expectations_callback(self, checkpoint_result: Dict):
        """
        Callback for GE checkpoint to trigger lineage propagation.
        """
        for result in checkpoint_result.get("results", []):
            if not result["success"]:
                table = result["expectation_config"]["meta"].get("table_name")
                if table:
                    alert = self.monitor.report_quality_failure(
                        table_name=table,
                        check_name=result["expectation_config"]["expectation_type"],
                        severity="critical" if result["severity"] == "critical" else "high"
                    )
                    self._send_alert(alert)

    def _send_alert(self, alert: QualityAlert):
        print(f"ALERT [{alert.severity}]: {alert.message}")
        print(f"Contaminated downstream: {len(alert.contaminated_downstream)} tables")
# Usage
G = nx.DiGraph()
G.add_edges_from([
    ("stg_orders", "fct_sales"),
    ("stg_customers", "fct_sales"),
    ("fct_sales", "exec_dashboard")
])
monitor = LineageAwareQualityMonitor(G)
alert = monitor.report_quality_failure("stg_orders", "unique_order_id_check")
status = monitor.is_safe_to_query("exec_dashboard")
