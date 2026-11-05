from dataclasses import dataclass
from typing import List, Dict, Set, Optional
from datetime import datetime, timedelta
import yaml
from pathlib import Path

@dataclass
class DatasetOwnership:
    dataset_name: str
    owner_email: str
    team: str
    steward_email: str
    last_verified: datetime
    documentation_url: str
    lineage_completeness: float
class LineageGovernanceEngine:
    """
    Automated governance for lineage metadata quality and ownership.
    Implements audit cadence and deprecation protocols.
    """

    def __init__(self, config_path: str = "governance_config.yaml"):
        self.config = self._load_config(config_path)
        self.ownership_registry: Dict[str, DatasetOwnership] = {}
        self.audit_log: List[Dict] = []

    def _load_config(self, path: str) -> Dict:
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except:
            return {
                "audit_interval_days": 90,
                "minimum_lineage_coverage": 0.8,
                "alerts_enabled": True
            }

    def register_ownership(self, ownership: DatasetOwnership):
        self.ownership_registry[ownership.dataset_name] = ownership

    def audit_lineage_completeness(self) -> Dict:
        """
        Quarterly audit: Find datasets missing lineage or ownership.
        """
        issues = []
        current_time = datetime.utcnow()

        for dataset, ownership in self.ownership_registry.items():
            if (current_time - ownership.last_verified).days > self.config["audit_interval_days"]:
                issues.append({
                    "dataset": dataset,
                    "issue": "Stale lineage",
                    "days_overdue": (current_time - ownership.last_verified).days,
                    "owner": ownership.owner_email
                })

            if ownership.lineage_completeness < self.config["minimum_lineage_coverage"]:
                issues.append({
                    "dataset": dataset,
                    "issue": "Incomplete lineage",
                    "coverage": ownership.lineage_completeness,
                    "owner": ownership.owner_email
                })

        self.audit_log.append({
            "timestamp": current_time,
            "issues_found": len(issues),
            "details": issues
        })

        return {
            "total_audited": len(self.ownership_registry),
            "issues_found": issues,
            "compliance_rate": 1 - (len(issues) / len(self.ownership_registry)) if self.ownership_registry else 0
        }

    def notify_upstream_change(self,
                               upstream_table: str,
                               change_description: str,
                               change_author: str):
        """
        Automatic notification to all downstream owners when upstream changes.
        """
        affected = self._get_downstream_owners(upstream_table)

        if not affected:
            return

        for owner in affected:
            self._send_notification(
                to=owner["email"],
                subject=f"Lineage Alert: Upstream change to {upstream_table}",
                body=f"""
                The table {upstream_table} has been modified by {change_author}.
                Change: {change_description}

                Your downstream dataset {owner['dataset']} may be affected.
                Please verify data quality and lineage integrity.
                """
            )

    def assess_deprecation_impact(self, table_name: str) -> Dict:
        """
        Deprecation protocol: Assess blast radius before decommissioning.
        """
        downstream = self._get_downstream_owners(table_name)

        critical_consumers = [
            d for d in downstream
            if d.get("criticality") == "high"
        ]

        if critical_consumers:
            return {
                "approved": False,
                "reason": "Critical dependencies found",
                "blocking_consumers": critical_consumers,
                "required_action": "Migrate critical consumers first"
            }

        return {
            "approved": True,
            "downstream_count": len(downstream),
            "notification_list": [d["email"] for d in downstream],
            "grace_period_days": 30
        }

    def _get_downstream_owners(self, table: str) -> List[Dict]:
        return [
            {
                "dataset": "exec_dashboard",
                "email": "ceo-team@company.com",
                "steward": "data-steward@company.com",
                "criticality": "high"
            }
        ]

    def _send_notification(self, to: str, subject: str, body: str):
        if not self.config.get("alerts_enabled"):
            print(f"MOCK EMAIL to {to}: {subject}")
            return
        print(f"Sent notification to {to}")
class OwnershipCLI:
    def __init__(self, engine: LineageGovernanceEngine):
        self.engine = engine

    def run_audit(self):
        print("Running lineage governance audit...")
        results = self.engine.audit_lineage_completeness()

        print(f"Compliance Rate: {results['compliance_rate']:.1%}")
        print(f"Issues Found: {len(results['issues_found'])}")

        for issue in results['issues_found']:
            print(f"  - {issue['dataset']}: {issue['issue']} (Owner: {issue['owner']})")
# Usage
engine = LineageGovernanceEngine()
engine.register_ownership(DatasetOwnership(
    dataset_name="fct_revenue",
    owner_email="finance-team@company.com",
    team="Finance Data",
    steward_email="data-governance@company.com",
    last_verified=datetime.utcnow() - timedelta(days=100),
    documentation_url="https://wiki.company.com/fct_revenue",
    lineage_completeness=0.95
))
cli = OwnershipCLI(engine)
cli.run_audit()
