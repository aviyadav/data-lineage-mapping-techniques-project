# from pydantic import BaseModel, Field, validator
from pydantic import BaseModel, Field
# from typing import List, Dict, Optional, Literal
from typing import List, Dict, Optional, Literal, Any
from datetime import datetime
import json
import hashlib

class LineageMetadata(BaseModel):
    """Lineage specifications embedded in data contracts."""
    # upstream_sources: List[Dict[str, str]] = Field(
    upstream_sources: List[Dict[str, Any]] = Field(
        ...,
        min_length=1,
        description="List of source datasets with version pins"
    )
    downstream_consumers: List[str] = Field(
        default_factory=list,
        description="Known downstream dependencies"
    )
    transformation_logic: str = Field(
        ...,
        description="SQL or Python logic hash for verification"
    )
    sensitivity_level: Literal["high", "medium", "low"] = "medium"
class DataContract(BaseModel):
    """
    Production-grade data contract with embedded lineage expectations.
    Enforced at pipeline ingress points.
    """
    dataset_name: str
    version: str = "1.0.0"
    schema_definition: Dict
    lineage: LineageMetadata
    sla: Dict[str, str] = Field(default_factory=dict)
    owner: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

    # @validator('upstream_sources')
    # def validate_sources_exist(cls, v):
    #     if not v:
    #         raise ValueError("At least one upstream source required")
    #     return v

    def validate_lineage_integrity(self, actual_sources: List[str]) -> bool:
        """
        Compare declared lineage vs actual runtime lineage.
        Returns False if undocumented dependencies detected.
        """
        declared = {src['table'] for src in self.lineage.upstream_sources}
        actual = set(actual_sources)

        undocumented = actual - declared
        if undocumented:
            raise LineageViolationError(
                f"Undocumented dependencies detected: {undocumented}"
            )
        return True

    def generate_fingerprint(self) -> str:
        """Create immutable contract hash for versioning."""
        # content = f"{self.dataset_name}:{self.version}:{json.dumps(self.lineage.dict(), sort_keys=True)}"
        content = f"{self.dataset_name}:{self.version}:{json.dumps(self.lineage.model_dump(), sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
class LineageViolationError(Exception):
    """Raised when runtime lineage violates contract."""
    pass
class ContractRegistry:
    """Central registry for lineage-first data contracts."""

    def __init__(self):
        self.contracts: Dict[str, DataContract] = {}

    def register(self, contract: DataContract):
        """Register contract with lineage validation."""
        fingerprint = contract.generate_fingerprint()
        self.contracts[contract.dataset_name] = contract
        print(f"Registered contract {contract.dataset_name} with lineage hash {fingerprint}")

    def get_impact_analysis(self, source_table: str) -> List[str]:
        """Find all downstream datasets affected by source change."""
        impacted = []
        for name, contract in self.contracts.items():
            upstream_tables = {src['table'] for src in contract.lineage.upstream_sources}
            if source_table in upstream_tables:
                impacted.append(name)
        return impacted
# Usage
contract = DataContract(
    dataset_name="customer_revenue_metrics",
    schema_definition={"customer_id": "STRING", "revenue": "DECIMAL"},
    lineage=LineageMetadata(
        upstream_sources=[
            {"table": "raw_orders", "version": "v2.1", "column_mapping": ["order_id", "amount"]},
            {"table": "customers", "version": "v1.5", "column_mapping": ["customer_id"]}
        ],
        downstream_consumers=["exec_dashboard", "rfm_model"],
        transformation_logic="hash://sha256/abc123...",
        sensitivity_level="high"
    ),
    owner="data-platform@company.com"
)
registry = ContractRegistry()
registry.register(contract)
impacted = registry.get_impact_analysis("raw_orders")
print(f"Warning: Changing raw_orders will affect: {impacted}")
