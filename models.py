import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field


class LineageMetadata(BaseModel):
    upstream_sources: List[Dict[str, Any]] = Field(..., min_length=1)
    downstream_consumers: List[str] = Field(default_factory=list)
    transformation_logic: str
    sensitivity_level: Literal["high", "medium", "low"] = "medium"


class DataContract(BaseModel):
    dataset_name: str
    version: str = "1.0.0"
    schema_definition: Dict[str, str]
    lineage: LineageMetadata
    sla: Dict[str, str] = Field(default_factory=dict)
    owner: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def generate_fingerprint(self) -> str:
        payload = {
            "dataset_name": self.dataset_name,
            "version": self.version,
            "lineage": self.lineage.model_dump(mode="json"),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()
        ).hexdigest()[:16]


class LineageViolationError(Exception):
    """Raised when runtime/static lineage violates a declared data contract."""