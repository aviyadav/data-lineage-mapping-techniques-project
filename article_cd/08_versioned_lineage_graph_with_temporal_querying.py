from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from datetime import datetime
import hashlib
import json
import git
from pathlib import Path

@dataclass
class LineageVersion:
    timestamp: datetime
    commit_sha: str
    pipeline_version: str
    lineage_graph: Dict
    schema_versions: Dict[str, str]

    def to_dict(self):
        return asdict(self)
class VersionedLineageStore:
    """
    Event-sourced lineage storage with temporal querying.
    Supports point-in-time reconstruction for debugging and compliance.
    """

    def __init__(self, storage_path: str = "./lineage_store"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(exist_ok=True)
        self.current_version: Optional[LineageVersion] = None

    def capture_version(self,
                       lineage_graph: Dict,
                       git_repo_path: str = ".") -> LineageVersion:
        """
        Capture current lineage state with git context.
        """
        try:
            repo = git.Repo(git_repo_path)
            commit_sha = repo.head.commit.hexsha
            dirty = repo.is_dirty()
        except:
            commit_sha = "unknown"
            dirty = False

        schema_versions = {}
        for node in lineage_graph.get("nodes", []):
            if "schema" in node:
                schema_str = json.dumps(node["schema"], sort_keys=True)
                schema_versions[node["id"]] = hashlib.md5(schema_str.encode()).hexdigest()

        version = LineageVersion(
            timestamp=datetime.utcnow(),
            commit_sha=f"{commit_sha}{'-dirty' if dirty else ''}",
            pipeline_version=self._get_pipeline_version(),
            lineage_graph=lineage_graph,
            schema_versions=schema_versions
        )

        filename = f"lineage_{version.timestamp.isoformat()}_{commit_sha[:8]}.json"
        with open(self.storage_path / filename, 'w') as f:
            json.dump(version.to_dict(), f, indent=2, default=str)

        self.current_version = version
        return version

    def query_lineage_at_time(self,
                              target_time: datetime,
                              table_name: Optional[str] = None) -> Optional[Dict]:
        """
        Reconstruct lineage graph as it existed at specific time.
        """
        versions = sorted(
            self.storage_path.glob("lineage_*.json"),
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )

        selected_version = None
        for v_file in versions:
            try:
                ts_str = v_file.stem.split('_')[1]
                file_time = datetime.fromisoformat(ts_str)
                if file_time <= target_time:
                    with open(v_file) as f:
                        selected_version = json.load(f)
                    break
            except:
                continue

        return selected_version

    def diff_versions(self,
                     version_a: datetime,
                     version_b: datetime) -> Dict:
        """
        Compare lineage between two points in time.
        """
        v1 = self.query_lineage_at_time(version_a)
        v2 = self.query_lineage_at_time(version_b)

        if not v1 or not v2:
            return {"error": "One or both versions not found"}

        edges_a = set(tuple(e) for e in v1["lineage_graph"].get("edges", []))
        edges_b = set(tuple(e) for e in v2["lineage_graph"].get("edges", []))

        added = edges_b - edges_a
        removed = edges_a - edges_b

        schema_changes = {}
        for table, hash_a in v1.get("schema_versions", {}).items():
            hash_b = v2.get("schema_versions", {}).get(table)
            if hash_a != hash_b:
                schema_changes[table] = {"before": hash_a, "after": hash_b}

        return {
            "added_dependencies": list(added),
            "removed_dependencies": list(removed),
            "schema_changes": schema_changes,
            "time_range": f"{version_a} to {version_b}"
        }

    def _get_pipeline_version(self) -> str:
        import os
        return os.getenv("PIPELINE_VERSION", "unknown")
# Usage
store = VersionedLineageStore()
graph = {
    "nodes": [{"id": "orders", "schema": {"id": "int", "amount": "float"}}],
    "edges": [["raw_orders", "orders"]]
}
version = store.capture_version(graph)
historical = store.query_lineage_at_time(datetime(2024, 1, 15, 10, 0, 0))
