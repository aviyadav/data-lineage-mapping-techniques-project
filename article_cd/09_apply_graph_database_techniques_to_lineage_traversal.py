from neo4j import GraphDatabase
from typing import List, Dict, Optional
import logging

class Neo4jLineageManager:
    """
    Production-grade lineage storage using Neo4j.
    Optimized for multi-hop dependency traversal and impact analysis.
    """

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.logger = logging.getLogger(__name__)

    def close(self):
        self.driver.close()

    def create_lineage_graph(self, edges: List[Dict]):
        """
        Batch insert lineage edges with properties.
        """
        with self.driver.session() as session:
            for edge in edges:
                session.run("""
                    MERGE (source:Dataset {name: $source_table, system: $source_system})
                    MERGE (target:Dataset {name: $target_table, system: $target_system})
                    CREATE (source)-[r:TRANSFORMS_TO {
                        timestamp: $timestamp,
                        job_name: $job,
                        sql_hash: $sql_hash
                    }]->(target)
                """,
                source_table=edge["source"],
                source_system=edge.get("source_system", "unknown"),
                target_table=edge["target"],
                target_system=edge.get("target_system", "unknown"),
                timestamp=edge.get("timestamp"),
                job=edge.get("job_name"),
                sql_hash=edge.get("sql_hash")
                )

    def get_impact_analysis(self,
                           source_table: str,
                           max_hops: int = 10) -> List[Dict]:
        """
        Find all downstream tables affected by source change.
        Uses Neo4j's native graph traversal (fast vs recursive SQL).
        """
        query = """
        MATCH path = (source:Dataset {name: $table})-[r:TRANSFORMS_TO*1..$hops]->(downstream:Dataset)
        RETURN downstream.name AS table_name,
               downstream.system AS system,
               length(path) AS hop_distance,
               [node in nodes(path) | node.name] AS full_path
        ORDER BY hop_distance ASC
        """

        with self.driver.session() as session:
            result = session.run(query, table=source_table, hops=max_hops)
            return [dict(record) for record in result]

    def find_common_ancestors(self, table_a: str, table_b: str) -> List[str]:
        """
        Find shared upstream dependencies between two tables.
        Critical for identifying single points of failure.
        """
        query = """
        MATCH (a:Dataset {name: $table_a})<-[:TRANSFORMS_TO*]-(common:Dataset)-[:TRANSFORMS_TO*]->(b:Dataset {name: $table_b})
        RETURN DISTINCT common.name AS shared_source
        """

        with self.driver.session() as session:
            result = session.run(query, table_a=table_a, table_b=table_b)
            return [record["shared_source"] for record in result]

    def calculate_centrality(self) -> List[Dict]:
        """
        Calculate PageRank to find most critical tables in pipeline.
        High centrality = high impact if failed.
        """
        query = """
        CALL gds.graph.project('lineage', 'Dataset', 'TRANSFORMS_TO')
        CALL gds.pageRank.stream('lineage')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).name AS table, score
        ORDER BY score DESC
        LIMIT 20
        """

        try:
            with self.driver.session() as session:
                result = session.run(query)
                return [dict(record) for record in result]
        except Exception as e:
            self.logger.error(f"Graph Data Science library not available: {e}")
            return []
# Usage
manager = Neo4jLineageManager("bolt://localhost:7687", "neo4j", "password")
edges = [
    {"source": "raw_orders", "target": "stg_orders", "job_name": "ingestion"},
    {"source": "stg_orders", "target": "fct_revenue", "job_name": "transform"}
]
manager.create_lineage_graph(edges)
impacts = manager.get_impact_analysis("raw_orders")
print(f"Affected tables: {len(impacts)}")
