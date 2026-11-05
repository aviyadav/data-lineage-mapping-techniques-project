from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from datetime import datetime
from typing import Dict, List, Optional
import uuid

class CrossSystemLineageEmitter:
    """
    Emits standardized lineage events across ingestion, storage, and BI layers.
    Implements OpenLineage spec for multi-system orchestration.
    """

    def __init__(self, endpoint: str = "http://localhost:5000"):
        self.client = OpenLineageClient(url=endpoint)
        self.namespace = "production.data-platform"

    def emit_ingestion_event(self,
                           source_system: str,
                           source_table: str,
                           destination_warehouse: str,
                           destination_table: str,
                           row_count: int = 0,
                           extraction_method: str = "fivetran"):
        """
        Emit lineage for Fivetran/Airbyte ingestion into warehouse.
        """
        run_id = str(uuid.uuid4())

        source_dataset = Dataset(
            namespace=f"{source_system}.production",
            name=source_table,
            facets={"schema": {"fields": []}}
        )

        target_dataset = Dataset(
            namespace=destination_warehouse,
            name=destination_table
        )

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.utcnow().isoformat(),
            run=Run(runId=run_id),
            job=Job(
                namespace=self.namespace,
                name=f"{extraction_method}.{source_table}_sync",
                facets={"documentation": {"description": f"Sync from {source_system}"}}
            ),
            inputs=[source_dataset],
            outputs=[target_dataset],
            producer="cross-system-emitter/v1.0"
        )

        self.client.emit(event)
        return run_id

    def emit_transformation_event(self,
                                  sql_query: str,
                                  input_tables: List[str],
                                  output_table: str,
                                  transformation_tool: str = "dbt"):
        """
        Emit lineage for dbt/Spark transformations within warehouse.
        """
        inputs = [
            Dataset(namespace=self.namespace, name=table)
            for table in input_tables
        ]

        outputs = [Dataset(namespace=self.namespace, name=output_table)]

        sql_facet = {"query": sql_query[:1000], "dialect": "snowflake"}

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.utcnow().isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(
                namespace=self.namespace,
                name=f"{transformation_tool}.{output_table}_transform",
                facets={"sql": sql_facet}
            ),
            inputs=inputs,
            outputs=outputs,
            producer="cross-system-emitter/v1.0"
        )

        self.client.emit(event)

    def emit_bi_consumption_event(self,
                                  warehouse_table: str,
                                  dashboard_name: str,
                                  bi_tool: str = "tableau"):
        """
        Emit lineage from warehouse to BI layer.
        """
        bi_dataset = Dataset(namespace=f"{bi_tool}.production", name=dashboard_name)
        warehouse_dataset = Dataset(namespace=self.namespace, name=warehouse_table)

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.utcnow().isoformat(),
            run=Run(runId=str(uuid.uuid4())),
            job=Job(namespace=self.namespace, name=f"{bi_tool}.{dashboard_name}_refresh"),
            inputs=[warehouse_dataset],
            outputs=[bi_dataset],
            producer="cross-system-emitter/v1.0"
        )

        self.client.emit(event)

    def create_cross_system_lineage(self,
                                   salesforce_table: str,
                                   warehouse_table: str,
                                   dashboard_name: str):
        """
        Complete end-to-end lineage: Salesforce -> Snowflake -> Tableau
        """
        self.emit_ingestion_event(
            source_system="salesforce",
            source_table=salesforce_table,
            destination_warehouse="snowflake",
            destination_table=warehouse_table
        )

        self.emit_bi_consumption_event(
            warehouse_table=warehouse_table,
            dashboard_name=dashboard_name
        )

        print(f"Emitted cross-system lineage: {salesforce_table} -> {warehouse_table} -> {dashboard_name}")
# Usage
emitter = CrossSystemLineageEmitter("http://marquez:5000")
emitter.create_cross_system_lineage(
    salesforce_table="opportunities",
    warehouse_table="fct_opportunities",
    dashboard_name="revenue_dashboard_q3"
)
