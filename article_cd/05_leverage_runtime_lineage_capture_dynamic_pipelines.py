from openlineage.client import OpenLineageClient
from contextlib import contextmanager
from typing import Optional, List, Dict
from datetime import datetime
import functools

class RuntimeLineageTracker:
    """
    Captures actual execution-time lineage for dynamic pipelines.
    Handles parameterized queries and conditional logic invisible to static analysis.
    """

    def __init__(self, namespace: str = "dynamic-pipelines"):
        self.client = OpenLineageClient()
        self.namespace = namespace
        self.active_run = None

    @contextmanager
    def track_run(self, job_name: str, run_id: Optional[str] = None):
        """
        Context manager for tracking runtime lineage of Python transformations.
        Captures actual datasets touched during execution.
        """
        import uuid
        run_id = run_id or str(uuid.uuid4())

        self.active_run = {
            "run_id": run_id,
            "job_name": job_name,
            "start_time": datetime.utcnow(),
            "inputs": set(),
            "outputs": set(),
            "metrics": {}
        }

        try:
            yield self
            self._emit_success()
        except Exception as e:
            self._emit_failure(str(e))
            raise
        finally:
            self.active_run = None

    def record_input(self, dataset_name: str, schema: Optional[Dict] = None):
        """Record dataset read during execution."""
        if self.active_run:
            self.active_run["inputs"].add((dataset_name, str(schema) if schema else ""))

    def record_output(self, dataset_name: str, row_count: Optional[int] = None):
        """Record dataset write during execution."""
        if self.active_run:
            self.active_run["outputs"].add((dataset_name, row_count or 0))

    def _emit_success(self):
        from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

        inputs = [
            Dataset(namespace=self.namespace, name=name, facets={"schema": {"fields": []}})
            for name, _ in self.active_run["inputs"]
        ]

        outputs = [Dataset(namespace=self.namespace, name=name) for name, _ in self.active_run["outputs"]]

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.utcnow().isoformat(),
            run=Run(runId=self.active_run["run_id"]),
            job=Job(namespace=self.namespace, name=self.active_run["job_name"]),
            inputs=inputs,
            outputs=outputs,
            producer="runtime-lineage-tracker/v1.0"
        )

        self.client.emit(event)
        print(f"Emitted runtime lineage for {self.active_run['job_name']}: {len(inputs)} inputs, {len(outputs)} outputs")

    def _emit_failure(self, error: str):
        from openlineage.client.run import RunEvent, RunState, Run, Job

        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.utcnow().isoformat(),
            run=Run(
                runId=self.active_run["run_id"],
                facets={"errorMessage": {"message": error}}
            ),
            job=Job(namespace=self.namespace, name=self.active_run["job_name"]),
            inputs=[],
            outputs=[],
            producer="runtime-lineage-tracker/v1.0"
        )
        self.client.emit(event)

def track_spark_lineage(func):
    """
    Decorator for PySpark jobs to auto-capture runtime lineage.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        tracker = RuntimeLineageTracker()

        with tracker.track_run(job_name=func.__name__):
            result = func(*args, tracker=tracker, **kwargs)
            return result

    return wrapper
# Production Usage Example
@track_spark_lineage
def dynamic_customer_segmentation(spark, date_param: str, tracker: RuntimeLineageTracker):
    """
    Dynamic pipeline where table names depend on runtime parameters.
    Static analysis cannot predict 'customers_20240101' vs 'customers_20240102'.
    """
    source_table = f"customers_{date_param}"
    tracker.record_input(source_table)

    if spark.table(source_table).count() > 1000000:
        output_table = "customer_segments_large"
    else:
        output_table = "customer_segments_small"

    df = spark.table(source_table)
    df.write.mode("overwrite").saveAsTable(output_table)
    tracker.record_output(output_table, row_count=df.count())

    return output_table
