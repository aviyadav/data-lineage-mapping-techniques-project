import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional, Set, Tuple

from psycopg2.extras import Json


class RuntimeLineageTracker:
    def __init__(self, conn, namespace: str = "dynamic-pipelines"):
        self.conn = conn
        self.namespace = namespace
        self.active_run: Optional[dict] = None

    @contextmanager
    def track_run(self, job_name: str, run_id: Optional[str] = None):
        run_id = run_id or str(uuid.uuid4())

        self.active_run = {
            "run_id": run_id,
            "job_name": job_name,
            "start_time": datetime.now(timezone.utc),
            "inputs": set(),
            "outputs": set(),
            "metrics": {},
        }

        try:
            yield self
            self._emit(status="SUCCESS")
        except Exception as exc:
            self._emit(status="FAILED", error=str(exc))
            raise
        finally:
            self.active_run = None

    def record_input(self, namespace: str, dataset_name: str) -> None:
        if self.active_run is None:
            return
        self.active_run["inputs"].add((namespace, dataset_name))

    def record_output(
        self,
        namespace: str,
        dataset_name: str,
        row_count: Optional[int] = None,
    ) -> None:
        if self.active_run is None:
            return
        self.active_run["outputs"].add((namespace, dataset_name, row_count or 0))

    def _emit(self, status: str, error: Optional[str] = None) -> None:
        if self.active_run is None:
            return

        run = self.active_run
        end_time = datetime.now(timezone.utc)

        inputs = [
            {"namespace": ns, "name": name}
            for ns, name in run["inputs"]
        ]

        outputs = [
            {"namespace": ns, "name": name, "row_count": rc}
            for ns, name, rc in run["outputs"]
        ]

        metrics = {"status": status}
        if error:
            metrics["error"] = error

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lineage.runs (
                    run_id,
                    job_name,
                    start_time,
                    end_time,
                    status,
                    inputs,
                    outputs,
                    metrics
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    end_time = EXCLUDED.end_time,
                    status = EXCLUDED.status,
                    inputs = EXCLUDED.inputs,
                    outputs = EXCLUDED.outputs,
                    metrics = EXCLUDED.metrics
                """,
                (
                    run["run_id"],
                    run["job_name"],
                    run["start_time"],
                    end_time,
                    status,
                    Json(inputs),
                    Json(outputs),
                    Json(metrics),
                ),
            )

            cur.execute(
                """
                INSERT INTO lineage.lineage_events (
                    event_id,
                    run_id,
                    event_time,
                    event_type,
                    job_namespace,
                    job_name,
                    producer,
                    inputs,
                    outputs,
                    facets
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(uuid.uuid4()),
                    run["run_id"],
                    end_time,
                    "COMPLETE" if status == "SUCCESS" else "FAIL",
                    self.namespace,
                    run["job_name"],
                    "runtime-lineage-tracker/v1.0",
                    Json(inputs),
                    Json(outputs),
                    Json(metrics),
                ),
            )

            # Persist actual runtime dependency edges.
            for input_ns, input_name in run["inputs"]:
                for output_ns, output_name, row_count in run["outputs"]:
                    cur.execute(
                        """
                        INSERT INTO lineage.table_lineage (
                            source_table,
                            target_table,
                            transformation_type,
                            confidence,
                            source_file,
                            run_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            input_name,
                            output_name,
                            "RUNTIME",
                            0.99,
                            "runtime",
                            run["run_id"],
                        ),
                    )

        self.conn.commit()