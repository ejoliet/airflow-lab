"""controller_fanout — Scenario D: controller/worker split.

Doorbell rings the controller; controller drains files-d and maps
TriggerDagRunOperator over the keys with a DETERMINISTIC trigger_run_id
(dedupe: duplicate claims collide instead of double-processing).

worker_per_file stays untouched -> one file per run is preserved.
"""

import os
import re

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

from doorbell_expand import drain_queue  # same claiming logic, different queue

QUEUE_NAME = os.environ.get("CONTROLLER_QUEUE", "files-d")


def sanitize(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "-", key)


@dag(dag_id="controller_fanout", schedule=None, catchup=False, tags=["compare"])
def controller_fanout():
    @task
    def claim_kwargs() -> list[dict]:
        keys = drain_queue(QUEUE_NAME)
        print(f"claimed {len(keys)} keys")
        return [
            {
                "trigger_run_id": f"d__{sanitize(k)}",
                "conf": {"key": k, "mechanism": "d"},
            }
            for k in keys
        ]

    TriggerDagRunOperator.partial(
        task_id="fanout",
        trigger_dag_id="worker_per_file",
        # AIDEV-NOTE: do NOT set wait_for_completion=True at scale — it holds a
        # controller slot per worker run. Fire-and-forget; throttle the worker
        # with max_active_runs / pools instead.
    ).expand_kwargs(claim_kwargs())


controller_fanout()
