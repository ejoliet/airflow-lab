"""Lab 8: the four-scenario shootout (see attached decision guide).

  A  Lambda/poller push  -> scripts/scenario_a_poller.py -> fts_worker (1 file = 1 run)
  B  Native watcher      -> lab6_doorbell_ingest (coalesces; already in lab)
  C  Doorbell + .expand()-> scenario_c_expand   (1 run = 1 batch, 1 mapped TI = 1 file)
  D  Controller/worker   -> scenario_d_controller -> fts_worker (1 file = 1 run)

C and D schedule on the SAME asset lab6 defines (identity by name — no second
watcher). Keep lab6_doorbell_ingest UNPAUSED: its watcher is what turns queue
messages into asset events for all three.
"""

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Asset, Variable, task

BUCKET = "roman-incoming"
PREFIX = "incoming/"
DOORBELL = Asset(name="s3_incoming_doorbell")  # defined+watched in lab6_doorbell.py


def _claim(hwm_var):
    """Shared claiming logic: list keys past this scenario's own bookmark."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hwm = Variable.get(hwm_var, default="")
    keys = sorted(S3Hook(aws_conn_id="aws_default").list_keys(BUCKET, prefix=PREFIX) or [])
    new = [k for k in keys if k > hwm]
    if new:
        Variable.set(hwm_var, new[-1])
    print(f"{hwm_var}: claimed {len(new)} new keys")
    return new


# ---------------------------------------------------------------- worker (A+D)
with DAG(dag_id="fts_worker", schedule=None, catchup=False) as worker:

    @task
    def process(dag_run=None):
        key = (dag_run.conf or {}).get("key")
        assert key, "expected conf['key'] — one file per run"
        print(f"run_id={dag_run.run_id} processing s3://{BUCKET}/{key}")

    process()


# ------------------------------------------------------------------- C: expand
with DAG(
    dag_id="scenario_c_expand",
    schedule=[DOORBELL],
    catchup=False,
    max_active_runs=1,
) as c_dag:

    @task
    def claim_c():
        return _claim("scenario_c_hwm")

    @task
    def process_file(key: str):
        # AIDEV-NOTE: on KubernetesExecutor each mapped TI is its own pod.
        print(f"[mapped] processing s3://{BUCKET}/{key}")

    process_file.expand(key=claim_c())


# --------------------------------------------------------------- D: controller
with DAG(
    dag_id="scenario_d_controller",
    schedule=[DOORBELL],
    catchup=False,
    max_active_runs=1,  # serializes HWM claims
) as d_dag:

    @task
    def claim_d():
        # Deterministic per-file run ids: duplicates skip instead of colliding.
        return [
            {"trigger_run_id": f"fts_{k.replace('/', '_')}", "conf": {"key": k}}
            for k in _claim("scenario_d_hwm")
        ]

    TriggerDagRunOperator.partial(
        task_id="fan_out",
        trigger_dag_id="fts_worker",
        skip_when_already_exists=True,  # dedupe: existing run_id -> skipped, not failed
        reset_dag_run=False,
    ).expand_kwargs(claim_d())
