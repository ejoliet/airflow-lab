"""Lab 6: doorbell pattern — SQS event says "new data", the DAG lists S3 itself.

Why: at thousands of files, per-message payloads are the wrong contract.
The watcher only wakes the DAG; the DAG lists the bucket past a high-water
mark and processes everything found. Lost or duplicate notifications become
harmless — the next doorbell catches the full backlog.
"""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, Variable, task

BUCKET = "roman-incoming"
PREFIX = "incoming/"
DOORBELL_QUEUE = "http://localstack:4566/000000000000/doorbell-events"
HWM_VAR = "lab6_high_water_mark"  # last processed key (lexicographic)

doorbell = MessageQueueTrigger(
    scheme="sqs",
    sqs_queue=DOORBELL_QUEUE,
    aws_conn_id="aws_default",
    waiter_delay=2,
    max_messages=10,
    num_batches=3,
)

incoming = Asset(
    name="s3_incoming_doorbell",
    watchers=[AssetWatcher(name="doorbell_watcher", trigger=doorbell)],
)

with DAG(
    dag_id="lab6_doorbell_ingest",
    schedule=[incoming],
    catchup=False,
    max_active_runs=1,  # AIDEV-NOTE: serializes HWM read/update; events arriving
                        # mid-run coalesce into the next run — nothing is missed.
) as dag:

    @task
    def ingest():
        """List keys past the high-water mark and process them idempotently.

        We deliberately IGNORE the message payload — the doorbell only says
        "look at the bucket". Requires monotonically increasing key names
        (timestamp prefix); see GUIDE Lab 6 for the trade-offs.
        """
        hook = S3Hook(aws_conn_id="aws_default")
        hwm = Variable.get(HWM_VAR, default="")
        keys = sorted(hook.list_keys(bucket_name=BUCKET, prefix=PREFIX) or [])
        new = [k for k in keys if k > hwm]
        print(f"hwm={hwm!r} listed={len(keys)} new={len(new)}")

        for key in new:
            # Idempotent work goes here: deterministic output key + overwrite,
            # so reprocessing after a crash is harmless.
            print(f"processing s3://{BUCKET}/{key}")

        if new:
            Variable.set(HWM_VAR, new[-1])
        return len(new)

    ingest()
