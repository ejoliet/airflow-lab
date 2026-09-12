"""doorbell_expand — Scenario C: doorbell + dynamic task mapping.

The trigger only rings the bell (here: `airflow dags trigger` from compare.sh).
The claim task drains the scenario queue, then process_file.expand() fans out:
one run per batch, one mapped task instance per file.

Measurement contract: claim() returns the ORDERED key list; measure.py joins
map_index -> key via the claim task's XCom (return_value).
"""

import json
import os
import time

import boto3
from airflow.sdk import dag, task

BUCKET = os.environ.get("PIPELINE_BUCKET", "pipeline-bucket")
QUEUE_NAME = os.environ.get("DOORBELL_QUEUE", "files-c")
PROCESS_SECONDS = float(os.environ.get("PROCESS_SECONDS", "2"))
MAX_DRAIN = int(os.environ.get("MAX_DRAIN", "2000"))


def _client(service):
    return boto3.client(service, endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))


def drain_queue(queue_name: str, max_messages: int = MAX_DRAIN) -> list[str]:
    """Drain SQS until empty; return unique S3 keys in arrival order.

    This is the 'what is new' claiming logic that Scenario C/D moves INTO the
    DAG. Dedupe of at-least-once S3 events happens here (the `seen` set).
    """
    sqs = _client("sqs")
    queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    keys: list[str] = []
    seen: set[str] = set()
    empty_polls = 0
    while len(keys) < max_messages and empty_polls < 2:
        resp = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            empty_polls += 1
            continue
        empty_polls = 0
        for m in msgs:
            body = json.loads(m["Body"])
            for rec in body.get("Records", []):
                key = rec["s3"]["object"]["key"]
                if key not in seen:
                    seen.add(key)
                    keys.append(key)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
    return keys


@dag(dag_id="doorbell_expand", schedule=None, catchup=False, tags=["compare"])
def doorbell_expand():
    @task
    def claim() -> list[str]:
        keys = drain_queue(QUEUE_NAME)
        print(f"claimed {len(keys)} keys")
        return keys  # XCom return_value: index == map_index of process_file

    @task
    def process_file(key: str):
        head = _client("s3").head_object(Bucket=BUCKET, Key=key)
        time.sleep(PROCESS_SECONDS)
        print(f"processed key={key} size={head['ContentLength']}")

    process_file.expand(key=claim())


doorbell_expand()
