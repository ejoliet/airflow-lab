"""Scenario A stand-in for the SOCnotifierHandler Lambda.

Polls files-a, and for EVERY S3 record makes one Airflow REST call:
POST /api/v2/dags/{dag}/dagRuns with a DETERMINISTIC run_id derived from the
key. Duplicate S3 notifications collide with 409 Conflict = dedupe for free
(same trick the real Lambda should use).

Faithful to the handler logic, NOT to Lambda scaling — MAX_CONCURRENCY here is
a thread pool, not an event source mapping.
"""

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import requests

QUEUE_NAME = os.environ.get("QUEUE_NAME", "files-a")
BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://airflow:8080").rstrip("/")
USERNAME = os.environ.get("AIRFLOW_USERNAME", "admin")
PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")
DAG_ID = os.environ.get("TARGET_DAG_ID", "worker_per_file")
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "10"))

_token_lock = threading.Lock()
_token: dict = {"value": None, "ts": 0.0}


def sanitize(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "-", key)


def get_token() -> str:
    """JWT from /auth/token, cached ~4 min.

    AIDEV-NOTE: with SIMPLE_AUTH_MANAGER_ALL_ADMINS=True any user/password
    pair is accepted locally. In prod this is the SSM-stored credential the
    CF stack's Lambda reads.
    """
    with _token_lock:
        if _token["value"] and time.time() - _token["ts"] < 240:
            return _token["value"]
        resp = requests.post(
            f"{BASE_URL}/auth/token",
            json={"username": USERNAME, "password": PASSWORD},
            timeout=10,
        )
        resp.raise_for_status()
        _token["value"] = resp.json()["access_token"]
        _token["ts"] = time.time()
        return _token["value"]


def trigger_run(key: str) -> None:
    run_id = f"a__{sanitize(key)}"
    resp = requests.post(
        f"{BASE_URL}/api/v2/dags/{DAG_ID}/dagRuns",
        headers={"Authorization": f"Bearer {get_token()}"},
        json={"dag_run_id": run_id, "conf": {"key": key, "mechanism": "a"}},
        timeout=30,
    )
    if resp.status_code == 409:
        print(f"dedupe 409: {run_id}")
        return
    resp.raise_for_status()
    print(f"triggered {run_id}")


def main() -> None:
    sqs = boto3.client("sqs", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENCY)
    print(f"polling {QUEUE_NAME} -> {DAG_ID} (concurrency {MAX_CONCURRENCY})")
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
        )
        for m in resp.get("Messages", []):
            body = json.loads(m["Body"])
            futures = [
                pool.submit(trigger_run, rec["s3"]["object"]["key"])
                for rec in body.get("Records", [])
            ]
            try:
                for f in futures:
                    f.result()  # raise before delete -> SQS redelivers (retry)
                sqs.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"]
                )
            except Exception as exc:  # noqa: BLE001 — visibility timeout is the retry
                print(f"trigger failed, message will redeliver: {exc}")


if __name__ == "__main__":
    main()
