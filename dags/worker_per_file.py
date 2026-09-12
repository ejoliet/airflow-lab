"""worker_per_file — the existing DAG contract: ONE file per DAG run.

Receives the S3 key in dag_run.conf["key"]. Used unchanged by:
  Scenario A (notifier poller -> REST API per file)
  Scenario D (controller DAG -> TriggerDagRunOperator per file)
"""

import os
import time

import boto3
from airflow.sdk import dag, task

BUCKET = os.environ.get("PIPELINE_BUCKET", "pipeline-bucket")
PROCESS_SECONDS = float(os.environ.get("PROCESS_SECONDS", "2"))


def _s3():
    # AIDEV-NOTE: AWS_ENDPOINT_URL env routes boto3 to LocalStack; in prod this
    # is absent and boto3 resolves the real endpoint (IRSA on EKS).
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))


@dag(dag_id="worker_per_file", schedule=None, catchup=False, tags=["compare"])
def worker_per_file():
    @task
    def process_file(**context):
        key = (context["dag_run"].conf or {}).get("key")
        if not key:
            raise ValueError("dag_run.conf['key'] is required (one file per run)")
        head = _s3().head_object(Bucket=BUCKET, Key=key)
        time.sleep(PROCESS_SECONDS)  # simulated per-file work
        print(f"processed key={key} size={head['ContentLength']}")

    process_file()


worker_per_file()
