"""Scenario A: per-file push to the Airflow REST API (stand-in for the Lambda).

Runs INSIDE the airflow container so it can read the standalone-generated
admin password and reach the api-server on localhost:

    docker compose exec airflow python /opt/airflow/scripts/scenario_a_poller.py

Drains scenario-a-events; for each S3 record posts
POST /api/v2/dags/fts_worker/dagRuns with a deterministic dag_run_id derived
from the key. Duplicate notifications -> 409 Conflict = free dedupe (counted).
Fidelity note: this reproduces the HANDLER logic and API pressure, not Lambda
scaling/VPC/SSM — those only fail in the real account.
"""

import json
import os
import pathlib
import time
import urllib.error
import urllib.request

import boto3

API = "http://localhost:8080"
QUEUE = "http://localstack:4566/000000000000/scenario-a-events"
DAG_ID = "fts_worker"


def token():
    creds_file = pathlib.Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / \
        "simple_auth_manager_passwords.json.generated"
    user, pw = next(iter(json.loads(creds_file.read_text()).items()))
    req = urllib.request.Request(
        f"{API}/auth/token", method="POST",
        data=json.dumps({"username": user, "password": pw}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["access_token"]


def post_run(tok, key):
    run_id = "fts_" + key.replace("/", "_")
    body = {"dag_run_id": run_id, "logical_date": None, "conf": {"key": key}}
    req = urllib.request.Request(
        f"{API}/api/v2/dags/{DAG_ID}/dagRuns", method="POST",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {tok}"})
    try:
        urllib.request.urlopen(req)
        return "created"
    except urllib.error.HTTPError as e:
        if e.code == 409:
            return "duplicate(409)"
        raise


def main():
    sqs = boto3.client("sqs", endpoint_url="http://localstack:4566",
                       region_name="us-east-1",
                       aws_access_key_id="test", aws_secret_access_key="test")
    tok = token()
    stats = {"created": 0, "duplicate(409)": 0}
    idle = 0
    t0 = time.time()
    while idle < 3:  # exit after ~3 empty polls
        resp = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        msgs = resp.get("Messages", [])
        idle = idle + 1 if not msgs else 0
        for m in msgs:
            for rec in json.loads(m["Body"]).get("Records", []):
                outcome = post_run(tok, rec["s3"]["object"]["key"])
                stats[outcome] = stats.get(outcome, 0) + 1
            sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=m["ReceiptHandle"])
    print(f"done in {time.time()-t0:.1f}s: {stats}")


if __name__ == "__main__":
    main()
