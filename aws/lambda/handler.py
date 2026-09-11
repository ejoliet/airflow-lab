"""SQS -> Airflow asset-event bridge (doorbell flavor).

One Lambda invocation receives a BATCH of SQS messages (assembled by the
event source mapping). We post ONE asset event per invocation summarizing
the batch — the DAG lists S3 itself (doorbell + high-water mark), so the
event payload is informational only.

Env: AIRFLOW_API_BASE, ASSET_NAME, AIRFLOW_SECRET_ARN
Secret JSON: {"username": "...", "password": "..."}
"""

import json
import os
import time
import urllib.request

import boto3

BASE = os.environ["AIRFLOW_API_BASE"].rstrip("/")
ASSET_NAME = os.environ["ASSET_NAME"]
SECRET_ARN = os.environ["AIRFLOW_SECRET_ARN"]

_cache = {"token": None, "exp": 0.0, "asset_id": None}


def _req(path, method="GET", body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read() or "{}")


def _token():
    if _cache["token"] and time.time() < _cache["exp"]:
        return _cache["token"]
    sec = json.loads(
        boto3.client("secretsmanager").get_secret_value(SecretId=SECRET_ARN)["SecretString"]
    )
    # AIDEV-NOTE: token endpoint path depends on your auth manager; verify
    # against your deployment (Airflow 3.x JWT auth: POST /auth/token).
    out = _req("/auth/token", "POST", {"username": sec["username"], "password": sec["password"]})
    _cache["token"] = out["access_token"]
    _cache["exp"] = time.time() + 600  # refresh well before typical expiry
    return _cache["token"]


def _asset_id(token):
    # CreateAssetEventsBody requires the numeric asset_id, not the name/uri.
    if _cache["asset_id"] is None:
        assets = _req(f"/api/v2/assets?name_pattern={ASSET_NAME}", token=token)["assets"]
        _cache["asset_id"] = next(a["id"] for a in assets if a["name"] == ASSET_NAME)
    return _cache["asset_id"]


def handler(event, context):
    keys = []
    for rec in event.get("Records", []):
        body = json.loads(rec["body"])
        for s3rec in body.get("Records", []):  # S3 notification schema
            keys.append(s3rec["s3"]["object"]["key"])

    if keys:
        tok = _token()
        _req(
            "/api/v2/assets/events",
            "POST",
            {
                "asset_id": _asset_id(tok),
                "extra": {"source": "s3-sqs-lambda", "n_files": len(keys), "sample": keys[:10]},
            },
            token=tok,
        )
    # All-or-nothing: any exception above -> whole batch retried by the ESM,
    # then DLQ after maxReceiveCount. Duplicate events are harmless with the
    # doorbell DAG (extra runs find new=0), so retries are safe.
    return {"batchItemFailures": []}
