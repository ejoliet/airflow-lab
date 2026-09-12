"""native_pull_asset — Scenario B: Airflow-native pull (event-driven scheduling).

An AssetWatcher wraps MessageQueueTrigger on the files-b SQS queue; the
triggerer consumes messages and emits asset events; pending events COALESCE
into one run (this is the scenario that breaks one-file-per-run).

EXPERIMENTAL — requires extra providers not bundled in the official image:
  AIRFLOW_EXTRA_PIP="apache-airflow-providers-common-messaging" (see .env)

AIDEV-NOTE: MessageQueueTrigger kwargs are version-sensitive across 3.x
minors (queue vs scheme+queue_url). Verify against your installed
common-messaging provider before trusting Scenario B numbers.
AIDEV-NOTE: the TRIGGERER process consumes the SQS messages here — watch its
health; it is the ingestion component in this scenario.
"""

import json
import os

from airflow.sdk import Asset, AssetWatcher, dag, task

QUEUE_URL = os.environ.get(
    "ASSET_QUEUE_URL", "http://localstack:4566/000000000000/files-b"
)

try:
    from airflow.providers.common.messaging.triggers.msg_queue import (
        MessageQueueTrigger,
    )

    trigger = MessageQueueTrigger(queue=QUEUE_URL)
    files_asset = Asset(
        "s3_files_b", watchers=[AssetWatcher(name="files_b_watcher", trigger=trigger)]
    )

    @dag(dag_id="native_pull_asset", schedule=[files_asset], catchup=False,
         tags=["compare", "experimental"])
    def native_pull_asset():
        @task
        def process_events(**context):
            # One run may carry MANY asset events (coalescing).
            events = context["triggering_asset_events"][files_asset]
            keys = []
            for ev in events:
                payload = ev.extra.get("payload", {})
                body = payload if isinstance(payload, dict) else json.loads(payload)
                for rec in body.get("Records", []):
                    keys.append(rec["s3"]["object"]["key"])
            print(f"run carried {len(events)} events / {len(keys)} keys: {keys}")

        process_events()

    native_pull_asset()

except ImportError:
    print("Scenario B disabled: apache-airflow-providers-common-messaging not installed")
