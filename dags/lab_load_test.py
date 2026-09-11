"""Lab 3/4 DAGs: tuned watcher for load tests + a slow consumer for backpressure.

Uses a SECOND queue (`lab-events`) so experiments don't interfere with the
baseline `sqs_event_driven` DAG.
"""

import json
import time

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, task

LAB_QUEUE_URL = "http://localstack:4566/000000000000/lab-events"

# AIDEV-NOTE: tuned for the load lab — poll every 2s, grab up to 10 msgs/call,
# 3 API calls per poll cycle. Defaults are waiter_delay=60, max_messages=5,
# num_batches=1 — the 60s default is why "nothing happens" for a minute.
lab_trigger = MessageQueueTrigger(
    scheme="sqs",
    sqs_queue=LAB_QUEUE_URL,
    aws_conn_id="aws_default",
    waiter_delay=2,
    max_messages=10,
    num_batches=3,
)

lab_asset = Asset(
    name="sqs_lab_asset",
    watchers=[AssetWatcher(name="lab_watcher", trigger=lab_trigger)],
)


def _extract_messages(triggering_asset_events):
    """Yield (message_id, body_dict, sent_ts_ms) for every SQS message in the
    triggering AssetEvents. One AssetEvent may carry a BATCH of messages —
    inspect the raw extra once to see the exact shape on your version."""
    for _, events in (triggering_asset_events or {}).items():
        for event in events:
            extra = event.extra or {}
            print(f"RAW extra: {json.dumps(extra, default=str)[:2000]}")
            payload = extra.get("payload", extra)
            batch = payload.get("message_batch", [payload])
            for msg in batch:
                if not isinstance(msg, dict):
                    continue
                body = msg.get("Body") or msg.get("body") or "{}"
                try:
                    body = json.loads(body)
                except (TypeError, ValueError):
                    body = {"raw": body}
                yield msg.get("MessageId", "n/a"), body, body.get("sent_ms")


with DAG(dag_id="lab_fast_consumer", schedule=[lab_asset], catchup=False) as fast:

    @task
    def measure(triggering_asset_events=None):
        now_ms = time.time() * 1000
        n = 0
        for msg_id, body, sent_ms in _extract_messages(triggering_asset_events):
            n += 1
            latency = f"{(now_ms - float(sent_ms))/1000:.1f}s" if sent_ms else "n/a"
            print(f"msg={msg_id} seq={body.get('seq')} latency={latency}")
        print(f"TOTAL messages in this run: {n}")

    measure()


with DAG(
    dag_id="lab_slow_consumer",
    schedule=[lab_asset],       # same asset: fan-out — both DAGs run per event
    catchup=False,
    max_active_runs=1,          # backpressure lever — watch runs queue up
) as slow:

    @task
    def crawl(triggering_asset_events=None):
        n = sum(1 for _ in _extract_messages(triggering_asset_events))
        print(f"processing {n} message(s) slowly...")
        time.sleep(30)

    crawl()
