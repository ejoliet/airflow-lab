"""Event-driven scheduling demo: DAG runs when a message lands in SQS.

Flow:
  SQS message -> MessageQueueTrigger (runs in the TRIGGERER) yields TriggerEvent
  -> AssetWatcher records an AssetEvent on the Asset (payload in .extra)
  -> scheduler starts a run for every DAG with this Asset in `schedule`.

The trigger CONSUMES the message (deletes it from the queue).
"""

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import DAG, Asset, AssetWatcher, task

# AIDEV-NOTE: inside the compose network the queue host is `localstack`,
# not localhost. On EKS this becomes the real SQS URL (env var / Variable).
SQS_QUEUE_URL = "http://localstack:4566/000000000000/demo-events"

trigger = MessageQueueTrigger(
    scheme="sqs",                 # common-messaging >=2.0 API (1.x used `queue=`)
    sqs_queue=SQS_QUEUE_URL,
    aws_conn_id="aws_default",    # extra params pass through to SqsSensorTrigger
)

demo_asset = Asset(
    name="sqs_demo_asset",
    watchers=[AssetWatcher(name="sqs_demo_watcher", trigger=trigger)],
)

with DAG(
    dag_id="sqs_event_driven",
    schedule=[demo_asset],  # no cron: runs on AssetEvents
    catchup=False,
) as dag:

    @task
    def process_message(triggering_asset_events=None):
        """Read the SQS payload off the AssetEvent(s) that caused this run."""
        for asset, events in (triggering_asset_events or {}).items():
            for event in events:
                # event.extra carries the trigger payload (message body inside)
                print(f"asset={asset.name} extra={event.extra}")

    process_message()
