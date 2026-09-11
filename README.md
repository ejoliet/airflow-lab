# Airflow 3.1.8 AssetWatcher Lab (macOS, LocalStack SQS)

Event-driven scheduling (AIP-82): a DAG runs the moment a message lands in an
SQS queue — no cron, no sensor task burning a worker slot.

## Quickstart

```bash
docker compose up -d
chmod +x send-test-message.sh
# Airflow UI: http://localhost:8080 (standalone prints admin password in logs)
docker compose logs airflow | grep -i password

# fire an event
./send-test-message.sh
```

Unpause `sqs_event_driven` in the UI, send a message, watch a run appear.

## How it works

```
producer -> SQS (LocalStack) -> MessageQueueTrigger   (async, in TRIGGERER)
         -> TriggerEvent -> AssetWatcher -> AssetEvent (payload in .extra)
         -> scheduler starts every DAG with schedule=[asset]
```

Key facts:

- Watchers run in the **triggerer**, not the scheduler or workers.
  If the triggerer is down, no events are consumed.
- The trigger **deletes the message** from the queue after firing.
  One message = one AssetEvent = one run per subscribed DAG.
- The payload rides on `AssetEvent.extra`; read it in tasks via the
  `triggering_asset_events` context variable.
- Asset identity is name/URI. Several DAGs can schedule on the same asset;
  you can also mix: `schedule=(asset1 | asset2)` or asset + time.

## Gotchas

- `common-messaging >= 2.0` changed the API: `MessageQueueTrigger(scheme="sqs", sqs_queue=...)`.
  Older 1.x examples show `queue=...` — they won't work here.
- Queue URL host: `localstack` inside the compose network, `localhost` from your Mac.
- Idempotency: make the downstream task safe to re-run. Around triggerer
  failover, duplicate events are possible (known issue with multiple
  triggerer replicas; fixed upstream in 3.2.2 — run one triggerer replica
  on 3.1.x, or dedupe on a message ID).
- `_PIP_ADDITIONAL_REQUIREMENTS` is dev-only; bake providers into your image
  for EKS.

## Mapping to EKS

- Helm chart: `triggerer.enabled: true`, keep `triggerer.replicas: 1` on 3.1.x
  (see gotcha above).
- Replace the endpoint hack with IRSA: the triggerer's service account needs
  `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`.
- Feed the real queue URL via env var or Airflow Variable, not hardcoded.
- Producers: S3 event notifications -> SQS is the classic pattern
  (new file in a bucket triggers the pipeline directly).
