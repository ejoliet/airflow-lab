# Hands-On: Airflow 3.1.8 Asset Watchers — Edge Cases Lab (macOS)

> Learn event-driven scheduling (AIP-82) by breaking it locally, so the EKS +
> KubernetesExecutor + real SQS/S3 setup holds no surprises.

## Overview

Nine labs, all bash. Lab 8 verifies the four FTS trigger-mechanism scenarios (Lambda push / native pull / doorbell+expand / controller-worker) claim by claim. Labs 0–5 run on docker compose (LocalExecutor) for fast iteration; Lab 6 adds emulated S3 with real bucket notifications; Lab 7 moves the same DAGs to kind + Helm + KubernetesExecutor. Labs 1–2 build the mental model;
Labs 3–5 are the point: load, latency knobs, backpressure, idempotency, and
failure injection. Each lab ends with a **What transfers to EKS** note.

Everything runs in Docker on your Mac: Airflow 3.1.8 (`standalone` = api-server
+ scheduler + dag-processor + triggerer), Postgres, LocalStack (SQS emulation).

## Prerequisites

- Docker Desktop or colima running (`docker info` works)
- This repo unzipped; `chmod +x send-test-message.sh burst.sh localstack/init-queue.sh`
- No AWS CLI needed — all queue commands use `awslocal` inside the LocalStack container

---

## Lab 0 — Bring It Up (10 min)

1. Start and wait for healthy:

   ```bash
   docker compose up -d
   docker compose ps            # wait until localstack is healthy
   docker compose logs -f airflow   # Ctrl-C once you see the api-server banner
   ```

2. Grab the admin password and log in at <http://localhost:8080>:

   ```bash
   docker compose logs airflow | grep -i "password"
   ```

3. Unpause both `sqs_event_driven` and `lab_fast_consumer` (UI toggle, or):

   ```bash
   docker compose exec airflow airflow dags unpause sqs_event_driven
   docker compose exec airflow airflow dags unpause lab_fast_consumer
   ```

4. Smoke test:

   ```bash
   ./send-test-message.sh
   ```

   Within seconds a run of `sqs_event_driven` appears. Open the task log —
   the message body is printed from `AssetEvent.extra`.

> ⚠️ First boot is slow: `_PIP_ADDITIONAL_REQUIREMENTS` pip-installs the
> common-messaging provider at startup. Dev-only shortcut — on EKS you bake
> providers into the image.

**What transfers to EKS**: the container layout mirrors the Helm chart — the
watcher lives in the *triggerer* pod, not scheduler or workers. Executor choice
(Kubernetes vs Celery vs Local) does not change the event path at all; it only
changes what happens *after* the run is created.

---

## Lab 1 — Anatomy: Where Everything Runs (15 min)

Goal: prove to yourself which component does what.

1. Watch the triggerer poll. It logs the trigger lifecycle:

   ```bash
   docker compose logs -f airflow 2>&1 | grep -i -E "trigger|asset"
   ```

2. Send a message and trace the chain in the UI:
   - **Assets** page → `sqs_demo_asset` → an AssetEvent per firing, `extra` visible
   - **DAG runs** → run type is `asset_triggered`, no logical schedule

3. Inspect from the CLI:

   ```bash
   docker compose exec airflow airflow assets list
   docker compose exec airflow airflow dags list-runs -d sqs_event_driven
   ```

4. Peek at the database (this is what you'll query on EKS when debugging):

   ```bash
   docker compose exec postgres psql -U airflow -c \
     "SELECT id, timestamp, extra FROM asset_event ORDER BY id DESC LIMIT 5;"
   ```

**Key fact**: the SQS trigger *deletes* messages on reception
(`delete_message_on_reception=True` default). The queue is drained by the
watcher itself — there is no separate consumer to build.

**What transfers to EKS**: `asset_event` lives in your Aurora metadata DB;
same queries work. If the triggerer pod is down, messages simply accumulate
in SQS — that's your buffer, and why queue depth is the metric to alarm on.

---

## Lab 2 — Payload Plumbing and Filtering (20 min)

Goal: know exactly what lands in `extra`, and filter noise at the trigger.

1. Unpause `lab_fast_consumer`, then:

   ```bash
   ./burst.sh 1
   ```

2. Read the task log. The DAG prints `RAW extra: ...` — study the shape.
   One AssetEvent can carry a **batch** of SQS messages (`message_batch`),
   because one poll can receive up to `max_messages`. This is the single
   most important shape to internalize before writing consumers.

3. Exercise — filter at the trigger. Edit `dags/lab_load_test.py`, add to
   `lab_trigger`:

   ```python
   message_filtering="jsonpath",
   message_filtering_match_values=["new_product"],
   message_filtering_config="event",
   ```

   Then send one matching and one non-matching message:

   ```bash
   docker compose exec localstack awslocal sqs send-message \
     --queue-url http://localhost:4566/000000000000/lab-events \
     --message-body '{"event": "heartbeat"}'
   ./burst.sh 1
   ```

   Only the `new_product` message creates an event. The heartbeat is still
   consumed (deleted) — filtering drops it, it doesn't leave it queued.

> 💡 Real-world shape: S3 event notifications → SQS wraps everything in a
> `Records` array. Your jsonpath and body-parsing must match *that* schema,
> not a hand-rolled one. Test with a captured real S3 event body.

**What transfers to EKS**: filtering in the trigger saves DAG runs — every
run on KubernetesExecutor is a pod launch (~10–60 s overhead). Filtering
noise before the AssetEvent is a real cost lever there.

---

## Lab 3 — Load, Latency, and the Knobs (30 min)

Goal: measure message→run latency, then see how bursts fan in.

### 3a. Latency baseline

`lab_fast_consumer` computes latency from `sent_ms` in the body:

```bash
./burst.sh 1
# then read the task log: "latency=2.3s"
```

Now the knob experiment. The trigger defaults are `waiter_delay=60`,
`max_messages=5`, `num_batches=1`. The lab DAG overrides to
`waiter_delay=2, max_messages=10, num_batches=3`. Temporarily remove those
three lines, restart, and measure again:

```bash
docker compose restart airflow
./burst.sh 1
```

Latency jumps to up-to-60 s. This is the #1 "event-driven feels slow" cause.

| Knob | Default | Effect |
|------|---------|--------|
| `waiter_delay` | 60 s | Poll interval → dominates event latency |
| `max_messages` | 5 | Msgs per SQS API call (SQS caps receive at 10) |
| `num_batches` | 1 | API calls per poll cycle → burst drain rate |

### 3b. Burst fan-in

Put the tuned knobs back, restart, then:

```bash
./burst.sh 50
docker compose exec airflow airflow dags list-runs -d lab_fast_consumer | head -30
```

**Observe and record**: 50 messages did *not* make 50 runs. Messages received
in the same poll batch ride one TriggerEvent → one AssetEvent → one run.
Count `TOTAL messages in this run` across the run logs — it sums to 50, but
run count depends on arrival timing vs poll timing.

> ⚠️ Consequence: never design as "one message = one run". Design as
> "one run processes ≥1 messages". This changes how you write the consumer.

### 3c. Backpressure

`lab_slow_consumer` shares the same asset (fan-out: both DAGs run per event)
but sleeps 30 s with `max_active_runs=1`:

```bash
docker compose exec airflow airflow dags unpause lab_slow_consumer
./burst.sh 50
```

Watch the UI: `lab_fast_consumer` chews through quickly; `lab_slow_consumer`
stacks queued runs. Events keep being consumed from SQS regardless — the
backlog moves from the queue into Airflow's run queue.

**What transfers to EKS**: on KubernetesExecutor, each queued run becomes a
pod when released — a burst becomes a pod storm. Levers: `max_active_runs`
per DAG, bigger `max_messages` (more consolidation per run), and cluster
autoscaler headroom. Decide *where* you want the backlog to live: SQS
(triggerer down / filtered) or Airflow run queue (consumed but unprocessed).
SQS is the better buffer — it has DLQs and depth metrics.

---

## Lab 4 — Idempotency and Failure Injection (40 min)

Goal: find every path to a duplicate or lost message, then defend in the DAG.

### 4a. Producer retries (duplicate bodies)

SQS standard queues are at-least-once; producers also retry. Simulate:

```bash
BODY='{"seq": 999, "event": "new_product", "dedupe_key": "obs_000123"}'
for i in 1 2; do
  docker compose exec -T localstack awslocal sqs send-message \
    --queue-url http://localhost:4566/000000000000/lab-events \
    --message-body "$BODY"
done
```

Two events, two runs, same payload. Defense — dedupe in the consumer:

```python
@task
def process(triggering_asset_events=None):
    from airflow.sdk import Variable
    for msg_id, body, _ in _extract_messages(triggering_asset_events):
        key = f"seen_{body.get('dedupe_key', msg_id)}"
        if Variable.get(key, default=None):
            print(f"SKIP duplicate {key}")
            continue
        # ... do the work ...
        Variable.set(key, "1")
```

> 💡 Variables are fine for the lab. In production use a Postgres table with
> a unique constraint (insert-or-skip in one transaction with the work), or
> make the work naturally idempotent (deterministic S3 output key +
> overwrite). Note `MessageId` changes on redelivery — dedupe on a
> *business* key from the body, not on MessageId.

### 4b. Triggerer death — the receive/delete window

The trigger receives, deletes, then yields the event. Kill it mid-burst and
look for gaps or duplicates:

```bash
./burst.sh 50 & sleep 2 && docker compose restart airflow
# after restart, unpause persists; sum "TOTAL messages" across runs — is it 50?
```

Run this a few times. You may see fewer than 50 (deleted but event lost —
message gone) or exactly 50. The window is small but real: **delete-on-receive
means a crash between delete and event-record loses the message.**

Defense options, trade-offs:

| Option | Pros | Cons |
|--------|------|------|
| Accept loss risk, alarm on gaps | Simple | Bad for must-process pipelines |
| `delete_message_on_reception=False` + delete in the DAG task after work | At-least-once end-to-end | You own deletion; needs `visibility_timeout` > poll+run time or you get redeliveries (more duplicates → 4a dedupe becomes mandatory) |
| Upstream ledger (S3 inventory / manifest reconciliation job) | Catches anything | Extra pipeline |

Try option 2: set `delete_message_on_reception=False` and
`visibility_timeout=120` on the trigger, add an explicit
`SqsHook().conn.delete_message(...)` at the end of the task, burst, and watch
what happens when the task fails before deleting (message reappears after
120 s → new run → your dedupe saves you).

### 4c. Triggerer outage (clean)

```bash
docker compose stop airflow
./burst.sh 20        # messages sit in the queue
docker compose exec localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/lab-events \
  --attribute-names ApproximateNumberOfMessages
docker compose start airflow   # backlog drains on its own
```

This is the *good* failure mode: nothing lost, SQS buffers.

### 4d. Poison configuration

Point a watcher at a queue that doesn't exist (edit the URL to
`.../no-such-queue`), restart, and read triggerer logs. Know what this error
storm looks like *before* you see it at 2 a.m. Related bug class to remember:
some triggers have yielded a TriggerEvent **on exception**, causing runaway
DAG runs — treat "DAG firing repeatedly with empty payloads" as a trigger
misconfiguration signal.

> ⚠️ 3.1.x-specific: with **multiple triggerer replicas**, the same asset
> watcher could run on each replica → N duplicate events per message. Fixed
> in 3.2.2. On 3.1.8: `triggerer.replicas: 1` in Helm, and dedupe anyway.

**What transfers to EKS**: all of it — this lab *is* the production design
review. Your answers here (dedupe key, delete strategy, replica count,
DLQ policy) become the PR description for the EKS rollout.

---

## Lab 5 — Observability Checklist (15 min)

What you watched by hand here must become dashboards on EKS:

| Signal | Local command | EKS equivalent |
|--------|---------------|----------------|
| Queue depth | `awslocal sqs get-queue-attributes ...` (4c) | CloudWatch `ApproximateNumberOfMessagesVisible` + alarm |
| Watcher alive | `docker compose logs airflow \| grep -i trigger` | Triggerer pod liveness + `triggers.running` metric |
| Event flow | `asset_event` table (Lab 1) | Same query on Aurora; UI Assets page |
| Run backlog | `airflow dags list-runs` | Scheduler `dagrun.queued` metrics / Grafana |
| Msg→run latency | `sent_ms` trick (Lab 3a) | Emit as a custom metric from the task |
| Lost messages | Sum of processed vs sent (4b) | DLQ + reconciliation job against S3 inventory |

---

## Lab 6 — Doorbell Pattern: Real S3 Events at Scale (30 min)

Goal: the design that survives "thousands of files in one blast". LocalStack
now emulates **S3 too**, with a real bucket-notification wired to SQS:
`s3://roman-incoming` → `s3:ObjectCreated:*` → `doorbell-events` queue.

The `lab6_doorbell_ingest` DAG *ignores the message payload*. The event is
only a doorbell; the task lists the bucket past a **high-water mark**
(Airflow Variable holding the last processed key) and processes everything
newer. `max_active_runs=1` serializes HWM updates; events arriving mid-run
coalesce into the next run.

1. Recreate LocalStack with the new init (S3 + notification):

   ```bash
   docker compose down localstack && docker compose up -d
   docker compose exec airflow airflow dags unpause lab6_doorbell_ingest
   ```

2. Blast files and watch:

   ```bash
   ./drop-files.sh 500
   ```

   Observe: 500 S3 events → a handful of runs → each run's log shows
   `new=N` summing to 500. Run count is small no matter the blast size.

3. Resilience checks:
   - **Lost doorbell**: `docker compose stop airflow`, drop 100 files, purge
     the queue (`awslocal sqs purge-queue ...`), start Airflow, drop 1 file.
     That single doorbell's run picks up all 101 — the pattern self-heals.
   - **Duplicate doorbell**: send the same S3 event twice → second run finds
     `new=0`. Idempotent by construction, no dedupe table needed.

> ⚠️ HWM trade-offs: requires monotonically increasing key names (timestamp
> prefix). Producers writing out-of-order keys need a different mark
> (`LastModified` window with overlap, or per-file `.done` markers, or an
> S3 Inventory/manifest diff). Same idea, different bookmark.

**What transfers to EKS**: this is the production shape for survey ingest.
The doorbell tolerates every S3-notification weakness (at-least-once,
unordered, occasional single event for near-simultaneous writes), and the
Inventory reconciliation job becomes a weekly backstop instead of a crutch.

---

## Lab 7 — kind + Helm + KubernetesExecutor (60–90 min)

Goal: same DAGs, real Kubernetes semantics — triggerer placement, worker
pod-per-task, and the burst→pod-storm behavior you'll manage on EKS.

Setup: LocalStack runs **inside** the cluster as a Service named `localstack`
in namespace `airflow`, so the unchanged DAG files resolve the same hostname.
Providers and DAGs are baked into a custom image (`Dockerfile`) and
`kind load`-ed — no registry, `pullPolicy: Never`.

1. Prereqs and bring-up:

   ```bash
   brew install kind kubectl helm   # if needed
   ./k8s/up.sh
   kubectl -n airflow port-forward svc/airflow-api-server 8080:8080
   ```

2. Repeat Lab 6 on k8s:

   ```bash
   kubectl -n airflow exec deploy/airflow-scheduler -- \
     airflow dags unpause lab6_doorbell_ingest
   ./drop-files.sh 500 kind
   kubectl -n airflow get pods -w
   ```

   **Observe**: each task = one worker pod. Time pod startup vs task runtime —
   on small tasks, pod overhead dominates. This is why Lab 3's consolidation
   (fewer, fatter runs) matters twice as much on KubernetesExecutor.

3. Repeat Lab 4b here: `kubectl -n airflow delete pod -l component=triggerer`
   mid-burst. Kubernetes restarts it; verify the watcher resumes and count
   processed files (the doorbell pattern makes the answer "all of them").

4. DAG changes now require an image rebuild:

   ```bash
   docker build -t airflow-lab:3.1.8-lab . \
     && kind load docker-image airflow-lab:3.1.8-lab --name airflow-lab \
     && kubectl -n airflow rollout restart deploy
   ```

> ⚠️ **Untested scaffolding — expect to debug.** I could not run kind/Helm
> here. Likely friction points: chart version vs Airflow 3.1.x support
> (`helm search repo apache-airflow`), component/service names differing by
> chart release (`airflow-api-server` vs `airflow-webserver` — check
> `kubectl -n airflow get svc`), and admin credentials (check chart notes
> from `helm status airflow -n airflow`). The architecture is right; the
> names may need a 10-minute reconciliation against your chart version.

**What transfers to EKS**: everything except `pullPolicy: Never` (→ ECR),
the in-cluster LocalStack (→ delete `endpoint_url`, add IRSA on triggerer
*and* worker service accounts — workers call S3, triggerer calls SQS), and
chart Postgres (→ Aurora).



---

## Lab 8 — Scenario Shootout: Verifying the Decision Guide (60 min)

Goal: turn each claim in the FTS trigger-mechanism comparison (A Lambda push /
B native pull / C doorbell+expand / D controller-worker) into an **Action →
Check** pair with measurable output. New pieces: `dags/lab8_scenarios.py`
(`fts_worker`, `scenario_c_expand`, `scenario_d_controller`),
`scripts/scenario_a_poller.py`, `scripts/measure.sh`. The bucket now fans out
to **two** queues: `doorbell-events` (feeds the lab6 watcher → B/C/D) and
`scenario-a-events` (drained by the A poller). One `./drop-files.sh` burst
feeds all four mechanisms at once.

Setup: `docker compose down localstack && docker compose up -d`, then unpause
`lab6_doorbell_ingest` (its watcher creates the asset events C and D schedule
on) and `fts_worker`. Unpause only the scenario DAG under test; pause the
others so `measure.sh` stays readable.

### Claim A1 — "A keeps one file per run, as-is"

- **Action**: `./drop-files.sh 50`, then
  `docker compose exec airflow python /opt/airflow/scripts/scenario_a_poller.py`
- **Check**: poller prints `created: 50`; `./scripts/measure.sh` shows
  `fts_worker runs = 50`, each `run_id` derived from its key, each conf
  holding exactly one key.

### Claim A2 — "409-based dedupe is free"

- **Action**: rerun the poller after re-delivering an event — copy an object
  onto itself inside localstack:
  `docker compose exec localstack awslocal s3 cp s3://roman-incoming/incoming/<key> s3://roman-incoming/incoming/<key>`
- **Check**: poller prints `duplicate(409): 1`; `fts_worker` run count
  unchanged. That's the deterministic-`dag_run_id` dedupe.

### Claim A3 — "lowest single-file latency / burst = run pileup"

- **Action**: `./drop-files.sh 1` + poller + `measure.sh` (note `latency_s`);
  then `./drop-files.sh 500` + poller.
- **Check**: single file lands in seconds; at 500, runs queue behind
  `max_active_runs` (default 16). Locally the burst is DB rows; on
  KubernetesExecutor those become pods.

### Claim B — "native pull coalesces; no knob changes this"

- **Action**: `./drop-files.sh 50`, wait ~30 s, `./scripts/measure.sh`.
- **Check**: last panel shows `events ≫ runs` (e.g. 50 events, 1–3 runs).
  Event count ≠ run count is structural — this is why B breaks the
  one-file-per-run contract.

### Claim C — "fewest runs; per-file isolation at task level; map cap"

- **Action**: unpause `scenario_c_expand`, `./drop-files.sh 200`, wait,
  `./scripts/measure.sh`.
- **Check**: 1–2 runs whose `mapped_files` sum to 200 — each map index its
  own TI with its own log/retries (own pod on k8s). Then probe the ceiling:
  `./drop-files.sh 1100` in one burst → the expand fails on `max_map_length`
  (default 1024) — raise `AIRFLOW__CORE__MAX_MAP_LENGTH` or cap claim size.

### Claim D — "worker unchanged, run-per-file, dedupe via trigger_run_id"

- **Action**: pause `scenario_c_expand`, unpause `scenario_d_controller`,
  `./drop-files.sh 50`, wait, `./scripts/measure.sh`.
- **Check**: controller ran 1–3 times; `fts_worker` gained exactly 50 runs
  with the same deterministic run_ids Scenario A produces — same worker DAG,
  untouched. Duplicate doorbells: controller claims 0 new keys; a colliding
  `trigger_run_id` is **skipped** (`skip_when_already_exists=True`), not failed.

### Head-to-head latency (A vs D)

- **Action**: fresh burst of 50 with one mechanism active at a time;
  compare `latency_s` on `fts_worker` after each.
- **Check**: A ≈ poller speed (sub-second per file after token fetch). D adds
  one hop — doorbell poll (2 s here, up to 60 s at defaults) + controller task
  — before worker runs queue. That delta is the price of staying in-cluster.

### On EKS — and what the sandbox cannot prove

| Scenario | On EKS with KubernetesExecutor | Only testable in AWS |
|---|---|---|
| A | Lambda (concurrency 90) hammers api-server pods; each run = a worker pod; scale `apiServer.replicas`, watch Aurora writes on `dag_run` | Lambda scaling, VPC→ELB path, SSM/secret rotation, IAM |
| B | Triggerer pod is the ingestion component (`replicas: 1` on 3.1.x) | IRSA on the triggerer service account |
| C | Every mapped TI = a pod; a 500-file batch = 500 pods through the K8s API — autoscaler headroom + `parallelism`/pools set the drain rate | Pod-storm behavior and K8s API throttling at real scale |
| D | Controller = one small recurring pod; worker fleet identical to A; throttle with worker `max_active_runs` + pools | Same as C for the worker fleet |

Common to all: the sandbox validates **logic and latency shape, not auth or
scale**. IRSA, SG→ELB paths, credential rotation, and 10k-burst concurrency
only fail in the real account — budget a dev-account load test for the finalist.

---

## EKS Translation Table

| Local piece | EKS / production |
|-------------|------------------|
| LocalStack endpoint in `aws_default` extra | Remove `endpoint_url`; IRSA on the **triggerer** service account (`sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes`) |
| Hardcoded queue URL | Env var / Airflow Variable per environment |
| `_PIP_ADDITIONAL_REQUIREMENTS` | Providers baked into the image (amazon + common-messaging ≥2.0) |
| `standalone` container | Helm chart: `triggerer.enabled: true`, `triggerer.replicas: 1` (3.1.x) |
| `./burst.sh` producer | S3 event notifications → SQS (add a DLQ with `maxReceiveCount`) |
| Manual queue create | CloudFormation: queue + DLQ + redrive policy |

## Troubleshooting

- **Nothing fires for ~60 s** → default `waiter_delay=60`; tune it (Lab 3a).
- **DAG import error on `MessageQueueTrigger`** → common-messaging <2.0 or old
  `queue=` API; need `scheme="sqs", sqs_queue=...`.
- **`Connection refused` in triggerer logs** → queue URL uses `localhost`
  instead of `localstack` (container network).
- **Runs fire but `extra` is empty / repeated** → trigger exception loop;
  check triggerer logs, verify queue URL and connection (Lab 4d).
- **Slow first start** → pip install at boot; `docker compose logs -f airflow`.

## References

Airflow docs (verify against your deployed versions):

- Messaging triggers / `MessageQueueTrigger` (`scheme=` API, common-messaging ≥2.0): <https://airflow.apache.org/docs/apache-airflow-providers-common-messaging/stable/triggers.html>
- SQS queue provider + `SqsSensorTrigger` knobs (`waiter_delay`, `max_messages`, `num_batches`, `delete_message_on_reception`): <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/message-queues/index.html>
- common-messaging changelog (2.0.0 breaking interface refactor): <https://airflow.apache.org/docs/apache-airflow-providers-common-messaging/stable/changelog.html>
- SQS operators/sensors and AWS connection generics: <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/sqs.html>
- Astronomer guide to event-driven scheduling (concepts, SQS example): <https://www.astronomer.io/docs/learn/airflow-event-driven-scheduling>

Known issues behind the Lab 4 warnings:

- Duplicate events/runs with >1 triggerer replica (fixed in 3.2.2): <https://github.com/apache/airflow/discussions/69319>
- Scheduler memory grows linearly with asset events consumed per run (~480 MB / 50k events): <https://github.com/apache/airflow/issues/69848>

Doorbell-pattern lineage (Lab 6 design rationale):

- Fowler, "What do you mean by 'Event-Driven'?" (2017) — **Event Notification**: thin events, receiver queries the source back. The doorbell is this pattern at its thinnest: <https://martinfowler.com/tags/event%20architectures.html>
- Kubernetes design principles — **level-based control**: correct behavior from observed state regardless of missed updates; edge-triggering as optimization only: <https://github.com/kubernetes/design-proposals-archive/blob/acc25e14ca83dfda4f66d8cb1f1b491f26e78ffe/architecture/principles.md>
- Amazon S3 Event Notifications — at-least-once delivery, seconds-to-minutes latency; design for missed/duplicate events, audit via LIST/Inventory: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html>

> 💡 "Doorbell" is this lab's shorthand, not an official term. Citable names:
> *event notification* (Fowler) for the shape, *level-triggered reconciliation*
> (Kubernetes) for the justification.

## Next Steps

1. Run Labs 0–4; write down your answers to: dedupe key? delete strategy?
   acceptable msg→run latency? where does backlog live?
2. Run Labs 6–7: doorbell + high-water mark, then the same on kind with
   KubernetesExecutor (expect chart-name reconciliation, see Lab 7 warning).
3. Phase 3: real AWS — S3 → SQS (+DLQ) via CloudFormation, IRSA on triggerer
   and worker service accounts, one staging queue per environment.
