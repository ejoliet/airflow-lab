# airflow-trigger-compare

> Local rig to measure four S3→Airflow trigger mechanisms side by side —
> no AWS account needed. Companion to `airflow-trigger-mechanisms-guide.md`.

## Overview

FTS drops files in S3; each file must be processed by an Airflow DAG whose
contract is **one file per run**. This rig reproduces the four candidate
trigger mechanisms against the same LocalStack bucket and emits one latency
table per scenario per burst.

| Scenario | Mechanism | Consumer of the queue | One file/run? |
|---|---|---|---|
| **A** | Lambda push → REST API per file | `notifier` container (Lambda stand-in) | ✅ |
| **B** | Native pull (asset watcher on SQS) | Airflow **triggerer** (experimental) | ❌ coalesces |
| **C** | Doorbell + `.expand()` | `claim` task in `doorbell_expand` | ❌ mapped tasks |
| **D** | Controller/worker (`TriggerDagRunOperator`) | `claim` task in `controller_fanout` | ✅ worker untouched |

## Architecture

```
seed.py ──puts──▶ pipeline-bucket (LocalStack S3)
                    │ S3 notifications, prefix-routed
                    ▼
        files-a  files-b  files-c  files-d   (LocalStack SQS)
           │        │        │        │
       notifier  triggerer  claim   claim
       (REST/    (asset     task    task + TriggerDagRunOperator
        file)    events)     └─.expand()─┐      └──▶ worker_per_file (1 run/file)
           └──▶ worker_per_file          └▶ mapped pods/tasks
                                  ▼
              Postgres metadata DB ◀── measure.py (put→queued/start/end)
```

## Prerequisites

- Docker + Docker Compose v2
- Python 3.11+ on the host with `boto3` (`pip install boto3`) — seed script only

## Quick Start

```bash
cp .env.example .env          # pin AIRFLOW_IMAGE to your prod minor
make up                       # stack + unpause DAGs; UI at :8080 (auth open locally)
make compare N=50             # scenarios a c d, 50 files each
cat results/compare_*.md
```

Single scenario: `make compare SCENARIOS="d" N=500`.

## Configuration

| Var | Default | Purpose |
|---|---|---|
| `AIRFLOW_IMAGE` | `apache/airflow:3.1.8` | Match your EKS minor |
| `PROCESS_SECONDS` | `2` | Simulated per-file work |
| `AIRFLOW_EXTRA_PIP` | empty | Set to `apache-airflow-providers-common-messaging` to enable Scenario B |
| `MAX_CONCURRENCY` (notifier) | `10` | Mirrors ESM MaximumConcurrency, scaled down |

## What Gets Measured

`measure.py` joins seed put-timestamps to metadata-DB timestamps:

- **put→queued** — the mechanism latency (this is what the scenarios differ on)
- **put→start** — + scheduler/executor pickup (identical territory, cancels out)
- **put→end** — + processing

Join strategy: A/D via deterministic `run_id` (`a__<key>` / `d__<key>` — 409
collisions double as dedupe); C via the `claim` task's XCom key list →
`map_index`; B by nearest-run approximation (per-file mapping isn't
first-class there — that's a finding, not a bug).

## Known Limits

> ⚠️ Validates **logic and latency shape only**. Not exercised: IRSA/IAM, the
> SG→ELB path, SSM credential rotation, Lambda scaling, and KubernetesExecutor
> pod-startup cost (LocalExecutor here — add a constant per-pod offset mentally,
> or port to the kind-based Tier 2 sandbox).

> ⚠️ Scenario B is version-sensitive (`MessageQueueTrigger` kwargs changed
> across 3.x minors). The DAG import is guarded; verify against your installed
> provider before trusting its numbers.

- `SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` is a local-only shortcut. Never mirror it.
- LocalStack S3 notifications are near-instant; real S3→SQS adds ~sub-second.

## Non-Goals (v1)

- No DLQ / redrive emulation (visibility timeout is the retry in the poller)
- No KubernetesExecutor (Tier 2 / kind port is a follow-up)
- No load testing of the API server at real concurrency-90

## Repository Layout

```
docker-compose.yml        postgres + localstack + airflow standalone + notifier
localstack/init-aws.sh    bucket, files-{a..d} queues, prefix-routed notifications
dags/worker_per_file.py   the existing contract: one file per run (A, D)
dags/doorbell_expand.py   scenario C: claim -> process_file.expand()
dags/controller_fanout.py scenario D: claim -> mapped TriggerDagRunOperator
dags/native_pull_asset.py scenario B: asset watcher on SQS (experimental)
notifier/poller.py        scenario A: Lambda stand-in, REST call per file
scripts/seed.py           burst of N puts + ground-truth CSV
scripts/measure.py        DB join -> p50/p95/max latency table
scripts/compare.sh        seed -> bell -> wait-idle -> measure, per scenario
Makefile                  up / compare / seed-<s> / clean
```

## Next Steps

- [ ] `make up && make compare N=50`, then N=500 for burst behavior
- [ ] Duplicate-event test: re-run `seed.py` with the same `--burst` id — A/D should show 409s, C should dedupe in `claim`
- [ ] Enable Scenario B (`AIRFLOW_EXTRA_PIP`) and record its coalescing ratio
- [ ] Port winner to Tier 2 (kind + KubernetesExecutor) to add pod-startup cost
