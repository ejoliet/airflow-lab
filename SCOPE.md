# FTS File Ingestion → Airflow Trigger Mechanisms — Decision Guide

**Author**: Emmanuel Joliet
**Date**: 2026-09-11
**Status**: Draft
**Context**: Airflow 3.x on EKS (KubernetesExecutor), Aurora metadata DB, FTS files landing in S3

> One S3 file arrives → one DAG must process it. This guide compares four trigger
> mechanisms, their pressure points, and how to measure them side by side in the
> local sandbox before committing.

## Problem Statement

FTS drops files into an S3 pipeline bucket. Each file must be processed by an
Airflow DAG. The **existing DAG contract is one file per DAG run** (file key in
`dag_run.conf`). The other team's mechanism (CloudFormation stack) pushes per-file
triggers through Lambda. The question: keep that, or move ingestion inside Airflow —
and at what cost in latency, load, and DAG rewrites.

## Hard Constraint

> ⚠️ **One file per run is only preserved by Scenarios A and D.**
> B and C require rewriting the DAG's entry logic. Changing that contract is a
> product decision (provenance, per-file reprocessing semantics), not plumbing.

## The Four Mechanisms

### A — Lambda push (the existing CF stack)

S3 `ObjectCreated` → SQS → Lambda (`SOCnotifierHandler`, batch size 1, max
concurrency 90) → `POST /dags/{id}/dagRuns` per file, key in `conf`.

- Lambda runs in-VPC to reach the Airflow ELB; credentials from SSM.
- **Dedupe**: derive a deterministic `run_id` from the S3 key — duplicate S3
  notifications collide with `409 Conflict` for free.
- Failure path: 5 receives → DLQ.

### B — Native pull (asset watcher / deferrable SQS sensor)

Airflow consumes the queue itself: Airflow 3 event-driven scheduling
(AIP-82, `MessageQueueTrigger` on SQS via the common-messaging provider) or a
deferrable sensor.

- ❌ **Coalesces events**: pending asset events merge into one run by design.
  10 messages during a queued run → 1 run with 10 events. No knob changes this.
- DAG must be rewritten to iterate triggering asset events.

### C — Doorbell + dynamic task mapping

Any cheap trigger rings the bell; the DAG's first task lists/claims new files,
then `process_file.expand(key=...)` — one run per batch, one mapped task
instance (= one pod) per file.

- ❌ DAG rewritten into mapped tasks / mapped task groups.
- Claiming logic ("what's new") lives in the DAG: drain SQS or diff an S3
  listing against a processed-manifest.
- Watch `max_map_length` (default 1024, raisable) per batch.

### D — Controller/worker split (doorbell + `TriggerDagRunOperator`)

Doorbell DAG lists new files, then maps `TriggerDagRunOperator` over them with
deterministic `trigger_run_id` and per-file `conf`.

- ✅ **Existing DAG becomes the worker, unchanged** — still one file per run.
- Blast absorption and throttling stay inside Airflow (`max_active_runs`, pools).
- No Lambda, no SSM credentials, no VPC/ELB path.

## Comparison

| Scenario | One file/run kept? | Advantage | Pressure point | Load to watch | Latency / bottleneck |
|---|---|---|---|---|---|
| **A** Lambda → API per file | ✅ as-is | Lowest single-file latency; 409-based dedupe; producer stays outside Airflow | Airflow **API server** absorbs the burst (up to 90 concurrent callers); creds + VPC/ELB = extra failure surface | API server pods, `max_active_runs`, queued-run pileup in scheduler/DB | Seconds per file; 10k-file burst = 10k runs → scheduler churn + pod-startup storm |
| **B** Native pull (watcher/sensor) | ❌ coalesces | No external plumbing — one provider package, all in-cluster | **Triggerer** becomes the ingestion component; DAG rewrite required | Triggerer health/capacity, queue depth, event→run coalescing ratio | Low trigger latency, but batch semantics: files ride along in shared runs; per-file provenance weakens |
| **C** Doorbell + `.expand()` | ❌ rewrite to mapped tasks | Best burst absorption; fewest runs; per-file pods keep per-file logs/retries | Claiming logic in your DAG; `max_map_length`; per-file backfill is clunky | Mapped-TI queue depth per run, doorbell run duration, TI count on scheduler | First file waits for its batch; big batches serialize behind pool/parallelism caps |
| **D** Controller/worker | ✅ worker unchanged | Run-per-file **and** blast absorption; native per-file retry/clear/backfill; nothing outside the cluster | Controller DAG is a new single point — its failure stalls all intake; two DAGs to operate | Controller cadence, worker `max_active_runs`, queued worker runs | One extra hop (controller trigger latency) before each worker run; throttling deliberate, inside Airflow |

## Measuring Them in the Sandbox

Reuse the Tier 1 sandbox (docker-compose + LocalExecutor + LocalStack): same
bucket, same SQS queue, one DAG variant per scenario, one seed script.

1. **Seed**: drop N objects into the LocalStack bucket (bursts of 1, 50, 500);
   record put timestamp per key.
2. **Scenario A**: run the notifier handler as a LocalStack Lambda, or as a small
   poller container running the same `notifier_lambda.lambda_handler` code against
   LocalStack SQS + local Airflow API (more faithful to your handler than to
   Lambda scaling).
3. **Scenarios B/D**: point the watcher/sensor or controller DAG at the same queue.
4. **Measure** from the metadata DB. Per-file latency = S3 put → run/task start:

```sql
-- A, B, D: run-level
SELECT run_id, queued_at, start_date, end_date
FROM dag_run WHERE dag_id = :dag;

-- C: mapped-task level (doorbell logs key → map_index)
SELECT map_index, queued_dttm, start_date, end_date
FROM task_instance
WHERE dag_id = 'doorbell_pipeline' AND task_id = 'process_file';
```

The delta **put → queued_at** isolates the mechanism; everything after
`queued_at` is identical scheduler/executor territory and cancels out.

Also record per burst: run count, duplicate handling (S3 notifications are
at-least-once), and DLQ/retry behavior on an injected failure.

## Final Notes

- **Real contest: A vs D**, given the one-file-per-run constraint.
  A wins if sub-second reaction matters and the producer must stay outside
  Airflow. D wins on fewer moving parts, no external credentials, and native
  per-file reprocessing.
- **B/C only enter if the run contract changes** — decide that first, on
  provenance and ops grounds, before comparing their numbers.
- > ⚠️ The sandbox validates **logic and latency shape, not auth**. IRSA/IAM,
  > the SG→ELB path, and SSM credential rotation only fail in the real account.
  > Concurrency-90 behavior under a 10k-file burst is only testable in AWS.
- > ⚠️ Verify the `apache-airflow-providers-common-messaging` / SQS trigger
  > maturity against your exact 3.x minor before betting on B — new surface area.
- Duplicate S3 events: A and D dedupe via deterministic run IDs; B/C must
  handle dedupe in event/claiming logic explicitly.

## Next Steps

- [ ] Extend the sandbox compose file with the four DAG variants + poller container
- [ ] Add seed + measure scripts (`make compare`) emitting one latency table per burst size
- [ ] Decide the run contract (one file/run: requirement or habit?) with the team
- [ ] If D shortlisted: spike controller DAG with `TriggerDagRunOperator.partial(...).expand()`
- [ ] If A retained: load-test API server with 90 concurrent trigger calls in dev
