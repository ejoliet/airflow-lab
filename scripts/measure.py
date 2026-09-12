"""Join S3 put timestamps to Airflow metadata timestamps; emit a latency table.

Runs INSIDE the airflow container (SQLAlchemy + psycopg2 already present):
  docker compose exec airflow python /opt/airflow/scripts/measure.py \
      --scenario a --burst <burst-id>

Per-file latency definitions (seconds):
  put->queued : mechanism latency (the thing the scenarios differ on)
  put->start  : + scheduler/executor pickup
  put->end    : + processing (PROCESS_SECONDS)

Join strategy per scenario:
  a, d : deterministic run_id  {s}__{sanitized_key}  on dag worker_per_file
  c    : claim task XCom (ordered key list) -> map_index of process_file TIs
  b    : APPROXIMATION — each key assigned to the earliest native_pull_asset
         run created at/after its put (events coalesce; per-file mapping is
         not first-class in this mechanism, which is itself a finding)
"""

import argparse
import csv
import json
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text

DB = "postgresql+psycopg2://airflow:airflow@postgres/airflow"


def sanitize(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "-", key)


def load_puts(results_dir: str, scenario: str, burst: str) -> dict[str, float]:
    path = Path(results_dir) / f"puts_{scenario}_{burst}.csv"
    with path.open() as fh:
        return {r["key"]: float(r["put_epoch"]) for r in csv.DictReader(fh)}


def epoch(dt) -> float | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def rows_ad(conn, scenario: str, puts: dict[str, float]):
    out = []
    for key, put_ts in puts.items():
        run_id = f"{scenario}__{sanitize(key)}"
        r = conn.execute(
            text(
                "SELECT queued_at, start_date, end_date FROM dag_run "
                "WHERE dag_id='worker_per_file' AND run_id=:rid"
            ),
            {"rid": run_id},
        ).fetchone()
        out.append((key, put_ts, *(map(epoch, r) if r else (None, None, None))))
    return out


def rows_c(conn, puts: dict[str, float]):
    runs = conn.execute(
        text(
            "SELECT dr.id, dr.run_id FROM dag_run dr "
            "WHERE dr.dag_id='doorbell_expand' ORDER BY dr.queued_at DESC LIMIT 20"
        )
    ).fetchall()
    key_to_ti: dict[str, tuple] = {}
    for run_pk, run_id in runs:
        xcom = conn.execute(
            text(
                "SELECT value FROM xcom WHERE dag_run_id=:pk AND task_id='claim' "
                "AND key='return_value'"
            ),
            {"pk": run_pk},
        ).fetchone()
        if not xcom:
            continue
        keys = xcom[0] if isinstance(xcom[0], list) else json.loads(xcom[0])
        tis = conn.execute(
            text(
                "SELECT map_index, queued_dttm, start_date, end_date "
                "FROM task_instance WHERE run_id=:rid AND dag_id='doorbell_expand' "
                "AND task_id='process_file'"
            ),
            {"rid": run_id},
        ).fetchall()
        ti_by_idx = {t[0]: t for t in tis}
        for idx, key in enumerate(keys):
            if key in puts and idx in ti_by_idx:
                _, q, s, e = ti_by_idx[idx]
                key_to_ti[key] = (epoch(q), epoch(s), epoch(e))
    return [
        (k, put, *(key_to_ti.get(k, (None, None, None)))) for k, put in puts.items()
    ]


def rows_b(conn, puts: dict[str, float]):
    runs = conn.execute(
        text(
            "SELECT queued_at, start_date, end_date FROM dag_run "
            "WHERE dag_id='native_pull_asset' ORDER BY queued_at"
        )
    ).fetchall()
    runs = [(epoch(q), epoch(s), epoch(e)) for q, s, e in runs]
    out = []
    for key, put_ts in puts.items():
        match = next((r for r in runs if r[0] and r[0] >= put_ts), None)
        out.append((key, put_ts, *(match or (None, None, None))))
    n_runs = len({r[0] for r in runs})
    print(f"scenario b: {len(puts)} files coalesced into {n_runs} runs (approx join)")
    return out


def pct(vals, p):
    return statistics.quantiles(vals, n=100)[p - 1] if len(vals) > 1 else vals[0]


def summarize(scenario: str, burst: str, rows) -> str:
    complete = [r for r in rows if r[4] is not None]  # has end_date
    missing = len(rows) - len(complete)
    lines = [f"### Scenario {scenario.upper()} — burst {burst} (n={len(rows)})", ""]
    if not complete:
        lines.append("> ⚠️ no completed runs found — did the scenario run?")
        return "\n".join(lines)
    lines.append("| metric | p50 (s) | p95 (s) | max (s) |")
    lines.append("|---|---|---|---|")
    for label, idx in (("put→queued", 2), ("put→start", 3), ("put→end", 4)):
        deltas = [r[idx] - r[1] for r in complete if r[idx] is not None]
        if deltas:
            lines.append(
                f"| {label} | {pct(deltas, 50):.2f} | {pct(deltas, 95):.2f} "
                f"| {max(deltas):.2f} |"
            )
    if missing:
        lines.append(f"\n> ⚠️ {missing} file(s) had no matching run/TI yet.")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", choices=["a", "b", "c", "d"], required=True)
    p.add_argument("--burst", required=True)
    p.add_argument("--results-dir", default="/opt/airflow/results")
    args = p.parse_args()

    puts = load_puts(args.results_dir, args.scenario, args.burst)
    engine = create_engine(DB)
    with engine.connect() as conn:
        if args.scenario in ("a", "d"):
            rows = rows_ad(conn, args.scenario, puts)
        elif args.scenario == "c":
            rows = rows_c(conn, puts)
        else:
            rows = rows_b(conn, puts)

    report = summarize(args.scenario, args.burst, rows)
    out = Path(args.results_dir) / f"report_{args.scenario}_{args.burst}.md"
    out.write_text(report + "\n")
    print(report)


if __name__ == "__main__":
    main()
