#!/usr/bin/env bash
set -euo pipefail
# Lab 8 scoreboard. Latency uses the epoch prefix baked into keys by
# drop-files.sh (incoming/<epoch>_<seq>.asdf): put -> queued_at.
PSQL="docker compose exec -T postgres psql -U airflow -P pager=off -c"

echo "== Run count per DAG (last 15 min) =="
$PSQL "SELECT dag_id, count(*) runs FROM dag_run
       WHERE queued_at > now() - interval '15 minutes'
       GROUP BY 1 ORDER BY 1;"

echo "== fts_worker: one file per run? latency put->queued =="
$PSQL "SELECT run_id,
              to_timestamp(split_part(split_part(conf->>'key','/',2),'_',1)::bigint) AS put_ts,
              queued_at,
              round(extract(epoch FROM (queued_at - to_timestamp(
                split_part(split_part(conf->>'key','/',2),'_',1)::bigint)))::numeric,1) AS latency_s
       FROM dag_run WHERE dag_id='fts_worker'
       ORDER BY queued_at DESC LIMIT 10;"

echo "== scenario_c_expand: files per run (mapped TIs) =="
$PSQL "SELECT run_id, count(*) FILTER (WHERE map_index >= 0) AS mapped_files
       FROM task_instance
       WHERE dag_id='scenario_c_expand' AND task_id='process_file'
       GROUP BY 1 ORDER BY 1 DESC LIMIT 5;"

echo "== lab6 (Scenario B shape): asset events vs runs =="
$PSQL "SELECT (SELECT count(*) FROM asset_event ae JOIN asset a ON a.id=ae.asset_id
               WHERE a.name='s3_incoming_doorbell'
               AND ae.timestamp > now() - interval '15 minutes') AS events,
              (SELECT count(*) FROM dag_run WHERE dag_id='lab6_doorbell_ingest'
               AND queued_at > now() - interval '15 minutes') AS runs;"
