#!/usr/bin/env bash
# Run the scenario comparison end to end. Host-side orchestrator.
#   ./scripts/compare.sh "a c d" 50
# Scenario A is consumed by the notifier service automatically; C and D need
# the bell rung (one CLI trigger) after seeding.
set -euo pipefail

SCENARIOS=${1:-"a c d"}
N=${2:-20}
TIMEOUT=${3:-600}

DC="docker compose"
AF() { $DC exec -T airflow "$@"; }

bell_dag() {
  case "$1" in
    c) echo doorbell_expand ;;
    d) echo controller_fanout ;;
    *) echo "" ;;
  esac
}

wait_idle() {
  # Wait until no queued/running runs remain for the compare DAGs.
  local deadline=$((SECONDS + TIMEOUT))
  while (( SECONDS < deadline )); do
    local active
    active=$(AF python - << 'PY'
from sqlalchemy import create_engine, text
e = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
with e.connect() as c:
    n = c.execute(text(
        "SELECT count(*) FROM dag_run WHERE state IN ('queued','running') "
        "AND dag_id IN ('worker_per_file','doorbell_expand',"
        "'controller_fanout','native_pull_asset')")).scalar()
print(n)
PY
)
    [[ "$active" == "0" ]] && return 0
    sleep 5
  done
  echo "WARN: timeout waiting for runs to finish" >&2
}

STAMP=$(date +%s)
COMBINED="results/compare_${STAMP}.md"
echo "# Trigger mechanism comparison — n=${N}/burst — $(date -u +%FT%TZ)" > "$COMBINED"

for s in $SCENARIOS; do
  echo "=== scenario $s: seeding $N files ==="
  BURST=$(python scripts/seed.py --scenario "$s" --n "$N" | tail -1)

  DAG=$(bell_dag "$s")
  if [[ -n "$DAG" ]]; then
    sleep 3  # let S3 events land in the queue before draining
    echo "=== scenario $s: ringing the bell ($DAG) ==="
    AF airflow dags trigger "$DAG" >/dev/null
  fi

  wait_idle
  # Give C's mapped-TI XComs / A's last runs a beat to commit
  sleep 3

  echo "=== scenario $s: measuring burst $BURST ==="
  AF python /opt/airflow/scripts/measure.py --scenario "$s" --burst "$BURST" \
    | tee -a "$COMBINED"
  echo "" >> "$COMBINED"
done

echo ""
echo "Combined report: $COMBINED"
