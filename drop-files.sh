#!/usr/bin/env bash
set -euo pipefail
# Drop N files into s3://roman-incoming/incoming/ with monotonically
# increasing keys (epoch prefix) so the high-water mark works.
# Usage: ./drop-files.sh [N] [compose|kind]     defaults: 100 compose
N="${1:-100}"
TARGET="${2:-compose}"
TS="$(date +%s)"

LOOP='for i in $(seq 1 '"$N"'); do
  awslocal s3api put-object \
    --bucket roman-incoming \
    --key "incoming/'"$TS"'_$(printf %06d $i).asdf" \
    --body /etc/hostname > /dev/null
done; echo "Dropped '"$N"' files (batch '"$TS"')."'

if [[ "$TARGET" == "kind" ]]; then
  kubectl -n airflow exec deploy/localstack -- sh -c "$LOOP"
else
  docker compose exec -T localstack sh -c "$LOOP"
fi
