#!/usr/bin/env bash
set -euo pipefail
# Burst N messages into the lab queue with a sequence number and send timestamp.
# Usage: ./burst.sh [N]   (default 20)
N="${1:-20}"
for i in $(seq 1 "$N"); do
  SENT_MS=$(($(date +%s) * 1000))
  docker compose exec -T localstack awslocal sqs send-message \
    --queue-url http://localhost:4566/000000000000/lab-events \
    --message-body "{\"seq\": $i, \"sent_ms\": $SENT_MS, \"event\": \"new_product\"}" \
    > /dev/null
done
echo "Sent $N messages to lab-events."
