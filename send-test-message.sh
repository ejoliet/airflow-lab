#!/usr/bin/env bash
set -euo pipefail
# Publish a test message. No AWS CLI needed on the host — uses awslocal inside LocalStack.
BODY="${1:-{\"file\":\"s3://roman-l2/obs_000123.asdf\",\"event\":\"new_product\"}}"
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/demo-events \
  --message-body "$BODY"
echo "Sent. Watch the DAG 'sqs_event_driven' fire in ~a few seconds."
