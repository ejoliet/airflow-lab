#!/usr/bin/env bash
# Runs inside LocalStack once it is ready.
# One bucket, one queue per scenario, prefix-routed S3 notifications:
#   a/ -> files-a (notifier poller / Lambda stand-in)
#   b/ -> files-b (Airflow asset watcher, scenario B)
#   c/ -> files-c (doorbell claim task, scenario C)
#   d/ -> files-d (controller claim task, scenario D)
set -euo pipefail

BUCKET=pipeline-bucket
REGION=us-east-1
ACCOUNT=000000000000

awslocal s3 mb "s3://${BUCKET}" || true

for s in a b c d; do
  awslocal sqs create-queue --queue-name "files-${s}" \
    --attributes '{"ReceiveMessageWaitTimeSeconds":"2","VisibilityTimeout":"60"}'
done

qarn() { echo "arn:aws:sqs:${REGION}:${ACCOUNT}:files-$1"; }

awslocal s3api put-bucket-notification-configuration \
  --bucket "${BUCKET}" \
  --notification-configuration "{
    \"QueueConfigurations\": [
      {\"QueueArn\": \"$(qarn a)\", \"Events\": [\"s3:ObjectCreated:*\"],
       \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\": \"prefix\", \"Value\": \"a/\"}]}}},
      {\"QueueArn\": \"$(qarn b)\", \"Events\": [\"s3:ObjectCreated:*\"],
       \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\": \"prefix\", \"Value\": \"b/\"}]}}},
      {\"QueueArn\": \"$(qarn c)\", \"Events\": [\"s3:ObjectCreated:*\"],
       \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\": \"prefix\", \"Value\": \"c/\"}]}}},
      {\"QueueArn\": \"$(qarn d)\", \"Events\": [\"s3:ObjectCreated:*\"],
       \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\": \"prefix\", \"Value\": \"d/\"}]}}}
    ]
  }"

echo "init-aws.sh done: bucket=${BUCKET}, queues files-{a,b,c,d} wired"
