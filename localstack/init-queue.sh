#!/usr/bin/env bash
set -euo pipefail
# Runs inside LocalStack once it is ready.
awslocal sqs create-queue --queue-name demo-events
awslocal sqs create-queue --queue-name lab-events
awslocal sqs create-queue --queue-name doorbell-events

# Lab 6: bucket wired to SQS via real S3 event notifications
awslocal s3 mb s3://roman-incoming
awslocal s3api put-bucket-notification-configuration \
  --bucket roman-incoming \
  --notification-configuration '{
    "QueueConfigurations": [{
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:doorbell-events",
      "Events": ["s3:ObjectCreated:*"]
    }]
  }'

echo "Created: queues demo-events, lab-events, doorbell-events; bucket roman-incoming (notifications -> doorbell-events)"
