#!/usr/bin/env bash
set -euo pipefail
# Runs inside LocalStack once it is ready.
awslocal sqs create-queue --queue-name demo-events
awslocal sqs create-queue --queue-name lab-events
awslocal sqs create-queue --queue-name doorbell-events
awslocal sqs create-queue --queue-name scenario-a-events

# Lab 6/8: bucket fans out to BOTH queues (doorbell for B/C/D, scenario-a for A)
awslocal s3 mb s3://roman-incoming
awslocal s3api put-bucket-notification-configuration \
  --bucket roman-incoming \
  --notification-configuration '{
    "QueueConfigurations": [
      {"QueueArn": "arn:aws:sqs:us-east-1:000000000000:doorbell-events",
       "Events": ["s3:ObjectCreated:*"]},
      {"QueueArn": "arn:aws:sqs:us-east-1:000000000000:scenario-a-events",
       "Events": ["s3:ObjectCreated:*"]}
    ]
  }'

echo "Created: queues demo-events, lab-events, doorbell-events, scenario-a-events; bucket roman-incoming (notifications -> both)"
