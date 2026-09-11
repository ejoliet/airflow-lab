#!/usr/bin/env bash
set -euo pipefail
# Lab 7 bring-up: kind + LocalStack + official Airflow Helm chart (KubernetesExecutor).
# Prereqs: docker, kind, kubectl, helm (brew install kind kubectl helm).
# Run from repo root: ./k8s/up.sh

echo "==> 1/5 kind cluster"
kind get clusters | grep -q '^airflow-lab$' || kind create cluster --config k8s/kind-config.yaml

echo "==> 2/5 build + load image"
docker build -t airflow-lab:3.1.8-lab .
kind load docker-image airflow-lab:3.1.8-lab --name airflow-lab

echo "==> 3/5 LocalStack"
kubectl create namespace airflow --dry-run=client -o yaml | kubectl apply -f -
kubectl -n airflow apply -f k8s/localstack.yaml
kubectl -n airflow rollout status deploy/localstack --timeout=180s

echo "==> 4/5 Airflow via Helm"
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update apache-airflow
helm upgrade --install airflow apache-airflow/airflow \
  -n airflow -f k8s/values.yaml --timeout 10m

echo "==> 5/5 done"
kubectl -n airflow get pods
cat <<'EOF'

Next:
  kubectl -n airflow port-forward svc/airflow-api-server 8080:8080
  # UI: http://localhost:8080 (chart default creds: admin/admin unless overridden)
  # if that svc name 404s: kubectl -n airflow get svc   (older charts: airflow-webserver)

Unpause + fire:
  kubectl -n airflow exec deploy/airflow-scheduler -- airflow dags unpause lab6_doorbell_ingest
  ./drop-files.sh 200 kind
  kubectl -n airflow get pods -w        # watch worker pods spawn (the pod storm)
EOF
