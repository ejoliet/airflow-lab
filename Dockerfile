# Lab 7 image: providers + DAGs baked in (no _PIP_ADDITIONAL_REQUIREMENTS on k8s).
# Build from repo root:  docker build -t airflow-lab:3.1.8-lab .
FROM apache/airflow:3.1.8
RUN pip install --no-cache-dir \
    "apache-airflow-providers-common-messaging>=2.0" \
    apache-airflow-providers-amazon
COPY --chown=airflow:root dags/ /opt/airflow/dags/
