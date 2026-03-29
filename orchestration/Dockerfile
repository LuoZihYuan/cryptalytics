FROM apache/airflow:3.1.7
RUN uv pip install --no-cache \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "pymongo>=4.16.0" \
  "apache-airflow-providers-cncf-kubernetes>=10.12.0" \
  "apache-airflow-providers-grpc>=3.9.1"