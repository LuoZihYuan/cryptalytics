FROM apache/airflow:3.1.7
RUN uv pip install --no-cache \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "pymongo>=4.16.0" \
  "apache-airflow-providers-cncf-kubernetes>=10.12.0" \
  "apache-airflow-providers-grpc>=3.9.1" \
  "protobuf>=6.33.0" \
  "grpcio>=1.78.0"
COPY shared/pb/src/pb/ingestion_pb2.py shared/pb/src/pb/ingestion_pb2_grpc.py /home/airflow/.local/lib/python3.12/site-packages/
RUN sed -i 's/from \. import ingestion_pb2/import ingestion_pb2/' /home/airflow/.local/lib/python3.12/site-packages/ingestion_pb2_grpc.py