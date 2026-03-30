from datetime import datetime, timedelta, timezone

import grpc
import httpx
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG, BaseSensorOperator, Param, task
from pymongo import MongoClient

import ingestion_pb2
import ingestion_pb2_grpc

default_args = {
  "owner": "cryptalytics",
  "retries": 3,
  "retry_delay": timedelta(minutes=5),
}


class ExternalCallbackSensor(BaseSensorOperator):
  """
  Sensor that waits for external service to mark it as success via Airflow REST API.

  The sensor keeps returning False (waiting). An external service calls:
  PATCH /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}
  with {"new_state": "success"} to complete the task.

  Uses mode="reschedule" to release worker slot between pokes.
  """

  def poke(self, context):
    return False


with DAG(
  dag_id="symbol_onboarding",
  default_args=default_args,
  description="Onboard a new trading symbol",
  schedule=None,
  start_date=datetime(2026, 1, 1),
  catchup=False,
  params={
    "symbol": Param("BTCUSDT", type="string", description="Trading pair"),
  },
) as dag:

  @task
  def mark_symbol_unavailable(symbol: str, mongo_uri: str):
    client = MongoClient(mongo_uri)
    db = client.cryptalytics
    db.symbols.update_one(
      {"symbol": symbol},
      {"$set": {"symbol": symbol, "state": "unavailable"}},
      upsert=True,
    )
    client.close()

  @task
  def get_backfill_dates(symbol: str) -> dict:
    """
    Fetch the earliest available candle date from Binance.
    Returns start_date (inclusive) and end_date (exclusive, today UTC).
    """
    with httpx.Client() as client:
      response = client.get(
        "https://api.binance.us/api/v3/klines",
        params={
          "symbol": symbol,
          "interval": "1m",
          "limit": 1,
          "startTime": 0,
        },
      )
      response.raise_for_status()
      data = response.json()

      if not data:
        raise ValueError(f"No data found for {symbol}")

      earliest_timestamp = data[0][0]
      start_date = datetime.fromtimestamp(
        earliest_timestamp / 1000, tz=timezone.utc
      ).strftime("%Y-%m-%d")

    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {"start_date": start_date, "end_date": end_date}

  @task
  def subscribe_symbol(symbol: str, dag_run_id: str):
    """Subscribe to real-time data via gRPC."""
    channel = grpc.insecure_channel("ingestion:50051")
    stub = ingestion_pb2_grpc.IngestionServiceStub(channel)
    request = ingestion_pb2.SubscribeRequest(
      symbols=[symbol],
      dag_run_id=dag_run_id,
    )
    response = stub.Subscribe(request)
    channel.close()
    return {"success": response.success, "message": response.message}

  @task
  def mark_symbol_available(symbol: str, mongo_uri: str):
    client = MongoClient(mongo_uri)
    db = client.cryptalytics
    db.symbols.update_one(
      {"symbol": symbol},
      {"$set": {"state": "available"}},
    )
    client.close()

  # Step 1: Mark symbol as unavailable
  mark_unavailable = mark_symbol_unavailable(
    symbol="{{ params.symbol }}",
    mongo_uri="{{ var.value.mongo_uri }}",
  )

  # Step 2.1a: Subscribe to real-time data (returns immediately)
  subscribe = subscribe_symbol(
    symbol="{{ params.symbol }}",
    dag_run_id="{{ run_id }}",
  )

  # Step 2.1b: Wait for ingestion service to call back via REST API
  realtime_ready = ExternalCallbackSensor(
    task_id="realtime_ready",
    mode="reschedule",
    poke_interval=60,
    timeout=600,
  )

  # Step 2.2: Get backfill date range
  backfill_dates = get_backfill_dates(symbol="{{ params.symbol }}")

  # Step 2.3: Backfill historical data
  backfill = KubernetesPodOperator(
    task_id="backfill",
    namespace="cryptalytics",
    image="cryptalytics/backfill:latest",
    image_pull_policy=Variable.get("image_pull_policy", default_var="IfNotPresent"),
    env_vars=[
      k8s.V1EnvVar(
        name="BACKFILL_DELTA_CANDLES_PATH",
        value=Variable.get(
          "delta_candles_path", default_var="s3://cryptalytics/candles"
        ),
      ),
    ],
    volumes=[
      k8s.V1Volume(
        name="delta-data",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
          claim_name="delta-data",
        ),
      ),
    ],
    volume_mounts=[
      k8s.V1VolumeMount(
        name="delta-data",
        mount_path="/data/delta",
      ),
    ],
    arguments=[
      "--symbol",
      "{{ params.symbol }}",
      "--start-date",
      "{{ ti.xcom_pull(task_ids='get_backfill_dates')['start_date'] }}",
      "--end-date",
      "{{ ti.xcom_pull(task_ids='get_backfill_dates')['end_date'] }}",
    ],
    container_resources={
      "requests": {"memory": "512Mi", "cpu": "250m"},
      "limits": {"memory": "1Gi", "cpu": "500m"},
    },
    is_delete_operator_pod=True,
    get_logs=True,
  )

  # Step 4: Mark symbol as available
  mark_available = mark_symbol_available(
    symbol="{{ params.symbol }}",
    mongo_uri="{{ var.value.mongo_uri }}",
  )

  # Dependencies
  mark_unavailable >> [subscribe, backfill_dates]
  subscribe >> realtime_ready
  backfill_dates >> backfill
  [realtime_ready, backfill] >> mark_available
