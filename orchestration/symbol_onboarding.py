import time
from datetime import datetime, timedelta, timezone

import grpc
import httpx
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG, Param, task
from pymongo import MongoClient, UpdateOne

import ingestion_pb2
import ingestion_pb2_grpc

default_args = {
  "owner": "cryptalytics",
  "retries": 3,
  "retry_delay": timedelta(minutes=5),
}


with DAG(
  dag_id="symbol_onboarding",
  default_args=default_args,
  description="Onboard one or more trading symbols",
  schedule=None,
  start_date=datetime(2026, 1, 1),
  catchup=False,
  render_template_as_native_obj=True,  # ← list params stay as lists
  params={
    "symbols": Param(
      ["BTCUSDT"],
      type="array",
      items={"type": "string"},
      description="Trading pairs",
    ),
  },
) as dag:

  @task
  def mark_symbols_unavailable(symbols: list[str], mongo_uri: str):
    client = MongoClient(mongo_uri)
    db = client.cryptalytics
    db.symbols.bulk_write(
      [
        UpdateOne(
          {"symbol": s},
          {"$set": {"symbol": s, "state": "unavailable"}},
          upsert=True,
        )
        for s in symbols
      ]
    )
    client.close()

  @task
  def subscribe_symbols(symbols: list[str], dag_run_id: str) -> dict:
    channel = grpc.insecure_channel("ingestion:50051")
    stub = ingestion_pb2_grpc.IngestionServiceStub(channel)
    request = ingestion_pb2.SubscribeRequest(
      symbols=symbols,
      dag_run_id=dag_run_id,
    )
    response = stub.Subscribe(request)
    channel.close()
    if not response.success:
      raise RuntimeError(f"Subscribe failed: {response.message}")
    return {"success": response.success, "message": response.message}

  @task
  def get_backfill_args(symbols: list[str]) -> list[list[str]]:
    """Returns list of [--symbol X --start-date Y --end-date Z] arg arrays."""
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    args_list = []
    with httpx.Client() as client:
      for symbol in symbols:
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
        start_date = datetime.fromtimestamp(
          data[0][0] / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d")
        args_list.append(
          [
            "--symbol",
            symbol,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
          ]
        )
    return args_list

  @task
  def wait_for_realtime(symbols: list[str], dag_run_id: str):
    timeout_seconds = 600
    poll_interval = 30
    deadline = time.time() + timeout_seconds
    pending = set(symbols)

    while pending and time.time() < deadline:
      for symbol in list(pending):
        key = f"realtime_ready_{dag_run_id}_{symbol}"
        value = Variable.get(key, default_var=None)
        if value is not None:
          Variable.delete(key)
          pending.discard(symbol)
      if pending:
        time.sleep(poll_interval)

    if pending:
      raise TimeoutError(
        f"Realtime not ready after {timeout_seconds}s for: {sorted(pending)}"
      )

  @task
  def mark_symbols_available(symbols: list[str], mongo_uri: str):
    client = MongoClient(mongo_uri)
    db = client.cryptalytics
    db.symbols.bulk_write(
      [UpdateOne({"symbol": s}, {"$set": {"state": "available"}}) for s in symbols]
    )
    client.close()

  symbols_param = "{{ params.symbols }}"
  mongo_uri_var = "{{ var.value.mongo_uri }}"
  run_id_var = "{{ run_id }}"

  mark_unavailable = mark_symbols_unavailable(
    symbols=symbols_param,
    mongo_uri=mongo_uri_var,
  )

  subscribe = subscribe_symbols(
    symbols=symbols_param,
    dag_run_id=run_id_var,
  )

  realtime_ready = wait_for_realtime(
    symbols=symbols_param,
    dag_run_id=run_id_var,
  )

  backfill_args = get_backfill_args(symbols=symbols_param)

  backfill = KubernetesPodOperator.partial(
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
    container_resources={
      "requests": {"memory": "512Mi", "cpu": "250m"},
      "limits": {"memory": "1Gi", "cpu": "500m"},
    },
    is_delete_operator_pod=True,
    get_logs=True,
  ).expand(arguments=backfill_args)

  mark_available = mark_symbols_available(
    symbols=symbols_param,
    mongo_uri=mongo_uri_var,
  )

  mark_unavailable >> [subscribe, backfill_args]
  subscribe >> realtime_ready
  backfill_args >> backfill
  [realtime_ready, backfill] >> mark_available
