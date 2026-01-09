from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.grpc.operators.grpc import GrpcOperator
from pymongo import MongoClient

default_args = {
  "owner": "cryptalytics",
  "retries": 3,
  "retry_delay": timedelta(minutes=5),
}

with DAG(
  dag_id="symbol_onboarding",
  default_args=default_args,
  description="Onboard a new trading symbol",
  schedule=None,
  start_date=datetime(2026, 1, 1),
  catchup=False,
  params={
    "symbol": Param("BTCUSDT", type="string", description="Trading pair"),
    "start_date": Param("2026-01-01", type="string", description="Backfill start date"),
    "end_date": Param("2026-01-07", type="string", description="Backfill end date"),
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
  def build_backfill_args(symbol: str, start_date: str, end_date: str) -> list[dict]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    args = []
    current = start
    while current <= end:
      date = current.strftime("%Y-%m-%d")
      args.append(
        {
          "name": f"backfill-{symbol.lower()}-{date}",
          "arguments": ["--symbol", symbol, "--date", date],
          "labels": {"app": "backfill", "symbol": symbol, "date": date},
        }
      )
      current += timedelta(days=1)
    return args

  @task
  def mark_symbol_available(symbol: str, mongo_uri: str):
    client = MongoClient(mongo_uri)
    db = client.cryptalytics
    db.symbols.update_one(
      {"symbol": symbol},
      {"$set": {"state": "available"}},
    )
    client.close()

  subscribe_symbol = GrpcOperator(
    task_id="subscribe_symbol",
    stub_class="ingestion_pb2_grpc.IngestionServiceStub",
    call_func="Subscribe",
    grpc_conn_id="ingestion_grpc",
    data={"symbol": "{{ params.symbol }}"},
  )

  mark_unavailable = mark_symbol_unavailable(
    symbol="{{ params.symbol }}",
    mongo_uri="{{ var.value.mongo_uri }}",
  )

  backfill_args = build_backfill_args(
    symbol="{{ params.symbol }}",
    start_date="{{ params.start_date }}",
    end_date="{{ params.end_date }}",
  )

  backfill = KubernetesPodOperator.partial(
    task_id="backfill",
    namespace="cryptalytics",
    image="cryptalytics/backfill:latest",
    container_resources={
      "requests": {"memory": "256Mi", "cpu": "100m"},
      "limits": {"memory": "512Mi", "cpu": "500m"},
    },
    is_delete_operator_pod=True,
    get_logs=True,
  ).expand_kwargs(backfill_args)

  mark_available = mark_symbol_available(
    symbol="{{ params.symbol }}",
    mongo_uri="{{ var.value.mongo_uri }}",
  )

  mark_unavailable >> [subscribe_symbol, backfill] >> mark_available
