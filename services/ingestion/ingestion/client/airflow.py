import httpx
import structlog

from ingestion.settings import settings

log = structlog.get_logger()


class AirflowClient:
  def __init__(self):
    self.client: httpx.AsyncClient | None = None

  async def start(self):
    self.client = httpx.AsyncClient(base_url=settings.airflow_base_url)
    log.info("Airflow client started")

  async def stop(self):
    if self.client:
      await self.client.aclose()
      log.info("Airflow client stopped")

  async def mark_task_success(self, dag_run_id: str):
    if not self.client:
      raise RuntimeError("Client not started")

    url = (
      f"/api/v1/dags/{settings.airflow_dag_id}"
      f"/dagRuns/{dag_run_id}"
      f"/taskInstances/{settings.airflow_task_id}"
    )

    response = await self.client.patch(
      url,
      json={"state": "success"},
      headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    log.info("Airflow task marked success", dag_run_id=dag_run_id)
