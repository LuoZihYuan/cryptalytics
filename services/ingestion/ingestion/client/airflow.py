import httpx
import structlog

from ingestion.settings import settings

log = structlog.get_logger()

TASK_INSTANCE_URL = "/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"


class AirflowClient:
  def __init__(self):
    self.client = httpx.AsyncClient(base_url=settings.airflow_base_url)

  async def close(self):
    await self.client.aclose()
    log.info("Airflow client closed")

  async def mark_task_success(self, dag_run_id: str):
    url = TASK_INSTANCE_URL.format(
      dag_id=settings.airflow_dag_id,
      dag_run_id=dag_run_id,
      task_id=settings.airflow_task_id,
    )

    response = await self.client.patch(
      url,
      json={"state": "success"},
      headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    log.info("Airflow task marked success", dag_run_id=dag_run_id)
