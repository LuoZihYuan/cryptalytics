import httpx
import structlog

from ingestion.settings import settings

log = structlog.get_logger()


class AirflowClient:
  def __init__(self):
    self.client = httpx.AsyncClient(base_url=settings.airflow_base_url)
    self._token: str | None = None

  async def close(self):
    await self.client.aclose()
    log.info("Airflow client closed")

  async def _ensure_token(self):
    if self._token is not None:
      return

    response = await self.client.post(
      "/auth/token",
      json={
        "username": settings.airflow_username,
        "password": settings.airflow_password,
      },
    )
    response.raise_for_status()
    self._token = response.json()["access_token"]
    log.info("Airflow JWT token acquired")

  async def mark_realtime_ready(self, dag_run_id: str, symbol: str):
    await self._ensure_token()

    key = f"realtime_ready_{dag_run_id}_{symbol}"
    headers = {
      "Content-Type": "application/json",
      "Authorization": f"Bearer {self._token}",
    }

    response = await self.client.post(
      "/api/v2/variables",
      json={"key": key, "value": "true"},
      headers=headers,
    )

    if response.status_code == 409:
      response = await self.client.patch(
        f"/api/v2/variables/{key}",
        json={"value": "true"},
        headers=headers,
      )

    response.raise_for_status()

    log.info("Airflow variable set", key=key, dag_run_id=dag_run_id, symbol=symbol)
