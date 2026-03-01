from datetime import datetime, timedelta


def build_download_plan(start_date: datetime, end_date: datetime) -> dict:
  """
  Build a plan of monthly and daily files to download.

  Uses half-open interval [start_date, end_date).
  All dates are expected to be in UTC.

  Strategy:
  - Monthly files for complete months
  - Daily files for partial months (start and end)
  """
  monthly = []
  daily = []

  current = start_date

  # First partial month (daily)
  if current.day != 1:
    month_end = (current.replace(day=1) + timedelta(days=32)).replace(
      day=1
    ) - timedelta(days=1)
    month_end = min(month_end, end_date - timedelta(days=1))
    while current <= month_end:
      daily.append(current.strftime("%Y-%m-%d"))
      current += timedelta(days=1)

  # Complete months (monthly)
  while current < end_date:
    month_end = (current + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    if month_end < end_date and current.day == 1:
      monthly.append(current.strftime("%Y-%m"))
      current = (current + timedelta(days=32)).replace(day=1)
    else:
      break

  # Last partial month (daily)
  while current < end_date:
    daily.append(current.strftime("%Y-%m-%d"))
    current += timedelta(days=1)

  return {"monthly": monthly, "daily": daily}
