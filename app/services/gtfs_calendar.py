from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping, Set

from app.utils.text_utils import clean_text, safe_int


def active_service_ids(
    calendar: Mapping[str, Mapping[str, Any]],
    calendar_dates: Mapping[str, Mapping[str, int]],
    service_day: date,
) -> Set[str]:
    ymd = service_day.strftime("%Y%m%d")
    weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][service_day.weekday()]
    active: Set[str] = set()
    for service_id, row in calendar.items():
        start, end = clean_text(row.get("start_date")), clean_text(row.get("end_date"))
        if start and ymd < start:
            continue
        if end and ymd > end:
            continue
        if clean_text(row.get(weekday)) == "1":
            active.add(service_id)
    for service_id, changes in calendar_dates.items():
        exception_type = safe_int(changes.get(ymd), 0)
        if exception_type == 1:
            active.add(service_id)
        elif exception_type == 2:
            active.discard(service_id)
    return active


def service_days(reference: datetime) -> list[date]:
    return [(reference - timedelta(days=1)).date(), reference.date(), (reference + timedelta(days=1)).date()]
