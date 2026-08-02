from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from app.utils.text_utils import clean_text


LONDON = ZoneInfo("Europe/London")


def now_london() -> datetime:
    return datetime.now(LONDON)


def parse_iso_dt(value: object) -> Optional[datetime]:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(LONDON)
    except Exception:
        return None


def gtfs_time_to_datetime(service_day: date, gtfs_time: str) -> Optional[datetime]:
    value = clean_text(gtfs_time)
    if not value:
        return None
    try:
        hours, minutes, *rest = value.split(":")
        hour = int(hours)
        second = int(rest[0]) if rest else 0
        extra_days = hour // 24
        return datetime.combine(
            service_day + timedelta(days=extra_days),
            time(hour % 24, int(minutes), second),
            tzinfo=LONDON,
        )
    except Exception:
        return None


def hhmm(value: Optional[datetime]) -> str:
    return value.astimezone(LONDON).strftime("%H:%M") if value else ""


def minutes_until(value: Optional[datetime], *, reference: Optional[datetime] = None) -> Optional[int]:
    if not value:
        return None
    current = reference or now_london()
    return int(round((value.astimezone(LONDON) - current).total_seconds() / 60))
