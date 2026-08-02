from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping, Sequence

from app.services.gtfs_loader import GTFSStore
from app.services.live_matching import match_live_to_departure
from app.utils.text_utils import short_destination
from app.utils.time_utils import hhmm, parse_iso_dt


def enrich_departure(store: GTFSStore, departure: Mapping[str, Any], service_day: date, scheduled_at: datetime, vehicles: Sequence[Mapping[str, Any]], *, reference_time: datetime, matching_minutes: int) -> dict[str, Any]:
    live = match_live_to_departure(store, departure, scheduled_at, vehicles, matching_minutes=matching_minutes)
    live_at = parse_iso_dt(live.get("liveTime")) if live else None
    display_at = live_at or scheduled_at
    minutes = int(round((display_at - reference_time).total_seconds() / 60))
    if minutes < 0: minutes = 0 if minutes >= -2 else None
    due = bool(minutes is not None and minutes <= 1)
    return {
        "tripId": departure.get("trip_id"), "trip_id": departure.get("trip_id"), "serviceDate": service_day.isoformat(),
        "line": departure.get("line", ""), "routeId": departure.get("route_id", ""), "stopId": departure.get("stop_id", ""),
        "stopName": departure.get("stop_name", ""), "stopSequence": departure.get("stop_sequence", 0),
        "destination": short_destination(departure.get("headsign_full") or departure.get("headsign")),
        "destinationFull": departure.get("headsign_full") or departure.get("headsign"),
        "scheduledTime": hhmm(scheduled_at), "scheduledTimeIso": scheduled_at.isoformat(),
        "displayTime": hhmm(display_at), "displayTimeIso": display_at.isoformat(), "minutes": minutes,
        "minutesText": "Due" if due else (f"{minutes} min" if minutes is not None else ""),
        "live": bool(live), "isDue": due, "vehicleRef": live.get("vehicleRef") if live else "",
        "fleet": live.get("fleet") if live else "", "delayMinutes": live.get("delayMinutes") if live else None,
    }
