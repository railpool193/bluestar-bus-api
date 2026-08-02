from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_calendar import service_days
from app.services.live_matching import match_live_to_departure
from app.services.live_store_provider import LiveSnapshot
from app.utils.text_utils import clean_text
from app.utils.text_utils import short_destination
from app.utils.time_utils import gtfs_time_to_datetime, hhmm, parse_iso_dt


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


def stop_departures(
    store: GTFSStore,
    live_snapshot: LiveSnapshot,
    stop_id: str,
    *,
    reference_time: datetime,
    window_minutes: int,
    departure_limit: int,
    matching_minutes: int,
) -> dict[str, Any] | None:
    stop = store.stops.get(stop_id)
    if not stop:
        return None
    end = reference_time + timedelta(minutes=max(10, min(window_minutes, 360)))
    vehicles = tuple(dict(vehicle) for vehicle in live_snapshot.vehicles)
    result: list[dict[str, Any]] = []
    for service_day in service_days(reference_time):
        active = store.active_service_ids(service_day)
        for departure in store.stop_departures_index.get(stop_id, []):
            if active and departure.get("service_id") not in active:
                continue
            if clean_text(departure.get("pickup_type")) == "1" or departure.get("is_last_stop"):
                continue
            scheduled_at = gtfs_time_to_datetime(
                service_day,
                departure.get("departure_time") or departure.get("arrival_time"),
            )
            if (
                not scheduled_at
                or scheduled_at < reference_time - timedelta(minutes=2)
                or scheduled_at > end
            ):
                continue
            result.append(
                enrich_departure(
                    store,
                    departure,
                    service_day,
                    scheduled_at,
                    vehicles,
                    reference_time=reference_time,
                    matching_minutes=matching_minutes,
                )
            )
    result.sort(key=lambda item: item.get("displayTimeIso") or item.get("scheduledTimeIso") or "")
    deduplicated: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any, Any]] = set()
    for item in result:
        key = (item.get("tripId"), item.get("serviceDate"), item.get("stopSequence"))
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(item)
    return {
        "ok": True,
        "stop": dict(stop),
        "departures": deduplicated[:departure_limit],
        "now": reference_time.isoformat(),
    }
