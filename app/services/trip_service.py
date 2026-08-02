from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

from app.services.gtfs_loader import GTFSStore
from app.services.live_matching import find_live_for_trip, stop_same
from app.services.map_service import shape_for_trip
from app.utils.text_utils import human_name, safe_int, short_destination
from app.utils.time_utils import gtfs_time_to_datetime, hhmm, minutes_until, parse_iso_dt


def present_trip(
    store: GTFSStore,
    vehicles: Sequence[Mapping[str, Any]],
    trip_id: str,
    service_day: date,
    reference_time: datetime,
    vehicle_hint: str = "",
) -> dict[str, Any] | None:
    trip = store.trips.get(trip_id)
    if not trip:
        return None
    route = store.routes.get(trip.get("route_id"), {})
    vehicle_values = tuple(dict(vehicle) for vehicle in vehicles)
    live, current_sequence = find_live_for_trip(
        store,
        trip,
        service_day,
        vehicle_values,
        vehicle_hint=vehicle_hint,
    )
    delay = live.get("delayMinutes") if live else None
    stops: list[dict[str, Any]] = []
    for row in store.stop_times_by_trip.get(trip_id, []):
        stop = store.stops.get(row.get("stop_id"), {})
        scheduled_at = gtfs_time_to_datetime(
            service_day,
            row.get("departure_time") or row.get("arrival_time"),
        )
        live_at = None
        is_current = False
        live_future = False
        sequence = safe_int(row.get("stop_sequence"), 0)
        if live:
            if stop_same(
                stop,
                live.get("currentStopRef", ""),
                live.get("currentStopName", ""),
            ):
                is_current = True
                live_at = parse_iso_dt(live.get("liveTime")) or scheduled_at
            elif current_sequence and sequence > current_sequence and isinstance(delay, int) and scheduled_at:
                live_future = True
                live_at = scheduled_at + timedelta(minutes=delay)
        display_at = live_at or scheduled_at
        minutes = minutes_until(display_at, reference=reference_time)
        if minutes is not None and minutes < 0:
            minutes = None
        past = bool(
            display_at
            and display_at < reference_time - timedelta(seconds=30)
            and not is_current
        )
        if current_sequence and sequence < current_sequence:
            past = True
        if is_current:
            right_label = "LIVE" if live.get("vehicleAtStop") else "Due"
        elif minutes is not None:
            right_label = "Due" if minutes <= 1 and (live_future or live) else f"{minutes}'"
        else:
            right_label = ""
        stops.append(
            {
                "stopId": row.get("stop_id"),
                "stopCode": stop.get("code", ""),
                "name": stop.get("stop_name", row.get("stop_id")),
                "sequence": sequence,
                "lat": stop.get("lat"),
                "lon": stop.get("lon"),
                "scheduledTime": hhmm(scheduled_at),
                "scheduledTimeIso": scheduled_at.isoformat() if scheduled_at else "",
                "displayTime": hhmm(display_at),
                "displayTimeIso": display_at.isoformat() if display_at else "",
                "minutes": minutes,
                "rightLabel": right_label,
                "live": bool(is_current or live_future),
                "current": is_current,
                "past": past,
            }
        )
    if isinstance(delay, int):
        delay_label = f"{delay:+d}"
    elif live:
        delay_label = "LIVE"
    else:
        delay_label = "--"
    destination_full = human_name(trip.get("trip_headsign") or trip.get("destination") or "")
    return {
        "ok": True,
        "trip": {
            **trip,
            "destination": short_destination(destination_full),
            "destinationFull": destination_full,
        },
        "route": dict(route),
        "serviceDate": service_day.isoformat(),
        "stops": stops,
        "live": dict(live) if live else None,
        "delayLabel": delay_label,
        "currentSequence": current_sequence,
        "shape": shape_for_trip(store, trip),
        "now": reference_time.isoformat(),
    }
