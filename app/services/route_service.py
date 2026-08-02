from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from app.services.gtfs_loader import GTFSStore
from app.services.vehicle_service import vehicles_for_line
from app.utils.text_utils import clean_text, human_name, line_norm, short_destination


def present_route(
    store: GTFSStore,
    live_vehicles: Sequence[Mapping[str, Any]],
    line: str,
    reference_time: datetime,
) -> dict[str, Any] | None:
    normalized_line = line_norm(line)
    route_ids = list(store.route_by_short.get(normalized_line, []))
    if not route_ids:
        route_ids = [
            route_id
            for route_id in store.routes
            if line_norm(route_id) == normalized_line
        ]
    if not route_ids:
        return None
    vehicles = vehicles_for_line(live_vehicles, line)
    active = store.active_service_ids(reference_time.date()) | store.active_service_ids(
        (reference_time - timedelta(days=1)).date()
    )
    directions: dict[tuple[Any, str], dict[str, Any]] = {}
    for trip in store.trips.values():
        if trip.get("route_id") not in route_ids:
            continue
        if active and trip.get("service_id") not in active:
            continue
        destination_full = human_name(
            trip.get("trip_headsign") or trip.get("destination") or ""
        )
        destination = short_destination(destination_full)
        key = (trip.get("direction_id", ""), destination)
        if key in directions:
            continue
        stops: list[dict[str, Any]] = []
        for row in store.stop_times_by_trip.get(trip.get("trip_id"), []):
            stop = store.stops.get(row.get("stop_id"), {})
            stops.append(
                {
                    "id": row.get("stop_id"),
                    "name": stop.get("stop_name", row.get("stop_id")),
                    "code": stop.get("code", "BUS"),
                    "lat": stop.get("lat"),
                    "lon": stop.get("lon"),
                    "sequence": row.get("stop_sequence"),
                }
            )
        directions[key] = {
            "directionId": key[0],
            "destination": destination,
            "tripId": trip.get("trip_id"),
            "stops": stops,
        }
        if len(directions) >= 6:
            break
    return {
        "ok": True,
        "line": clean_text(line),
        "routes": [dict(store.routes[route_id]) for route_id in route_ids],
        "vehicles": vehicles,
        "directions": list(directions.values()),
    }
