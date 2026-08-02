from __future__ import annotations

from typing import Any

from app.services.gtfs_loader import GTFSStore
from app.utils.text_utils import clean_text


def shape_for_trip(store: GTFSStore, trip: dict[str, Any]) -> list[dict[str, float]]:
    shape_id = clean_text(trip.get("shape_id"))
    if shape_id and store.shapes.get(shape_id):
        return store.shapes[shape_id][:3000]
    points: list[dict[str, float]] = []
    for row in store.stop_times_by_trip.get(trip.get("trip_id"), []):
        stop = store.stops.get(row.get("stop_id"), {})
        if stop.get("lat") is not None and stop.get("lon") is not None:
            points.append({"lat": stop["lat"], "lon": stop["lon"]})
    return points
