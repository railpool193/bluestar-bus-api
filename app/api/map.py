from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable

from fastapi import APIRouter

from app.api.dependencies import APIRuntime
from app.services.map_service import shape_for_trip
from app.services.vehicle_service import vehicles_for_line
from app.services.fleet_registry import enrich_vehicle
from app.utils.text_utils import clean_text, line_norm


def create_map_router(
    *,
    runtime: APIRuntime,
    unavailable_response: Callable[[str, int], Any],
) -> tuple[APIRouter, Callable[..., Any]]:
    router = APIRouter()

    @router.get("/api/map")
    def api_map(line: str = "") -> Any:
        store = runtime.gtfs_snapshot()
        if not store.loaded:
            return unavailable_response(f"GTFS data unavailable: {store.error or 'no usable dataset'}", 503)
        live_snapshot = runtime.live_snapshot()
        current = runtime.now()
        normalized_line = line_norm(line)
        vehicles = vehicles_for_line(live_snapshot.vehicles, line)
        if runtime.fleet_provider is not None:
            registry = runtime.fleet_provider.get()
            vehicles = [enrich_vehicle(vehicle, registry, operator=runtime.fleet_operator_id) for vehicle in vehicles]
        shapes: list[dict[str, Any]] = []
        if normalized_line:
            route_ids = store.route_by_short.get(normalized_line, [])
            active = store.active_service_ids(current.date()) | store.active_service_ids((current - timedelta(days=1)).date())
            seen: set[str] = set()
            for trip in store.trips.values():
                shape_id = clean_text(trip.get("shape_id"))
                if trip.get("route_id") in route_ids and shape_id and shape_id not in seen:
                    if active and trip.get("service_id") not in active:
                        continue
                    points = store.shapes.get(shape_id, [])
                    if points:
                        shapes.append({"shapeId": shape_id, "points": points[:3000]})
                        seen.add(shape_id)
                if len(shapes) >= 8:
                    break
            if not shapes:
                for trip in store.trips.values():
                    if trip.get("route_id") in route_ids:
                        shapes.append({"shapeId": trip.get("trip_id"), "points": shape_for_trip(store, trip)})
                        break
        positions = [(vehicle.get("latitude"), vehicle.get("longitude")) for vehicle in vehicles if vehicle.get("latitude") is not None and vehicle.get("longitude") is not None]
        center = (
            {"lat": sum(point[0] for point in positions) / len(positions), "lon": sum(point[1] for point in positions) / len(positions)}
            if positions else {"lat": 50.9097, "lon": -1.4044}
        )
        return {"ok": True, "line": clean_text(line), "vehicles": vehicles, "shapes": shapes, "center": center, "now": current.isoformat()}

    return router, api_map
