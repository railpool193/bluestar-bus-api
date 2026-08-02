from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter

from app.api.dependencies import APIRuntime
from app.services.vehicle_service import vehicles_for_line
from app.services.fleet_registry import enrich_vehicle
from app.utils.text_utils import clean_text


def create_vehicles_router(
    *,
    runtime: APIRuntime,
) -> tuple[APIRouter, Callable[..., dict[str, Any]]]:
    router = APIRouter()

    @router.get("/api/vehicles")
    def api_vehicles(line: str = "") -> dict[str, Any]:
        snapshot = runtime.live_snapshot()
        vehicles = vehicles_for_line(snapshot.vehicles, line)
        if runtime.fleet_provider is not None:
            registry = runtime.fleet_provider.get()
            vehicles = [enrich_vehicle(vehicle, registry, operator=runtime.fleet_operator_id) for vehicle in vehicles]
        return {
            "ok": True,
            "line": clean_text(line),
            "vehicles": vehicles,
            "now": runtime.now().isoformat(),
        }

    return router, api_vehicles
