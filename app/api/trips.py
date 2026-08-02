from __future__ import annotations

from datetime import date
from typing import Any, Callable

from fastapi import APIRouter

from app.api.dependencies import TripRuntime
from app.services.trip_service import present_trip


def create_trips_router(
    *,
    runtime: TripRuntime,
    unavailable_response: Callable[[str, int], Any],
) -> tuple[APIRouter, Callable[..., Any]]:
    router = APIRouter()

    @router.get("/api/trips/{trip_id}")
    def api_trip(
        trip_id: str,
        service_date: str = "",
        vehicle: str = "",
    ) -> Any:
        store = runtime.gtfs_snapshot()
        live_snapshot = runtime.live_snapshot()
        current = runtime.now()
        if not store.loaded:
            return unavailable_response(
                f"GTFS data unavailable: {store.error or 'no usable dataset'}",
                503,
            )
        try:
            service_day = date.fromisoformat(service_date) if service_date else current.date()
        except (TypeError, ValueError):
            service_day = current.date()
        response = present_trip(
            store,
            live_snapshot.vehicles,
            trip_id,
            service_day,
            current,
            vehicle,
        )
        if response is None:
            return unavailable_response("Trip not found", 404)
        return response

    return router, api_trip
