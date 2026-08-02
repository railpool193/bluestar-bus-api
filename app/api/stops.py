from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter

from app.api.dependencies import StopRuntime
from app.services.departure_service import stop_departures


def create_stops_router(
    *,
    runtime: StopRuntime,
    unavailable_response: Callable[[str, int], Any],
) -> tuple[APIRouter, Callable[..., Any]]:
    router = APIRouter()

    @router.get("/api/stops/{stop_id}/departures")
    def api_stop_departures(
        stop_id: str,
        minutes: int = runtime.departure_window_minutes,
    ) -> Any:
        store = runtime.gtfs_snapshot()
        if not store.loaded:
            return unavailable_response(
                f"GTFS data unavailable: {store.error or 'no usable dataset'}",
                503,
            )
        current = runtime.now()
        live_snapshot = runtime.live_snapshot()
        response = stop_departures(
            store,
            live_snapshot,
            stop_id,
            reference_time=current,
            window_minutes=minutes,
            departure_limit=runtime.departure_limit,
            matching_minutes=runtime.matching_minutes,
        )
        if response is None:
            return unavailable_response("Stop not found", 404)
        return response

    return router, api_stop_departures
