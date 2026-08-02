from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter

from app.api.dependencies import RouteRuntime
from app.services.route_service import present_route


def create_routes_router(
    *,
    runtime: RouteRuntime,
    unavailable_response: Callable[[str, int], Any],
) -> tuple[APIRouter, Callable[..., Any]]:
    router = APIRouter()

    @router.get("/api/routes/{line}")
    def api_route(line: str) -> Any:
        store = runtime.gtfs_snapshot()
        live_snapshot = runtime.live_snapshot()
        current = runtime.now()
        if not store.loaded:
            return unavailable_response(
                f"GTFS data unavailable: {store.error or 'no usable dataset'}",
                503,
            )
        response = present_route(store, live_snapshot.vehicles, line, current)
        if response is None:
            return unavailable_response("Route not found", 404)
        return response

    return router, api_route
