from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter

from app.api.dependencies import SearchRuntime
from app.services.search_service import search_gtfs


def create_search_router(
    *,
    runtime: SearchRuntime,
    unavailable_response: Callable[[str, int], Any],
) -> tuple[APIRouter, Callable[..., Any]]:
    router = APIRouter()

    @router.get("/api/search")
    def api_search(q: str = "") -> Any:
        store = runtime.gtfs_snapshot()
        if not store.loaded:
            return unavailable_response(
                f"GTFS data unavailable: {store.error or 'no usable dataset'}",
                503,
            )
        return search_gtfs(store, q)

    return router, api_search
