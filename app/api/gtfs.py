from __future__ import annotations

from typing import Any, Callable, Mapping

from fastapi import APIRouter


def create_gtfs_router(
    *, snapshot: Callable[[], Mapping[str, Any]],
) -> tuple[APIRouter, Callable[[], dict[str, Any]]]:
    router = APIRouter()

    @router.get("/api/gtfs/refresh/status")
    def gtfs_refresh_status() -> dict[str, Any]:
        return dict(snapshot())

    return router, gtfs_refresh_status
