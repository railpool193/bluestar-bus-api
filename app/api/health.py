from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from fastapi import APIRouter


def create_health_router(
    *,
    app_name: str,
    now: Callable[[], datetime],
) -> tuple[APIRouter, Callable[[], dict[str, Any]]]:
    router = APIRouter()

    @router.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "app": app_name, "time": now().isoformat()}

    return router, health
