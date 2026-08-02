from __future__ import annotations

from pathlib import Path
from typing import Callable

from fastapi import APIRouter
from fastapi.responses import FileResponse, HTMLResponse


def create_frontend_router(
    *, templates_path: Path,
) -> tuple[APIRouter, Callable, Callable]:
    router = APIRouter()

    def frontend_response():
        index_path = templates_path / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path))
        return HTMLResponse("Bluestar Unilink")

    @router.get("/")
    def index():
        return frontend_response()

    @router.get("/{path:path}", response_class=HTMLResponse)
    def spa_fallback(path: str):
        return frontend_response()

    return router, index, spa_fallback
