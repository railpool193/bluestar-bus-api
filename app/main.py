from __future__ import annotations

import main as legacy

from app.config import settings
from app.services.gtfs_refresh import GTFSRefreshService


app = legacy.app


def _reload_gtfs() -> None:
    replacement = legacy.GTFSStore()
    replacement.load()
    if not replacement.loaded:
        raise RuntimeError(replacement.error or "Refreshed GTFS could not be loaded")
    legacy.gtfs = replacement


gtfs_refresh = GTFSRefreshService(
    source_url=settings.gtfs_download_url,
    target_path=settings.gtfs_zip_path,
    interval_seconds=settings.gtfs_refresh_interval_seconds,
    timeout_seconds=settings.gtfs_download_timeout_seconds,
    max_download_bytes=settings.gtfs_max_download_bytes,
    enabled=settings.gtfs_auto_refresh,
    on_changed=_reload_gtfs,
)
app.state.gtfs_refresh = gtfs_refresh


@app.on_event("startup")
def start_gtfs_refresh() -> None:
    gtfs_refresh.start()


@app.on_event("shutdown")
def stop_gtfs_refresh() -> None:
    gtfs_refresh.stop()


@app.get("/api/gtfs/refresh/status")
def gtfs_refresh_status() -> dict:
    return gtfs_refresh.snapshot()
