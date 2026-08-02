from __future__ import annotations

from contextlib import asynccontextmanager

import main as legacy

from app.config import settings
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_refresh import GTFSRefreshService
from app.services.live_refresh import LiveRefreshService
from app.services.siri_client import SIRIClient, SIRIClientConfig


app = legacy.app


def _build_candidate(path):
    return GTFSStore().load_from_path(path)


def _activate_candidate(candidate) -> None:
    legacy.gtfs_provider.replace(candidate)


gtfs_refresh = GTFSRefreshService(
    source_url=settings.gtfs_download_url,
    target_path=settings.gtfs_zip_path,
    metadata_path=settings.gtfs_metadata_path,
    interval_seconds=settings.gtfs_refresh_interval_seconds,
    timeout_seconds=settings.gtfs_download_timeout_seconds,
    max_download_bytes=settings.gtfs_max_download_bytes,
    max_uncompressed_bytes=settings.gtfs_max_uncompressed_bytes,
    max_attempts=settings.gtfs_refresh_attempts,
    enabled=settings.gtfs_auto_refresh,
    build_candidate=_build_candidate,
    activate_candidate=_activate_candidate,
)
app.state.gtfs_refresh = gtfs_refresh

live_config = SIRIClientConfig.from_env()
live_refresh = LiveRefreshService(
    client=SIRIClient(live_config),
    provider=legacy.live_snapshot_provider,
    interval_seconds=legacy.LIVE_CACHE_TTL_SEC,
    max_age_seconds=legacy.LIVE_MAX_AGE_SECONDS,
    operator_filter=legacy.LIVE_OPERATOR_FILTER,
)
app.state.live_refresh = live_refresh


@asynccontextmanager
async def lifespan(_app):
    legacy.initialize_legacy_stores()
    gtfs_refresh.start()
    live_refresh.start()
    try:
        yield
    finally:
        live_refresh.stop()
        gtfs_refresh.stop()


app.router.lifespan_context = lifespan


@app.get("/api/gtfs/refresh/status")
def gtfs_refresh_status() -> dict:
    return gtfs_refresh.snapshot()
