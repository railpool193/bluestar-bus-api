"""Compatibility exports for historic imports; production uses app.main:app."""

from app.api.errors import api_error
from app.main import (
    api_map,
    api_route,
    api_search,
    api_stop_departures,
    api_trip,
    api_vehicles,
    app,
    gtfs,
    gtfs_provider,
    gtfs_refresh,
    gtfs_refresh_status,
    health,
    index,
    live_refresh,
    live_snapshot_provider,
    live_store,
    runtime,
    spa_fallback,
    status,
)
from app.services.gtfs_loader import GTFSStore
from app.utils.time_utils import LONDON, now_london


settings = runtime.settings
APP_NAME = settings.app_name
GTFS_DIR = settings.gtfs_directory_path
GTFS_ZIP = settings.gtfs_zip_path
LIVE_CACHE_TTL_SEC = settings.live_cache_ttl_seconds
LIVE_MAX_AGE_SECONDS = settings.live_max_age_seconds
LIVE_OPERATOR_FILTER = settings.live_operator_filter
DEPARTURE_WINDOW_MIN = settings.departure_window_minutes
DEPARTURE_LIMIT = settings.departure_limit
LIVE_MATCH_MINUTES = settings.live_match_minutes
TEMPLATES_DIR = settings.templates_path
STATIC_DIR = settings.static_path


def initialize_legacy_stores():
    return runtime.initialize_local_gtfs()
