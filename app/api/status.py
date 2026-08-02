from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter

from app.api.dependencies import StatusRuntime
from app.services.gtfs_refresh import masked_url


def create_status_router(
    *,
    runtime: StatusRuntime,
) -> tuple[APIRouter, Callable[[], dict[str, Any]]]:
    router = APIRouter()

    @router.get("/api/status")
    def status() -> dict[str, Any]:
        current = runtime.now()
        store = runtime.gtfs_snapshot()
        live_snapshot = runtime.live_snapshot().with_age(current)
        refresh = dict(runtime.gtfs_refresh_snapshot())
        return {
            "live": {
                "ok": live_snapshot.ok,
                "activeCount": live_snapshot.active_count,
                "rawCount": live_snapshot.raw_count,
                "maxAgeSeconds": runtime.live_max_age_seconds,
                "operatorFilter": runtime.live_operator_filter,
                "error": live_snapshot.error or None,
                "lastFetchTime": live_snapshot.last_attempt_at.isoformat() if live_snapshot.last_attempt_at else None,
                "lastAttemptAt": live_snapshot.last_attempt_at.isoformat() if live_snapshot.last_attempt_at else None,
                "lastSuccessAt": live_snapshot.last_success_at.isoformat() if live_snapshot.last_success_at else None,
                "stale": live_snapshot.stale,
                "ageSeconds": live_snapshot.age_seconds,
                "fetchDurationMs": live_snapshot.fetch_duration_ms,
                "refreshIntervalSeconds": runtime.live_refresh_interval_seconds,
            },
            "gtfs": {
                "ok": store.loaded,
                "loaded": store.loaded,
                "error": store.error or None,
                "source": masked_url(refresh.get("source") or store.source),
                "activeDataSource": masked_url(store.source),
                "refreshEnabled": refresh.get("enabled", False),
                "refreshRunning": refresh.get("running", False),
                "lastCheckedAt": refresh.get("lastCheckedAt"),
                "lastUpdatedAt": refresh.get("lastUpdatedAt"),
                "lastSuccessfulLoadAt": refresh.get("lastSuccessfulLoadAt"),
                "sha256": refresh.get("sha256"),
                "etag": refresh.get("etag"),
                "lastModified": refresh.get("lastModified"),
                "usingCachedData": refresh.get("usingCachedData", bool(store.loaded)),
                "refreshIntervalSeconds": refresh.get("refreshIntervalSeconds"),
                "lastError": refresh.get("lastError") or (store.error or None),
                "metadataPersistenceError": refresh.get("metadataPersistenceError"),
                "counts": {
                    "agency": len(store.agency),
                    "stops": len(store.stops),
                    "routes": len(store.routes),
                    "trips": len(store.trips),
                    "stop_times_trips": len(store.stop_times_by_trip),
                    "stop_departures_index_stops": len(store.stop_departures_index),
                    "shapes": len(store.shapes),
                },
            },
            "serverTime": current.isoformat(),
            "timezone": "Europe/London",
        }

    return router, status
