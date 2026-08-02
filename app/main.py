from __future__ import annotations

from app.factory import create_app
from app.runtime import create_runtime


runtime = create_runtime()
app = create_app(runtime)

gtfs_refresh = runtime.gtfs_refresh
live_refresh = runtime.live_refresh
gtfs_provider = runtime.gtfs_provider
live_snapshot_provider = runtime.live_provider
gtfs = runtime.gtfs
live_store = runtime.live_store

health = runtime.endpoints["health"]
status = runtime.endpoints["status"]
api_search = runtime.endpoints["api_search"]
api_stop_departures = runtime.endpoints["api_stop_departures"]
api_trip = runtime.endpoints["api_trip"]
api_route = runtime.endpoints["api_route"]
api_vehicles = runtime.endpoints["api_vehicles"]
api_map = runtime.endpoints["api_map"]
gtfs_refresh_status = runtime.endpoints["gtfs_refresh_status"]
index = runtime.endpoints["index"]
spa_fallback = runtime.endpoints["spa_fallback"]
