import os
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider, GTFSStoreProxy
from app.services.live_store_provider import LiveSnapshotProvider
from app.services.siri_parser import children_by_local, xml_text
from app.services.vehicle_service import vehicles_for_line
from app.api.map import create_map_router
from app.api.health import create_health_router
from app.api.search import create_search_router
from app.api.status import create_status_router
from app.api.stops import create_stops_router
from app.api.trips import create_trips_router
from app.api.vehicles import create_vehicles_router
from app.api.dependencies import APIRuntime, SearchRuntime, StatusRuntime, StopRuntime, TripRuntime
from app.utils.geo_utils import haversine_m
from app.utils.text_utils import (
    clean_text, destination_match, extract_codes, fleet_from_vehicle_ref,
    human_name, line_norm, norm, public_stop_code, safe_float, safe_int,
    short_destination, stop_code_from_name,
)
from app.utils.time_utils import (
    LONDON, gtfs_time_to_datetime, hhmm, minutes_until, now_london, parse_iso_dt,
)

APP_NAME = "Bluestar Unilink Menetrend"
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
GTFS_DIR = Path(os.getenv("GTFS_DIR", str(BASE_DIR / "gtfs")))
GTFS_ZIP = Path(os.getenv("GTFS_ZIP", str(BASE_DIR / "gtfs.zip")))

LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "8"))
LIVE_MAX_AGE_SECONDS = int(os.getenv("LIVE_MAX_AGE_SECONDS", "360"))
LIVE_OPERATOR_FILTER = os.getenv("LIVE_OPERATOR_FILTER", "BLUS").strip().upper()
DEPARTURE_WINDOW_MIN = int(os.getenv("DEPARTURE_WINDOW_MIN", "120"))
DEPARTURE_LIMIT = int(os.getenv("DEPARTURE_LIMIT", "80"))
LIVE_MATCH_MINUTES = int(os.getenv("LIVE_MATCH_MINUTES", "38"))

app = FastAPI(title=APP_NAME)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


gtfs_provider = GTFSStoreProvider(
    GTFSStore(zip_path=GTFS_ZIP, directory_path=GTFS_DIR)
)
gtfs = GTFSStoreProxy(gtfs_provider)


live_snapshot_provider = LiveSnapshotProvider()


class LiveStoreProxy:
    """Read-only compatibility facade over the active live snapshot."""

    def fetch(self, force: bool = False) -> List[Dict[str, Any]]:
        return [dict(vehicle) for vehicle in live_snapshot_provider.get().vehicles]

    @property
    def vehicles(self): return self.fetch()
    @property
    def ok(self): return live_snapshot_provider.get().ok
    @property
    def error(self): return live_snapshot_provider.get().error
    @property
    def last_fetch(self): return live_snapshot_provider.get().last_attempt_at
    @property
    def raw_count(self): return live_snapshot_provider.get().raw_count


live_store = LiveStoreProxy()


def trip_headsign(trip: Dict[str, Any]) -> str:
    return human_name(trip.get("trip_headsign") or trip.get("destination") or "")


def api_error(text: str, status: int = 400):
    return JSONResponse({"ok": False, "error": text}, status_code=status)


def require_gtfs():
    store = gtfs_provider.get()
    if not store.loaded:
        candidate = GTFSStore(zip_path=GTFS_ZIP, directory_path=GTFS_DIR).load()
        if candidate.loaded:
            gtfs_provider.replace(candidate)
            store = candidate
    if not store.loaded:
        return store, api_error(f"GTFS data unavailable: {store.error or 'no usable dataset'}", 503)
    return store, None


def initialize_legacy_stores():
    candidate = GTFSStore(zip_path=GTFS_ZIP, directory_path=GTFS_DIR).load()
    if candidate.loaded:
        gtfs_provider.replace(candidate)


def current_live_vehicles() -> List[Dict[str, Any]]:
    return [dict(vehicle) for vehicle in live_snapshot_provider.get().vehicles]


@app.get("/")
def index():
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")


@app.get("/api/routes/{line}")
def api_route(line: str):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    ln = line_norm(line)
    route_ids = store.route_by_short.get(ln, [])
    if not route_ids:
        route_ids = [rid for rid, r in store.routes.items() if line_norm(r.get("route_id")) == ln]
    if not route_ids:
        return api_error("Route not found", 404)
    vehicles = vehicles_for_line(current_live_vehicles(), line)
    active = store.active_service_ids(now_london().date()) | store.active_service_ids((now_london() - timedelta(days=1)).date())
    directions_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for trip in store.trips.values():
        if trip.get("route_id") not in route_ids:
            continue
        if active and trip.get("service_id") not in active:
            continue
        dest = short_destination(trip_headsign(trip))
        key = (trip.get("direction_id", ""), dest)
        if key in directions_map:
            continue
        stops = []
        for r in store.stop_times_by_trip.get(trip.get("trip_id"), []):
            st = store.stops.get(r.get("stop_id"), {})
            stops.append({"id": r.get("stop_id"), "name": st.get("stop_name", r.get("stop_id")), "code": st.get("code", "BUS"), "lat": st.get("lat"), "lon": st.get("lon"), "sequence": r.get("stop_sequence")})
        directions_map[key] = {"directionId": key[0], "destination": dest, "tripId": trip.get("trip_id"), "stops": stops}
        if len(directions_map) >= 6:
            break
    return {"ok": True, "line": clean_text(line), "routes": [store.routes[rid] for rid in route_ids], "vehicles": vehicles, "directions": list(directions_map.values())}


vehicles_router, api_vehicles = create_vehicles_router(
    runtime=APIRuntime(
        gtfs_provider=gtfs_provider,
        live_provider=live_snapshot_provider,
        gtfs_zip_path=GTFS_ZIP,
        gtfs_directory_path=GTFS_DIR,
        now=lambda: now_london(),
    ),
)
health_router, health = create_health_router(
    app_name=APP_NAME,
    now=lambda: now_london(),
)
status_router, status = create_status_router(
    runtime=StatusRuntime(
        gtfs_provider=gtfs_provider,
        live_provider=live_snapshot_provider,
        gtfs_refresh_snapshot=lambda: (
            getattr(app.state, "gtfs_refresh").snapshot()
            if getattr(app.state, "gtfs_refresh", None)
            else {}
        ),
        now=lambda: now_london(),
        live_max_age_seconds=LIVE_MAX_AGE_SECONDS,
        live_operator_filter=LIVE_OPERATOR_FILTER,
        live_refresh_interval_seconds=LIVE_CACHE_TTL_SEC,
    ),
)
search_router, api_search = create_search_router(
    runtime=SearchRuntime(gtfs_provider=gtfs_provider),
    unavailable_response=api_error,
)
stops_router, api_stop_departures = create_stops_router(
    runtime=StopRuntime(
        gtfs_provider=gtfs_provider,
        live_provider=live_snapshot_provider,
        now=lambda: now_london(),
        departure_window_minutes=DEPARTURE_WINDOW_MIN,
        departure_limit=DEPARTURE_LIMIT,
        matching_minutes=LIVE_MATCH_MINUTES,
    ),
    unavailable_response=api_error,
)
trips_router, api_trip = create_trips_router(
    runtime=TripRuntime(
        gtfs_provider=gtfs_provider,
        live_provider=live_snapshot_provider,
        now=lambda: now_london(),
    ),
    unavailable_response=api_error,
)
map_router, api_map = create_map_router(
    runtime=APIRuntime(
        gtfs_provider=gtfs_provider,
        live_provider=live_snapshot_provider,
        gtfs_zip_path=GTFS_ZIP,
        gtfs_directory_path=GTFS_DIR,
        now=lambda: now_london(),
    ),
    unavailable_response=api_error,
)
app.router.routes.extend(health_router.routes)
app.router.routes.extend(status_router.routes)
app.router.routes.extend(search_router.routes)
app.router.routes.extend(stops_router.routes)
app.router.routes.extend(trips_router.routes)
app.router.routes.extend(vehicles_router.routes)
app.router.routes.extend(map_router.routes)


@app.get("/{path:path}", response_class=HTMLResponse)
def spa_fallback(path: str):
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")
