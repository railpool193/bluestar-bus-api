import os
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.services.gtfs_calendar import service_days
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider, GTFSStoreProxy
from app.services.departure_service import enrich_departure
from app.services.live_matching import find_live_for_trip, match_live_to_departure, stop_same
from app.services.live_store_provider import LiveSnapshotProvider
from app.services.siri_parser import children_by_local, xml_text
from app.services.vehicle_service import vehicles_for_line
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


def service_days_for_departures() -> List[date]:
    return service_days(now_london())


def trip_headsign(trip: Dict[str, Any]) -> str:
    return human_name(trip.get("trip_headsign") or trip.get("destination") or "")


def shape_for_trip(store: GTFSStore, trip: Dict[str, Any]) -> List[Dict[str, float]]:
    sid = clean_text(trip.get("shape_id"))
    if sid and store.shapes.get(sid):
        return store.shapes[sid][:3000]
    out = []
    for r in store.stop_times_by_trip.get(trip.get("trip_id"), []):
        st = store.stops.get(r.get("stop_id"), {})
        if st.get("lat") is not None and st.get("lon") is not None:
            out.append({"lat": st["lat"], "lon": st["lon"]})
    return out


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


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "time": now_london().isoformat()}


@app.get("/api/status")
def status():
    store, _ = require_gtfs()
    live_snapshot = live_snapshot_provider.get().with_age(now_london())
    vehicles = [dict(vehicle) for vehicle in live_snapshot.vehicles]
    refresh_service = getattr(app.state, "gtfs_refresh", None)
    refresh = refresh_service.snapshot() if refresh_service else {}
    return {
        "live": {
            "ok": live_snapshot.ok,
            "activeCount": len(vehicles),
            "rawCount": live_snapshot.raw_count,
            "maxAgeSeconds": LIVE_MAX_AGE_SECONDS,
            "operatorFilter": LIVE_OPERATOR_FILTER,
            "error": live_snapshot.error or None,
            "lastFetchTime": live_snapshot.last_attempt_at.isoformat() if live_snapshot.last_attempt_at else None,
            "lastAttemptAt": live_snapshot.last_attempt_at.isoformat() if live_snapshot.last_attempt_at else None,
            "lastSuccessAt": live_snapshot.last_success_at.isoformat() if live_snapshot.last_success_at else None,
            "stale": live_snapshot.stale,
            "ageSeconds": live_snapshot.age_seconds,
            "fetchDurationMs": live_snapshot.fetch_duration_ms,
            "refreshIntervalSeconds": LIVE_CACHE_TTL_SEC,
        },
        "gtfs": {
            "ok": store.loaded,
            "loaded": store.loaded,
            "error": store.error or None,
            "source": refresh.get("source") or store.source,
            "activeDataSource": store.source,
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
        "serverTime": now_london().isoformat(),
        "timezone": "Europe/London",
    }


@app.get("/")
def index():
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")


@app.get("/api/search")
def api_search(q: str = ""):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    query = clean_text(q)
    nq = norm(query)
    stops = []
    routes = []
    if nq:
        for sid, st in store.stops.items():
            hay = norm(st.get("stop_name", "") + " " + sid + " " + st.get("code", ""))
            if nq in hay:
                stops.append({
                    "id": sid,
                    "stop_id": sid,
                    "name": st.get("stop_name", sid),
                    "code": st.get("code", "BUS"),
                    "lat": st.get("lat"),
                    "lon": st.get("lon"),
                })
                if len(stops) >= 50:
                    break
        for rid, rt in store.routes.items():
            line = rt.get("route_short_name", "")
            hay = norm(line + " " + rt.get("route_long_name", "") + " " + rid)
            if nq in hay or nq == norm(line):
                routes.append({
                    "id": line,
                    "routeId": rid,
                    "line": line,
                    "name": line,
                    "subtitle": rt.get("route_long_name", ""),
                })
        routes.sort(key=lambda r: (0 if line_norm(r.get("line")) == line_norm(query) else 1, r.get("line")))
    return {"ok": True, "query": query, "stops": stops[:50], "routes": routes[:40]}


@app.get("/api/stops/{stop_id}/departures")
def api_stop_departures(stop_id: str, minutes: int = DEPARTURE_WINDOW_MIN):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    stop = store.stops.get(stop_id)
    if not stop:
        return api_error("Stop not found", 404)
    n = now_london()
    end = n + timedelta(minutes=max(10, min(minutes, 360)))
    vehicles = current_live_vehicles()
    result = []
    for service_day in service_days_for_departures():
        active = store.active_service_ids(service_day)
        for dep in store.stop_departures_index.get(stop_id, []):
            if active and dep.get("service_id") not in active:
                continue
            if clean_text(dep.get("pickup_type")) == "1" or dep.get("is_last_stop"):
                continue
            sched_dt = gtfs_time_to_datetime(service_day, dep.get("departure_time") or dep.get("arrival_time"))
            if not sched_dt or sched_dt < n - timedelta(minutes=2) or sched_dt > end:
                continue
            result.append(enrich_departure(store, dep, service_day, sched_dt, vehicles, reference_time=n, matching_minutes=LIVE_MATCH_MINUTES))
    result.sort(key=lambda x: x.get("displayTimeIso") or x.get("scheduledTimeIso") or "")
    dedup = []
    seen = set()
    for x in result:
        key = (x.get("tripId"), x.get("serviceDate"), x.get("stopSequence"))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(x)
    return {"ok": True, "stop": stop, "departures": dedup[:DEPARTURE_LIMIT], "now": now_london().isoformat()}


@app.get("/api/trips/{trip_id}")
def api_trip(trip_id: str, service_date: str = "", vehicle: str = ""):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    trip = store.trips.get(trip_id)
    if not trip:
        return api_error("Trip not found", 404)
    try:
        service_day = date.fromisoformat(service_date) if service_date else now_london().date()
    except Exception:
        service_day = now_london().date()
    route = store.routes.get(trip.get("route_id"), {})
    vehicles = current_live_vehicles()
    live, current_seq = find_live_for_trip(store, trip, service_day, vehicles, vehicle_hint=vehicle)
    rows = store.stop_times_by_trip.get(trip_id, [])
    n = now_london()
    delay = live.get("delayMinutes") if live else None
    out = []
    for r in rows:
        st = store.stops.get(r.get("stop_id"), {})
        sched_dt = gtfs_time_to_datetime(service_day, r.get("departure_time") or r.get("arrival_time"))
        live_dt = None
        is_current = False
        live_future = False
        seq = safe_int(r.get("stop_sequence"), 0)
        if live:
            if stop_same(st, live.get("currentStopRef", ""), live.get("currentStopName", "")):
                is_current = True
                live_dt = parse_iso_dt(live.get("liveTime")) or sched_dt
            elif current_seq and seq > current_seq and isinstance(delay, int) and sched_dt:
                live_future = True
                live_dt = sched_dt + timedelta(minutes=delay)
        display_dt = live_dt or sched_dt
        mins = minutes_until(display_dt)
        if mins is not None and mins < 0:
            mins = None
        past = bool(display_dt and display_dt < n - timedelta(seconds=30) and not is_current)
        if current_seq and seq < current_seq:
            past = True
        if is_current:
            right = "LIVE" if live.get("vehicleAtStop") else "Due"
        elif mins is not None:
            right = "Due" if mins <= 1 and (live_future or live) else f"{mins}'"
        else:
            right = ""
        out.append({
            "stopId": r.get("stop_id"),
            "name": st.get("stop_name", r.get("stop_id")),
            "sequence": seq,
            "lat": st.get("lat"),
            "lon": st.get("lon"),
            "scheduledTime": hhmm(sched_dt),
            "scheduledTimeIso": sched_dt.isoformat() if sched_dt else "",
            "displayTime": hhmm(display_dt),
            "displayTimeIso": display_dt.isoformat() if display_dt else "",
            "minutes": mins,
            "rightLabel": right,
            "live": bool(is_current or live_future),
            "current": is_current,
            "past": past,
        })
    if isinstance(delay, int):
        delay_label = f"{delay:+d}"
    elif live:
        delay_label = "LIVE"
    else:
        delay_label = "--"
    return {
        "ok": True,
        "trip": {**trip, "destination": short_destination(trip_headsign(trip)), "destinationFull": trip_headsign(trip)},
        "route": route,
        "serviceDate": service_day.isoformat(),
        "stops": out,
        "live": live,
        "delayLabel": delay_label,
        "currentSequence": current_seq,
        "shape": shape_for_trip(store, trip),
        "now": now_london().isoformat(),
    }


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


@app.get("/api/vehicles")
def api_vehicles(line: str = ""):
    vehicles = vehicles_for_line(current_live_vehicles(), line)
    return {"ok": True, "line": clean_text(line), "vehicles": vehicles, "now": now_london().isoformat()}


@app.get("/api/map")
def api_map(line: str = ""):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    ln = line_norm(line)
    vehicles = vehicles_for_line(current_live_vehicles(), line)
    shapes = []
    if ln:
        route_ids = store.route_by_short.get(ln, [])
        active = store.active_service_ids(now_london().date()) | store.active_service_ids((now_london() - timedelta(days=1)).date())
        seen = set()
        for trip in store.trips.values():
            sid = clean_text(trip.get("shape_id"))
            if trip.get("route_id") in route_ids and sid and sid not in seen:
                if active and trip.get("service_id") not in active:
                    continue
                pts = store.shapes.get(sid, [])
                if pts:
                    shapes.append({"shapeId": sid, "points": pts[:3000]})
                    seen.add(sid)
            if len(shapes) >= 8:
                break
        if not shapes:
            for trip in store.trips.values():
                if trip.get("route_id") in route_ids:
                    shapes.append({"shapeId": trip.get("trip_id"), "points": shape_for_trip(store, trip)})
                    break
    pts = [(v.get("latitude"), v.get("longitude")) for v in vehicles if v.get("latitude") is not None and v.get("longitude") is not None]
    if pts:
        center = {"lat": sum(p[0] for p in pts) / len(pts), "lon": sum(p[1] for p in pts) / len(pts)}
    else:
        center = {"lat": 50.9097, "lon": -1.4044}
    return {"ok": True, "line": clean_text(line), "vehicles": vehicles, "shapes": shapes, "center": center, "now": now_london().isoformat()}


@app.get("/{path:path}", response_class=HTMLResponse)
def spa_fallback(path: str):
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")
