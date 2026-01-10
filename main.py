import csv
import os
import time
import math
import unicodedata
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Set, Any

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # fallback


# -----------------------------
# Config
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GTFS_DIR = os.getenv("GTFS_DIR", os.path.join(BASE_DIR, "gtfs"))

APP_TZ_NAME = os.getenv("APP_TZ", "Europe/London")
APP_TZ = None
if ZoneInfo:
    try:
        APP_TZ = ZoneInfo(APP_TZ_NAME)
    except Exception:
        APP_TZ = None

DFT_API_KEY = os.getenv("DFT_API_KEY", "9d2f6818e2723996467fedb958ba682aa9860a93")
DFT_FEED_URL = os.getenv(
    "DFT_FEED_URL",
    f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={DFT_API_KEY}",
)
LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "12"))


# -----------------------------
# App
# -----------------------------
app = FastAPI(title="Bluestar Stop Departures API")


# -----------------------------
# Helpers
# -----------------------------
def _norm(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    return " ".join(s.split())


def _read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _safe_float(x: str) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def _parse_gtfs_time_to_seconds(t: str) -> Optional[int]:
    if not t:
        return None
    try:
        parts = t.strip().split(":")
        if len(parts) != 3:
            return None
        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        if m < 0 or m > 59 or s < 0 or s > 59 or h < 0:
            return None
        return h * 3600 + m * 60 + s
    except Exception:
        return None


def _seconds_to_hhmm(sec: int) -> str:
    sec = sec % 86400
    h = sec // 3600
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _now_dt() -> datetime:
    if APP_TZ:
        return datetime.now(APP_TZ)
    return datetime.utcnow()


# -----------------------------
# GTFS Data
# -----------------------------
@dataclass(frozen=True)
class Stop:
    stop_id: str
    stop_name: str
    lat: Optional[float]
    lon: Optional[float]
    stop_code: Optional[str]
    parent_station: Optional[str]
    norm_name: str


@dataclass(frozen=True)
class Route:
    route_id: str
    short_name: str
    long_name: str


@dataclass(frozen=True)
class Trip:
    trip_id: str
    route_id: str
    service_id: str
    headsign: str
    direction_id: Optional[str]


STOPS: Dict[str, Stop] = {}
ROUTES: Dict[str, Route] = {}
TRIPS: Dict[str, Trip] = {}
STOP_TIMES_BY_STOP: Dict[str, List[Tuple[int, str]]] = {}
CALENDAR: Dict[str, Dict[str, Any]] = {}
CAL_ADDED: Dict[str, Set[str]] = {}
CAL_REMOVED: Dict[str, Set[str]] = {}
DATA_LOADED_AT: Optional[float] = None


def load_gtfs() -> None:
    global STOPS, ROUTES, TRIPS, STOP_TIMES_BY_STOP, CALENDAR, CAL_ADDED, CAL_REMOVED, DATA_LOADED_AT

    stops_path = os.path.join(GTFS_DIR, "stops.txt")
    routes_path = os.path.join(GTFS_DIR, "routes.txt")
    trips_path = os.path.join(GTFS_DIR, "trips.txt")
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")
    calendar_path = os.path.join(GTFS_DIR, "calendar.txt")
    calendar_dates_path = os.path.join(GTFS_DIR, "calendar_dates.txt")

    if not os.path.exists(stops_path):
        raise FileNotFoundError(f"Missing stops.txt in {GTFS_DIR}")

    STOPS = {}
    for row in _read_csv(stops_path):
        sid = (row.get("stop_id") or "").strip()
        if not sid:
            continue
        name = (row.get("stop_name") or "").strip()
        STOPS[sid] = Stop(
            stop_id=sid,
            stop_name=name,
            lat=_safe_float(row.get("stop_lat", "")),
            lon=_safe_float(row.get("stop_lon", "")),
            stop_code=(row.get("stop_code") or "").strip() or None,
            parent_station=(row.get("parent_station") or "").strip() or None,
            norm_name=_norm(name),
        )

    ROUTES = {}
    if os.path.exists(routes_path):
        for row in _read_csv(routes_path):
            rid = (row.get("route_id") or "").strip()
            if not rid:
                continue
            ROUTES[rid] = Route(
                route_id=rid,
                short_name=(row.get("route_short_name") or "").strip(),
                long_name=(row.get("route_long_name") or "").strip(),
            )

    TRIPS = {}
    if os.path.exists(trips_path):
        for row in _read_csv(trips_path):
            tid = (row.get("trip_id") or "").strip()
            if not tid:
                continue
            TRIPS[tid] = Trip(
                trip_id=tid,
                route_id=(row.get("route_id") or "").strip(),
                service_id=(row.get("service_id") or "").strip(),
                headsign=(row.get("trip_headsign") or "").strip(),
                direction_id=(row.get("direction_id") or "").strip() or None,
            )

    STOP_TIMES_BY_STOP = {}
    if os.path.exists(stop_times_path):
        for row in _read_csv(stop_times_path):
            stop_id = (row.get("stop_id") or "").strip()
            trip_id = (row.get("trip_id") or "").strip()
            dep = _parse_gtfs_time_to_seconds(row.get("departure_time", "") or "")
            if not stop_id or not trip_id or dep is None:
                continue
            STOP_TIMES_BY_STOP.setdefault(stop_id, []).append((dep, trip_id))
        for sid in STOP_TIMES_BY_STOP:
            STOP_TIMES_BY_STOP[sid].sort(key=lambda x: x[0])

    CALENDAR = {}
    CAL_ADDED = {}
    CAL_REMOVED = {}

    if os.path.exists(calendar_path):
        for row in _read_csv(calendar_path):
            sid = (row.get("service_id") or "").strip()
            if sid:
                CALENDAR[sid] = row

    if os.path.exists(calendar_dates_path):
        for row in _read_csv(calendar_dates_path):
            sid = (row.get("service_id") or "").strip()
            d = (row.get("date") or "").strip()
            ex = (row.get("exception_type") or "").strip()
            if not sid or not d or ex not in ("1", "2"):
                continue
            if ex == "1":
                CAL_ADDED.setdefault(sid, set()).add(d)
            else:
                CAL_REMOVED.setdefault(sid, set()).add(d)

    DATA_LOADED_AT = time.time()


def _service_active_on(service_id: str, d: date) -> bool:
    ds = _yyyymmdd(d)

    if ds in CAL_REMOVED.get(service_id, set()):
        return False
    if ds in CAL_ADDED.get(service_id, set()):
        return True

    cal = CALENDAR.get(service_id)
    if not cal:
        return False

    start = (cal.get("start_date") or "").strip()
    end = (cal.get("end_date") or "").strip()
    if start and ds < start:
        return False
    if end and ds > end:
        return False

    weekday = d.weekday()  # 0=Mon
    keys = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    return (cal.get(keys[weekday]) or "0").strip() == "1"


# -----------------------------
# Live vehicles cache + parse
# -----------------------------
_live_cache: Dict[str, Any] = {"ts": 0.0, "data": []}


def _xml_findtext_suffix(elem, suffix: str) -> Optional[str]:
    for child in elem.iter():
        if isinstance(child.tag, str) and child.tag.endswith(suffix):
            if child.text:
                return child.text.strip()
    return None


def fetch_live_vehicles() -> List[Dict[str, Any]]:
    now = time.time()
    if (now - _live_cache["ts"]) < LIVE_CACHE_TTL_SEC and isinstance(_live_cache["data"], list):
        return _live_cache["data"]

    try:
        r = requests.get(DFT_FEED_URL, timeout=20)
        r.raise_for_status()
        content = r.content
    except Exception:
        _live_cache["ts"] = now
        _live_cache["data"] = []
        return []

    vehicles: List[Dict[str, Any]] = []
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(content)

        for va in root.iter():
            if not (isinstance(va.tag, str) and va.tag.endswith("VehicleActivity")):
                continue

            lat = _xml_findtext_suffix(va, "Latitude")
            lon = _xml_findtext_suffix(va, "Longitude")
            line_ref = _xml_findtext_suffix(va, "LineRef") or _xml_findtext_suffix(va, "PublishedLineName")
            vehicle_ref = _xml_findtext_suffix(va, "VehicleRef")
            dest = _xml_findtext_suffix(va, "DestinationName")
            recorded = _xml_findtext_suffix(va, "RecordedAtTime") or _xml_findtext_suffix(va, "ValidUntilTime")

            if not lat or not lon:
                continue

            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except Exception:
                continue

            vehicles.append(
                {
                    "vehicle_id": vehicle_ref,
                    "line": line_ref,
                    "destination": dest,
                    "lat": lat_f,
                    "lon": lon_f,
                    "recorded_at": recorded,
                }
            )
    except Exception:
        vehicles = []

    _live_cache["ts"] = now
    _live_cache["data"] = vehicles
    return vehicles


# -----------------------------
# Startup
# -----------------------------
@app.on_event("startup")
def _startup():
    # Load GTFS on boot
    try:
        load_gtfs()
    except Exception as e:
        # Don't crash the server; health will show 0 stops
        print("GTFS load error:", e)


# -----------------------------
# API endpoints
# -----------------------------
@app.get("/api/health")
def api_health():
    return {
        "ok": True,
        "loaded_at": DATA_LOADED_AT,
        "stops": len(STOPS),
        "routes": len(ROUTES),
        "trips": len(TRIPS),
        "gtfs_dir": GTFS_DIR,
        "tz": APP_TZ_NAME,
    }


@app.get("/api/stops")
def api_stops(q: str = Query("", min_length=0), limit: int = 10):
    qn = _norm(q)
    if not qn:
        return []

    limit = max(1, min(limit, 50))
    results = []
    for s in STOPS.values():
        hay = s.norm_name
        code = (s.stop_code or "").lower()
        sid = s.stop_id.lower()

        score = None
        if hay.startswith(qn) or any(word.startswith(qn) for word in hay.split()):
            score = 0
        elif qn in hay:
            score = 1
        elif qn == sid or (qn and qn in sid):
            score = 2
        elif qn and code and qn in code:
            score = 2

        if score is None:
            continue
        results.append((score, len(s.stop_name), s))

    results.sort(key=lambda x: (x[0], x[1]))

    out = []
    for _, __, s in results[:limit]:
        out.append(
            {
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "lat": s.lat,
                "lon": s.lon,
                "stop_code": s.stop_code,
                "parent_station": s.parent_station,
            }
        )
    return out


@app.get("/api/stop/{stop_id}")
def api_stop(stop_id: str):
    s = STOPS.get(stop_id)
    if not s:
        raise HTTPException(status_code=404, detail="stop_not_found")
    return {
        "stop_id": s.stop_id,
        "stop_name": s.stop_name,
        "lat": s.lat,
        "lon": s.lon,
        "stop_code": s.stop_code,
        "parent_station": s.parent_station,
    }


@app.get("/api/nearby")
def api_nearby(lat: float, lon: float, radius_m: float = 600, limit: int = 15):
    radius_m = max(50.0, min(radius_m, 5000.0))
    limit = max(1, min(limit, 50))

    candidates = []
    for s in STOPS.values():
        if s.lat is None or s.lon is None:
            continue
        d = _haversine_m(lat, lon, s.lat, s.lon)
        if d <= radius_m:
            candidates.append((d, s))

    candidates.sort(key=lambda x: x[0])
    out = []
    for d, s in candidates[:limit]:
        out.append(
            {
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "lat": s.lat,
                "lon": s.lon,
                "distance_m": round(d, 1),
            }
        )
    return out


@app.get("/api/departures")
def api_departures(stop_id: str, window_min: int = 90, max_results: int = 40):
    if stop_id not in STOPS:
        raise HTTPException(status_code=404, detail="stop_not_found")

    window_min = max(10, min(window_min, 240))
    max_results = max(1, min(max_results, 80))

    s = STOPS[stop_id]
    now_dt = _now_dt()
    today = now_dt.date()
    yesterday = today - timedelta(days=1)

    now_sec = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
    window_sec = window_min * 60

    today_from = now_sec
    today_to = now_sec + window_sec

    y_from = now_sec + 86400
    y_to = now_sec + 86400 + window_sec

    active_today = set()
    active_yesterday = set()

    def is_active(service_id: str, which: str) -> bool:
        if which == "today":
            if service_id not in active_today and _service_active_on(service_id, today):
                active_today.add(service_id)
            return service_id in active_today
        else:
            if service_id not in active_yesterday and _service_active_on(service_id, yesterday):
                active_yesterday.add(service_id)
            return service_id in active_yesterday

    rows = STOP_TIMES_BY_STOP.get(stop_id, [])
    out = []

    for dep_sec, trip_id in rows:
        trip = TRIPS.get(trip_id)
        if not trip:
            continue

        if today_from <= dep_sec <= today_to:
            if is_active(trip.service_id, "today"):
                route = ROUTES.get(trip.route_id)
                out.append(
                    {
                        "time": _seconds_to_hhmm(dep_sec),
                        "dep_sec": dep_sec,
                        "route": (route.short_name if route else "") or "",
                        "route_name": (route.long_name if route else "") or "",
                        "headsign": trip.headsign or "",
                        "trip_id": trip.trip_id,
                        "service_day": "today",
                    }
                )

        if y_from <= dep_sec <= y_to:
            if is_active(trip.service_id, "yesterday"):
                route = ROUTES.get(trip.route_id)
                out.append(
                    {
                        "time": _seconds_to_hhmm(dep_sec - 86400),
                        "dep_sec": dep_sec,
                        "route": (route.short_name if route else "") or "",
                        "route_name": (route.long_name if route else "") or "",
                        "headsign": trip.headsign or "",
                        "trip_id": trip.trip_id,
                        "service_day": "yesterday",
                    }
                )

    def sort_key(x):
        return x["dep_sec"] if x["service_day"] == "today" else (x["dep_sec"] - 86400)

    out.sort(key=sort_key)
    out = out[:max_results]

    return {
        "stop": {"stop_id": s.stop_id, "stop_name": s.stop_name, "lat": s.lat, "lon": s.lon},
        "now": now_dt.isoformat(),
        "window_min": window_min,
        "departures": out,
    }


@app.get("/api/vehicles")
def api_vehicles(line: str = "", max_results: int = 200):
    max_results = max(1, min(max_results, 500))
    qline = _norm(line)

    vehicles = fetch_live_vehicles()
    if qline:
        vehicles = [v for v in vehicles if qline in _norm(str(v.get("line") or ""))]

    vehicles = vehicles[:max_results]
    return {"count": len(vehicles), "vehicles": vehicles, "cached_ttl_sec": LIVE_CACHE_TTL_SEC}


# -----------------------------
# Serve frontend
# -----------------------------
def _pick_index_file() -> str:
    # Prefer root index.html, fallback to templates/index.html
    root_idx = os.path.join(BASE_DIR, "index.html")
    tpl_idx = os.path.join(BASE_DIR, "templates", "index.html")
    if os.path.exists(root_idx):
        return root_idx
    if os.path.exists(tpl_idx):
        return tpl_idx
    return root_idx  # will 404

@app.get("/")
def home():
    idx = _pick_index_file()
    if not os.path.exists(idx):
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(idx)

# Optional: serve /static if you use it
static_dir = os.path.join(BASE_DIR, "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
