import csv
import os
import time
import math
import unicodedata
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional, Set, Any

import requests
from flask import Flask, jsonify, request, send_from_directory


# -----------------------------
# Config
# -----------------------------
APP_TZ = ZoneInfo(os.getenv("APP_TZ", "Europe/London"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GTFS_DIR = os.getenv("GTFS_DIR", os.path.join(BASE_DIR, "gtfs"))

# Keep API key out of the frontend; backend proxies the feed.
DFT_API_KEY = os.getenv("DFT_API_KEY", "9d2f6818e2723996467fedb958ba682aa9860a93")
DFT_FEED_URL = os.getenv(
    "DFT_FEED_URL",
    f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={DFT_API_KEY}",
)

# live vehicle cache (avoid hammering the feed)
LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "12"))

app = Flask(__name__)


# -----------------------------
# Helpers
# -----------------------------
def _norm(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    return " ".join(s.split())


def _parse_gtfs_time_to_seconds(t: str) -> Optional[int]:
    """
    GTFS time can exceed 24:00:00 (e.g. 25:10:00).
    Returns seconds since 00:00 of service day.
    """
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


# -----------------------------
# GTFS Data Structures
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


# In-memory stores
STOPS: Dict[str, Stop] = {}
ROUTES: Dict[str, Route] = {}
TRIPS: Dict[str, Trip] = {}
STOP_TIMES_BY_STOP: Dict[str, List[Tuple[int, str]]] = {}  # stop_id -> [(dep_sec, trip_id), ...] sorted

CALENDAR: Dict[str, Dict[str, Any]] = {}  # service_id -> calendar row
CAL_ADDED: Dict[str, Set[str]] = {}       # service_id -> {yyyymmdd}
CAL_REMOVED: Dict[str, Set[str]] = {}     # service_id -> {yyyymmdd}

DATA_LOADED_AT: Optional[float] = None


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


def load_gtfs() -> None:
    global STOPS, ROUTES, TRIPS, STOP_TIMES_BY_STOP, CALENDAR, CAL_ADDED, CAL_REMOVED, DATA_LOADED_AT

    # Stops
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

    # Routes
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

    # Trips
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

    # Stop times index
    STOP_TIMES_BY_STOP = {}
    if os.path.exists(stop_times_path):
        for row in _read_csv(stop_times_path):
            stop_id = (row.get("stop_id") or "").strip()
            trip_id = (row.get("trip_id") or "").strip()
            dep = _parse_gtfs_time_to_seconds(row.get("departure_time", "") or "")
            if not stop_id or not trip_id or dep is None:
                continue
            STOP_TIMES_BY_STOP.setdefault(stop_id, []).append((dep, trip_id))

        # Sort each stop's list by departure time
        for sid in STOP_TIMES_BY_STOP:
            STOP_TIMES_BY_STOP[sid].sort(key=lambda x: x[0])

    # Calendar
    CALENDAR = {}
    CAL_ADDED = {}
    CAL_REMOVED = {}

    if os.path.exists(calendar_path):
        for row in _read_csv(calendar_path):
            sid = (row.get("service_id") or "").strip()
            if not sid:
                continue
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

    # Exceptions override
    if ds in CAL_REMOVED.get(service_id, set()):
        return False
    if ds in CAL_ADDED.get(service_id, set()):
        return True

    cal = CALENDAR.get(service_id)
    if not cal:
        # No calendar row -> only active if explicitly added in calendar_dates
        return False

    start = (cal.get("start_date") or "").strip()
    end = (cal.get("end_date") or "").strip()
    if start and ds < start:
        return False
    if end and ds > end:
        return False

    weekday = d.weekday()  # 0=Mon
    keys = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    k = keys[weekday]
    return (cal.get(k) or "0").strip() == "1"


# -----------------------------
# Live vehicles (SIRI-VM parsing)
# -----------------------------
_live_cache: Dict[str, Any] = {"ts": 0.0, "data": []}


def _xml_findtext_suffix(elem, suffix: str) -> Optional[str]:
    # find first child element whose tag endswith suffix, anywhere under elem
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
    except Exception as e:
        _live_cache["ts"] = now
        _live_cache["data"] = []
        return []

    vehicles: List[Dict[str, Any]] = []
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(content)

        # SIRI VehicleMonitoring -> VehicleActivity
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
# API
# -----------------------------
@app.get("/api/health")
def api_health():
    return jsonify(
        {
            "ok": True,
            "loaded_at": DATA_LOADED_AT,
            "stops": len(STOPS),
            "routes": len(ROUTES),
            "trips": len(TRIPS),
        }
    )


@app.get("/api/stops")
def api_stops():
    q = _norm(request.args.get("q", ""))
    limit = int(request.args.get("limit", "10"))

    if not q:
        return jsonify([])

    results = []
    for s in STOPS.values():
        # allow searching by stop_id/stop_code too
        hay = s.norm_name
        code = (s.stop_code or "").lower()
        sid = s.stop_id.lower()

        score = None
        if hay.startswith(q) or any(word.startswith(q) for word in hay.split()):
            score = 0
        elif q in hay:
            score = 1
        elif q == sid or (q and q in sid):
            score = 2
        elif q and code and q in code:
            score = 2

        if score is None:
            continue

        results.append((score, len(s.stop_name), s))

    results.sort(key=lambda x: (x[0], x[1]))

    out = []
    for _, __, s in results[: max(1, min(limit, 50))]:
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
    return jsonify(out)


@app.get("/api/stop/<stop_id>")
def api_stop(stop_id: str):
    s = STOPS.get(stop_id)
    if not s:
        return jsonify({"error": "stop_not_found"}), 404
    return jsonify(
        {
            "stop_id": s.stop_id,
            "stop_name": s.stop_name,
            "lat": s.lat,
            "lon": s.lon,
            "stop_code": s.stop_code,
            "parent_station": s.parent_station,
        }
    )


@app.get("/api/nearby")
def api_nearby():
    try:
        lat = float(request.args.get("lat", ""))
        lon = float(request.args.get("lon", ""))
    except Exception:
        return jsonify({"error": "invalid_lat_lon"}), 400

    radius_m = float(request.args.get("radius_m", "600"))
    limit = int(request.args.get("limit", "15"))

    candidates = []
    for s in STOPS.values():
        if s.lat is None or s.lon is None:
            continue
        d = _haversine_m(lat, lon, s.lat, s.lon)
        if d <= radius_m:
            candidates.append((d, s))

    candidates.sort(key=lambda x: x[0])
    out = []
    for d, s in candidates[: max(1, min(limit, 50))]:
        out.append(
            {
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "lat": s.lat,
                "lon": s.lon,
                "distance_m": round(d, 1),
            }
        )
    return jsonify(out)


@app.get("/api/departures")
def api_departures():
    stop_id = request.args.get("stop_id", "").strip()
    if not stop_id:
        return jsonify({"error": "missing_stop_id"}), 400

    window_min = int(request.args.get("window_min", "90"))
    max_results = int(request.args.get("max", "40"))

    s = STOPS.get(stop_id)
    if not s:
        return jsonify({"error": "stop_not_found"}), 404

    now_dt = datetime.now(APP_TZ)
    today = now_dt.date()
    yesterday = today - timedelta(days=1)

    now_sec = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
    window_sec = max(10, min(window_min, 240)) * 60  # clamp 10..240min

    today_from = now_sec
    today_to = now_sec + window_sec

    y_from = now_sec + 86400
    y_to = now_sec + 86400 + window_sec

    active_today = set()
    active_yesterday = set()

    # We’ll lazily compute service activeness only for services we see.
    def is_active(service_id: str, which: str) -> bool:
        if which == "today":
            if service_id not in active_today:
                if _service_active_on(service_id, today):
                    active_today.add(service_id)
                else:
                    # mark as checked via negative cache? simple skip
                    pass
            return service_id in active_today
        else:
            if service_id not in active_yesterday:
                if _service_active_on(service_id, yesterday):
                    active_yesterday.add(service_id)
                else:
                    pass
            return service_id in active_yesterday

    rows = STOP_TIMES_BY_STOP.get(stop_id, [])
    out = []

    # Because rows are sorted by dep_sec, we can early-break for today's window.
    # For yesterday window (dep_sec > 86400), we can't guarantee ordering vs <86400,
    # but still ok to scan—MVP.
    for dep_sec, trip_id in rows:
        trip = TRIPS.get(trip_id)
        if not trip:
            continue

        # Today window
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

        # Yesterday late-night trips encoded as 24:xx etc
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

        # small optimization: if dep_sec exceeds today_to and is still < 86400,
        # we can break (since sorted).
        if dep_sec < 86400 and dep_sec > today_to:
            # still need to continue in case there are >86400 times later in list,
            # but those usually come after; keep scanning lightly.
            pass

    # Sort by the actual "arrival moment" relative to now:
    # today dep_sec -> +0 day, yesterday encoded dep_sec -> (dep_sec - 86400) next-day time
    def sort_key(x):
        if x["service_day"] == "today":
            return x["dep_sec"]
        return x["dep_sec"] - 86400  # display time is next-day clock time

    out.sort(key=sort_key)
    out = out[: max(1, min(max_results, 80))]

    return jsonify(
        {
            "stop": {
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "lat": s.lat,
                "lon": s.lon,
            },
            "now": now_dt.isoformat(),
            "window_min": window_min,
            "departures": out,
        }
    )


@app.get("/api/vehicles")
def api_vehicles():
    qline = _norm(request.args.get("line", ""))
    max_results = int(request.args.get("max", "200"))

    vehicles = fetch_live_vehicles()

    if qline:
        filtered = []
        for v in vehicles:
            line = _norm(str(v.get("line") or ""))
            if qline in line:
                filtered.append(v)
        vehicles = filtered

    vehicles = vehicles[: max(1, min(max_results, 500))]
    return jsonify(
        {
            "count": len(vehicles),
            "vehicles": vehicles,
            "cached_ttl_sec": LIVE_CACHE_TTL_SEC,
        }
    )


# -----------------------------
# Frontend: serve index.html from repo root
# -----------------------------
@app.get("/")
def home():
    return send_from_directory(BASE_DIR, "index.html")


@app.get("/<path:path>")
def static_passthrough(path: str):
    # allow serving favicon etc if you add them later
    return send_from_directory(BASE_DIR, path)


# -----------------------------
# Boot
# -----------------------------
try:
    load_gtfs()
except Exception as e:
    # Start anyway so /api/health shows what's wrong
    print("GTFS load error:", e)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
