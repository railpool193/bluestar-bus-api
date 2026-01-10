import csv
import os
import time
import math
import unicodedata
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Set, Any

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


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

app = FastAPI(title="Stop Departures (GTFS + SIRI-VM)")


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


def _safe_int(x: str, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return default
        return int(x)
    except Exception:
        return default


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


def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if APP_TZ:
            return dt.astimezone(APP_TZ)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _parse_iso8601_duration_seconds(s: Optional[str]) -> Optional[int]:
    # "PT1M30S", "PT180S", "-PT2M" (néha mínusz is előfordul)
    if not s:
        return None
    neg = False
    s = s.strip()
    if s.startswith("-"):
        neg = True
        s = s[1:]
    if not s.startswith("PT"):
        return None
    s = s[2:]
    num = ""
    total = 0
    for ch in s:
        if ch.isdigit():
            num += ch
            continue
        if not num:
            return None
        val = int(num)
        num = ""
        if ch == "H":
            total += val * 3600
        elif ch == "M":
            total += val * 60
        elif ch == "S":
            total += val
        else:
            return None
    return -total if neg else total


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
    shape_id: Optional[str]


# In-memory stores
STOPS: Dict[str, Stop] = {}
ROUTES: Dict[str, Route] = {}
TRIPS: Dict[str, Trip] = {}

# For stop departures: only real "departures" (pickup allowed, not terminal stop)
STOP_DEPS_BY_STOP: Dict[str, List[Tuple[int, str, int]]] = {}  # stop_id -> [(dep_sec, trip_id, stop_seq), ...]

# For trip view:
TRIP_STOP_TIMES: Dict[str, List[Dict[str, Any]]] = {}  # trip_id -> ordered rows
TRIP_LAST_SEQ: Dict[str, int] = {}

CALENDAR: Dict[str, Dict[str, Any]] = {}
CAL_ADDED: Dict[str, Set[str]] = {}
CAL_REMOVED: Dict[str, Set[str]] = {}
CAL_MIN_START: Optional[str] = None
CAL_MAX_END: Optional[str] = None

DATA_LOADED_AT: Optional[float] = None


def load_gtfs() -> None:
    global STOPS, ROUTES, TRIPS
    global STOP_DEPS_BY_STOP, TRIP_STOP_TIMES, TRIP_LAST_SEQ
    global CALENDAR, CAL_ADDED, CAL_REMOVED, CAL_MIN_START, CAL_MAX_END, DATA_LOADED_AT

    stops_path = os.path.join(GTFS_DIR, "stops.txt")
    routes_path = os.path.join(GTFS_DIR, "routes.txt")
    trips_path = os.path.join(GTFS_DIR, "trips.txt")
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")
    calendar_path = os.path.join(GTFS_DIR, "calendar.txt")
    calendar_dates_path = os.path.join(GTFS_DIR, "calendar_dates.txt")

    if not os.path.exists(stops_path):
        raise FileNotFoundError(f"Missing stops.txt in {GTFS_DIR}")

    # Stops
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

    # Trips (shape_id included)
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
                shape_id=(row.get("shape_id") or "").strip() or None,
            )

    # Stop times -> build TRIP_STOP_TIMES and STOP_DEPS_BY_STOP with filters
    TRIP_STOP_TIMES = {}
    TRIP_LAST_SEQ = {}

    if os.path.exists(stop_times_path):
        rows_tmp = []
        for row in _read_csv(stop_times_path):
            trip_id = (row.get("trip_id") or "").strip()
            stop_id = (row.get("stop_id") or "").strip()
            if not trip_id or not stop_id:
                continue

            stop_seq = _safe_int(row.get("stop_sequence"), default=-1)
            if stop_seq < 0:
                continue

            arr_sec = _parse_gtfs_time_to_seconds(row.get("arrival_time") or "")
            dep_sec = _parse_gtfs_time_to_seconds(row.get("departure_time") or "")

            pickup_type = _safe_int(row.get("pickup_type"), default=0)
            drop_off_type = _safe_int(row.get("drop_off_type"), default=0)

            rows_tmp.append((trip_id, stop_seq, {
                "trip_id": trip_id,
                "stop_id": stop_id,
                "stop_sequence": stop_seq,
                "arrival_sec": arr_sec,
                "departure_sec": dep_sec,
                "pickup_type": pickup_type,
                "drop_off_type": drop_off_type,
            }))

            # track last seq
            cur = TRIP_LAST_SEQ.get(trip_id)
            TRIP_LAST_SEQ[trip_id] = stop_seq if (cur is None or stop_seq > cur) else cur

        # build per-trip ordered list
        rows_tmp.sort(key=lambda x: (x[0], x[1]))
        for trip_id, _, item in rows_tmp:
            TRIP_STOP_TIMES.setdefault(trip_id, []).append(item)

    # build STOP_DEPS_BY_STOP (only real departures)
    STOP_DEPS_BY_STOP = {}
    for trip_id, st_list in TRIP_STOP_TIMES.items():
        last_seq = TRIP_LAST_SEQ.get(trip_id, 10**9)
        for st in st_list:
            stop_id = st["stop_id"]
            stop_seq = st["stop_sequence"]
            dep_sec = st["departure_sec"]

            # 1) must have departure time
            if dep_sec is None:
                continue
            # 2) pickup allowed (pickup_type 1 = no pickup)
            if st.get("pickup_type", 0) == 1:
                continue
            # 3) must NOT be terminal stop for this trip
            if stop_seq >= last_seq:
                continue

            STOP_DEPS_BY_STOP.setdefault(stop_id, []).append((dep_sec, trip_id, stop_seq))

    for sid in STOP_DEPS_BY_STOP:
        STOP_DEPS_BY_STOP[sid].sort(key=lambda x: x[0])

    # Calendar
    CALENDAR = {}
    CAL_ADDED = {}
    CAL_REMOVED = {}
    CAL_MIN_START = None
    CAL_MAX_END = None

    if os.path.exists(calendar_path):
        for row in _read_csv(calendar_path):
            sid = (row.get("service_id") or "").strip()
            if not sid:
                continue
            CALENDAR[sid] = row
            s = (row.get("start_date") or "").strip()
            e = (row.get("end_date") or "").strip()
            if s:
                CAL_MIN_START = s if (CAL_MIN_START is None or s < CAL_MIN_START) else CAL_MIN_START
            if e:
                CAL_MAX_END = e if (CAL_MAX_END is None or e > CAL_MAX_END) else CAL_MAX_END

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


def _calendar_likely_stale(today_str: str) -> bool:
    return bool(CAL_MAX_END and today_str > CAL_MAX_END)


def _service_active_on(service_id: str, d: date, ignore_calendar: bool) -> bool:
    if ignore_calendar:
        return True
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

    weekday = d.weekday()
    keys = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    return (cal.get(keys[weekday]) or "0").strip() == "1"


def _dt_from_service_date_and_seconds(service_date: date, sec: int) -> datetime:
    # GTFS times may exceed 24h; sec can be > 86400 -> rolls into next day
    base = datetime(service_date.year, service_date.month, service_date.day, tzinfo=APP_TZ) if APP_TZ else datetime(service_date.year, service_date.month, service_date.day)
    return base + timedelta(seconds=sec)


# -----------------------------
# SIRI feed parsing + cache
# -----------------------------
_siri_cache: Dict[str, Any] = {"ts": 0.0, "journeys": []}


def _xml_findtext_suffix(elem, suffix: str) -> Optional[str]:
    for child in elem.iter():
        if isinstance(child.tag, str) and child.tag.endswith(suffix):
            if child.text:
                return child.text.strip()
    return None


def _xml_find_first(elem, suffix: str):
    for child in elem.iter():
        if isinstance(child.tag, str) and child.tag.endswith(suffix):
            return child
    return None


def fetch_siri_journeys() -> List[Dict[str, Any]]:
    """
    VehicleMonitoring best-effort:
    returns journeys with:
      vehicle_id (fleet), line, destination, lat, lon, recorded_at,
      delay_sec (if exists), dated_vehicle_journey_ref (if exists)
    """
    now = time.time()
    if (now - _siri_cache["ts"]) < LIVE_CACHE_TTL_SEC:
        return _siri_cache["journeys"]

    journeys: List[Dict[str, Any]] = []
    try:
        r = requests.get(DFT_FEED_URL, timeout=25)
        r.raise_for_status()
        content = r.content
    except Exception:
        _siri_cache["ts"] = now
        _siri_cache["journeys"] = []
        return []

    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(content)

        for va in root.iter():
            if not (isinstance(va.tag, str) and va.tag.endswith("VehicleActivity")):
                continue

            recorded = _xml_findtext_suffix(va, "RecordedAtTime") or _xml_findtext_suffix(va, "ValidUntilTime")

            mvj = _xml_find_first(va, "MonitoredVehicleJourney")
            if mvj is None:
                continue

            lat = _xml_findtext_suffix(mvj, "Latitude")
            lon = _xml_findtext_suffix(mvj, "Longitude")
            line_ref = _xml_findtext_suffix(mvj, "LineRef") or _xml_findtext_suffix(mvj, "PublishedLineName")
            vehicle_ref = _xml_findtext_suffix(mvj, "VehicleRef")
            dest = _xml_findtext_suffix(mvj, "DestinationName")
            delay_dur = _xml_findtext_suffix(mvj, "Delay")
            delay_sec = _parse_iso8601_duration_seconds(delay_dur)

            # try to get DatedVehicleJourneyRef
            dated_vj = None
            framed = _xml_find_first(mvj, "FramedVehicleJourneyRef")
            if framed is not None:
                dated_vj = _xml_findtext_suffix(framed, "DatedVehicleJourneyRef")

            # also try VehicleJourneyRef / JourneyRef fallback
            vjref = _xml_findtext_suffix(mvj, "VehicleJourneyRef") or _xml_findtext_suffix(mvj, "JourneyRef")

            item = {
                "vehicle_id": vehicle_ref,
                "line": line_ref,
                "destination": dest,
                "lat": float(lat) if lat else None,
                "lon": float(lon) if lon else None,
                "recorded_at": recorded,
                "delay_sec": delay_sec,
                "dated_vehicle_journey_ref": dated_vj,
                "vehicle_journey_ref": vjref,
            }
            journeys.append(item)

    except Exception:
        journeys = []

    _siri_cache["ts"] = now
    _siri_cache["journeys"] = journeys
    return journeys


def _best_live_match_for_trip(trip: Trip, service_date: date) -> Optional[Dict[str, Any]]:
    """
    Best-effort: exact match by dated_vehicle_journey_ref == trip_id (or vehicle_journey_ref == trip_id),
    otherwise heuristic by line + destination.
    """
    journeys = fetch_siri_journeys()
    if not journeys:
        return None

    # exact id match
    for j in journeys:
        if j.get("dated_vehicle_journey_ref") == trip.trip_id or j.get("vehicle_journey_ref") == trip.trip_id:
            j2 = dict(j)
            j2["match"] = "trip_id"
            j2["score"] = 100
            return j2

    # heuristic: line match + destination similarity
    route = ROUTES.get(trip.route_id)
    line = route.short_name if route else ""
    head = _norm(trip.headsign)

    # use last stop name as extra hint
    last_stop_name = ""
    st_list = TRIP_STOP_TIMES.get(trip.trip_id, [])
    if st_list:
        last_stop_id = st_list[-1]["stop_id"]
        last_stop_name = _norm(STOPS.get(last_stop_id).stop_name if last_stop_id in STOPS else "")

    best = None
    best_score = -1
    for j in journeys:
        jline = _norm(str(j.get("line") or ""))
        jdest = _norm(str(j.get("destination") or ""))

        score = 0
        if line and _norm(line) == jline:
            score += 50
        if head and head in jdest:
            score += 25
        if last_stop_name and last_stop_name and last_stop_name in jdest:
            score += 25

        if score <= 0:
            continue

        # prefer those that actually have delay
        if j.get("delay_sec") is not None:
            score += 10

        if score > best_score:
            best_score = score
            best = dict(j)

    if best:
        best["match"] = "heuristic"
        best["score"] = best_score
    return best


# -----------------------------
# Startup
# -----------------------------
@app.on_event("startup")
def _startup():
    try:
        load_gtfs()
    except Exception as e:
        print("GTFS load error:", e)


# -----------------------------
# API
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
        "calendar_min_start": CAL_MIN_START,
        "calendar_max_end": CAL_MAX_END,
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
        if hay.startswith(qn) or any(w.startswith(qn) for w in hay.split()):
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


@app.get("/api/vehicles")
def api_vehicles(line: str = "", max_results: int = 250):
    max_results = max(1, min(max_results, 500))
    qline = _norm(line)
    journeys = fetch_siri_journeys()
    if qline:
        journeys = [v for v in journeys if qline in _norm(str(v.get("line") or ""))]
    journeys = journeys[:max_results]
    return {"count": len(journeys), "vehicles": journeys, "cached_ttl_sec": LIVE_CACHE_TTL_SEC}


@app.get("/api/departures")
def api_departures(
    stop_id: str,
    window_min: int = 90,
    max_results: int = 40,
    include_live: int = 1,
):
    if stop_id not in STOPS:
        raise HTTPException(status_code=404, detail="stop_not_found")

    window_min = max(10, min(window_min, 240))
    max_results = max(1, min(max_results, 80))

    s = STOPS[stop_id]
    now_dt = _now_dt()
    service_date = now_dt.date()
    today_str = _yyyymmdd(service_date)

    ignore_calendar = _calendar_likely_stale(today_str)
    calendar_ignored = ignore_calendar

    now_sec = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
    window_sec = window_min * 60
    to_sec = now_sec + window_sec

    deps = STOP_DEPS_BY_STOP.get(stop_id, [])
    out = []

    # Build scheduled departures (already filtered: not terminal, pickup allowed)
    for dep_sec, trip_id, stop_seq in deps:
        if dep_sec < now_sec - 60:
            continue
        if dep_sec > to_sec:
            continue

        trip = TRIPS.get(trip_id)
        if not trip:
            continue

        if not _service_active_on(trip.service_id, service_date, ignore_calendar):
            continue

        route = ROUTES.get(trip.route_id)
        sched_dt = _dt_from_service_date_and_seconds(service_date, dep_sec)
        mins_to = int(round((sched_dt - now_dt).total_seconds() / 60.0))

        out.append(
            {
                "trip_id": trip_id,
                "service_date": service_date.isoformat(),
                "stop_sequence": stop_seq,
                "route": (route.short_name if route else "") or "",
                "headsign": trip.headsign or "",
                "aimed_time": sched_dt.strftime("%H:%M"),
                "expected_time": None,
                "delta_min": None,
                "mins_to": mins_to,
                "source": "timetable",
                "vehicle_id": None,
                "recorded_at": None,
                "status": "timetable",
            }
        )

    # If calendar not stale but we got nothing although data exists, auto ignore calendar
    if (not ignore_calendar) and len(out) == 0 and len(deps) > 0:
        calendar_ignored = True
        for dep_sec, trip_id, stop_seq in deps:
            if dep_sec < now_sec - 60:
                continue
            if dep_sec > to_sec:
                continue
            trip = TRIPS.get(trip_id)
            if not trip:
                continue
            route = ROUTES.get(trip.route_id)
            sched_dt = _dt_from_service_date_and_seconds(service_date, dep_sec)
            mins_to = int(round((sched_dt - now_dt).total_seconds() / 60.0))
            out.append(
                {
                    "trip_id": trip_id,
                    "service_date": service_date.isoformat(),
                    "stop_sequence": stop_seq,
                    "route": (route.short_name if route else "") or "",
                    "headsign": trip.headsign or "",
                    "aimed_time": sched_dt.strftime("%H:%M"),
                    "expected_time": None,
                    "delta_min": None,
                    "mins_to": mins_to,
                    "source": "timetable",
                    "vehicle_id": None,
                    "recorded_at": None,
                    "status": "timetable",
                }
            )

    # Add "live" (delay-based) if we can match the trip to a vehicle journey
    if include_live and out:
        for d in out:
            trip = TRIPS.get(d["trip_id"])
            if not trip:
                continue

            route = ROUTES.get(trip.route_id)
            sched_hhmm = d["aimed_time"]
            # reconstruct scheduled dt
            # (OK for the next ~240 minutes)
            sched_dt = now_dt.replace(hour=int(sched_hhmm[:2]), minute=int(sched_hhmm[3:]), second=0, microsecond=0)
            if sched_dt < now_dt - timedelta(hours=2):
                sched_dt = sched_dt + timedelta(days=1)

            live = _best_live_match_for_trip(trip, service_date)
            if not live:
                continue

            delay_sec = live.get("delay_sec")
            if delay_sec is None:
                continue

            expected_dt = sched_dt + timedelta(seconds=delay_sec)
            delta_min = int(round(delay_sec / 60.0))
            mins_to = int(round((expected_dt - now_dt).total_seconds() / 60.0))

            # status coloring
            status = "on_time"
            if mins_to <= 1 and mins_to >= -1:
                status = "due"
            elif delta_min >= 1:
                status = "late"
            elif delta_min <= -1:
                status = "early"

            d.update(
                {
                    "source": "live",
                    "expected_time": expected_dt.strftime("%H:%M"),
                    "delta_min": delta_min,
                    "mins_to": mins_to,
                    "vehicle_id": live.get("vehicle_id"),
                    "recorded_at": live.get("recorded_at"),
                    "status": status,
                    "live_match": live.get("match"),
                }
            )

    # If still timetable, apply Due based on mins_to
    for d in out:
        if d["source"] != "live":
            if d["mins_to"] <= 1 and d["mins_to"] >= -1:
                d["status"] = "due"

    out.sort(key=lambda x: (999999 if x.get("mins_to") is None else x["mins_to"]))
    out = out[:max_results]

    return {
        "stop": {"stop_id": s.stop_id, "stop_name": s.stop_name, "lat": s.lat, "lon": s.lon},
        "now": now_dt.isoformat(),
        "window_min": window_min,
        "calendar_ignored": calendar_ignored,
        "departures": out,
    }


@app.get("/api/trip")
def api_trip(trip_id: str, service_date: str = ""):
    trip = TRIPS.get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="trip_not_found")

    now_dt = _now_dt()
    if service_date:
        try:
            y, m, d = [int(x) for x in service_date.split("-")]
            svc_date = date(y, m, d)
        except Exception:
            raise HTTPException(status_code=400, detail="bad_service_date")
    else:
        svc_date = now_dt.date()

    st_list = TRIP_STOP_TIMES.get(trip_id, [])
    if not st_list:
        raise HTTPException(status_code=404, detail="trip_stop_times_missing")

    route = ROUTES.get(trip.route_id)
    live = _best_live_match_for_trip(trip, svc_date)

    delay_sec = live.get("delay_sec") if live else None
    delay_min = int(round(delay_sec / 60.0)) if delay_sec is not None else None

    # compute rows
    rows = []
    next_idx = None
    for idx, st in enumerate(st_list):
        stop_id = st["stop_id"]
        stop_name = STOPS.get(stop_id).stop_name if stop_id in STOPS else stop_id

        # scheduled time: use departure if exists else arrival
        sec = st["departure_sec"] if st["departure_sec"] is not None else st["arrival_sec"]
        if sec is None:
            continue

        sched_dt = _dt_from_service_date_and_seconds(svc_date, sec)
        expected_dt = (sched_dt + timedelta(seconds=delay_sec)) if delay_sec is not None else None
        mins_to = int(round((expected_dt - now_dt).total_seconds() / 60.0)) if expected_dt else int(round((sched_dt - now_dt).total_seconds() / 60.0))

        status = "timetable"
        if expected_dt and delay_min is not None:
            status = "on_time"
            if mins_to <= 1 and mins_to >= -1:
                status = "due"
            elif delay_min >= 1:
                status = "late"
            elif delay_min <= -1:
                status = "early"

        if next_idx is None and mins_to >= 0:
            next_idx = idx

        rows.append(
            {
                "idx": idx,
                "stop_id": stop_id,
                "stop_name": stop_name,
                "sched_time": sched_dt.strftime("%H:%M"),
                "expected_time": expected_dt.strftime("%H:%M") if expected_dt else None,
                "mins_to": mins_to,
                "delta_min": delay_min,
                "status": status,
            }
        )

    return {
        "trip": {
            "trip_id": trip.trip_id,
            "route": (route.short_name if route else "") or "",
            "headsign": trip.headsign or "",
            "shape_id": trip.shape_id,
        },
        "service_date": svc_date.isoformat(),
        "now": now_dt.isoformat(),
        "live": live,
        "delay_min": delay_min,
        "next_index": next_idx,
        "stops": rows,
    }


# -----------------------------
# Serve frontend
# -----------------------------
def _pick_index_file() -> str:
    root_idx = os.path.join(BASE_DIR, "index.html")
    tpl_idx = os.path.join(BASE_DIR, "templates", "index.html")
    if os.path.exists(root_idx):
        return root_idx
    if os.path.exists(tpl_idx):
        return tpl_idx
    return root_idx


@app.get("/")
def home():
    idx = _pick_index_file()
    if not os.path.exists(idx):
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(idx)


static_dir = os.path.join(BASE_DIR, "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
