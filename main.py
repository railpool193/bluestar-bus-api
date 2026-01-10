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
        # handle ...Z
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
    # e.g. "PT180S", "PT2M", "PT1H2M3S"
    if not s or not s.startswith("PT"):
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
    return total


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
CAL_MIN_START: Optional[str] = None
CAL_MAX_END: Optional[str] = None

DATA_LOADED_AT: Optional[float] = None


def load_gtfs() -> None:
    global STOPS, ROUTES, TRIPS, STOP_TIMES_BY_STOP
    global CALENDAR, CAL_ADDED, CAL_REMOVED, CAL_MIN_START, CAL_MAX_END, DATA_LOADED_AT

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
    # If GTFS calendar range exists and today is beyond it, treat as stale
    if CAL_MAX_END and today_str > CAL_MAX_END:
        return True
    return False


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


# -----------------------------
# SIRI feed parsing + cache
# -----------------------------
_siri_cache: Dict[str, Any] = {"ts": 0.0, "vehicles": [], "calls": []}


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


def fetch_siri() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (vehicles, monitored_calls)
    monitored_calls entries:
      stop_id, line, destination, aimed_dep_iso, expected_dep_iso, delta_min, vehicle_id, recorded_at
    """
    now = time.time()
    if (now - _siri_cache["ts"]) < LIVE_CACHE_TTL_SEC:
        return _siri_cache["vehicles"], _siri_cache["calls"]

    try:
        r = requests.get(DFT_FEED_URL, timeout=25)
        r.raise_for_status()
        content = r.content
    except Exception:
        _siri_cache["ts"] = now
        _siri_cache["vehicles"] = []
        _siri_cache["calls"] = []
        return [], []

    vehicles: List[Dict[str, Any]] = []
    calls: List[Dict[str, Any]] = []

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

            # vehicles list (map)
            if lat and lon:
                try:
                    vehicles.append(
                        {
                            "vehicle_id": vehicle_ref,
                            "line": line_ref,
                            "destination": dest,
                            "lat": float(lat),
                            "lon": float(lon),
                            "recorded_at": recorded,
                        }
                    )
                except Exception:
                    pass

            # monitored call (stop predictions)
            mcall = _xml_find_first(mvj, "MonitoredCall")
            if mcall is not None:
                stop_ref = _xml_findtext_suffix(mcall, "StopPointRef")
                aimed_dep = _xml_findtext_suffix(mcall, "AimedDepartureTime")
                exp_dep = _xml_findtext_suffix(mcall, "ExpectedDepartureTime")

                # If Expected missing but Delay exists, compute it
                delay = _xml_findtext_suffix(mcall, "DepartureProximityText")  # not reliable, just in case
                delay_dur = _xml_findtext_suffix(mcall, "Delay")
                delay_sec = _parse_iso8601_duration_seconds(delay_dur)

                delta_min = None
                if aimed_dep and exp_dep:
                    adt = _parse_iso_dt(aimed_dep)
                    edt = _parse_iso_dt(exp_dep)
                    if adt and edt:
                        delta_min = int(round((edt - adt).total_seconds() / 60.0))
                elif aimed_dep and delay_sec is not None:
                    adt = _parse_iso_dt(aimed_dep)
                    if adt:
                        edt = adt + timedelta(seconds=delay_sec)
                        exp_dep = edt.astimezone(adt.tzinfo).isoformat()
                        delta_min = int(round(delay_sec / 60.0))

                if stop_ref and (aimed_dep or exp_dep):
                    calls.append(
                        {
                            "stop_id": stop_ref,
                            "line": line_ref,
                            "destination": dest,
                            "aimed_dep": aimed_dep,
                            "expected_dep": exp_dep,
                            "delta_min": delta_min,
                            "vehicle_id": vehicle_ref,
                            "recorded_at": recorded,
                        }
                    )

    except Exception:
        vehicles, calls = [], []

    _siri_cache["ts"] = now
    _siri_cache["vehicles"] = vehicles
    _siri_cache["calls"] = calls
    return vehicles, calls


def live_calls_for_stop(stop: Stop) -> List[Dict[str, Any]]:
    _, calls = fetch_siri()
    keys = {stop.stop_id}
    if stop.stop_code:
        keys.add(stop.stop_code)
    out = [c for c in calls if (c.get("stop_id") in keys)]
    return out


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
def api_vehicles(line: str = "", max_results: int = 200):
    max_results = max(1, min(max_results, 500))
    qline = _norm(line)
    vehicles, _ = fetch_siri()
    if qline:
        vehicles = [v for v in vehicles if qline in _norm(str(v.get("line") or ""))]
    vehicles = vehicles[:max_results]
    return {"count": len(vehicles), "vehicles": vehicles, "cached_ttl_sec": LIVE_CACHE_TTL_SEC}


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
    today = now_dt.date()
    yesterday = today - timedelta(days=1)
    today_str = _yyyymmdd(today)

    ignore_calendar = _calendar_likely_stale(today_str)
    calendar_ignored = ignore_calendar

    now_sec = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
    window_sec = window_min * 60

    today_from = now_sec
    today_to = now_sec + window_sec

    y_from = now_sec + 86400
    y_to = now_sec + 86400 + window_sec

    rows = STOP_TIMES_BY_STOP.get(stop_id, [])

    sched = []
    for dep_sec, trip_id in rows:
        trip = TRIPS.get(trip_id)
        if not trip:
            continue

        # Choose service day based on dep_sec (can exceed 86400)
        if today_from <= dep_sec <= today_to:
            active = _service_active_on(trip.service_id, today, ignore_calendar)
            if not active:
                continue
            hhmm = _seconds_to_hhmm(dep_sec)
            dep_dt = now_dt.replace(hour=int(hhmm[:2]), minute=int(hhmm[3:]), second=0, microsecond=0)
            route = ROUTES.get(trip.route_id)
            sched.append(
                {
                    "source": "timetable",
                    "route": (route.short_name if route else "") or "",
                    "route_name": (route.long_name if route else "") or "",
                    "headsign": trip.headsign or "",
                    "aimed_time": hhmm,
                    "expected_time": None,
                    "delta_min": None,
                    "mins_to": int(round((dep_dt - now_dt).total_seconds() / 60.0)),
                    "vehicle_id": None,
                    "recorded_at": None,
                }
            )

        if y_from <= dep_sec <= y_to:
            active = _service_active_on(trip.service_id, yesterday, ignore_calendar)
            if not active:
                continue
            hhmm = _seconds_to_hhmm(dep_sec - 86400)
            # this departure is after midnight relative to now
            dep_dt = (now_dt + timedelta(days=1)).replace(hour=int(hhmm[:2]), minute=int(hhmm[3:]), second=0, microsecond=0)
            route = ROUTES.get(trip.route_id)
            sched.append(
                {
                    "source": "timetable",
                    "route": (route.short_name if route else "") or "",
                    "route_name": (route.long_name if route else "") or "",
                    "headsign": trip.headsign or "",
                    "aimed_time": hhmm,
                    "expected_time": None,
                    "delta_min": None,
                    "mins_to": int(round((dep_dt - now_dt).total_seconds() / 60.0)),
                    "vehicle_id": None,
                    "recorded_at": None,
                }
            )

    # If calendar is not stale but still zero results, and stop has times, auto-ignore calendar (common when GTFS range wrong)
    if (not ignore_calendar) and (len(sched) == 0) and (len(rows) > 0):
        calendar_ignored = True
        # rebuild without calendar filtering
        sched = []
        for dep_sec, trip_id in rows:
            trip = TRIPS.get(trip_id)
            if not trip:
                continue
            if today_from <= dep_sec <= today_to:
                hhmm = _seconds_to_hhmm(dep_sec)
                dep_dt = now_dt.replace(hour=int(hhmm[:2]), minute=int(hhmm[3:]), second=0, microsecond=0)
                route = ROUTES.get(trip.route_id)
                sched.append(
                    {
                        "source": "timetable",
                        "route": (route.short_name if route else "") or "",
                        "route_name": (route.long_name if route else "") or "",
                        "headsign": trip.headsign or "",
                        "aimed_time": hhmm,
                        "expected_time": None,
                        "delta_min": None,
                        "mins_to": int(round((dep_dt - now_dt).total_seconds() / 60.0)),
                        "vehicle_id": None,
                        "recorded_at": None,
                    }
                )
            if y_from <= dep_sec <= y_to:
                hhmm = _seconds_to_hhmm(dep_sec - 86400)
                dep_dt = (now_dt + timedelta(days=1)).replace(hour=int(hhmm[:2]), minute=int(hhmm[3:]), second=0, microsecond=0)
                route = ROUTES.get(trip.route_id)
                sched.append(
                    {
                        "source": "timetable",
                        "route": (route.short_name if route else "") or "",
                        "route_name": (route.long_name if route else "") or "",
                        "headsign": trip.headsign or "",
                        "aimed_time": hhmm,
                        "expected_time": None,
                        "delta_min": None,
                        "mins_to": int(round((dep_dt - now_dt).total_seconds() / 60.0)),
                        "vehicle_id": None,
                        "recorded_at": None,
                    }
                )

    # Merge LIVE into schedule (by same route and near aimed time)
    if include_live:
        live = live_calls_for_stop(s)
        # Build candidate list with parsed datetimes
        live_parsed = []
        for c in live:
            aimed_dt = _parse_iso_dt(c.get("aimed_dep"))
            exp_dt = _parse_iso_dt(c.get("expected_dep")) or aimed_dt
            if not exp_dt:
                continue
            live_parsed.append(
                {
                    "route": (c.get("line") or "") or "",
                    "destination": (c.get("destination") or "") or "",
                    "aimed_dt": aimed_dt,
                    "exp_dt": exp_dt,
                    "delta_min": c.get("delta_min"),
                    "vehicle_id": c.get("vehicle_id"),
                    "recorded_at": c.get("recorded_at"),
                }
            )

        used = set()
        # try match: same route, within 20 minutes of scheduled aimed time
        for lp in live_parsed:
            best_i = None
            best_diff = 99999
            for i, d in enumerate(sched):
                if i in used:
                    continue
                if _norm(d.get("route", "")) != _norm(lp["route"]):
                    continue
                # compare against aimed time if we have aimed_dt
                if lp["aimed_dt"] is None:
                    continue
                # build sched aimed dt
                try:
                    hhmm = d["aimed_time"]
                    sdt = now_dt.replace(hour=int(hhmm[:2]), minute=int(hhmm[3:]), second=0, microsecond=0)
                    # if mins_to was next-day, shift
                    if d["mins_to"] is not None and d["mins_to"] > 720 and hhmm < _seconds_to_hhmm(now_sec):
                        sdt = sdt + timedelta(days=1)
                except Exception:
                    continue
                diff = abs((sdt - lp["aimed_dt"]).total_seconds() / 60.0)
                if diff <= 20 and diff < best_diff:
                    best_diff = diff
                    best_i = i

            if best_i is not None:
                used.add(best_i)
                d = sched[best_i]
                exp_dt = lp["exp_dt"]
                mins_to = int(round((exp_dt - now_dt).total_seconds() / 60.0))
                d.update(
                    {
                        "source": "live",
                        "headsign": lp["destination"] or d.get("headsign") or "",
                        "expected_time": exp_dt.strftime("%H:%M"),
                        "delta_min": lp["delta_min"],
                        "mins_to": mins_to,
                        "vehicle_id": lp["vehicle_id"],
                        "recorded_at": lp["recorded_at"],
                    }
                )

        # add unmatched live calls as extra rows
        for lp in live_parsed:
            # if route+expected already present, skip duplicates
            exp_hhmm = lp["exp_dt"].strftime("%H:%M")
            dup = any((_norm(x.get("route", "")) == _norm(lp["route"]) and x.get("expected_time") == exp_hhmm) for x in sched)
            if dup:
                continue
            mins_to = int(round((lp["exp_dt"] - now_dt).total_seconds() / 60.0))
            if mins_to < -2 or mins_to > window_min:
                continue
            sched.append(
                {
                    "source": "live",
                    "route": lp["route"],
                    "route_name": "",
                    "headsign": lp["destination"] or "",
                    "aimed_time": lp["aimed_dt"].strftime("%H:%M") if lp["aimed_dt"] else None,
                    "expected_time": exp_hhmm,
                    "delta_min": lp["delta_min"],
                    "mins_to": mins_to,
                    "vehicle_id": lp["vehicle_id"],
                    "recorded_at": lp["recorded_at"],
                }
            )

    # compute status + display time
    out = []
    for d in sched:
        mins_to = d.get("mins_to")
        source = d.get("source")
        delta = d.get("delta_min")
        expected = d.get("expected_time") or d.get("aimed_time")

        status = "timetable"
        if source == "live":
            status = "on_time"
            if delta is not None:
                if delta >= 1:
                    status = "late"
                elif delta <= -1:
                    status = "early"

        # Due logic (blink)
        display_time = expected
        if mins_to is not None and mins_to <= 1 and mins_to >= -1:
            display_time = "Due"
            status = "due"

        out.append(
            {
                "time": display_time,
                "aimed_time": d.get("aimed_time"),
                "expected_time": d.get("expected_time"),
                "mins_to": mins_to,
                "source": source,
                "status": status,
                "delta_min": delta,
                "route": d.get("route") or "",
                "headsign": d.get("headsign") or "",
                "vehicle_id": d.get("vehicle_id"),
                "recorded_at": d.get("recorded_at"),
            }
        )

    # sort by mins_to (null last)
    out.sort(key=lambda x: (999999 if x.get("mins_to") is None else x["mins_to"]))
    out = out[:max_results]

    return {
        "stop": {"stop_id": s.stop_id, "stop_name": s.stop_name, "lat": s.lat, "lon": s.lon},
        "now": now_dt.isoformat(),
        "window_min": window_min,
        "calendar_ignored": calendar_ignored,
        "departures": out,
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
