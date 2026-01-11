import csv
import io
import os
import time
import math
import zipfile
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET


APP_TZ = ZoneInfo(os.getenv("APP_TZ", "Europe/London"))

GTFS_ZIP_PATH = os.getenv("GTFS_ZIP_PATH", "gtfs.zip")

DFT_API_KEY = os.getenv("DFT_API_KEY", "").strip()
DFT_VM_URL = os.getenv("DFT_VM_URL", "https://data.bus-data.dft.gov.uk/api/v1/datafeed/").strip()
DFT_OPERATOR_REF = os.getenv("DFT_OPERATOR_REF", "BLUS").strip()
DFT_VM_BBOX = os.getenv("DFT_VM_BBOX", "").strip()

LIVE_TTL_SEC = int(os.getenv("LIVE_TTL_SEC", "12"))

# Matching tolerances
# >>> emelve, hogy a feed/GTFS start-idők ne essenek ki pár perc miatt
ORIGIN_MATCH_TOL_SEC = int(os.getenv("ORIGIN_MATCH_TOL_SEC", "600"))  # 10 min
TRIP_ACTIVE_PAD_MIN = int(os.getenv("TRIP_ACTIVE_PAD_MIN", "30"))

# Heuristic distance parameters
NEAR_STOP_METERS = float(os.getenv("NEAR_STOP_METERS", "120"))
MIN_SEG_METERS = float(os.getenv("MIN_SEG_METERS", "10"))

DEFAULT_WINDOW_MIN = int(os.getenv("DEFAULT_WINDOW_MIN", "60"))
MAX_WINDOW_MIN = int(os.getenv("MAX_WINDOW_MIN", "240"))

IGNORE_CALENDAR_IF_OUTSIDE_RANGE = os.getenv("IGNORE_CALENDAR_IF_OUTSIDE_RANGE", "1") == "1"


def now_local() -> datetime:
    return datetime.now(tz=APP_TZ)


def norm_route(s: str) -> str:
    """
    Normalize route short names / LineRef for matching:
    - trim
    - uppercase
    - remove spaces
    Examples: "19a" -> "19A", " QC " -> "QC"
    """
    if s is None:
        return ""
    return "".join(str(s).strip().upper().split())


def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def fmt_hhmm(dt: datetime) -> str:
    return dt.astimezone(APP_TZ).strftime("%H:%M")


def strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def parse_gtfs_time_to_seconds(hhmmss: str) -> int:
    parts = hhmmss.strip().split(":")
    if len(parts) != 3:
        raise ValueError(f"Bad GTFS time: {hhmmss}")
    h = int(parts[0])
    m = int(parts[1])
    s = int(parts[2])
    return h * 3600 + m * 60 + s


def seconds_to_service_dt(service_date: date, sec: int) -> datetime:
    base = datetime(service_date.year, service_date.month, service_date.day, tzinfo=APP_TZ)
    return base + timedelta(seconds=sec)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _to_xy_m(lat: float, lon: float, lat0: float) -> Tuple[float, float]:
    R = 6371000.0
    x = math.radians(lon) * R * math.cos(math.radians(lat0))
    y = math.radians(lat) * R
    return x, y


def point_segment_distance_fraction_m(
    plat: float, plon: float,
    alat: float, alon: float,
    blat: float, blon: float
) -> Tuple[float, float]:
    lat0 = (alat + blat) / 2.0
    px, py = _to_xy_m(plat, plon, lat0)
    ax, ay = _to_xy_m(alat, alon, lat0)
    bx, by = _to_xy_m(blat, blon, lat0)

    abx, aby = bx - ax, by - ay
    apx, apy = px - ax, py - ay
    ab2 = abx * abx + aby * aby
    if ab2 < 1e-9:
        dx = px - ax
        dy = py - ay
        return math.hypot(dx, dy), 0.0

    t = (apx * abx + apy * aby) / ab2
    t_clamped = max(0.0, min(1.0, t))
    cx = ax + t_clamped * abx
    cy = ay + t_clamped * aby
    dist = math.hypot(px - cx, py - cy)
    return dist, t_clamped


@dataclass
class Stop:
    stop_id: str
    stop_name: str
    lat: float
    lon: float


@dataclass
class Route:
    route_id: str
    short_name: str
    long_name: str


@dataclass
class Trip:
    trip_id: str
    route_id: str
    service_id: str
    headsign: str
    direction_id: Optional[str]
    trip_short_name: str


@dataclass
class StopTime:
    trip_id: str
    stop_id: str
    arrival_sec: int
    departure_sec: int
    stop_sequence: int


@dataclass
class CalendarSvc:
    service_id: str
    start_date: date
    end_date: date
    weekdays: Dict[int, bool]  # 0=Mon .. 6=Sun


class GTFS:
    def __init__(self) -> None:
        self.stops: Dict[str, Stop] = {}
        self.routes: Dict[str, Route] = {}
        self.trips: Dict[str, Trip] = {}
        self.stop_times_by_stop: Dict[str, List[StopTime]] = {}
        self.stop_times_by_trip: Dict[str, List[StopTime]] = {}
        self.trip_max_seq: Dict[str, int] = {}
        self.calendar: Dict[str, CalendarSvc] = {}
        self.calendar_dates_add: Dict[Tuple[str, date], bool] = {}
        self.calendar_dates_remove: Dict[Tuple[str, date], bool] = {}

        self.route_id_to_short: Dict[str, str] = {}
        self.route_short_to_route_ids: Dict[str, List[str]] = {}
        self.trip_first_last: Dict[str, Tuple[int, int]] = {}

    def load(self, zip_path: str) -> None:
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"GTFS zip not found: {zip_path}")

        with zipfile.ZipFile(zip_path, "r") as z:
            def read_csv(name: str) -> List[Dict[str, str]]:
                try:
                    raw = z.read(name)
                except KeyError:
                    return []
                text = raw.decode("utf-8-sig", errors="replace")
                f = io.StringIO(text)
                reader = csv.DictReader(f)
                return [row for row in reader]

            stops_rows = read_csv("stops.txt")
            for r in stops_rows:
                sid = (r.get("stop_id") or "").strip()
                if not sid:
                    continue
                name = (r.get("stop_name") or "").strip()
                try:
                    lat = float(r.get("stop_lat") or "0")
                    lon = float(r.get("stop_lon") or "0")
                except ValueError:
                    continue
                self.stops[sid] = Stop(sid, name, lat, lon)

            routes_rows = read_csv("routes.txt")
            for r in routes_rows:
                rid = (r.get("route_id") or "").strip()
                if not rid:
                    continue
                short = (r.get("route_short_name") or "").strip()
                longn = (r.get("route_long_name") or "").strip()
                self.routes[rid] = Route(rid, short, longn)
                self.route_id_to_short[rid] = short
                self.route_short_to_route_ids.setdefault(short, []).append(rid)

            trips_rows = read_csv("trips.txt")
            for r in trips_rows:
                tid = (r.get("trip_id") or "").strip()
                if not tid:
                    continue
                rid = (r.get("route_id") or "").strip()
                sid = (r.get("service_id") or "").strip()
                headsign = (r.get("trip_headsign") or "").strip()
                direction = (r.get("direction_id") or "").strip() or None
                tshort = (r.get("trip_short_name") or "").strip()
                self.trips[tid] = Trip(tid, rid, sid, headsign, direction, tshort)

            cal_rows = read_csv("calendar.txt")
            for r in cal_rows:
                sid = (r.get("service_id") or "").strip()
                if not sid:
                    continue
                try:
                    start = parse_yyyymmdd((r.get("start_date") or "").strip())
                    end = parse_yyyymmdd((r.get("end_date") or "").strip())
                except Exception:
                    continue
                weekdays = {}
                keys = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                for i, k in enumerate(keys):
                    weekdays[i] = (r.get(k) or "0").strip() == "1"
                self.calendar[sid] = CalendarSvc(sid, start, end, weekdays)

            cald_rows = read_csv("calendar_dates.txt")
            for r in cald_rows:
                sid = (r.get("service_id") or "").strip()
                ds = (r.get("date") or "").strip()
                et = (r.get("exception_type") or "").strip()
                if not sid or not ds or not et:
                    continue
                try:
                    d = parse_yyyymmdd(ds)
                except Exception:
                    continue
                if et == "1":
                    self.calendar_dates_add[(sid, d)] = True
                elif et == "2":
                    self.calendar_dates_remove[(sid, d)] = True

            st_rows = read_csv("stop_times.txt")
            for r in st_rows:
                tid = (r.get("trip_id") or "").strip()
                stop_id = (r.get("stop_id") or "").strip()
                if not tid or not stop_id:
                    continue
                try:
                    arr = parse_gtfs_time_to_seconds((r.get("arrival_time") or "00:00:00").strip())
                    dep = parse_gtfs_time_to_seconds((r.get("departure_time") or "00:00:00").strip())
                    seq = int((r.get("stop_sequence") or "0").strip())
                except Exception:
                    continue
                st = StopTime(tid, stop_id, arr, dep, seq)
                self.stop_times_by_stop.setdefault(stop_id, []).append(st)
                self.stop_times_by_trip.setdefault(tid, []).append(st)

            for stop_id, lst in self.stop_times_by_stop.items():
                lst.sort(key=lambda x: (x.departure_sec, x.stop_sequence))

            for tid, lst in self.stop_times_by_trip.items():
                lst.sort(key=lambda x: x.stop_sequence)
                if lst:
                    self.trip_max_seq[tid] = max(x.stop_sequence for x in lst)
                    self.trip_first_last[tid] = (lst[0].departure_sec, lst[-1].arrival_sec)

            print(f"[GTFS] stops={len(self.stops)} routes={len(self.routes)} trips={len(self.trips)} stop_times(trips)={len(self.stop_times_by_trip)}")

    def is_service_active(self, service_id: str, d: date) -> bool:
        if self.calendar_dates_remove.get((service_id, d)):
            return False
        if self.calendar_dates_add.get((service_id, d)):
            return True
        svc = self.calendar.get(service_id)
        if not svc:
            return True
        if d < svc.start_date or d > svc.end_date:
            return False
        wd = d.weekday()
        return bool(svc.weekdays.get(wd, False))


GTFS_DATA = GTFS()


class LiveCache:
    def __init__(self) -> None:
        self._last_fetch = 0.0
        self._data: Optional[List[Dict[str, Any]]] = None

    def get(self) -> Optional[List[Dict[str, Any]]]:
        if self._data is None:
            return None
        if time.time() - self._last_fetch > LIVE_TTL_SEC:
            return None
        return self._data

    def set(self, data: List[Dict[str, Any]]) -> None:
        self._data = data
        self._last_fetch = time.time()

    @property
    def last_fetch(self) -> float:
        return self._last_fetch


LIVE_CACHE = LiveCache()


async def fetch_vm_xml() -> str:
    if not DFT_API_KEY:
        raise HTTPException(status_code=400, detail="Missing DFT_API_KEY env var")

    params = {"api_key": DFT_API_KEY}
    if DFT_OPERATOR_REF:
        params["operatorRef"] = DFT_OPERATOR_REF
    if DFT_VM_BBOX:
        params["boundingBox"] = DFT_VM_BBOX

    url = DFT_VM_URL
    timeout = httpx.Timeout(25.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.text


def find_desc_text(node: ET.Element, path_locals: List[str]) -> Optional[str]:
    cur = node
    for local in path_locals:
        found = None
        for ch in list(cur):
            if strip_ns(ch.tag) == local:
                found = ch
                break
        if found is None:
            return None
        cur = found
    if cur.text is None:
        return None
    return cur.text.strip()


def parse_dt_iso(s: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc).astimezone(APP_TZ)
        return dt.astimezone(APP_TZ)
    except Exception:
        return None


def parse_vehicle_activities(xml_text: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_text)

    vehicles: List[Dict[str, Any]] = []

    stack = [root]
    acts: List[ET.Element] = []
    while stack:
        n = stack.pop()
        if strip_ns(n.tag) == "VehicleActivity":
            acts.append(n)
        stack.extend(list(n))

    for act in acts:
        recorded_at = find_desc_text(act, ["RecordedAtTime"])

        mvj = None
        for ch in list(act):
            if strip_ns(ch.tag) == "MonitoredVehicleJourney":
                mvj = ch
                break
        if mvj is None:
            continue

        line = find_desc_text(mvj, ["LineRef"]) or find_desc_text(mvj, ["PublishedLineName"]) or ""
        dest = find_desc_text(mvj, ["DestinationName"]) or ""
        direction = find_desc_text(mvj, ["DirectionRef"]) or ""

        lat_txt = find_desc_text(mvj, ["VehicleLocation", "Latitude"])
        lon_txt = find_desc_text(mvj, ["VehicleLocation", "Longitude"])
        try:
            lat = float(lat_txt) if lat_txt else None
            lon = float(lon_txt) if lon_txt else None
        except Exception:
            lat, lon = None, None

        vehicle_ref = find_desc_text(mvj, ["VehicleRef"]) or None

        framed = None
        for ch in list(mvj):
            if strip_ns(ch.tag) == "FramedVehicleJourneyRef":
                framed = ch
                break
        data_frame_ref = find_desc_text(framed, ["DataFrameRef"]) if framed is not None else None
        dated_vj_ref = find_desc_text(framed, ["DatedVehicleJourneyRef"]) if framed is not None else None
        vj_ref = find_desc_text(mvj, ["VehicleJourneyRef"]) or None

        origin_aimed = find_desc_text(mvj, ["OriginAimedDepartureTime"])
        dest_aimed = find_desc_text(mvj, ["DestinationAimedArrivalTime"])

        # Extensions/JourneyCode
        journey_code = None
        stack2 = [mvj]
        while stack2:
            nn = stack2.pop()
            if strip_ns(nn.tag) == "JourneyCode" and (nn.text or "").strip():
                journey_code = (nn.text or "").strip()
                break
            stack2.extend(list(nn))

        line_clean = (line or "").strip()
        dest_clean = (dest or "").strip().replace("_", " ")

        vehicles.append({
            "vehicle_id": vehicle_ref,
            "line": line_clean,
            "line_norm": norm_route(line_clean),
            "destination": dest_clean,
            "direction": direction,
            "lat": lat,
            "lon": lon,
            "recorded_at": recorded_at,
            "data_frame_ref": data_frame_ref,
            "dated_vehicle_journey_ref": dated_vj_ref,
            "vehicle_journey_ref": vj_ref,
            "origin_aimed_departure": origin_aimed,
            "destination_aimed_arrival": dest_aimed,
            "journey_code": journey_code,
        })

    return vehicles


def get_or_guess_service_dates_for_now(n: datetime) -> List[date]:
    d0 = n.date()
    if n.hour < 3:
        return [d0 - timedelta(days=1), d0]
    return [d0]


def determine_calendar_ignored(d: date) -> bool:
    if not GTFS_DATA.calendar:
        return False
    active_any = False
    for tid, t in GTFS_DATA.trips.items():
        if GTFS_DATA.is_service_active(t.service_id, d):
            active_any = True
            break
    return not active_any


def build_trip_index_for_date(service_date: date) -> Dict[str, List[Dict[str, Any]]]:
    """
    Key: normalized route (norm_route(route_short_name))
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    for tid, t in GTFS_DATA.trips.items():
        if not GTFS_DATA.trip_first_last.get(tid):
            continue

        first_sec, last_sec = GTFS_DATA.trip_first_last[tid]
        first_dt = seconds_to_service_dt(service_date, first_sec)
        last_dt = seconds_to_service_dt(service_date, last_sec)

        route_short = GTFS_DATA.route_id_to_short.get(t.route_id, "")
        route_norm = norm_route(route_short)
        if not route_norm:
            continue

        out.setdefault(route_norm, []).append({
            "trip_id": tid,
            "route_display": route_short,
            "route_norm": route_norm,
            "headsign": t.headsign,
            "first_dt": first_dt,
            "last_dt": last_dt,
            "trip_short_name": t.trip_short_name,
        })

    for k in out:
        out[k].sort(key=lambda x: x["first_dt"])
    return out


def best_match_trip_for_vehicle(
    v: Dict[str, Any],
    trip_index: Dict[str, List[Dict[str, Any]]]
) -> Optional[Dict[str, Any]]:
    route_norm = norm_route(v.get("line") or "")
    if not route_norm or route_norm not in trip_index:
        return None

    odt = parse_dt_iso(v.get("origin_aimed_departure") or "")
    dat = parse_dt_iso(v.get("destination_aimed_arrival") or "")

    # Ha nincs origin aimed time, fallback: dest aimed time vs last stop
    if odt is None and dat is None:
        return None

    candidates = trip_index.get(route_norm, [])
    best = None
    best_score = 1e18

    dest = (v.get("destination") or "").strip().lower().replace("_", " ")

    for t in candidates:
        first_dt: datetime = t["first_dt"]
        last_dt: datetime = t["last_dt"]

        if odt is not None:
            delta = abs((first_dt - odt).total_seconds())
        else:
            delta = abs((last_dt - dat).total_seconds())  # type: ignore

        if delta > ORIGIN_MATCH_TOL_SEC:
            continue

        head = (t.get("headsign") or "").strip().lower()
        score = delta
        if dest and head:
            if dest in head or head in dest:
                score *= 0.5

        if score < best_score:
            best_score = score
            best = t

    return best


def compute_trip_delay_and_expected_times(
    trip_id: str,
    service_date: date,
    vehicle_lat: float,
    vehicle_lon: float,
    n: datetime
) -> Tuple[Optional[int], Dict[str, datetime], Optional[int]]:
    sts = GTFS_DATA.stop_times_by_trip.get(trip_id) or []
    if len(sts) < 2:
        return None, {}, None

    coords: List[Tuple[float, float]] = []
    sched_dts: List[datetime] = []
    seqs: List[int] = []
    stop_ids: List[str] = []

    for st in sts:
        s = GTFS_DATA.stops.get(st.stop_id)
        if not s:
            continue
        coords.append((s.lat, s.lon))
        sched_dts.append(seconds_to_service_dt(service_date, st.departure_sec))
        seqs.append(st.stop_sequence)
        stop_ids.append(st.stop_id)

    if len(coords) < 2:
        return None, {}, None

    nearest_i = 0
    nearest_d = 1e18
    for i, (slat, slon) in enumerate(coords):
        d = haversine_m(vehicle_lat, vehicle_lon, slat, slon)
        if d < nearest_d:
            nearest_d = d
            nearest_i = i

    if nearest_d <= NEAR_STOP_METERS:
        seg_i = min(nearest_i, len(coords) - 2)
        frac = 0.0
    else:
        best_seg = 0
        best_dist = 1e18
        best_frac = 0.0
        for i in range(len(coords) - 1):
            (alat, alon) = coords[i]
            (blat, blon) = coords[i + 1]
            dist, frac2 = point_segment_distance_fraction_m(
                vehicle_lat, vehicle_lon, alat, alon, blat, blon
            )
            if dist < best_dist:
                best_dist = dist
                best_seg = i
                best_frac = frac2
        seg_i = best_seg
        frac = best_frac

    tA = sched_dts[seg_i]
    tB = sched_dts[seg_i + 1]
    if (tB - tA).total_seconds() <= 1:
        sched_at_progress = tA
    else:
        sched_at_progress = tA + (tB - tA) * frac

    first_dt = sched_dts[0]
    last_dt = sched_dts[-1]
    pad = timedelta(minutes=TRIP_ACTIVE_PAD_MIN)
    if not (first_dt - pad <= n <= last_dt + pad):
        return None, {}, None

    delay_sec = int((n - sched_at_progress).total_seconds())

    expected: Dict[str, datetime] = {}
    for sid, sdt in zip(stop_ids, sched_dts):
        expected[sid] = sdt + timedelta(seconds=delay_sec)

    next_i = min(seg_i + 1, len(seqs) - 1)
    next_seq = seqs[next_i] if 0 <= next_i < len(seqs) else None

    return delay_sec, expected, next_seq


async def get_live_vehicles() -> List[Dict[str, Any]]:
    cached = LIVE_CACHE.get()
    if cached is not None:
        return cached

    xml_text = await fetch_vm_xml()
    vehicles = parse_vehicle_activities(xml_text)
    LIVE_CACHE.set(vehicles)
    return vehicles


def sanitize_int(q: Any, default: int) -> int:
    if q is None:
        return default
    if isinstance(q, int):
        return q
    s = str(q).strip()
    s = s.rstrip("/").strip()
    try:
        return int(s)
    except Exception:
        return default


app = FastAPI(title="Bluestar/Unilink Stop & Trip API", version="2.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup() -> None:
    GTFS_DATA.load(GTFS_ZIP_PATH)


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Missing index.html</h1>"


@app.get("/api/health")
def api_health() -> Dict[str, Any]:
    return {
        "ok": True,
        "time": now_local().isoformat(),
        "stops": len(GTFS_DATA.stops),
        "trips": len(GTFS_DATA.trips),
        "dft_key": bool(DFT_API_KEY),
        "live_cache_last_fetch_epoch": LIVE_CACHE.last_fetch,
    }


# >>> Alias, hogy ne legyen 404 ha /health-et nyitsz
@app.get("/health")
def health_alias() -> Dict[str, Any]:
    return api_health()


@app.get("/api/stops")
def stops(q: str = Query("", min_length=0), limit: int = 25) -> Dict[str, Any]:
    qn = (q or "").strip().lower()
    limit = max(1, min(100, limit))
    res = []
    for s in GTFS_DATA.stops.values():
        if not qn or qn in s.stop_name.lower() or qn in s.stop_id.lower():
            res.append({"stop_id": s.stop_id, "stop_name": s.stop_name})
    res.sort(key=lambda x: x["stop_name"])
    return {"count": len(res[:limit]), "stops": res[:limit]}


@app.get("/api/nearby")
def nearby(lat: float, lon: float, radius_m: int = 700, limit: int = 25) -> Dict[str, Any]:
    radius_m = max(50, min(5000, radius_m))
    limit = max(1, min(50, limit))
    scored = []
    for s in GTFS_DATA.stops.values():
        d = haversine_m(lat, lon, s.lat, s.lon)
        if d <= radius_m:
            scored.append((d, s))
    scored.sort(key=lambda x: x[0])
    out = [{"stop_id": s.stop_id, "stop_name": s.stop_name, "dist_m": int(d)} for d, s in scored[:limit]]
    return {"count": len(out), "stops": out}


@app.get("/api/vehicles")
async def vehicles(max_results: Any = Query(10)) -> Dict[str, Any]:
    mr = sanitize_int(max_results, 10)
    mr = max(1, min(200, mr))

    if not DFT_API_KEY:
        # Frontend így is tud üzenni, nem kell crash
        return {"count": 0, "vehicles": [], "cached_ttl_sec": LIVE_TTL_SEC, "error": "Missing DFT_API_KEY"}

    v = await get_live_vehicles()

    def keyfun(x: Dict[str, Any]) -> float:
        ra = x.get("recorded_at") or ""
        dt = parse_dt_iso(ra) if ra else None
        return dt.timestamp() if dt else 0.0

    v2 = sorted(v, key=keyfun, reverse=True)[:mr]
    return {"count": len(v2), "vehicles": v2, "cached_ttl_sec": LIVE_TTL_SEC}


@app.get("/api/departures")
async def departures(
    stop_id: str,
    minutes: int = DEFAULT_WINDOW_MIN,
    limit: int = 30
) -> Dict[str, Any]:
    minutes = max(10, min(MAX_WINDOW_MIN, minutes))
    limit = max(1, min(80, limit))

    s = GTFS_DATA.stops.get(stop_id)
    if not s:
        raise HTTPException(status_code=404, detail="stop_id not found")

    n = now_local()
    service_dates = get_or_guess_service_dates_for_now(n)

    st_list = GTFS_DATA.stop_times_by_stop.get(stop_id) or []
    if not st_list:
        return {"stop": {"stop_id": s.stop_id, "stop_name": s.stop_name, "lat": s.lat, "lon": s.lon}, "count": 0, "departures": []}

    calendar_ignored = False
    if IGNORE_CALENDAR_IF_OUTSIDE_RANGE:
        calendar_ignored = determine_calendar_ignored(service_dates[-1])

    live_vehicles: List[Dict[str, Any]] = []
    live_by_trip: Dict[Tuple[str, date], Dict[str, Any]] = {}
    expected_by_trip_stop: Dict[Tuple[str, date, str], datetime] = {}
    delay_by_trip: Dict[Tuple[str, date], int] = {}

    if DFT_API_KEY:
        try:
            live_vehicles = await get_live_vehicles()
        except Exception:
            live_vehicles = []

    trip_index_by_date: Dict[date, Dict[str, List[Dict[str, Any]]]] = {}
    for d in service_dates:
        trip_index_by_date[d] = build_trip_index_for_date(d)

    for v in live_vehicles:
        df = (v.get("data_frame_ref") or "").strip()
        vdate = None
        try:
            if df:
                vdate = datetime.fromisoformat(df).date()
        except Exception:
            vdate = None

        if vdate is None:
            ra = parse_dt_iso(v.get("recorded_at") or "")
            vdate = ra.date() if ra else None

        if vdate is None:
            continue
        idx = trip_index_by_date.get(vdate)
        if not idx:
            continue

        match = best_match_trip_for_vehicle(v, idx)
        if not match:
            continue

        tid = match["trip_id"]
        key = (tid, vdate)

        prev = live_by_trip.get(key)
        if prev is None:
            live_by_trip[key] = v
        else:
            pdt = parse_dt_iso(prev.get("recorded_at") or "") or datetime.min.replace(tzinfo=APP_TZ)
            ndt = parse_dt_iso(v.get("recorded_at") or "") or datetime.min.replace(tzinfo=APP_TZ)
            if ndt > pdt:
                live_by_trip[key] = v

    for (tid, d), v in live_by_trip.items():
        if d not in service_dates:
            continue
        lat = v.get("lat")
        lon = v.get("lon")
        if lat is None or lon is None:
            continue
        delay_sec, expected_map, _ = compute_trip_delay_and_expected_times(
            trip_id=tid,
            service_date=d,
            vehicle_lat=float(lat),
            vehicle_lon=float(lon),
            n=n
        )
        if delay_sec is None:
            continue
        delay_by_trip[(tid, d)] = delay_sec
        for sid2, edt in expected_map.items():
            expected_by_trip_stop[(tid, d, sid2)] = edt

    window_end = n + timedelta(minutes=minutes)
    window_start = n - timedelta(minutes=5)

    dep_rows = []
    for d in service_dates:
        for st in st_list:
            t = GTFS_DATA.trips.get(st.trip_id)
            if not t:
                continue

            if not calendar_ignored:
                if not GTFS_DATA.is_service_active(t.service_id, d):
                    continue

            # Termináló sorok kiszűrése
            max_seq = GTFS_DATA.trip_max_seq.get(st.trip_id, -1)
            if max_seq != -1 and st.stop_sequence >= max_seq:
                continue

            dep_dt = seconds_to_service_dt(d, st.departure_sec)
            if dep_dt < window_start or dep_dt > window_end:
                continue

            route_short = GTFS_DATA.route_id_to_short.get(t.route_id, "")
            headsign = t.headsign or ""

            expected_dt = expected_by_trip_stop.get((st.trip_id, d, stop_id))
            delay_sec = delay_by_trip.get((st.trip_id, d))
            live_delay_min = None
            if expected_dt is not None:
                live_delay_min = int(round((expected_dt - dep_dt).total_seconds() / 60.0))

            minutes_to = int(round(((expected_dt or dep_dt) - n).total_seconds() / 60.0))

            dep_rows.append({
                "service_date": d.isoformat(),
                "trip_id": st.trip_id,
                "route": route_short,
                "destination": headsign,
                "scheduled_dt": dep_dt.isoformat(),
                "scheduled_hhmm": fmt_hhmm(dep_dt),
                "minutes_to": minutes_to,
                "live": {
                    "has_live": expected_dt is not None,
                    "expected_dt": expected_dt.isoformat() if expected_dt else None,
                    "delay_min": live_delay_min,
                    "delay_sec": delay_sec,
                    "fleet": (live_by_trip.get((st.trip_id, d)) or {}).get("vehicle_id"),
                    "source": "vm_heuristic" if expected_dt else None,
                }
            })

    dep_rows.sort(key=lambda x: x["scheduled_dt"])
    dep_rows = dep_rows[:limit]

    return {
        "stop": {"stop_id": s.stop_id, "stop_name": s.stop_name, "lat": s.lat, "lon": s.lon},
        "now": n.isoformat(),
        "calendar_ignored": calendar_ignored,
        "count": len(dep_rows),
        "departures": dep_rows,
    }


@app.get("/api/trip")
async def trip(trip_id: str, service_date: str) -> Dict[str, Any]:
    if trip_id not in GTFS_DATA.trips:
        raise HTTPException(status_code=404, detail="trip_id not found")
    try:
        d = date.fromisoformat(service_date)
    except Exception:
        raise HTTPException(status_code=400, detail="Bad service_date (expected YYYY-MM-DD)")

    t = GTFS_DATA.trips[trip_id]
    route_short = GTFS_DATA.route_id_to_short.get(t.route_id, "")
    n = now_local()

    sts = GTFS_DATA.stop_times_by_trip.get(trip_id) or []
    if not sts:
        return {"trip_id": trip_id, "service_date": service_date, "stops": []}

    live_vehicle = None
    expected_map: Dict[str, datetime] = {}
    delay_sec = None
    next_seq = None

    if DFT_API_KEY:
        try:
            live_vehicles = await get_live_vehicles()
        except Exception:
            live_vehicles = []

        idx = build_trip_index_for_date(d)

        best_dt = None
        for v in live_vehicles:
            df = (v.get("data_frame_ref") or "").strip()
            vdate = None
            try:
                if df:
                    vdate = datetime.fromisoformat(df).date()
            except Exception:
                vdate = None
            if vdate != d:
                continue

            match = best_match_trip_for_vehicle(v, idx)
            if not match or match["trip_id"] != trip_id:
                continue

            vdt = parse_dt_iso(v.get("recorded_at") or "") or datetime.min.replace(tzinfo=APP_TZ)
            if best_dt is None or vdt > best_dt:
                best_dt = vdt
                live_vehicle = v

        if live_vehicle and live_vehicle.get("lat") is not None and live_vehicle.get("lon") is not None:
            delay_sec, expected_map, next_seq = compute_trip_delay_and_expected_times(
                trip_id=trip_id,
                service_date=d,
                vehicle_lat=float(live_vehicle["lat"]),
                vehicle_lon=float(live_vehicle["lon"]),
                n=n
            )

    stops_out = []
    for st in sts:
        s = GTFS_DATA.stops.get(st.stop_id)
        if not s:
            continue
        sched_dt = seconds_to_service_dt(d, st.departure_sec)
        exp_dt = expected_map.get(st.stop_id) if delay_sec is not None else None
        live_minutes_to = int(round(((exp_dt or sched_dt) - n).total_seconds() / 60.0))
        live_delay_min = None
        if exp_dt is not None:
            live_delay_min = int(round((exp_dt - sched_dt).total_seconds() / 60.0))

        stops_out.append({
            "stop_sequence": st.stop_sequence,
            "stop_id": s.stop_id,
            "stop_name": s.stop_name,
            "lat": s.lat,
            "lon": s.lon,
            "scheduled_dt": sched_dt.isoformat(),
            "scheduled_hhmm": fmt_hhmm(sched_dt),
            "live": {
                "has_live": exp_dt is not None,
                "expected_dt": exp_dt.isoformat() if exp_dt else None,
                "minutes_to": live_minutes_to,
                "delay_min": live_delay_min,
            }
        })

    overall_delay_min = None
    if delay_sec is not None:
        overall_delay_min = int(round(delay_sec / 60.0))

    return {
        "trip_id": trip_id,
        "service_date": d.isoformat(),
        "route": route_short,
        "destination": t.headsign,
        "trip_short_name": t.trip_short_name,
        "now": n.isoformat(),
        "overall_delay_min": overall_delay_min,
        "next_stop_sequence": next_seq,
        "vehicle": {
            "has_vehicle": live_vehicle is not None,
            "fleet": (live_vehicle or {}).get("vehicle_id"),
            "lat": (live_vehicle or {}).get("lat"),
            "lon": (live_vehicle or {}).get("lon"),
            "recorded_at": (live_vehicle or {}).get("recorded_at"),
            "source": "vm_heuristic" if (live_vehicle and delay_sec is not None) else None,
        },
        "stops": stops_out
    }
