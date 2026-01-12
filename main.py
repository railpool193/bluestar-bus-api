import os
import io
import csv
import zipfile
import time
import threading
import datetime as dt
import xml.etree.ElementTree as ET
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urlparse, parse_qs

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from zoneinfo import ZoneInfo


# =========================
# Logging
# =========================
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("bsu")


# =========================
# Config
# =========================
APP_TZ = os.getenv("APP_TZ", "Europe/London")
TZ = ZoneInfo(APP_TZ)

BODS_FEED_ID = os.getenv("BODS_FEED_ID", "7721").strip()

# Accept multiple env names (people often set different ones on Railway)
BODS_API_KEY = (
    os.getenv("BODS_API_KEY", "").strip()
    or os.getenv("BODS_KEY", "").strip()
    or os.getenv("API_KEY", "").strip()
    or os.getenv("BODS_APIKEY", "").strip()
)

BODS_FEED_URL = os.getenv(
    "BODS_FEED_URL",
    f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/",
).strip()

LIVE_REFRESH_SECONDS = int(os.getenv("LIVE_REFRESH_SECONDS", "10"))  # cache TTL
LIVE_HTTP_TIMEOUT = float(os.getenv("LIVE_HTTP_TIMEOUT", "15"))

# IMPORTANT FIX:
# default: NO filtering (keep all agencies). If you want filter, set env:
#   AGENCY_NAME_ALLOW="bluestar,unilink"
AGENCY_NAME_ALLOW = os.getenv("AGENCY_NAME_ALLOW", "").strip()
AGENCY_ALLOW_TOKENS = [t.strip().lower() for t in AGENCY_NAME_ALLOW.split(",") if t.strip()]


# =========================
# Helpers
# =========================
def now_tz() -> dt.datetime:
    return dt.datetime.now(tz=TZ)


def parse_gtfs_time_to_seconds(s: str) -> Optional[int]:
    """
    GTFS time can be HH:MM:SS and HH may exceed 24.
    Returns seconds since service-day midnight.
    """
    if not s:
        return None
    s = s.strip()
    parts = s.split(":")
    if len(parts) < 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
        ss = int(parts[2]) if len(parts) >= 3 else 0
        return hh * 3600 + mm * 60 + ss
    except Exception:
        return None


def iso_to_dt(s: str) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(TZ)
    except Exception:
        return None


def normalize_line(s: str) -> str:
    s = (s or "").strip()
    s2 = s.replace(" ", "")
    if s2.isdigit():
        s2 = s2.lstrip("0") or "0"
    return s2.upper()


def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    try:
        return int(float(x))
    except Exception:
        return None


# =========================
# GTFS store
# =========================
@dataclass
class StopTimeRow:
    stop_id: str
    arrival_s: Optional[int]
    departure_s: Optional[int]
    stop_sequence: int


class GTFSStore:
    def __init__(self) -> None:
        self.loaded = False
        self.agencies: Dict[str, Dict[str, str]] = {}
        self.stops: Dict[str, Dict[str, Any]] = {}
        self.routes: Dict[str, Dict[str, Any]] = {}
        self.trips: Dict[str, Dict[str, Any]] = {}
        self.calendar: Dict[str, Dict[str, Any]] = {}
        self.calendar_dates: Dict[str, Dict[str, int]] = {}  # service_id -> yyyymmdd -> exception_type
        self.trip_stop_times: Dict[str, List[StopTimeRow]] = {}
        self.stop_departures_index: Dict[str, List[Tuple[str, int, int]]] = {}  # stop_id -> (trip_id, dep_s, seq)
        self.trip_meta: Dict[str, Dict[str, Any]] = {}
        self.route_short_to_ids: Dict[str, List[str]] = {}
        self.shapes: Dict[str, List[Tuple[float, float, int]]] = {}
        self.trip_key_index: Dict[Tuple[str, int, str, str], List[Tuple[str, int, int]]] = {}

        # NEW: stop grouping (parent_station / children)
        self.parent_to_children: Dict[str, List[str]] = {}

    def _open_reader(self, base: str, filename: str):
        if os.path.isdir(base):
            path = os.path.join(base, filename)
            if not os.path.exists(path):
                return None
            f = open(path, "r", encoding="utf-8-sig", newline="")
            return f, csv.DictReader(f)
        else:
            if not os.path.exists(base):
                return None
            zf = zipfile.ZipFile(base, "r")
            try:
                raw = zf.open(filename, "r")
            except KeyError:
                zf.close()
                return None
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            return (zf, raw, text), csv.DictReader(text)

    def _close_reader(self, handle):
        if handle is None:
            return
        if isinstance(handle, tuple) and len(handle) == 2:
            f, _ = handle
            try:
                f.close()
            except Exception:
                pass
        elif isinstance(handle, tuple) and len(handle) == 3:
            zf, raw, text = handle
            try:
                text.close()
            except Exception:
                pass
            try:
                raw.close()
            except Exception:
                pass
            try:
                zf.close()
            except Exception:
                pass

    def load(self, gtfs_dir: str = "gtfs", gtfs_zip: str = "gtfs.zip") -> None:
        base = gtfs_dir if os.path.isdir(gtfs_dir) else gtfs_zip
        if not (os.path.isdir(base) or os.path.exists(base)):
            raise RuntimeError("GTFS not found. Create ./gtfs/ OR put gtfs.zip into project root.")

        # agency.txt
        h = self._open_reader(base, "agency.txt")
        if h:
            handle, reader = h
            for r in reader:
                aid = (r.get("agency_id") or "default").strip()
                self.agencies[aid] = r
            self._close_reader(handle)

        def agency_allowed(agency_id: str) -> bool:
            if not AGENCY_ALLOW_TOKENS:
                return True
            a = self.agencies.get(agency_id) or {}
            name = (a.get("agency_name") or "").lower()
            return any(tok in name for tok in AGENCY_ALLOW_TOKENS)

        # routes.txt
        h = self._open_reader(base, "routes.txt")
        if not h:
            raise RuntimeError("routes.txt missing from GTFS.")
        handle, reader = h
        for r in reader:
            rid = (r.get("route_id") or "").strip()
            if not rid:
                continue
            agency_id = (r.get("agency_id") or "default").strip()
            if not agency_allowed(agency_id):
                continue
            short = (r.get("route_short_name") or "").strip()
            longn = (r.get("route_long_name") or "").strip()
            self.routes[rid] = {
                **r,
                "route_id": rid,
                "agency_id": agency_id,
                "route_short_name": short,
                "route_long_name": longn,
                "route_short_norm": normalize_line(short),
            }
        self._close_reader(handle)

        for rid, r in self.routes.items():
            k = r.get("route_short_norm") or ""
            if not k:
                continue
            self.route_short_to_ids.setdefault(k, []).append(rid)

        # stops.txt (NEW: parent_station / location_type)
        h = self._open_reader(base, "stops.txt")
        if not h:
            raise RuntimeError("stops.txt missing from GTFS.")
        handle, reader = h
        for r in reader:
            sid = (r.get("stop_id") or "").strip()
            if not sid:
                continue
            parent = (r.get("parent_station") or "").strip()
            loc_type = safe_int(r.get("location_type")) if r.get("location_type") else 0
            self.stops[sid] = {
                "stop_id": sid,
                "stop_name": (r.get("stop_name") or "").strip(),
                "stop_code": (r.get("stop_code") or "").strip(),
                "stop_lat": safe_float(r.get("stop_lat")),
                "stop_lon": safe_float(r.get("stop_lon")),
                "parent_station": parent,
                "location_type": loc_type,
            }
            if parent:
                self.parent_to_children.setdefault(parent, []).append(sid)
        self._close_reader(handle)

        # calendar.txt (optional)
        h = self._open_reader(base, "calendar.txt")
        if h:
            handle, reader = h
            for r in reader:
                sid = (r.get("service_id") or "").strip()
                if not sid:
                    continue
                self.calendar[sid] = r
            self._close_reader(handle)

        # calendar_dates.txt (optional)
        h = self._open_reader(base, "calendar_dates.txt")
        if h:
            handle, reader = h
            for r in reader:
                sid = (r.get("service_id") or "").strip()
                date = (r.get("date") or "").strip()
                et = safe_int(r.get("exception_type")) or 0
                if sid and date and et:
                    self.calendar_dates.setdefault(sid, {})[date] = et
            self._close_reader(handle)

        # trips.txt
        h = self._open_reader(base, "trips.txt")
        if not h:
            raise RuntimeError("trips.txt missing from GTFS.")
        handle, reader = h
        for r in reader:
            tid = (r.get("trip_id") or "").strip()
            rid = (r.get("route_id") or "").strip()
            if not tid or rid not in self.routes:
                continue
            self.trips[tid] = {
                **r,
                "trip_id": tid,
                "route_id": rid,
                "service_id": (r.get("service_id") or "").strip(),
                "direction_id": safe_int(r.get("direction_id")) if r.get("direction_id") else None,
                "trip_headsign": (r.get("trip_headsign") or "").strip(),
                "shape_id": (r.get("shape_id") or "").strip(),
            }
        self._close_reader(handle)

        # stop_times.txt
        h = self._open_reader(base, "stop_times.txt")
        if not h:
            raise RuntimeError("stop_times.txt missing from GTFS.")
        handle, reader = h
        for r in reader:
            tid = (r.get("trip_id") or "").strip()
            if tid not in self.trips:
                continue
            stop_id = (r.get("stop_id") or "").strip()
            if stop_id not in self.stops:
                continue
            arr = parse_gtfs_time_to_seconds((r.get("arrival_time") or "").strip())
            dep = parse_gtfs_time_to_seconds((r.get("departure_time") or "").strip())
            seq = safe_int(r.get("stop_sequence")) or 0
            self.trip_stop_times.setdefault(tid, []).append(
                StopTimeRow(stop_id=stop_id, arrival_s=arr, departure_s=dep, stop_sequence=seq)
            )
        self._close_reader(handle)

        for tid, lst in self.trip_stop_times.items():
            lst.sort(key=lambda x: x.stop_sequence)

        for tid, lst in self.trip_stop_times.items():
            if not lst:
                continue
            start_s = lst[0].departure_s if lst[0].departure_s is not None else lst[0].arrival_s
            end_s = lst[-1].arrival_s if lst[-1].arrival_s is not None else lst[-1].departure_s
            origin_stop = lst[0].stop_id
            dest_stop = lst[-1].stop_id
            self.trip_meta[tid] = {
                "start_s": start_s,
                "end_s": end_s,
                "origin_stop_id": origin_stop,
                "dest_stop_id": dest_stop,
            }

            for st in lst:
                dep_s = st.departure_s if st.departure_s is not None else st.arrival_s
                if dep_s is None:
                    continue
                self.stop_departures_index.setdefault(st.stop_id, []).append((tid, dep_s, st.stop_sequence))

        for sid, lst in self.stop_departures_index.items():
            lst.sort(key=lambda x: x[1])

        # shapes.txt (optional)
        h = self._open_reader(base, "shapes.txt")
        if h:
            handle, reader = h
            for r in reader:
                shape_id = (r.get("shape_id") or "").strip()
                lat = safe_float(r.get("shape_pt_lat"))
                lon = safe_float(r.get("shape_pt_lon"))
                seq = safe_int(r.get("shape_pt_sequence")) or 0
                if shape_id and lat is not None and lon is not None:
                    self.shapes.setdefault(shape_id, []).append((lat, lon, seq))
            self._close_reader(handle)
            for sh, pts in self.shapes.items():
                pts.sort(key=lambda x: x[2])

        # live mapping index (line + direction + originRef + destRef)
        for tid, t in self.trips.items():
            rid = t["route_id"]
            rshort = self.routes[rid].get("route_short_norm") or ""
            direction = t.get("direction_id")
            if direction is None:
                direction = 0
            meta = self.trip_meta.get(tid) or {}
            key = (rshort, int(direction), meta.get("origin_stop_id", ""), meta.get("dest_stop_id", ""))
            if not key[0] or not key[2] or not key[3]:
                continue
            start_s = meta.get("start_s")
            end_s = meta.get("end_s")
            if start_s is None or end_s is None:
                continue
            self.trip_key_index.setdefault(key, []).append((tid, int(start_s), int(end_s)))

        for k, v in self.trip_key_index.items():
            v.sort(key=lambda x: x[1])

        self.loaded = True

        log.info(
            "GTFS loaded: stops=%s routes=%s trips=%s stop_times(trips)=%s calendar=%s",
            len(self.stops),
            len(self.routes),
            len(self.trips),
            len(self.trip_stop_times),
            len(self.calendar),
        )

    def service_active(self, service_id: str, date: dt.date) -> bool:
        if not service_id:
            return False
        yyyymmdd = date.strftime("%Y%m%d")

        ex = self.calendar_dates.get(service_id, {}).get(yyyymmdd)
        if ex == 1:
            return True
        if ex == 2:
            return False

        cal = self.calendar.get(service_id)
        if not cal:
            # no calendar.txt -> assume active
            return True

        start = cal.get("start_date")
        end = cal.get("end_date")
        if start and yyyymmdd < start:
            return False
        if end and yyyymmdd > end:
            return False

        weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][date.weekday()]
        return (cal.get(weekday) or "0").strip() == "1"

    def calendar_range(self) -> Dict[str, str]:
        # returns min start_date and max end_date from calendar.txt (if exists)
        starts = []
        ends = []
        for s in self.calendar.values():
            if s.get("start_date"):
                starts.append(s["start_date"])
            if s.get("end_date"):
                ends.append(s["end_date"])
        return {
            "calendar_start_min": min(starts) if starts else "",
            "calendar_end_max": max(ends) if ends else "",
        }

    def stop_group_ids(self, stop_id: str) -> List[str]:
        s = self.stops.get(stop_id) or {}
        parent = (s.get("parent_station") or "").strip()
        loc_type = int(s.get("location_type") or 0)

        # If it is a child -> group by same parent
        if parent:
            return sorted(self.parent_to_children.get(parent, [stop_id]))
        # If it is a parent/station -> include children
        if loc_type == 1 and stop_id in self.parent_to_children:
            return sorted(self.parent_to_children.get(stop_id, [stop_id]))
        return [stop_id]


GTFS = GTFSStore()


# =========================
# Live SIRI-VM cache + diagnostics
# =========================
SIRI_NS = {"s": "http://www.siri.org.uk/siri"}

_live_lock = threading.Lock()
_live_cache: Dict[str, Any] = {
    "ts": 0.0,
    "vehicles": [],
    "trip_map": {},
    "last_error": "",
    "last_http_status": None,
    "last_fetch_time": "",
}


def _siri_text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _siri_find_text(parent: Optional[ET.Element], path: str) -> str:
    if parent is None:
        return ""
    el = parent.find(path, SIRI_NS)
    return _siri_text(el)


def _parse_extensions(ext: Optional[ET.Element]) -> Dict[str, str]:
    out = {}
    if ext is None:
        return out
    for e in ext.iter():
        tag = e.tag.split("}")[-1]
        if tag == "TicketMachineServiceCode":
            out["ticketMachineServiceCode"] = _siri_text(e)
        elif tag == "JourneyCode":
            out["journeyCode"] = _siri_text(e)
        elif tag == "VehicleUniqueId":
            out["vehicleUniqueId"] = _siri_text(e)
    return out


def _direction_to_id(direction_ref: str) -> int:
    d = (direction_ref or "").strip().lower()
    if d == "inbound":
        return 0
    if d == "outbound":
        return 1
    if d.isdigit():
        return int(d)
    return 0


def _extract_key_from_url(url: str) -> str:
    try:
        q = parse_qs(urlparse(url).query)
        k = (q.get("api_key") or [""])[0].strip()
        return k
    except Exception:
        return ""


def _best_trip_match_for_vehicle(v: Dict[str, Any]) -> Optional[str]:
    if not GTFS.loaded:
        return None

    line_norm = normalize_line(v.get("publishedLineName") or v.get("lineRef") or "")
    direction_id = int(v.get("directionId") or 0)
    origin = (v.get("originRef") or "").strip()
    dest = (v.get("destinationRef") or "").strip()
    if not line_norm or not origin or not dest:
        return None

    key = (line_norm, direction_id, origin, dest)
    candidates = GTFS.trip_key_index.get(key)
    if not candidates:
        alt = normalize_line(v.get("lineRef") or "")
        if alt and alt != line_norm:
            candidates = GTFS.trip_key_index.get((alt, direction_id, origin, dest))
    if not candidates:
        return None

    o_dt = v.get("originAimedDepartureDT")
    d_dt = v.get("destinationAimedArrivalDT")
    if not o_dt or not d_dt:
        now = now_tz()
        mid = now.replace(hour=0, minute=0, second=0, microsecond=0)
        now_s = int((now - mid).total_seconds())
        best = min(candidates, key=lambda x: abs(x[1] - now_s))
        return best[0]

    service_date = o_dt.date()
    midnight = dt.datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)
    o_s = int((o_dt - midnight).total_seconds())
    d_s = int((d_dt - midnight).total_seconds())

    best_tid = None
    best_score = 10**12
    for tid, start_s, end_s in candidates:
        score = abs(start_s - o_s) + abs(end_s - d_s)
        if score < best_score:
            best_score = score
            best_tid = tid

    if best_tid is not None and best_score <= (60 * 60):
        return best_tid
    return None


def fetch_live_siri_vm() -> Tuple[List[Dict[str, Any]], Optional[int], str]:
    """
    returns: (vehicles, http_status, error_message)
    """
    key = BODS_API_KEY or _extract_key_from_url(BODS_FEED_URL)
    if not key:
        return [], None, "Missing BODS API key. Set env BODS_API_KEY (or include api_key in BODS_FEED_URL)."

    params = {"api_key": key}
    url = BODS_FEED_URL

    try:
        with httpx.Client(timeout=LIVE_HTTP_TIMEOUT, follow_redirects=True) as client:
            r = client.get(url, params=params)
            status = r.status_code
            r.raise_for_status()
            xml = r.text
    except Exception as e:
        return [], getattr(e, "response", None).status_code if hasattr(e, "response") and e.response else None, f"HTTP error: {e}"

    try:
        root = ET.fromstring(xml)
    except Exception as e:
        return [], status, f"XML parse error: {e}"

    vehicles: List[Dict[str, Any]] = []

    for va in root.findall(".//s:VehicleActivity", SIRI_NS):
        recorded = iso_to_dt(_siri_find_text(va, "s:RecordedAtTime"))
        valid_until = iso_to_dt(_siri_find_text(va, "s:ValidUntilTime"))

        mvj = va.find(".//s:MonitoredVehicleJourney", SIRI_NS)
        if mvj is None:
            continue

        line_ref = _siri_find_text(mvj, "s:LineRef")
        published_line = _siri_find_text(mvj, "s:PublishedLineName") or line_ref
        operator_ref = _siri_find_text(mvj, "s:OperatorRef")
        direction_ref = _siri_find_text(mvj, "s:DirectionRef")
        direction_id = _direction_to_id(direction_ref)

        origin_ref = _siri_find_text(mvj, "s:OriginRef")
        origin_name = _siri_find_text(mvj, "s:OriginName")
        dest_ref = _siri_find_text(mvj, "s:DestinationRef")
        dest_name = _siri_find_text(mvj, "s:DestinationName")

        o_aimed = iso_to_dt(_siri_find_text(mvj, "s:OriginAimedDepartureTime"))
        d_aimed = iso_to_dt(_siri_find_text(mvj, "s:DestinationAimedArrivalTime"))

        loc = mvj.find("s:VehicleLocation", SIRI_NS)
        lon = safe_float(_siri_find_text(loc, "s:Longitude")) if loc is not None else None
        lat = safe_float(_siri_find_text(loc, "s:Latitude")) if loc is not None else None

        bearing = safe_int(_siri_find_text(mvj, "s:Bearing")) or 0
        block_ref = _siri_find_text(mvj, "s:BlockRef")
        vehicle_ref = _siri_find_text(mvj, "s:VehicleRef")

        framed = mvj.find("s:FramedVehicleJourneyRef", SIRI_NS)
        data_frame_ref = _siri_find_text(framed, "s:DataFrameRef") if framed is not None else ""
        dated_vjr = _siri_find_text(framed, "s:DatedVehicleJourneyRef") if framed is not None else ""

        ext = mvj.find("s:Extensions", SIRI_NS)
        ext_info = _parse_extensions(ext)

        v: Dict[str, Any] = {
            "recordedAtTime": recorded.isoformat() if recorded else "",
            "validUntilTime": valid_until.isoformat() if valid_until else "",
            "lineRef": line_ref,
            "publishedLineName": published_line,
            "lineNorm": normalize_line(published_line or line_ref),
            "operatorRef": operator_ref,
            "directionRef": direction_ref,
            "directionId": direction_id,
            "originRef": origin_ref,
            "originName": origin_name,
            "destinationRef": dest_ref,
            "destinationName": dest_name,
            "originAimedDepartureTime": o_aimed.isoformat() if o_aimed else "",
            "destinationAimedArrivalTime": d_aimed.isoformat() if d_aimed else "",
            "longitude": lon,
            "latitude": lat,
            "bearing": bearing,
            "blockRef": block_ref,
            "vehicleRef": vehicle_ref,
            "dataFrameRef": data_frame_ref,
            "datedVehicleJourneyRef": dated_vjr,
            **ext_info,
        }

        # internal dt for matching
        v["originAimedDepartureDT"] = o_aimed
        v["destinationAimedArrivalDT"] = d_aimed
        trip_id = _best_trip_match_for_vehicle(v)
        v["tripId"] = trip_id or ""
        v.pop("originAimedDepartureDT", None)
        v.pop("destinationAimedArrivalDT", None)

        # keep only if coords exist
        if v.get("latitude") is not None and v.get("longitude") is not None:
            vehicles.append(v)

    return vehicles, status, ""


def get_live_cached() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    now = time.time()
    with _live_lock:
        if (now - _live_cache["ts"]) < LIVE_REFRESH_SECONDS:
            return _live_cache["vehicles"], _live_cache["trip_map"]

    vehicles: List[Dict[str, Any]] = []
    trip_map: Dict[str, Dict[str, Any]] = {}
    last_error = ""
    http_status = None

    vehicles, http_status, last_error = fetch_live_siri_vm()
    for v in vehicles:
        tid = (v.get("tripId") or "").strip()
        if tid:
            trip_map[tid] = v

    with _live_lock:
        _live_cache["ts"] = now
        _live_cache["vehicles"] = vehicles
        _live_cache["trip_map"] = trip_map
        _live_cache["last_error"] = last_error
        _live_cache["last_http_status"] = http_status
        _live_cache["last_fetch_time"] = now_tz().isoformat()

    if last_error:
        log.warning("LIVE fetch issue: %s (status=%s)", last_error, http_status)
    else:
        log.info("LIVE vehicles=%s", len(vehicles))

    return vehicles, trip_map


# =========================
# FastAPI app
# =========================
app = FastAPI(title="Bluestar & Unilink GTFS + Live (SIRI-VM)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    GTFS.load(gtfs_dir="gtfs", gtfs_zip="gtfs.zip")


@app.get("/", response_class=HTMLResponse)
def home():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    if os.path.exists(os.path.join("static", "index.html")):
        return FileResponse(os.path.join("static", "index.html"))
    return HTMLResponse("<h3>index.html not found</h3>", status_code=404)


# =========================
# Diagnostics endpoints
# =========================
@app.get("/api/health")
def health():
    # refresh cache lightly (won't spam because ttl)
    vehicles, _ = get_live_cached()
    key_present = bool(BODS_API_KEY or _extract_key_from_url(BODS_FEED_URL))
    return {
        "ok": True,
        "gtfsLoaded": GTFS.loaded,
        "tz": APP_TZ,
        "liveKeyPresent": key_present,
        "liveVehicleCount": len(vehicles),
        "liveLastError": _live_cache.get("last_error", ""),
        "liveLastHttpStatus": _live_cache.get("last_http_status"),
        "liveLastFetchTime": _live_cache.get("last_fetch_time", ""),
        "liveCacheSeconds": LIVE_REFRESH_SECONDS,
        "gtfsCounts": {
            "stops": len(GTFS.stops),
            "routes": len(GTFS.routes),
            "trips": len(GTFS.trips),
            "stop_times_trips": len(GTFS.trip_stop_times),
        },
        "gtfsCalendarRange": GTFS.calendar_range(),
        "agencyFilter": AGENCY_NAME_ALLOW,
    }


@app.get("/api/debug/live")
def debug_live():
    vehicles, _ = get_live_cached()
    return {
        "feedUrl": BODS_FEED_URL,
        "keyPresent": bool(BODS_API_KEY or _extract_key_from_url(BODS_FEED_URL)),
        "vehicleCount": len(vehicles),
        "lastError": _live_cache.get("last_error", ""),
        "lastHttpStatus": _live_cache.get("last_http_status"),
        "lastFetchTime": _live_cache.get("last_fetch_time", ""),
        "sample": vehicles[:1],
    }


@app.get("/api/debug/gtfs")
def debug_gtfs():
    return {
        "loaded": GTFS.loaded,
        "counts": {
            "stops": len(GTFS.stops),
            "routes": len(GTFS.routes),
            "trips": len(GTFS.trips),
            "stop_times_trips": len(GTFS.trip_stop_times),
            "stop_departures_index_stops": len(GTFS.stop_departures_index),
        },
        "calendarRange": GTFS.calendar_range(),
        "agencyFilter": AGENCY_NAME_ALLOW,
    }


# =========================
# API endpoints
# =========================
@app.get("/api/search")
def search(q: str = Query("", min_length=0, max_length=80)):
    qn = (q or "").strip().lower()
    if not qn:
        return {"stops": [], "routes": []}

    stops = []
    for s in GTFS.stops.values():
        name = (s.get("stop_name") or "").lower()
        sid = (s.get("stop_id") or "").lower()
        code = (s.get("stop_code") or "").lower()
        if qn in name or qn in sid or (code and qn in code):
            stops.append(s)
            if len(stops) >= 50:
                break

    routes = []
    for r in GTFS.routes.values():
        short = (r.get("route_short_name") or "").lower()
        longn = (r.get("route_long_name") or "").lower()
        if qn in short or qn in longn:
            aid = r.get("agency_id") or "default"
            agency_name = (GTFS.agencies.get(aid) or {}).get("agency_name") or ""
            routes.append(
                {
                    "route_id": r["route_id"],
                    "route_short_name": r.get("route_short_name") or "",
                    "route_long_name": r.get("route_long_name") or "",
                    "agency_name": agency_name,
                }
            )
            if len(routes) >= 50:
                break

    return {"stops": stops, "routes": routes}


@app.get("/api/stop/{stop_id}")
def stop_info(stop_id: str):
    s = GTFS.stops.get(stop_id)
    if not s:
        raise HTTPException(404, "Stop not found")
    return s


def _service_dates_to_consider(now: dt.datetime) -> List[dt.date]:
    # After midnight, GTFS may use 24:xx+ times from previous service day
    if now.hour < 5:
        return [now.date() - dt.timedelta(days=1), now.date()]
    return [now.date()]


@app.get("/api/stop/{stop_id}/departures")
def stop_departures(
    stop_id: str,
    minutes: int = Query(180, ge=10, le=720),
    limit: int = Query(60, ge=5, le=200),
    group: int = Query(1, ge=0, le=1),  # NEW: default ON -> tries platform group
    date: str = Query("", max_length=10),  # optional: YYYY-MM-DD
):
    s = GTFS.stops.get(stop_id)
    if not s:
        raise HTTPException(404, "Stop not found")

    now = now_tz()

    # allow manual date test
    if date:
        try:
            base_date = dt.date.fromisoformat(date)
            service_dates = [base_date]
            now_for_window = dt.datetime(base_date.year, base_date.month, base_date.day, 0, 0, 0, tzinfo=TZ)
            now = now_for_window  # for "minutes_to" from midnight test
        except Exception:
            raise HTTPException(400, "Invalid date, use YYYY-MM-DD")
    else:
        service_dates = _service_dates_to_consider(now)

    end = now + dt.timedelta(minutes=minutes)
    _, live_trip_map = get_live_cached()

    # NEW: platform grouping
    stop_ids = [stop_id]
    if group == 1:
        stop_ids = GTFS.stop_group_ids(stop_id)

    results: List[Dict[str, Any]] = []
    for service_date in service_dates:
        midnight = dt.datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)

        for sid in stop_ids:
            deps = GTFS.stop_departures_index.get(sid, [])
            for trip_id, dep_s, seq in deps:
                trip = GTFS.trips.get(trip_id)
                if not trip:
                    continue

                if not GTFS.service_active(trip.get("service_id") or "", service_date):
                    continue

                dep_dt = midnight + dt.timedelta(seconds=int(dep_s))
                if dep_dt < now or dep_dt > end:
                    continue

                route = GTFS.routes.get(trip["route_id"]) or {}
                headsign = trip.get("trip_headsign") or ""
                direction_id = trip.get("direction_id")
                if direction_id is None:
                    direction_id = 0

                live = live_trip_map.get(trip_id)

                results.append(
                    {
                        "trip_id": trip_id,
                        "route_id": trip["route_id"],
                        "route_short_name": route.get("route_short_name") or "",
                        "route_long_name": route.get("route_long_name") or "",
                        "headsign": headsign,
                        "direction_id": int(direction_id),
                        "scheduled_departure": dep_dt.isoformat(),
                        "scheduled_hhmm": dep_dt.strftime("%H:%M"),
                        "minutes_to": int((dep_dt - now).total_seconds() // 60),
                        "served_stop_id": sid,  # which platform actually matched
                        "live": live or None,
                    }
                )
                if len(results) >= limit:
                    break
            if len(results) >= limit:
                break
        if len(results) >= limit:
            break

    results.sort(key=lambda x: x["scheduled_departure"])

    # add helpful hint if empty
    hint = ""
    if not results:
        cr = GTFS.calendar_range()
        if cr.get("calendar_end_max"):
            hint = f"No departures found. Check GTFS calendar range (end_max={cr.get('calendar_end_max')}). If it's old, update gtfs.zip."
        else:
            hint = "No departures found. GTFS may have no calendar.txt or no trips for this stop/time."

    return {
        "stop": s,
        "grouped_stop_ids": stop_ids,
        "departures": results[:limit],
        "hint": hint,
    }


@app.get("/api/live/vehicles")
def live_vehicles(line: str = Query("", max_length=20)):
    vehicles, _ = get_live_cached()
    if not vehicles:
        # now we return the actual reason too
        return {
            "vehicles": [],
            "note": _live_cache.get("last_error") or "No live data.",
            "httpStatus": _live_cache.get("last_http_status"),
        }

    ln = normalize_line(line) if line else ""
    if ln:
        vehicles = [
            v for v in vehicles
            if normalize_line(v.get("publishedLineName") or v.get("lineRef") or "") == ln
        ]
    return {"vehicles": vehicles}
