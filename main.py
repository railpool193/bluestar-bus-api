import os
import io
import csv
import zipfile
import time
import threading
import datetime as dt
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from zoneinfo import ZoneInfo


# =========================
# Config
# =========================
APP_TZ = os.getenv("APP_TZ", "Europe/London")
TZ = ZoneInfo(APP_TZ)

BODS_FEED_ID = os.getenv("BODS_FEED_ID", "7721")
BODS_API_KEY = os.getenv("BODS_API_KEY", "")
BODS_FEED_URL = os.getenv(
    "BODS_FEED_URL",
    f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/",
)

LIVE_REFRESH_SECONDS = int(os.getenv("LIVE_REFRESH_SECONDS", "10"))  # cache TTL
LIVE_HTTP_TIMEOUT = float(os.getenv("LIVE_HTTP_TIMEOUT", "12"))

# Optional agency filtering:
# If you want only Bluestar & Unilink from your GTFS:
# set AGENCY_NAME_ALLOW="bluestar,unilink"
AGENCY_NAME_ALLOW = os.getenv("AGENCY_NAME_ALLOW", "bluestar,unilink").strip()
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
        # handles "...+00:00"
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(TZ)
    except Exception:
        return None


def normalize_line(s: str) -> str:
    s = (s or "").strip()
    # keep letters, but remove spaces; strip leading zeros for numeric-only
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
        self.trip_meta: Dict[str, Dict[str, Any]] = {}  # trip_id -> start/end/origin/dest
        self.route_short_to_ids: Dict[str, List[str]] = {}
        self.shapes: Dict[str, List[Tuple[float, float, int]]] = {}  # shape_id -> [(lat,lon,seq)]

        # candidate index for live mapping:
        self.trip_key_index: Dict[Tuple[str, int, str, str], List[Tuple[str, int, int]]] = {}
        # key=(line_norm, direction_id, origin_stop_id, dest_stop_id) -> [(trip_id, start_s, end_s)]

    def _open_reader(self, base: str, filename: str):
        """
        base can be a directory path OR a .zip file path
        """
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

        # build route short index
        for rid, r in self.routes.items():
            k = r.get("route_short_norm") or ""
            if not k:
                continue
            self.route_short_to_ids.setdefault(k, []).append(rid)

        # stops.txt
        h = self._open_reader(base, "stops.txt")
        if not h:
            raise RuntimeError("stops.txt missing from GTFS.")
        handle, reader = h
        for r in reader:
            sid = (r.get("stop_id") or "").strip()
            if not sid:
                continue
            self.stops[sid] = {
                "stop_id": sid,
                "stop_name": (r.get("stop_name") or "").strip(),
                "stop_code": (r.get("stop_code") or "").strip(),
                "stop_lat": safe_float(r.get("stop_lat")),
                "stop_lon": safe_float(r.get("stop_lon")),
            }
        self._close_reader(handle)

        # calendar.txt (optional but common)
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

        # sort stop_times and build indices
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

        # candidate index for live mapping
        for tid, t in self.trips.items():
            rid = t["route_id"]
            rshort = self.routes[rid].get("route_short_norm") or ""
            direction = t.get("direction_id")
            if direction is None:
                # fallback: treat None as 0 (still matchable)
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

        # sort candidates by start time
        for k, v in self.trip_key_index.items():
            v.sort(key=lambda x: x[1])

        self.loaded = True

    def service_active(self, service_id: str, date: dt.date) -> bool:
        """
        Checks calendar + calendar_dates exceptions.
        """
        if not service_id:
            return False
        yyyymmdd = date.strftime("%Y%m%d")

        # exception overrides base
        ex = self.calendar_dates.get(service_id, {}).get(yyyymmdd)
        if ex == 1:
            return True
        if ex == 2:
            return False

        cal = self.calendar.get(service_id)
        if not cal:
            # if no calendar.txt, assume active unless explicitly removed (common in some feeds)
            return True

        start = cal.get("start_date")
        end = cal.get("end_date")
        if start and yyyymmdd < start:
            return False
        if end and yyyymmdd > end:
            return False

        weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][date.weekday()]
        return (cal.get(weekday) or "0").strip() == "1"


GTFS = GTFSStore()


# =========================
# Live SIRI-VM cache
# =========================
SIRI_NS = {"s": "http://www.siri.org.uk/siri"}

_live_lock = threading.Lock()
_live_cache: Dict[str, Any] = {
    "ts": 0.0,
    "vehicles": [],
    "trip_map": {},  # trip_id -> vehicle dict
}


def _siri_text(el: Optional[ET.Element]) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _siri_find_text(parent: ET.Element, path: str) -> str:
    el = parent.find(path, SIRI_NS)
    return _siri_text(el)


def _parse_ticket_machine(ext: ET.Element) -> Dict[str, str]:
    # Extensions/VehicleJourney/Operational/TicketMachine/*
    out = {}
    if ext is None:
        return out
    # walk without assuming namespaces inside Extensions (often none)
    # We’ll just search by suffix
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
    # most UK feeds: inbound/outbound
    if d == "inbound":
        return 0
    if d == "outbound":
        return 1
    # fallback numeric-ish
    if d.isdigit():
        return int(d)
    return 0


def _best_trip_match_for_vehicle(v: Dict[str, Any]) -> Optional[str]:
    """
    Try to map a SIRI vehicle activity to a GTFS trip_id.
    Uses (line, direction, originRef, destinationRef) + aimed times.
    """
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
        # try if route_short_name mapping differs: LineRef vs PublishedLineName
        alt = normalize_line(v.get("lineRef") or "")
        if alt and alt != line_norm:
            candidates = GTFS.trip_key_index.get((alt, direction_id, origin, dest))
    if not candidates:
        return None

    o_dt = v.get("originAimedDepartureDT")
    d_dt = v.get("destinationAimedArrivalDT")
    if not o_dt or not d_dt:
        # fallback: choose nearest start time to "now"
        now = now_tz()
        mid = now.replace(hour=0, minute=0, second=0, microsecond=0)
        now_s = int((now - mid).total_seconds())
        best = min(candidates, key=lambda x: abs(x[1] - now_s))
        return best[0]

    # convert aimed times to seconds from service-day midnight
    service_date = o_dt.date()
    midnight = dt.datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)
    o_s = int((o_dt - midnight).total_seconds())
    d_s = int((d_dt - midnight).total_seconds())

    # pick closest by combined distance
    best_tid = None
    best_score = 10**12
    for tid, start_s, end_s in candidates:
        score = abs(start_s - o_s) + abs(end_s - d_s)
        if score < best_score:
            best_score = score
            best_tid = tid

    # sanity threshold (still allow if none better)
    # typical: within ~15-25 mins combined
    if best_tid is not None and best_score <= (25 * 60):
        return best_tid

    # relax: allow within 60 mins
    if best_tid is not None and best_score <= (60 * 60):
        return best_tid

    return None


def fetch_live_siri_vm() -> List[Dict[str, Any]]:
    if not BODS_API_KEY:
        return []

    params = {"api_key": BODS_API_KEY}
    url = BODS_FEED_URL

    with httpx.Client(timeout=LIVE_HTTP_TIMEOUT, follow_redirects=True) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        xml = r.text

    root = ET.fromstring(xml)
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

        # Extensions (TicketMachine etc.)
        ext = mvj.find("s:Extensions", SIRI_NS)
        ext_info = _parse_ticket_machine(ext) if ext is not None else {}

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

        # keep parsed datetimes for matching (not returned directly)
        v["originAimedDepartureDT"] = o_aimed
        v["destinationAimedArrivalDT"] = d_aimed

        # map to GTFS trip if possible
        trip_id = _best_trip_match_for_vehicle(v)
        if trip_id:
            v["tripId"] = trip_id
        else:
            v["tripId"] = ""

        # remove internal dt objects
        v.pop("originAimedDepartureDT", None)
        v.pop("destinationAimedArrivalDT", None)

        # only keep vehicles with coordinates
        if v.get("latitude") is not None and v.get("longitude") is not None:
            vehicles.append(v)

    return vehicles


def get_live_cached() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Returns (vehicles, trip_map)
    """
    now = time.time()
    with _live_lock:
        if (now - _live_cache["ts"]) < LIVE_REFRESH_SECONDS:
            return _live_cache["vehicles"], _live_cache["trip_map"]

    vehicles: List[Dict[str, Any]] = []
    trip_map: Dict[str, Dict[str, Any]] = {}

    try:
        vehicles = fetch_live_siri_vm()
        for v in vehicles:
            tid = (v.get("tripId") or "").strip()
            if tid:
                trip_map[tid] = v
    except Exception:
        # don’t crash the app if live feed fails
        vehicles = []
        trip_map = {}

    with _live_lock:
        _live_cache["ts"] = now
        _live_cache["vehicles"] = vehicles
        _live_cache["trip_map"] = trip_map

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


# =========================
# Frontend
# =========================
@app.get("/", response_class=HTMLResponse)
def home():
    # serve root index.html (your repo has index.html in root)
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    # or static/index.html
    if os.path.exists(os.path.join("static", "index.html")):
        return FileResponse(os.path.join("static", "index.html"))
    return HTMLResponse("<h3>index.html not found</h3>", status_code=404)


# =========================
# API endpoints
# =========================
@app.get("/api/health")
def health():
    return {
        "ok": True,
        "gtfsLoaded": GTFS.loaded,
        "tz": APP_TZ,
        "liveConfigured": bool(BODS_API_KEY),
        "liveCacheSeconds": LIVE_REFRESH_SECONDS,
    }


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
            if len(stops) >= 40:
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
            if len(routes) >= 40:
                break

    return {"stops": stops, "routes": routes}


@app.get("/api/stop/{stop_id}")
def stop_info(stop_id: str):
    s = GTFS.stops.get(stop_id)
    if not s:
        raise HTTPException(404, "Stop not found")
    return s


@app.get("/api/route/{route_id}")
def route_info(route_id: str):
    r = GTFS.routes.get(route_id)
    if not r:
        raise HTTPException(404, "Route not found")
    aid = r.get("agency_id") or "default"
    agency_name = (GTFS.agencies.get(aid) or {}).get("agency_name") or ""
    return {
        "route_id": r["route_id"],
        "route_short_name": r.get("route_short_name") or "",
        "route_long_name": r.get("route_long_name") or "",
        "agency_name": agency_name,
    }


def _service_dates_to_consider(now: dt.datetime) -> List[dt.date]:
    # after midnight, some GTFS uses 24:xx+ times from previous service day
    if now.hour < 5:
        return [now.date() - dt.timedelta(days=1), now.date()]
    return [now.date()]


@app.get("/api/stop/{stop_id}/departures")
def stop_departures(
    stop_id: str,
    minutes: int = Query(120, ge=10, le=720),
    limit: int = Query(40, ge=5, le=120),
):
    s = GTFS.stops.get(stop_id)
    if not s:
        raise HTTPException(404, "Stop not found")

    now = now_tz()
    end = now + dt.timedelta(minutes=minutes)

    deps = GTFS.stop_departures_index.get(stop_id, [])
    if not deps:
        return {"stop": s, "departures": []}

    _, live_trip_map = get_live_cached()

    results: List[Dict[str, Any]] = []
    for service_date in _service_dates_to_consider(now):
        midnight = dt.datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)

        for trip_id, dep_s, seq in deps:
            trip = GTFS.trips.get(trip_id)
            if not trip:
                continue

            if not GTFS.service_active(trip.get("service_id") or "", service_date):
                continue

            dep_dt = midnight + dt.timedelta(seconds=int(dep_s))
            if dep_dt < now or dep_dt > end:
                continue

            rid = trip["route_id"]
            route = GTFS.routes.get(rid) or {}
            headsign = trip.get("trip_headsign") or ""
            direction_id = trip.get("direction_id")
            if direction_id is None:
                direction_id = 0

            live = live_trip_map.get(trip_id)

            results.append(
                {
                    "trip_id": trip_id,
                    "route_id": rid,
                    "route_short_name": route.get("route_short_name") or "",
                    "route_long_name": route.get("route_long_name") or "",
                    "headsign": headsign,
                    "direction_id": int(direction_id),
                    "scheduled_departure": dep_dt.isoformat(),
                    "scheduled_hhmm": dep_dt.strftime("%H:%M"),
                    "minutes_to": int((dep_dt - now).total_seconds() // 60),
                    "live": live or None,
                }
            )

            if len(results) >= limit:
                break
        if len(results) >= limit:
            break

    results.sort(key=lambda x: x["scheduled_departure"])
    return {"stop": s, "departures": results[:limit]}


@app.get("/api/route/{route_id}/trips")
def route_upcoming_trips(
    route_id: str,
    direction_id: int = Query(0, ge=0, le=1),
    minutes: int = Query(360, ge=30, le=1440),
    limit: int = Query(30, ge=5, le=100),
):
    r = GTFS.routes.get(route_id)
    if not r:
        raise HTTPException(404, "Route not found")

    now = now_tz()
    end = now + dt.timedelta(minutes=minutes)
    service_dates = _service_dates_to_consider(now)

    _, live_trip_map = get_live_cached()

    out: List[Dict[str, Any]] = []
    for service_date in service_dates:
        midnight = dt.datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)

        for tid, t in GTFS.trips.items():
            if t["route_id"] != route_id:
                continue
            d = t.get("direction_id")
            if d is None:
                d = 0
            if int(d) != int(direction_id):
                continue
            if not GTFS.service_active(t.get("service_id") or "", service_date):
                continue

            meta = GTFS.trip_meta.get(tid) or {}
            start_s = meta.get("start_s")
            if start_s is None:
                continue
            start_dt = midnight + dt.timedelta(seconds=int(start_s))
            if start_dt < now or start_dt > end:
                continue

            origin_stop_id = meta.get("origin_stop_id", "")
            dest_stop_id = meta.get("dest_stop_id", "")
            origin_name = (GTFS.stops.get(origin_stop_id) or {}).get("stop_name") or origin_stop_id
            dest_name = (GTFS.stops.get(dest_stop_id) or {}).get("stop_name") or dest_stop_id

            live = live_trip_map.get(tid)

            out.append(
                {
                    "trip_id": tid,
                    "headsign": t.get("trip_headsign") or dest_name,
                    "direction_id": int(direction_id),
                    "start_time": start_dt.isoformat(),
                    "start_hhmm": start_dt.strftime("%H:%M"),
                    "minutes_to": int((start_dt - now).total_seconds() // 60),
                    "origin_stop_id": origin_stop_id,
                    "origin_stop_name": origin_name,
                    "dest_stop_id": dest_stop_id,
                    "dest_stop_name": dest_name,
                    "shape_id": (t.get("shape_id") or ""),
                    "live": live or None,
                }
            )

    out.sort(key=lambda x: x["start_time"])
    return {"route": {"route_id": r["route_id"], "route_short_name": r.get("route_short_name") or "", "route_long_name": r.get("route_long_name") or ""}, "trips": out[:limit]}


@app.get("/api/trip/{trip_id}")
def trip_detail(trip_id: str):
    t = GTFS.trips.get(trip_id)
    if not t:
        raise HTTPException(404, "Trip not found")

    rid = t["route_id"]
    route = GTFS.routes.get(rid) or {}

    st = GTFS.trip_stop_times.get(trip_id, [])
    if not st:
        raise HTTPException(404, "No stop_times for trip")

    stops_out = []
    for row in st:
        s = GTFS.stops.get(row.stop_id) or {"stop_id": row.stop_id, "stop_name": row.stop_id}
        stops_out.append(
            {
                "stop_id": row.stop_id,
                "stop_name": s.get("stop_name") or row.stop_id,
                "stop_lat": s.get("stop_lat"),
                "stop_lon": s.get("stop_lon"),
                "arrival_s": row.arrival_s,
                "departure_s": row.departure_s,
                "stop_sequence": row.stop_sequence,
            }
        )

    vehicles, trip_map = get_live_cached()
    live = trip_map.get(trip_id)

    meta = GTFS.trip_meta.get(trip_id) or {}
    return {
        "trip_id": trip_id,
        "route_id": rid,
        "route_short_name": route.get("route_short_name") or "",
        "route_long_name": route.get("route_long_name") or "",
        "headsign": t.get("trip_headsign") or "",
        "direction_id": int(t.get("direction_id") or 0),
        "shape_id": (t.get("shape_id") or ""),
        "origin_stop_id": meta.get("origin_stop_id", ""),
        "dest_stop_id": meta.get("dest_stop_id", ""),
        "stops": stops_out,
        "live": live or None,
    }


@app.get("/api/shape/{shape_id}")
def shape(shape_id: str, limit: int = Query(4000, ge=200, le=20000)):
    pts = GTFS.shapes.get(shape_id)
    if not pts:
        raise HTTPException(404, "Shape not found")
    # return as lat/lon pairs
    out = [{"lat": lat, "lon": lon, "seq": seq} for (lat, lon, seq) in pts[:limit]]
    return {"shape_id": shape_id, "points": out}


@app.get("/api/live/vehicles")
def live_vehicles(line: str = Query("", max_length=12)):
    vehicles, _ = get_live_cached()
    if not vehicles:
        return {"vehicles": [], "note": "No live data (or API key missing)."}
    ln = normalize_line(line) if line else ""
    if ln:
        vehicles = [v for v in vehicles if normalize_line(v.get("publishedLineName") or v.get("lineRef") or "") == ln]
    return {"vehicles": vehicles}
