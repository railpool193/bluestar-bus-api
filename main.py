import os
import io
import csv
import time
import zipfile
import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta, timezone

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

APP_TZ = os.getenv("APP_TZ", "Europe/London")
TZ = ZoneInfo(APP_TZ) if ZoneInfo else timezone.utc

GTFS_ZIP_PATH = os.getenv("GTFS_ZIP_PATH", "gtfs.zip")

DFT_API_KEY = os.getenv("DFT_API_KEY", "").strip()
DFT_VM_URL = os.getenv("DFT_VM_URL", "https://data.bus-data.dft.gov.uk/api/v1/datafeed/").strip()
DFT_OPERATOR_REF = os.getenv("DFT_OPERATOR_REF", "BLUS").strip()

LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "12"))
LIVE_HTTP_TIMEOUT = float(os.getenv("LIVE_HTTP_TIMEOUT", "12"))

# Optional bounding box (minLon,minLat,maxLon,maxLat) – ha üres, akkor nincs bbox szűrés
DFT_VM_BBOX = os.getenv("DFT_VM_BBOX", "").strip()

# ------------------------
# Small helpers
# ------------------------

def now_local() -> datetime:
    return datetime.now(TZ)

def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def strip_ns(tag: str) -> str:
    # "{namespace}Tag" -> "Tag"
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(str(x).strip())
    except Exception:
        return default

def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return default

def read_text_file_from_zip(zf: zipfile.ZipFile, filename: str) -> str:
    with zf.open(filename) as f:
        return f.read().decode("utf-8-sig", errors="replace")

def gtfs_time_to_seconds(t: str) -> int:
    # "25:10:00" is allowed in GTFS
    hh, mm, ss = t.split(":")
    return int(hh) * 3600 + int(mm) * 60 + int(ss)

def seconds_to_hhmm(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    return f"{hh:02d}:{mm:02d}"

# ------------------------
# GTFS in-memory model
# ------------------------

@dataclass
class Stop:
    stop_id: str
    stop_name: str
    stop_lat: Optional[float]
    stop_lon: Optional[float]
    platform_code: Optional[str]

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
    trip_headsign: str
    direction_id: Optional[int]
    shape_id: Optional[str]

@dataclass
class StopTime:
    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: str
    stop_sequence: int

class GTFS:
    def __init__(self) -> None:
        self.loaded = False
        self.stops: Dict[str, Stop] = {}
        self.routes: Dict[str, Route] = {}
        self.trips: Dict[str, Trip] = {}
        self.stop_times_by_trip: Dict[str, List[StopTime]] = {}
        self.stop_times_by_stop: Dict[str, List[StopTime]] = {}
        self.shapes: Dict[str, List[Tuple[float, float]]] = {}

        # calendar
        self.calendar: Dict[str, Dict[str, Any]] = {}         # service_id -> calendar row
        self.calendar_dates: Dict[Tuple[str, date], int] = {} # (service_id, date) -> exception_type

    def load(self, zip_path: str) -> None:
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"GTFS zip not found: {zip_path}")

        with zipfile.ZipFile(zip_path, "r") as zf:
            # stops
            if "stops.txt" in zf.namelist():
                txt = read_text_file_from_zip(zf, "stops.txt")
                for r in csv.DictReader(io.StringIO(txt)):
                    stop_id = r.get("stop_id", "").strip()
                    if not stop_id:
                        continue
                    name = r.get("stop_name", "").strip()
                    lat = safe_float(r.get("stop_lat"))
                    lon = safe_float(r.get("stop_lon"))
                    platform = (r.get("platform_code") or "").strip() or None
                    self.stops[stop_id] = Stop(stop_id, name, lat, lon, platform)

            # routes
            if "routes.txt" in zf.namelist():
                txt = read_text_file_from_zip(zf, "routes.txt")
                for r in csv.DictReader(io.StringIO(txt)):
                    route_id = r.get("route_id", "").strip()
                    if not route_id:
                        continue
                    short_name = (r.get("route_short_name") or "").strip()
                    long_name = (r.get("route_long_name") or "").strip()
                    self.routes[route_id] = Route(route_id, short_name, long_name)

            # trips
            if "trips.txt" in zf.namelist():
                txt = read_text_file_from_zip(zf, "trips.txt")
                for r in csv.DictReader(io.StringIO(txt)):
                    trip_id = r.get("trip_id", "").strip()
                    if not trip_id:
                        continue
                    route_id = (r.get("route_id") or "").strip()
                    service_id = (r.get("service_id") or "").strip()
                    headsign = (r.get("trip_headsign") or "").strip()
                    direction_id = safe_int(r.get("direction_id"))
                    shape_id = (r.get("shape_id") or "").strip() or None
                    self.trips[trip_id] = Trip(trip_id, route_id, service_id, headsign, direction_id, shape_id)

            # stop_times
            if "stop_times.txt" in zf.namelist():
                txt = read_text_file_from_zip(zf, "stop_times.txt")
                for r in csv.DictReader(io.StringIO(txt)):
                    trip_id = (r.get("trip_id") or "").strip()
                    stop_id = (r.get("stop_id") or "").strip()
                    if not trip_id or not stop_id:
                        continue
                    arr = (r.get("arrival_time") or "").strip()
                    dep = (r.get("departure_time") or "").strip()
                    seq = safe_int(r.get("stop_sequence"), 0) or 0
                    st = StopTime(trip_id, arr, dep, stop_id, seq)

                    self.stop_times_by_trip.setdefault(trip_id, []).append(st)
                    self.stop_times_by_stop.setdefault(stop_id, []).append(st)

                for tid in self.stop_times_by_trip:
                    self.stop_times_by_trip[tid].sort(key=lambda x: x.stop_sequence)
                for sid in self.stop_times_by_stop:
                    self.stop_times_by_stop[sid].sort(key=lambda x: (x.departure_time, x.trip_id, x.stop_sequence))

            # shapes
            if "shapes.txt" in zf.namelist():
                txt = read_text_file_from_zip(zf, "shapes.txt")
                tmp: Dict[str, List[Tuple[int, float, float]]] = {}
                for r in csv.DictReader(io.StringIO(txt)):
                    shape_id = (r.get("shape_id") or "").strip()
                    if not shape_id:
                        continue
                    lat = safe_float(r.get("shape_pt_lat"))
                    lon = safe_float(r.get("shape_pt_lon"))
                    seq = safe_int(r.get("shape_pt_sequence"), 0) or 0
                    if lat is None or lon is None:
                        continue
                    tmp.setdefault(shape_id, []).append((seq, lat, lon))
                for sid, pts in tmp.items():
                    pts.sort(key=lambda x: x[0])
                    self.shapes[sid] = [(lat, lon) for _, lat, lon in pts]

            # calendar
            if "calendar.txt" in zf.namelist():
                txt = read_text_file_from_zip(zf, "calendar.txt")
                for r in csv.DictReader(io.StringIO(txt)):
                    service_id = (r.get("service_id") or "").strip()
                    if not service_id:
                        continue
                    self.calendar[service_id] = r

            if "calendar_dates.txt" in zf.namelist():
                txt = read_text_file_from_zip(zf, "calendar_dates.txt")
                for r in csv.DictReader(io.StringIO(txt)):
                    service_id = (r.get("service_id") or "").strip()
                    ds = (r.get("date") or "").strip()
                    et = safe_int(r.get("exception_type"), 0) or 0
                    if not service_id or not ds:
                        continue
                    d = datetime.strptime(ds, "%Y%m%d").date()
                    self.calendar_dates[(service_id, d)] = et

        self.loaded = True

    def service_active(self, service_id: str, d: date) -> bool:
        # calendar_dates override first
        ex = self.calendar_dates.get((service_id, d))
        if ex == 1:
            return True
        if ex == 2:
            return False

        row = self.calendar.get(service_id)
        if not row:
            # ha nincs calendar, akkor csak calendar_dates alapján tudnánk – itt false
            return False

        start = datetime.strptime(row["start_date"], "%Y%m%d").date()
        end = datetime.strptime(row["end_date"], "%Y%m%d").date()
        if d < start or d > end:
            return False

        weekday = d.weekday()  # Mon=0..Sun=6
        keys = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
        return (row.get(keys[weekday], "0").strip() == "1")

GTFS_DB = GTFS()

# ------------------------
# LIVE cache
# ------------------------

LIVE_CACHE: Dict[str, Any] = {
    "last_fetch_epoch": 0.0,
    "last_fetch_iso": None,
    "vehicles": [],
    "error": None,
    "error_detail": None,
}

_live_lock = asyncio.Lock()

async def fetch_dft_siri_vm() -> Dict[str, Any]:
    """
    Fetch DfT Bus Open Data SIRI-VM feed and parse vehicles.
    Always return a dict with keys: ok, status, vehicles, error, error_detail.
    """
    if not DFT_API_KEY:
        return {
            "ok": False,
            "status": None,
            "vehicles": [],
            "error": "Missing DFT_API_KEY",
            "error_detail": None,
        }

    url = DFT_VM_URL.rstrip("/") + "/"
    params = {
        "operatorRef": DFT_OPERATOR_REF,
        "api_key": DFT_API_KEY,  # query param (DfT gyakran ezt kéri)
    }

    # Optional bbox: "minLon,minLat,maxLon,maxLat"
    if DFT_VM_BBOX:
        params["boundingBox"] = DFT_VM_BBOX

    # Headeres kulcs (van, ahol ezt kéri)
    headers = {
        "User-Agent": "BluestarLive/1.0",
        "Ocp-Apim-Subscription-Key": DFT_API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=LIVE_HTTP_TIMEOUT) as client:
            r = await client.get(url, params=params, headers=headers)
            status = r.status_code
            text = r.text

        if status != 200:
            return {
                "ok": False,
                "status": status,
                "vehicles": [],
                "error": f"DfT HTTP {status}",
                "error_detail": (text[:500] if text else None),
            }

        vehicles = parse_siri_vm_xml(text, operator_ref=DFT_OPERATOR_REF)

        return {
            "ok": True,
            "status": 200,
            "vehicles": vehicles,
            "error": None,
            "error_detail": None,
        }

    except Exception as e:
        return {
            "ok": False,
            "status": None,
            "vehicles": [],
            "error": "DfT fetch exception",
            "error_detail": str(e),
        }

def parse_siri_vm_xml(xml_text: str, operator_ref: str) -> List[Dict[str, Any]]:
    import xml.etree.ElementTree as ET
    vehicles: List[Dict[str, Any]] = []

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        # néha zip/json jön vissza rossz URL-nél – akkor ezt látod majd error_detail-ben
        return vehicles

    # We walk through all VehicleActivity elements regardless of namespace
    for va in root.iter():
        if strip_ns(va.tag) != "VehicleActivity":
            continue

        mvj = None
        recorded_at = None

        # find RecordedAtTime + MonitoredVehicleJourney under this VehicleActivity
        for child in list(va):
            t = strip_ns(child.tag)
            if t == "RecordedAtTime":
                recorded_at = (child.text or "").strip() or None
            elif t == "MonitoredVehicleJourney":
                mvj = child

        if mvj is None:
            continue

        def mvj_text(name: str) -> Optional[str]:
            for x in mvj.iter():
                if strip_ns(x.tag) == name:
                    return (x.text or "").strip() or None
            return None

        op = mvj_text("OperatorRef")
        if operator_ref and op and op.strip() != operator_ref:
            continue

        line = mvj_text("LineRef") or mvj_text("PublishedLineName")
        direction = mvj_text("DirectionRef")
        destination = mvj_text("DestinationName") or mvj_text("DestinationRef")
        veh_ref = mvj_text("VehicleRef") or mvj_text("VehicleUniqueId")

        lon = None
        lat = None
        for loc in mvj.iter():
            if strip_ns(loc.tag) == "VehicleLocation":
                for ll in list(loc):
                    if strip_ns(ll.tag) == "Longitude":
                        lon = safe_float(ll.text)
                    if strip_ns(ll.tag) == "Latitude":
                        lat = safe_float(ll.text)

        dated_vjr = mvj_text("DatedVehicleJourneyRef")
        framed = None
        for f in mvj.iter():
            if strip_ns(f.tag) == "FramedVehicleJourneyRef":
                framed = f
                break

        data_frame_ref = None
        vehicle_journey_ref = None
        if framed is not None:
            for ff in framed.iter():
                if strip_ns(ff.tag) == "DataFrameRef":
                    data_frame_ref = (ff.text or "").strip() or None
                if strip_ns(ff.tag) == "DatedVehicleJourneyRef":
                    # sometimes repeated; keep outer dated_vjr too
                    pass
                if strip_ns(ff.tag) == "VehicleJourneyRef":
                    vehicle_journey_ref = (ff.text or "").strip() or None

        # Delay – optional, SIRI duration format "PT120S" etc – próbáljuk mp-re
        delay_sec = None
        delay_txt = mvj_text("Delay")
        if delay_txt:
            # rough parse: PT###S
            try:
                if delay_txt.startswith("PT") and delay_txt.endswith("S"):
                    delay_sec = int(delay_txt[2:-1])
            except Exception:
                delay_sec = None

        vehicles.append({
            "vehicle_id": veh_ref,
            "line": line,
            "destination": destination,
            "direction": direction,
            "lat": lat,
            "lon": lon,
            "recorded_at": recorded_at,
            "operator_ref": op,
            "dated_vehicle_journey_ref": dated_vjr,
            "vehicle_journey_ref": vehicle_journey_ref,
            "data_frame_ref": data_frame_ref,
            "delay_sec": delay_sec,
        })

    # sanity: drop ones without coordinates
    vehicles = [v for v in vehicles if v.get("lat") is not None and v.get("lon") is not None]
    return vehicles

async def get_live_cache(force: bool = False) -> Dict[str, Any]:
    async with _live_lock:
        now = time.time()
        age = now - float(LIVE_CACHE.get("last_fetch_epoch") or 0.0)
        if (not force) and LIVE_CACHE["vehicles"] and age < LIVE_CACHE_TTL_SEC:
            return LIVE_CACHE

        result = await fetch_dft_siri_vm()
        LIVE_CACHE["last_fetch_epoch"] = now
        LIVE_CACHE["last_fetch_iso"] = now_local().isoformat()
        LIVE_CACHE["vehicles"] = result.get("vehicles", [])
        LIVE_CACHE["error"] = result.get("error")
        LIVE_CACHE["error_detail"] = result.get("error_detail")
        return LIVE_CACHE

# ------------------------
# FastAPI app
# ------------------------

app = FastAPI(title="Bluestar API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # same-originnál mindegy, de mobil webview néha hisztizik
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup() -> None:
    # GTFS load
    if not GTFS_DB.loaded:
        GTFS_DB.load(GTFS_ZIP_PATH)

@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    # index.html-t a filesystemből szolgáljuk ki
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>index.html missing</h1>", status_code=500)

@app.get("/api/health")
async def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "time": now_local().isoformat(),
        "stops": len(GTFS_DB.stops),
        "trips": len(GTFS_DB.trips),
        "dft_key": bool(DFT_API_KEY),
        "dft_vm_url": DFT_VM_URL,
        "operator_ref": DFT_OPERATOR_REF,
        "live_cache_last_fetch_epoch": float(LIVE_CACHE.get("last_fetch_epoch") or 0.0),
        "live_cache_last_fetch_iso": LIVE_CACHE.get("last_fetch_iso"),
        "live_cache_ttl_sec": LIVE_CACHE_TTL_SEC,
        "live_cache_last_error": LIVE_CACHE.get("error"),
    }

@app.get("/api/stop_search")
def stop_search(q: str = Query("", min_length=1), limit: int = 10) -> Dict[str, Any]:
    q2 = q.strip().lower()
    results = []
    for s in GTFS_DB.stops.values():
        if q2 in s.stop_name.lower() or q2 in s.stop_id.lower():
            results.append({
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "lat": s.stop_lat,
                "lon": s.stop_lon,
            })
        if len(results) >= limit:
            break
    return {"count": len(results), "stops": results}

@app.get("/api/stop/{stop_id}/departures")
def stop_departures(stop_id: str, minutes: int = 60) -> Dict[str, Any]:
    if stop_id not in GTFS_DB.stops:
        raise HTTPException(status_code=404, detail="Stop not found")

    now = now_local()
    d = now.date()
    now_sec = now.hour * 3600 + now.minute * 60 + now.second
    end_sec = now_sec + minutes * 60

    deps = []
    for st in GTFS_DB.stop_times_by_stop.get(stop_id, []):
        trip = GTFS_DB.trips.get(st.trip_id)
        if not trip:
            continue
        if not GTFS_DB.service_active(trip.service_id, d):
            continue

        dep_sec = gtfs_time_to_seconds(st.departure_time)
        # only within [now..end]
        if dep_sec < now_sec or dep_sec > end_sec:
            continue

        route = GTFS_DB.routes.get(trip.route_id)
        line = (route.short_name if route else "") or ""
        headsign = trip.trip_headsign or (route.long_name if route else "")

        mins = int((dep_sec - now_sec) / 60)
        deps.append({
            "time": st.departure_time[:5],
            "trip_id": st.trip_id,
            "service_date": d.isoformat(),
            "line": line,
            "headsign": headsign,
            "minutes": mins,
        })

    deps.sort(key=lambda x: (x["time"], x["trip_id"]))
    return {
        "stop": {
            "stop_id": stop_id,
            "stop_name": GTFS_DB.stops[stop_id].stop_name,
            "lat": GTFS_DB.stops[stop_id].stop_lat,
            "lon": GTFS_DB.stops[stop_id].stop_lon,
        },
        "now": now.isoformat(),
        "count": len(deps),
        "departures": deps,
    }

def trip_shape_polyline(trip: Trip) -> List[List[float]]:
    if trip.shape_id and trip.shape_id in GTFS_DB.shapes:
        return [[lat, lon] for (lat, lon) in GTFS_DB.shapes[trip.shape_id]]
    # fallback: stop coords
    pts: List[List[float]] = []
    for st in GTFS_DB.stop_times_by_trip.get(trip.trip_id, []):
        s = GTFS_DB.stops.get(st.stop_id)
        if s and s.stop_lat is not None and s.stop_lon is not None:
            pts.append([s.stop_lat, s.stop_lon])
    return pts

@app.get("/api/trip/{trip_id}")
async def trip_view(trip_id: str, service_date: str) -> Dict[str, Any]:
    trip = GTFS_DB.trips.get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    d = parse_date(service_date)
    if not GTFS_DB.service_active(trip.service_id, d):
        # ettől még megmutatjuk a megállókat, csak jelezzük
        active = False
    else:
        active = True

    route = GTFS_DB.routes.get(trip.route_id)
    line = (route.short_name if route else "") or ""
    headsign = trip.trip_headsign or (route.long_name if route else "")

    st_list = GTFS_DB.stop_times_by_trip.get(trip_id, [])
    stops = []
    for st in st_list:
        s = GTFS_DB.stops.get(st.stop_id)
        stops.append({
            "time": st.departure_time[:5],
            "stop_id": st.stop_id,
            "stop_name": (s.stop_name if s else st.stop_id),
            "lat": (s.stop_lat if s else None),
            "lon": (s.stop_lon if s else None),
        })

    # Live match (heurisztika): line + destination/headsign + time proximity
    live_cache = await get_live_cache(force=False)
    vehicles = live_cache.get("vehicles", [])
    match = best_vehicle_match(line=line, headsign=headsign, trip_stops=stops, vehicles=vehicles)

    return {
        "trip": {
            "trip_id": trip.trip_id,
            "service_date": service_date,
            "active": active,
            "line": line,
            "headsign": headsign,
        },
        "shape": trip_shape_polyline(trip),
        "stops": stops,
        "live": {
            "available": bool(vehicles),
            "last_fetch_iso": live_cache.get("last_fetch_iso"),
            "error": live_cache.get("error"),
            "error_detail": live_cache.get("error_detail"),
            "match": match,
        }
    }

def best_vehicle_match(line: str, headsign: str, trip_stops: List[Dict[str, Any]], vehicles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # very simple scoring; you already had "heuristic (50)" – itt is ilyesmi
    if not vehicles:
        return None

    # pick a mid-trip stop coordinate as anchor
    anchor = None
    coords = [(s["lat"], s["lon"]) for s in trip_stops if s.get("lat") is not None and s.get("lon") is not None]
    if coords:
        anchor = coords[len(coords)//2]

    head = (headsign or "").lower().replace("_", " ").strip()

    def score(v: Dict[str, Any]) -> float:
        sc = 0.0
        vline = (v.get("line") or "").strip()
        if line and vline and (vline == line):
            sc += 50
        elif line and vline and (line in vline or vline in line):
            sc += 35

        dest = (v.get("destination") or "").lower().replace("_", " ").strip()
        if head and dest:
            # partial match
            if head == dest:
                sc += 30
            elif head in dest or dest in head:
                sc += 18

        # distance to anchor
        if anchor and v.get("lat") is not None and v.get("lon") is not None:
            lat, lon = float(v["lat"]), float(v["lon"])
            alat, alon = anchor
            # rough distance score (no haversine, enough for ranking)
            dist = abs(lat - alat) + abs(lon - alon)
            sc += max(0.0, 20.0 - dist * 300.0)

        return sc

    best = None
    best_sc = -1e9
    for v in vehicles:
        sc = score(v)
        if sc > best_sc:
            best_sc = sc
            best = v

    if not best:
        return None

    out = dict(best)
    out["score"] = round(best_sc, 1)
    return out

@app.get("/api/vehicles")
async def vehicles(max_results: int = 20, line: Optional[str] = None) -> JSONResponse:
    """
    IMPORTANT:
    - Always returns 200 JSON (even if DfT fails), so the frontend can show the error.
    """
    cache = await get_live_cache(force=False)
    vehicles = cache.get("vehicles", []) or []

    if line:
        line2 = line.strip()
        vehicles = [v for v in vehicles if (v.get("line") or "").strip() == line2]

    vehicles = vehicles[:max_results]

    return JSONResponse({
        "count": len(vehicles),
        "vehicles": vehicles,
        "cached_ttl_sec": LIVE_CACHE_TTL_SEC,
        "last_fetch_iso": cache.get("last_fetch_iso"),
        "error": cache.get("error"),
        "error_detail": cache.get("error_detail"),
    })
