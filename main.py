import io
import os
import csv
import json
import gzip
import math
import zipfile
import asyncio
import datetime as dt
from typing import Dict, List, Optional, Tuple

import uvicorn
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from zoneinfo import ZoneInfo
from pathlib import Path

APP_VERSION = "4.2.0"

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
GTFS_DIR = DATA_DIR / "gtfs"
INDEX_DIR = DATA_DIR / "index"
CONFIG_FILE = DATA_DIR / "live_config.json"
BUILD_FILE = DATA_DIR / "build_id.txt"

for p in [DATA_DIR, GTFS_DIR, INDEX_DIR]:
    p.mkdir(parents=True, exist_ok=True)

if not BUILD_FILE.exists():
    BUILD_FILE.write_text(str(int(dt.datetime.now().timestamp())).strip(), encoding="utf-8")

UK = ZoneInfo("Europe/London")

# --------------------------- Models ---------------------------

class LiveCfg(BaseModel):
    feed_url: str

class StopResult(BaseModel):
    id: str
    name: str

class Departure(BaseModel):
    route: str
    headsign: str
    trip_id: str
    departure: str  # 24h string
    live: bool = False

class TripStop(BaseModel):
    time: str
    stop_name: str
    status: str  # past | live | sched

class TripDetail(BaseModel):
    trip_id: str
    stops: List[TripStop]

class Vehicle(BaseModel):
    id: str
    lat: float
    lon: float
    bearing: Optional[int] = None
    route: Optional[str] = None

# --------------------------- Helpers ---------------------------

def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def _parse_hhmmss(time_str: str) -> Tuple[int, int, int]:
    """Supports times over 24h (e.g. 25:02:00)."""
    hh, mm, ss = time_str.split(":")
    return int(hh), int(mm), int(ss)

def hhmmss_to_dt(service_date: dt.date, time_str: str, tz=UK) -> dt.datetime:
    h, m, s = _parse_hhmmss(time_str)
    base = dt.datetime.combine(service_date, dt.time(0, 0, 0), tzinfo=tz)
    return base + dt.timedelta(hours=h, minutes=m, seconds=s)

def fmt_24h(t: dt.datetime) -> str:
    return t.strftime("%H:%M:%S")

def load_build_id() -> str:
    try:
        return BUILD_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        return "-"

def today_uk() -> dt.datetime:
    return dt.datetime.now(tz=UK)

def service_active(service_id: str, when: dt.date) -> bool:
    """Check calendar + calendar_dates rules."""
    cal = INDEX.get("calendar", {})
    c = cal.get(service_id)
    if not c:
        # If no calendar row, assume active (some feeds do this)
        return True

    start = dt.date.fromisoformat(c["start_date"])
    end = dt.date.fromisoformat(c["end_date"])
    if not (start <= when <= end):
        active = False
    else:
        weekday = when.weekday()  # Mon=0
        weekdays = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
        active = c.get(weekdays[weekday], "0") == "1"

    # exceptions
    for ex in INDEX.get("calendar_dates", []):
        if ex["service_id"] == service_id and ex["date"] == when.strftime("%Y%m%d"):
            if ex["exception_type"] == "1":
                active = True
            elif ex["exception_type"] == "2":
                active = False
    return active

def ensure_indexes():
    """Build simple in-memory indexes from GTFS CSVs."""
    global INDEX
    INDEX = {
        "routes": {},
        "trips": {},
        "stops": {},
        "stop_times_by_stop": {},
        "calendar": {},
        "calendar_dates": [],
        "route_trips": {},
        "trip_stop_times": {}
    }

    files = {
        "routes": GTFS_DIR / "routes.txt",
        "trips": GTFS_DIR / "trips.txt",
        "stops": GTFS_DIR / "stops.txt",
        "stop_times": GTFS_DIR / "stop_times.txt",
        "calendar": GTFS_DIR / "calendar.txt",
        "calendar_dates": GTFS_DIR / "calendar_dates.txt",
    }

    # routes
    for r in read_csv(files["routes"]):
        INDEX["routes"][r["route_id"]] = r

    # trips
    for t in read_csv(files["trips"]):
        INDEX["trips"][t["trip_id"]] = t
        INDEX["route_trips"].setdefault(t["route_id"], []).append(t["trip_id"])

    # stops
    for s in read_csv(files["stops"]):
        INDEX["stops"][s["stop_id"]] = s

    # stop_times
    for st in read_csv(files["stop_times"]):
        sid = st["stop_id"]
        INDEX["stop_times_by_stop"].setdefault(sid, []).append(st)
        INDEX["trip_stop_times"].setdefault(st["trip_id"], []).append(st)

    # sort stop_times per stop by departure_time
    for sid, lst in INDEX["stop_times_by_stop"].items():
        lst.sort(key=lambda x: tuple(map(int, x["departure_time"].split(":"))))
    for tid, lst in INDEX["trip_stop_times"].items():
        lst.sort(key=lambda x: int(x.get("stop_sequence", "0")))

    # calendar
    for c in read_csv(files["calendar"]):
        INDEX["calendar"][c["service_id"]] = {
            **c,
            "start_date": dt.datetime.strptime(c["start_date"], "%Y%m%d").date().isoformat(),
            "end_date": dt.datetime.strptime(c["end_date"], "%Y%m%d").date().isoformat(),
        }
    INDEX["calendar_dates"] = read_csv(files["calendar_dates"])

ensure_indexes()

def fuzzy_stop_search(q: str, limit: int = 20) -> List[StopResult]:
    ql = q.lower().strip()
    hits: List[Tuple[int, Dict[str,str]]] = []
    for s in INDEX["stops"].values():
        name = s.get("stop_name","")
        key = name.lower()
        if ql in key:
            hits.append((0, s))
        else:
            # very light fuzz: split tokens
            score = sum(1 for tok in ql.split() if tok in key)
            if score:
                hits.append((10 - score, s))
    hits.sort(key=lambda x: x[0])
    out = []
    for _, s in hits[:limit]:
        out.append(StopResult(id=s["stop_id"], name=name_with_code(s)))
    return out

def name_with_code(stop: Dict[str,str]) -> str:
    code = stop.get("stop_code") or ""
    nm = stop.get("stop_name") or ""
    if code:
        return f"{nm} ({code})"
    return nm

def load_live_cfg() -> Optional[str]:
    if CONFIG_FILE.exists():
        try:
            obj = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            return obj.get("feed_url")
        except Exception:
            return None
    return None

async def fetch_bods(feed_url: str) -> bytes:
    """Fetch raw body (handles gzip)."""
    timeout = httpx.Timeout(20, connect=10)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(feed_url, headers={"Accept": "*/*"})
        r.raise_for_status()
        data = r.content
        # BODS often gzips (even if content-encoding missing in some cases)
        try:
            return gzip.decompress(data)
        except Exception:
            return data

def parse_siri_vehicles(xml_bytes: bytes) -> List[Vehicle]:
    """Very tolerant SIRI-VM XML parser (works for typical BODS)."""
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []

    ns = {"s": root.tag.split("}")[0].strip("{")}
    out: List[Vehicle] = []

    for mv in root.findall(".//s:VehicleActivity", ns):
        try:
            veh = mv.find(".//s:VehicleRef", ns)
            line = mv.find(".//s:LineRef", ns)
            lat = mv.find(".//s:VehicleLocation/s:Latitude", ns)
            lon = mv.find(".//s:VehicleLocation/s:Longitude", ns)
            br = mv.find(".//s:Bearing", ns)
            if lat is None or lon is None:
                continue
            out.append(
                Vehicle(
                    id=(veh.text if veh is not None else ""),
                    route=(line.text if line is not None else None),
                    lat=float(lat.text),
                    lon=float(lon.text),
                    bearing=int(float(br.text)) if (br is not None and br.text) else None,
                )
            )
        except Exception:
            continue
    return out

VEH_CACHE: Dict[str, Tuple[float, List[Vehicle]]] = {}

async def get_live_vehicles() -> List[Vehicle]:
    """Cache live vehicles for ~15s to reduce BODS load."""
    feed = load_live_cfg()
    if not feed:
        return []
    now = dt.datetime.now().timestamp()
    cached = VEH_CACHE.get("all")
    if cached and (now - cached[0] < 15):
        return cached[1]
    try:
        raw = await fetch_bods(feed)
        vehicles = parse_siri_vehicles(raw)
        VEH_CACHE["all"] = (now, vehicles)
        return vehicles
    except Exception:
        return []

def nearest_vehicle_for_route(vehicles: List[Vehicle], route_short_name: str) -> Optional[Vehicle]:
    # Some BODS feed LineRef equals "BLUS:17" or "17" – normalize digits.
    def norm(x: Optional[str]) -> str:
        if not x:
            return ""
        # keep last token of colon-sep
        token = x.split(":")[-1]
        return token.strip()
    rn = norm(route_short_name)
    for v in vehicles:
        if norm(v.route) == rn:
            return v
    return None

# --------------------------- FastAPI ---------------------------

app = FastAPI(title="Bluestar Bus – API", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

@app.get("/api/status")
async def api_status():
    t = today_uk()
    try:
        feed = load_live_cfg()
    except Exception:
        feed = None
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": load_build_id(),
        "uk_time": t.strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool(feed),
    }

@app.get("/api/live/config")
async def get_live_cfg():
    return {"feed_url": load_live_cfg()}

@app.post("/api/live/config")
async def set_live_cfg(cfg: LiveCfg):
    CONFIG_FILE.write_text(json.dumps(cfg.dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    VEH_CACHE.clear()
    return {"ok": True}

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Please upload a GTFS .zip")
    buf = await file.read()
    try:
        with zipfile.ZipFile(io.BytesIO(buf)) as z:
            # wipe old
            for p in GTFS_DIR.iterdir():
                if p.is_file():
                    p.unlink()
            z.extractall(GTFS_DIR)
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Invalid ZIP")
    ensure_indexes()
    return {"status": "uploaded"}

@app.get("/api/stops/search")
async def api_stops_search(q: str = Query(..., min_length=2, description="Stop name fragment")):
    return [s.dict() for s in fuzzy_stop_search(q)]

@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = Query(60, ge=5, le=720)):
    if stop_id not in INDEX["stops"]:
        raise HTTPException(status_code=404, detail="Unknown stop")
    now = today_uk()
    service_date = now.date()

    stop_times = INDEX["stop_times_by_stop"].get(stop_id, [])
    results: List[Departure] = []

    # optional live
    vehicles = await get_live_vehicles()

    for st in stop_times:
        trip_id = st["trip_id"]
        trip = INDEX["trips"].get(trip_id)
        if not trip:
            continue
        if not service_active(trip["service_id"], service_date):
            continue

        dep_dt = hhmmss_to_dt(service_date, st["departure_time"], tz=UK)
        delta = (dep_dt - now).total_seconds() / 60.0
        if -2 <= delta <= minutes:
            route = INDEX["routes"].get(trip["route_id"], {})
            short = route.get("route_short_name") or route.get("route_id") or "?"
            headsign = trip.get("trip_headsign") or route.get("route_long_name") or ""
            live = False
            nv = nearest_vehicle_for_route(vehicles, short)
            if nv and delta <= 10:
                live = True
            results.append(
                Departure(
                    route=str(short),
                    headsign=headsign,
                    trip_id=trip_id,
                    departure=fmt_24h(dep_dt),
                    live=live,
                )
            )

    # sort by dep time
    results.sort(key=lambda d: d.departure)
    return [r.dict() for r in results]

@app.get("/api/trips/{trip_id}")
async def api_trip_details(trip_id: str):
    trip = INDEX["trips"].get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Unknown trip")
    now = today_uk()
    service_date = now.date()
    lst = INDEX["trip_stop_times"].get(trip_id, [])
    out: List[TripStop] = []
    live_cut = 2  # minutes window for "live now"

    for st in lst:
        t = hhmmss_to_dt(service_date, st["arrival_time"], tz=UK)
        diff_min = (t - now).total_seconds() / 60.0
        if diff_min < -1:
            status = "past"
        elif abs(diff_min) <= live_cut:
            status = "live"
        else:
            status = "sched"
        s = INDEX["stops"].get(st["stop_id"], {})
        out.append(TripStop(time=fmt_24h(t), stop_name=name_with_code(s), status=status))

    return TripDetail(trip_id=trip_id, stops=out).dict()

@app.get("/api/routes/search")
async def api_routes_search(q: str):
    ql = q.lower().strip()
    hits = []
    for r in INDEX["routes"].values():
        rn = (r.get("route_short_name") or r.get("route_id") or "").lower()
        if rn == ql or rn.startswith(ql):
            hits.append({"id": r["route_id"], "label": r.get("route_short_name") or r["route_id"]})
    if not hits:
        for r in INDEX["routes"].values():
            name = (r.get("route_long_name") or "").lower()
            if ql in name:
                hits.append({"id": r["route_id"], "label": r.get("route_short_name") or r["route_id"]})
    hits.sort(key=lambda x: x["label"])
    return hits[:10]

@app.get("/api/routes/{route}/vehicles")
async def api_route_vehicles(route: str):
    vehicles = await get_live_vehicles()
    out = []
    for v in vehicles:
        if not v.route:
            continue
        # normalize like in nearest_vehicle_for_route
        token = v.route.split(":")[-1].strip()
        if token == route or token == route.split(":")[-1]:
            out.append(v.dict())
    return out

# Serve frontend
@app.get("/")
async def index():
    return FileResponse(str(BASE_DIR / "index.html"))


# --------------------------- Run ---------------------------

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
