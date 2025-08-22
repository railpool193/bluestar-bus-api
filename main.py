# main.py
import io
import json
import csv
import zipfile
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import asyncio

import httpx
from fastapi import FastAPI, Query, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import pytz

APP_TZ = pytz.timezone("Europe/London")
DATA_DIR = Path("data")
GTFS_ZIP = DATA_DIR / "gtfs.zip"
GTFS_DIR = DATA_DIR / "gtfs"
LIVE_CONFIG_FILE = DATA_DIR / "live_config.json"

app = FastAPI(title="Bluestar Bus — API", version="5.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ---- storage ----
class LiveConfig(BaseModel):
    feed_url: Optional[str] = None  # SIRI-VM JSON feed (BODS)
    refresh_seconds: int = 10
    preview_minutes: int = 60

def read_live_config() -> LiveConfig:
    if LIVE_CONFIG_FILE.exists():
        try:
            return LiveConfig.parse_obj(json.loads(LIVE_CONFIG_FILE.read_text()))
        except Exception:
            pass
    return LiveConfig()

def write_live_config(cfg: LiveConfig) -> None:
    LIVE_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LIVE_CONFIG_FILE.write_text(cfg.json(indent=2, ensure_ascii=False))

LIVE = read_live_config()

# ---- very-lightweight GTFS cache ----
GTFS: Dict[str, List[Dict[str, str]]] = {}
GTFS_READY = False

def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def _read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
    with zf.open(name) as fp:
        data = fp.read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(data)))

def load_gtfs() -> bool:
    global GTFS, GTFS_READY
    try:
        if GTFS_ZIP.exists():
            with zipfile.ZipFile(GTFS_ZIP) as z:
                for name in ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt", "shapes.txt", "calendar.txt"]:
                    if name in z.namelist():
                        GTFS[name] = _read_csv_from_zip(z, name)
        elif GTFS_DIR.exists():
            for name in ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt", "shapes.txt", "calendar.txt"]:
                p = GTFS_DIR / name
                if p.exists():
                    GTFS[name] = _read_csv(p)
        GTFS_READY = "stops.txt" in GTFS and "routes.txt" in GTFS and "trips.txt" in GTFS and "stop_times.txt" in GTFS
        return GTFS_READY
    except Exception:
        GTFS_READY = False
        return False

load_gtfs()

def now_local() -> datetime:
    return datetime.now(tz=APP_TZ)

# ---- models for API ----
class StopOut(BaseModel):
    id: str
    name: str

class DepartureOut(BaseModel):
    route: str
    headsign: Optional[str]
    trip_id: str
    time: str           # HH:mm
    live: bool = False  # élő?
    due: bool = False   # 0-1 perc -> DUE
    delay_min: Optional[int] = None

class TripStopOut(BaseModel):
    time: Optional[str] = None
    stop_id: str
    stop_name: str

class TripOut(BaseModel):
    trip_id: str
    headsign: Optional[str]
    route: Optional[str]
    stops: List[TripStopOut]

class ShapeOut(BaseModel):
    coordinates: List[List[float]]  # [[lat,lon], ...]

class VehicleOut(BaseModel):
    id: str
    lat: float
    lon: float
    bearing: Optional[float] = None
    route: Optional[str] = None
    trip_id: Optional[str] = None
    updated: Optional[str] = None

# ---- helpers ----
def _gtfs_index(table: str, key: str) -> Dict[str, Dict[str, str]]:
    return {row[key]: row for row in GTFS.get(table, [])}

STOPS_BY_ID = _gtfs_index("stops.txt", "stop_id")
ROUTES_BY_ID = _gtfs_index("routes.txt", "route_id")
TRIPS_BY_ID = _gtfs_index("trips.txt", "trip_id")

def _fmt_hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")

# naive calendar check (if no calendar.txt, assume every day)
_service_ok_cache: Dict[str, bool] = {}
def _service_runs_today(service_id: str, today: datetime) -> bool:
    if "calendar.txt" not in GTFS:
        return True
    key = f"{service_id}:{today.date().isoformat()}"
    if key in _service_ok_cache:
        return _service_ok_cache[key]
    w = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][today.weekday()]
    ok = False
    for row in GTFS["calendar.txt"]:
        if row["service_id"] == service_id:
            sd = datetime.strptime(row["start_date"], "%Y%m%d").date()
            ed = datetime.strptime(row["end_date"], "%Y%m%d").date()
            if sd <= today.date() <= ed and row.get(w, "0") == "1":
                ok = True
                break
    _service_ok_cache[key] = ok
    return ok

# ---- endpoints ----
@app.get("/api/status")
async def api_status():
    return {
        "ok": True,
        "version": app.version,
        "build": int(now_local().timestamp()),
        "time": now_local().strftime("%H:%M:%S"),
        "tz": str(APP_TZ),
        "live_feed_configured": bool(read_live_config().feed_url),
        "gtfs_stops": len(GTFS.get("stops.txt", [])),
    }

@app.get("/api/live/config", response_model=LiveConfig)
async def get_live_cfg():
    return read_live_config()

@app.post("/api/live/config", response_model=LiveConfig)
async def set_live_cfg(cfg: LiveConfig = Body(...)):
    write_live_config(cfg)
    return cfg

@app.get("/api/live/stop-search", response_model=List[StopOut])
async def stop_search(q: str = Query(..., min_length=2)):
    if not GTFS_READY:
        raise HTTPException(503, "GTFS not loaded")
    ql = q.lower()
    out = []
    for s in GTFS["stops.txt"]:
        name = s.get("stop_name", "")
        if ql in name.lower():
            out.append(StopOut(id=s["stop_id"], name=name))
        if len(out) >= 50:
            break
    return out

@app.get("/api/route/search")
async def route_search(q: str = Query(..., min_length=1)):
    if not GTFS_READY:
        raise HTTPException(503, "GTFS not loaded")
    ql = q.lower()
    res = []
    for r in GTFS["routes.txt"]:
        key = " ".join([r.get("route_short_name",""), r.get("route_long_name","")]).strip()
        if ql in key.lower():
            res.append({
                "route_id": r["route_id"],
                "short_name": r.get("route_short_name"),
                "long_name": r.get("route_long_name"),
            })
        if len(res) >= 50: break
    return res

@app.get("/api/route/trip", response_model=TripOut)
async def route_trip(trip_id: str):
    if not GTFS_READY:
        raise HTTPException(503, "GTFS not loaded")
    trip = TRIPS_BY_ID.get(trip_id)
    if not trip:
        raise HTTPException(404, "trip not found")
    out_stops: List[TripStopOut] = []
    for st in sorted([x for x in GTFS["stop_times.txt"] if x["trip_id"] == trip_id],
                     key=lambda r: (int(r.get("stop_sequence","0")))):
        stop = STOPS_BY_ID.get(st["stop_id"], {})
        out_stops.append(TripStopOut(
            time=(st.get("departure_time") or st.get("arrival_time")),
            stop_id=st["stop_id"],
            stop_name=stop.get("stop_name","")
        ))
    route = ROUTES_BY_ID.get(trip.get("route_id",""), {})
    return TripOut(
        trip_id=trip_id,
        headsign=trip.get("trip_headsign"),
        route=route.get("route_short_name") or route.get("route_long_name"),
        stops=out_stops
    )

@app.get("/api/route/shape", response_model=ShapeOut)
async def route_shape(trip_id: str):
    if not GTFS_READY:
        raise HTTPException(503, "GTFS not loaded")
    trip = TRIPS_BY_ID.get(trip_id)
    if not trip:
        raise HTTPException(404, "trip not found")
    shape_id = trip.get("shape_id")
    coords: List[List[float]] = []
    if shape_id and "shapes.txt" in GTFS:
        pts = [r for r in GTFS["shapes.txt"] if r["shape_id"] == shape_id]
        pts.sort(key=lambda r: int(r.get("shape_pt_sequence","0")))
        for p in pts:
            coords.append([float(p["shape_pt_lat"]), float(p["shape_pt_lon"])])
    return ShapeOut(coordinates=coords)

@app.get("/api/live/departures", response_model=List[DepartureOut])
async def live_departures(stopId: str = Query(...), minutes: int = Query(60, ge=5, le=180)):
    if not GTFS_READY:
        raise HTTPException(503, "GTFS not loaded")
    now = now_local()
    limit_time = now + timedelta(minutes=minutes)
    # gyűjtjük a stop_id-hoz tartozó trip-id-ket az adott napra
    trips_here = [r for r in GTFS["stop_times.txt"] if r["stop_id"] == stopId]
    out: List[DepartureOut] = []
    for st in trips_here:
        trip = TRIPS_BY_ID.get(st["trip_id"]); 
        if not trip: 
            continue
        if not _service_runs_today(trip.get("service_id",""), now):
            continue
        ttext = (st.get("departure_time") or st.get("arrival_time") or "")
        if not ttext: 
            continue
        # HH:MM:SS → a mai napon
        try:
            h,m,s = [int(x) for x in ttext.split(":")]
            sched = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=h, minutes=m, seconds=s)
        except Exception:
            continue
        if now <= sched <= limit_time:
            route = ROUTES_BY_ID.get(trip.get("route_id",""), {})
            out.append(DepartureOut(
                route=(route.get("route_short_name") or route.get("route_long_name") or "?"),
                headsign=trip.get("trip_headsign"),
                trip_id=trip["trip_id"],
                time=_fmt_hhmm(sched),
                live=False, due=(0 <= (sched-now).total_seconds() <= 60),
            ))
    # TODO: Live összevetés (SIRI-VM ETA) – ha sikerül VM-t olvasni, akkor állítjuk live/delay-t
    out.sort(key=lambda d: d.time)
    return out[:50]

@app.get("/api/live/vehicles", response_model=List[VehicleOut])
async def live_vehicles(route_id: Optional[str] = None):
    cfg = read_live_config()
    if not cfg.feed_url:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(cfg.feed_url)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return []
    # nagyon egyszerű SIRI-VM JSON parser (BODS feed)
    vehicles: List[VehicleOut] = []
    try:
        deliveries = (
            data.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("VehicleMonitoringDelivery", [])
        )
        for d in deliveries:
            for mvj in d.get("VehicleActivity", []):
                mon = mvj.get("MonitoredVehicleJourney", {})
                line = mon.get("LineRef")
                tid = (mon.get("FramedVehicleJourneyRef") or {}).get("DatedVehicleJourneyRef") \
                      or mon.get("VehicleJourneyRef")
                loc = mon.get("VehicleLocation", {})
                lat = loc.get("Latitude"); lon = loc.get("Longitude")
                if lat is None or lon is None: 
                    continue
                if route_id and str(line) != str(route_id):
                    # ha route_id route_short_name volt, engedjük át – ne szűrjünk agresszívan
                    pass
                vehicles.append(VehicleOut(
                    id=str(mvj.get("VehicleRef") or mvj.get("ItemIdentifier") or tid or f"{lat},{lon}"),
                    lat=float(lat), lon=float(lon),
                    bearing=mon.get("Bearing"),
                    route=str(line) if line else None,
                    trip_id=str(tid) if tid else None,
                    updated=d.get("ResponseTimestamp")
                ))
    except Exception:
        return []
    return vehicles

# ---- static: serve root / (index.html + assets in /static) ----
app.mount("/static", StaticFiles(directory="static", html=False), name="static")

@app.get("/")
async def root():
    # kis kényelmi redirect az index.html-re
    return {"detail": "Open /index.html"}
