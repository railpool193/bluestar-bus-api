# main.py
from __future__ import annotations
import csv, io, json, os, zipfile, time, math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import requests
from pydantic import BaseModel

APP_VERSION = "5.0.0"

# ---------- FastAPI ----------
app = FastAPI(title="Bluestar Bus — API", version=APP_VERSION, openapi_url="/api/openapi.json")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)

# serve /static if exists
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ---------- Live config ----------
LIVE_CFG_PATH = os.path.join("data", "live_config.json")
os.makedirs("data", exist_ok=True)

def read_live_cfg() -> dict:
    if os.path.exists(LIVE_CFG_PATH):
        try:
            with open(LIVE_CFG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def write_live_cfg(cfg: dict) -> None:
    tmp = LIVE_CFG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, LIVE_CFG_PATH)

class LiveConfigIn(BaseModel):
    feed_url: str

# ---------- GTFS minimal loader ----------
@dataclass
class Stop:
    stop_id: str
    name: str
    lat: float
    lon: float
    code: Optional[str] = None

@dataclass
class Route:
    route_id: str
    short_name: str
    long_name: str

@dataclass
class Trip:
    trip_id: str
    route_id: str
    headsign: str

# indexes
STOPS: Dict[str, Stop] = {}
ROUTES: Dict[str, Route] = {}
TRIPS: Dict[str, Trip] = {}
# stop_id -> list[(dep_secs, trip_id)]
STOP_DEPS: Dict[str, List[Tuple[int, str]]] = {}

def _parse_time_to_secs(s: str) -> Optional[int]:
    # GTFS can be 24:xx:xx - allow > 24h by modulo days
    try:
        parts = s.split(":")
        h, m = int(parts[0]), int(parts[1])
        sec = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + sec
    except Exception:
        return None

def _load_csv_from_dir(path: str, name: str) -> Optional[List[Dict[str, str]]]:
    p = os.path.join(path, name)
    if not os.path.exists(p):
        return None
    with open(p, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def _load_csv_from_zip(zippath: str, name: str) -> Optional[List[Dict[str, str]]]:
    if not os.path.exists(zippath):
        return None
    with zipfile.ZipFile(zippath) as zf:
        if name not in zf.namelist():
            return None
        with zf.open(name) as f:
            data = f.read().decode("utf-8-sig")
            return list(csv.DictReader(io.StringIO(data)))

def load_gtfs() -> None:
    global STOPS, ROUTES, TRIPS, STOP_DEPS
    STOPS.clear(); ROUTES.clear(); TRIPS.clear(); STOP_DEPS.clear()

    source = None
    if os.path.isdir("gtfs"):
        source = ("dir", "gtfs")
    elif os.path.isdir("data") and os.path.exists("data/GTFS.zip"):
        source = ("zip", "data/GTFS.zip")

    if not source:
        print("[GTFS] No source found (gtfs/ or data/GTFS.zip).")
        return

    loader = _load_csv_from_dir if source[0] == "dir" else _load_csv_from_zip
    base = source[1]

    stops = loader(base, "stops.txt") or []
    for r in stops:
        sid = r.get("stop_id", "").strip()
        if not sid: 
            continue
        STOPS[sid] = Stop(
            stop_id=sid,
            name=(r.get("stop_name") or r.get("stop_desc") or "").strip(),
            lat=float(r.get("stop_lat") or 0),
            lon=float(r.get("stop_lon") or 0),
            code=(r.get("stop_code") or "").strip() or None,
        )

    routes = loader(base, "routes.txt") or []
    for r in routes:
        rid = r.get("route_id", "").strip()
        if not rid:
            continue
        ROUTES[rid] = Route(
            route_id=rid,
            short_name=(r.get("route_short_name") or "").strip(),
            long_name=(r.get("route_long_name") or "").strip()
        )

    trips = loader(base, "trips.txt") or []
    for r in trips:
        tid = r.get("trip_id", "").strip()
        if not tid: 
            continue
        TRIPS[tid] = Trip(
            trip_id=tid, 
            route_id=(r.get("route_id") or "").strip(),
            headsign=(r.get("trip_headsign") or "").strip()
        )

    stop_times = loader(base, "stop_times.txt") or []
    for r in stop_times:
        sid = (r.get("stop_id") or "").strip()
        if not sid: 
            continue
        dep = _parse_time_to_secs(r.get("departure_time") or "")
        tid = (r.get("trip_id") or "").strip()
        if dep is None or not tid:
            continue
        STOP_DEPS.setdefault(sid, []).append((dep, tid))

    # sort by time per stop
    for sid in STOP_DEPS:
        STOP_DEPS[sid].sort(key=lambda x: x[0])

    print(f"[GTFS] Loaded: stops={len(STOPS)}, routes={len(ROUTES)}, trips={len(TRIPS)}, stop_times={sum(len(v) for v in STOP_DEPS.values())}")

load_gtfs()

# ---------- helpers ----------
def build_number(s: str) -> str:
    # for display: prefer route short_name else long_name
    r = ROUTES.get(s)
    if not r: 
        return ""
    return r.short_name or r.long_name or r.route_id

def hhmm_from_secs(sec: int) -> str:
    sec = max(0, sec)
    h = (sec // 3600) % 24
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

def now_secs_local() -> int:
    # seconds since day start (local)
    lt = time.localtime()
    return lt.tm_hour*3600 + lt.tm_min*60 + lt.tm_sec

def safe_substr_match(hay: str, needle: str) -> bool:
    return needle.lower() in (hay or "").lower()

# ---------- SIRI-VM live reader (very defensive) ----------
def _fetch_live_map_for_stop(stop_id: str) -> Dict[str, float]:
    """
    Return map: key="<route_short>-<headsign>" or "<lineName>", value=unix_ts (expected dep)
    Used to annotate departures at this stop.
    """
    cfg = read_live_cfg()
    url = (cfg or {}).get("feed_url")
    if not url:
        return {}
    try:
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        j = r.json()
    except Exception:
        return {}

    out: Dict[str, float] = {}
    try:
        vm = j.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
        for d in vm:
            for a in d.get("VehicleActivity", []) or []:
                mvj = (a.get("MonitoredVehicleJourney") or {})
                line = (mvj.get("PublishedLineName") or mvj.get("LineRef") or "").strip()
                mc = (mvj.get("MonitoredCall") or {})
                sp = (mc.get("StopPointRef") or "").strip()
                if not line or not sp:
                    continue
                if sp != stop_id:
                    continue
                etd = mc.get("ExpectedDepartureTime") or mc.get("AimedDepartureTime")
                if not etd:
                    continue
                # parse ISO time to epoch
                try:
                    # simple parse: 'YYYY-MM-DDTHH:MM:SSZ' or +00:00
                    t = etd.replace("Z","+00:00")
                    from datetime import datetime
                    ts = datetime.fromisoformat(t).timestamp()
                    out_key = line
                    out[out_key] = ts
                except Exception:
                    continue
    except Exception:
        return {}
    return out

# ---------- API ----------
@app.get("/")
def root():
    if os.path.exists("index.html"):
        return RedirectResponse("/index.html")
    return JSONResponse({"detail": "Open /index.html"})

@app.get("/index.html")
def serve_index():
    if os.path.exists("index.html"):
        return FileResponse("index.html", media_type="text/html")
    return JSONResponse({"detail": "Not Found"}, status_code=404)

@app.get("/api/status")
def api_status():
    cfg = read_live_cfg()
    tz = time.tzname[0] if time.daylight == 0 else time.tzname[1]
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": str(int(time.time())),
        "time": time.strftime("%H:%M:%S", time.localtime()),
        "tz": tz,
        "live_feed_configured": bool(cfg.get("feed_url")) if cfg else False,
        "gtfs_stops": len(STOPS),
        "gtfs_routes": len(ROUTES)
    }

@app.get("/api/live/config")
def get_live_config():
    cfg = read_live_cfg()
    return cfg or {}

@app.post("/api/live/config")
def set_live_config(payload: LiveConfigIn = Body(...)):
    cfg = {"feed_url": payload.feed_url.strip()}
    write_live_cfg(cfg)
    return {"ok": True, **cfg}

@app.get("/api/stops/search")
def search_stops(query: str = Query(..., min_length=1)):
    q = query.strip()
    items = []
    for s in STOPS.values():
        if safe_substr_match(s.name, q) or (s.code and safe_substr_match(s.code, q)):
            items.append({
                "id": s.stop_id,
                "name": s.name,
                "code": s.code,
                "lat": s.lat, "lon": s.lon,
            })
            if len(items) >= 30:
                break
    return {"items": items}

@app.get("/api/routes/search")
def search_routes(query: str = Query(..., min_length=1)):
    q = query.strip()
    items = []
    for r in ROUTES.values():
        if safe_substr_match(r.short_name, q) or safe_substr_match(r.long_name, q) or safe_substr_match(r.route_id, q):
            items.append({
                "id": r.route_id,
                "short_name": r.short_name,
                "long_name": r.long_name,
            })
            if len(items) >= 30:
                break
    return {"items": items}

@app.get("/api/departures")
def departures(
    stopId: str = Query(..., alias="stopId"),
    lookahead: int = Query(60, ge=5, le=240),   # minutes
):
    # 1) Scheduled (GTFS)
    now_s = now_secs_local()
    max_sec = now_s + lookahead*60
    sched: List[dict] = []
    for dep_sec, trip_id in STOP_DEPS.get(stopId, []):
        # allow wrap after midnight by adding 24h if needed
        dep_abs = dep_sec
        if dep_abs < now_s:  # same-day wrap handling
            dep_abs += 24*3600
        if dep_abs > max_sec:
            break
        trip = TRIPS.get(trip_id)
        if not trip:
            continue
        r = ROUTES.get(trip.route_id)
        route_no = (r.short_name or r.long_name or r.route_id) if r else ""
        sched.append({
            "time": hhmm_from_secs(dep_abs % (24*3600)),
            "epoch": int(time.time()) - now_s + dep_abs,  # approximate
            "route": route_no,
            "headsign": trip.headsign,
            "trip_id": trip.trip_id,
            "type": "scheduled"
        })
        if len(sched) >= 40:
            break

    # 2) Live overlay (SIRI-VM)
    live_overlay = _fetch_live_map_for_stop(stopId)
    if live_overlay:
        now_epoch = time.time()
        for s in sched:
            key = s["route"]
            if key in live_overlay:
                ts = live_overlay[key]
                mins = (ts - now_epoch) / 60.0
                if mins <= 1.0:
                    s["type"] = "live"
                    s["live_text"] = "DUE"
                elif mins < 120:
                    # replace time with expected hh:mm
                    hhmm = time.strftime("%H:%M", time.localtime(ts))
                    s["type"] = "live"
                    s["live_text"] = hhmm

    return {
        "stop": STOPS.get(stopId).name if stopId in STOPS else stopId,
        "items": sched
    }

# ---------- run ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
