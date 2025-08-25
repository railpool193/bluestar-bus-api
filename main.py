# main.py (fault-tolerant)
import os, io, csv, json, zipfile, time, math
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Tuple
from fastapi import FastAPI, Query, Body
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import requests
import xml.etree.ElementTree as ET

APP_VERSION = "5.1.1"
TZ_NAME = os.getenv("APP_TZ", "Europe/London")
GTFS_DIR = os.getenv("GTFS_DIR", "data/gtfs")
CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

app = FastAPI(title="Bluestar Bus — API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# ---- time helpers
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    TZ = timezone.utc

def now_local() -> datetime:
    return datetime.now(TZ)

def hhmm(sec: int) -> str:
    sec %= 86400
    h = sec // 3600
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

def parse_hhmmss(s: str) -> int:
    try:
        h, m, sec = map(int, s.split(":"))
        return h*3600 + m*60 + sec
    except Exception:
        return 0

def normalize(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

# ---- in-memory GTFS (safe)
STOPS: Dict[str, dict] = {}
ROUTES: Dict[str, dict] = {}
TRIPS: Dict[str, dict] = {}
STOP_TIMES_BY_STOP: Dict[str, List[dict]] = {}
SERVICE_BY_ID: Dict[str, dict] = {}
CALENDAR_DATES: Dict[str, Dict[date, int]] = {}
SHAPES: Dict[str, List[Tuple[float, float]]] = {}

REQ_FILES = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]

def read_csv_safe(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def load_gtfs() -> dict:
    """Safe loader: sosem dob kivételt, csak üres adatokkal dolgozik."""
    global STOPS, ROUTES, TRIPS, STOP_TIMES_BY_STOP, SERVICE_BY_ID, CALENDAR_DATES, SHAPES
    STOPS, ROUTES, TRIPS = {}, {}, {}
    STOP_TIMES_BY_STOP, SERVICE_BY_ID, CALENDAR_DATES, SHAPES = {}, {}, {}, {}

    missing = [f for f in REQ_FILES if not os.path.exists(os.path.join(GTFS_DIR, f))]
    if missing:
        print(f"[GTFS] Missing required files: {missing} (server will run with empty data)")
        return {"ok": False, "missing": missing}

    # stops
    for row in read_csv_safe(os.path.join(GTFS_DIR, "stops.txt")):
        STOPS[row["stop_id"]] = row

    # routes
    for row in read_csv_safe(os.path.join(GTFS_DIR, "routes.txt")):
        ROUTES[row["route_id"]] = row

    # trips
    for row in read_csv_safe(os.path.join(GTFS_DIR, "trips.txt")):
        TRIPS[row["trip_id"]] = row

    # stop_times grouped by stop
    for row in read_csv_safe(os.path.join(GTFS_DIR, "stop_times.txt")):
        sid = row["stop_id"]
        row["_arr_sec"] = parse_hhmmss(row.get("arrival_time","00:00:00"))
        row["_dep_sec"] = parse_hhmmss(row.get("departure_time","00:00:00"))
        STOP_TIMES_BY_STOP.setdefault(sid, []).append(row)
    for sid in STOP_TIMES_BY_STOP:
        STOP_TIMES_BY_STOP[sid].sort(key=lambda r: (r["_dep_sec"], r["trip_id"], int(r.get("stop_sequence") or 0)))

    # calendar (optional)
    for row in read_csv_safe(os.path.join(GTFS_DIR, "calendar.txt")):
        SERVICE_BY_ID[row["service_id"]] = row

    # calendar_dates (optional)
    for row in read_csv_safe(os.path.join(GTFS_DIR, "calendar_dates.txt")):
        from datetime import datetime as _dt
        sid = row["service_id"]
        d = _dt.strptime(row["date"], "%Y%m%d").date()
        CALENDAR_DATES.setdefault(sid, {})[d] = int(row["exception_type"])

    # shapes (optional)
    tmp = {}
    for row in read_csv_safe(os.path.join(GTFS_DIR, "shapes.txt")):
        tmp.setdefault(row["shape_id"], []).append((
            int(row.get("shape_pt_sequence") or 0),
            float(row["shape_pt_lat"]), float(row["shape_pt_lon"])
        ))
    for sid, pts in tmp.items():
        pts.sort(key=lambda x: x[0])
        SHAPES[sid] = [(lat, lon) for _, lat, lon in pts]

    print(f"[GTFS] Loaded stops={len(STOPS)} routes={len(ROUTES)} trips={len(TRIPS)}")
    return {"ok": True}

LOAD_INFO = load_gtfs()

def service_active_today(service_id: str) -> bool:
    # Ha nincs calendar => engedjük (ne legyen üres)
    if not SERVICE_BY_ID and not CALENDAR_DATES:
        return True
    from datetime import datetime as _dt
    today = now_local().date()
    if service_id in CALENDAR_DATES and today in CALENDAR_DATES[service_id]:
        return CALENDAR_DATES[service_id][today] == 1
    row = SERVICE_BY_ID.get(service_id)
    if not row: return True
    wd = today.weekday()
    weekdays = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
    if row.get(weekdays[wd], "0") != "1": return False
    start = _dt.strptime(row["start_date"], "%Y%m%d").date()
    end   = _dt.strptime(row["end_date"], "%Y%m%d").date()
    return start <= today <= end

# ---- live feed (safe)
LIVE_CFG_PATH = os.path.join(CACHE_DIR, "live_config.json")
LIVE_CACHE_PATH = os.path.join(CACHE_DIR, "live_cache.json")
LIVE_CACHE_TTL = 20

def load_live_config() -> dict:
    try:
        if os.path.exists(LIVE_CFG_PATH):
            return json.load(open(LIVE_CFG_PATH, "r", encoding="utf-8"))
    except Exception as e:
        print("[LIVE] cfg read error:", e)
    return {"feed_url": ""}

def save_live_config(cfg: dict):
    try:
        json.dump(cfg, open(LIVE_CFG_PATH, "w", encoding="utf-8"))
    except Exception as e:
        print("[LIVE] cfg write error:", e)

def fetch_live() -> dict:
    # cache
    try:
        if os.path.exists(LIVE_CACHE_PATH):
            st = os.stat(LIVE_CACHE_PATH)
            if time.time() - st.st_mtime <= LIVE_CACHE_TTL:
                return json.load(open(LIVE_CACHE_PATH, "r", encoding="utf-8"))
    except Exception:
        pass

    url = (load_live_config().get("feed_url") or "").strip()
    if not url:
        data = {"ok": False, "vehicles": [], "ts": int(time.time()), "error":"feed not configured"}
        json.dump(data, open(LIVE_CACHE_PATH, "w", encoding="utf-8"))
        return data

    try:
        r = requests.get(url, timeout=25)
        r.raise_for_status()
        content = r.content
        # ZIP vagy XML
        if r.headers.get("Content-Type","").startswith("application/zip") or content[:2]==b"PK":
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                name = next(n for n in zf.namelist() if n.lower().endswith(".xml"))
                xml_bytes = zf.read(name)
        else:
            xml_bytes = content

        root = ET.fromstring(xml_bytes)
        ns = {"s": root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
        vehicles = []
        for va in root.findall(".//{*}VehicleActivity", ns):
            mvj = va.find(".//{*}MonitoredVehicleJourney", ns)
            if mvj is None: continue
            line = (mvj.findtext(".//{*}LineRef", namespaces=ns) or "").strip()
            dest = (mvj.findtext(".//{*}DestinationName", namespaces=ns) or "").strip()
            vr = (mvj.findtext(".//{*}VehicleRef", namespaces=ns) or "").strip()
            dref = (mvj.findtext(".//{*}DirectionRef", namespaces=ns) or "").strip()
            loc = mvj.find(".//{*}VehicleLocation", ns)
            lat = float(loc.findtext(".//{*}Latitude", default="0", namespaces=ns) or 0) if loc is not None else 0
            lon = float(loc.findtext(".//{*}Longitude", default="0", namespaces=ns) or 0) if loc is not None else 0
            call = mvj.find(".//{*}MonitoredCall", ns)
            stop_ref = call.findtext(".//{*}StopPointRef", default="", namespaces=ns) if call is not None else ""
            exp = call.findtext(".//{*}ExpectedArrivalTime", default="", namespaces=ns) if call is not None else ""
            aim = call.findtext(".//{*}AimedArrivalTime", default="", namespaces=ns) if call is not None else ""

            def to_epoch(dt: str) -> Optional[int]:
                if not dt: return None
                try:
                    return int(datetime.fromisoformat(dt.replace("Z","+00:00")).timestamp())
                except Exception:
                    return None

            vehicles.append({
                "vehicle_ref": vr, "line": line, "direction": dref, "dest": dest,
                "lat": lat, "lon": lon, "stop_ref": stop_ref,
                "expected_epoch": to_epoch(exp), "aimed_epoch": to_epoch(aim),
            })

        data = {"ok": True, "vehicles": vehicles, "ts": int(time.time())}
        json.dump(data, open(LIVE_CACHE_PATH, "w", encoding="utf-8"))
        return data
    except Exception as e:
        data = {"ok": False, "vehicles": [], "ts": int(time.time()), "error": str(e)}
        json.dump(data, open(LIVE_CACHE_PATH, "w", encoding="utf-8"))
        return data

# ---- API
@app.get("/", response_class=JSONResponse)
def root():
    return {"detail": "Open /index.html"}

@app.get("/index.html", response_class=HTMLResponse)
def serve_index():
    p = "index.html"
    if os.path.exists(p): return FileResponse(p, media_type="text/html; charset=utf-8")
    return HTMLResponse("<h1>index.html missing</h1>", status_code=404)

@app.get("/api/status")
def api_status():
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": str(int(time.time())),
        "time": now_local().strftime("%H:%M:%S"),
        "tz": TZ_NAME,
        "live_feed_configured": bool(load_live_config().get("feed_url")),
        "gtfs_dir": GTFS_DIR,
        "gtfs_ready": not LOAD_INFO.get("missing"),
        "gtfs_stops": len(STOPS),
    }

@app.post("/api/reload-gtfs")
def reload_gtfs():
    info = load_gtfs()
    return {"ok": True, **info, "stops": len(STOPS)}

@app.get("/api/live/config")
def get_live_cfg():
    return load_live_config()

@app.post("/api/live/config")
def set_live_cfg(cfg: dict = Body(...)):
    feed_url = (cfg.get("feed_url") or "").strip()
    save_live_config({"feed_url": feed_url})
    try:
        if os.path.exists(LIVE_CACHE_PATH): os.remove(LIVE_CACHE_PATH)
    except Exception: pass
    return {"ok": True, "feed_url": feed_url}

@app.get("/api/stops/search")
def stops_search(q: str = Query(..., min_length=1)):
    if not STOPS:
        return {"results": []}  # fontos: ne 500-azzon, üres lista legyen
    qn = normalize(q)
    out = []
    for sid, s in STOPS.items():
        name = s.get("stop_name","")
        code = s.get("stop_code","")
        if qn in normalize(name) or (code and qn in normalize(code)):
            out.append({
                "stop_id": sid, "name": name, "code": code,
                "lat": float(s.get("stop_lat") or 0), "lon": float(s.get("stop_lon") or 0),
            })
            if len(out) >= 25: break
    return {"results": out}

@app.get("/api/routes/search")
def routes_search(q: str = Query(..., min_length=1)):
    if not ROUTES:
        return {"results": []}
    qn = normalize(q)
    out = []
    for rid, r in ROUTES.items():
        short = r.get("route_short_name","") or ""
        longn = r.get("route_long_name","") or ""
        if qn in normalize(short) or qn in normalize(longn):
            out.append({"route_id": rid, "short_name": short, "long_name": longn, "type": r.get("route_type","")})
            if len(out) >= 25: break
    return {"results": out}

@app.get("/api/departures")
def departures(stopId: str = Query(..., alias="stopId"), lookahead_mins: int = Query(60, ge=5, le=360)):
    stop = STOPS.get(stopId)
    if not stop:
        return {"stop": {"stop_id": stopId}, "now_epoch": int(now_local().timestamp()), "items": []}

    now_dt = now_local()
    now_sec = now_dt.hour*3600 + now_dt.minute*60 + now_dt.second
    window = now_sec + lookahead_mins*60

    # live map
    live = fetch_live()
    live_map = {}
    stop_ref_candidates = {c for c in [stop.get("stop_code","").strip(), stopId] if c}
    if live.get("ok"):
        for v in live.get("vehicles", []):
            line = (v.get("line") or "").strip()
            dest = (v.get("dest") or "").strip()
            stop_ref = (v.get("stop_ref") or "").strip()
            expected = v.get("expected_epoch")
            if line and stop_ref and expected and stop_ref in stop_ref_candidates:
                key = (normalize(line), normalize(dest), stop_ref)
                if key not in live_map or expected < live_map[key]:
                    live_map[key] = expected

    out = []
    for row in STOP_TIMES_BY_STOP.get(stopId, []):
        trip = TRIPS.get(row["trip_id"], {})
        rid = trip.get("route_id","")
        route = ROUTES.get(rid, {})
        svc = trip.get("service_id","")
        if not service_active_today(svc): continue

        dep_sec = row["_dep_sec"]
        if not (now_sec <= dep_sec <= window or (dep_sec >= 86400 and dep_sec-86400 <= window)): continue

        midnight = int(now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        scheduled_epoch = midnight + dep_sec
        if dep_sec >= 86400: scheduled_epoch -= 86400

        short = route.get("route_short_name","")
        headsign = trip.get("trip_headsign","") or route.get("route_long_name","")
        predicted_epoch = None
        for ref in stop_ref_candidates:
            k = (normalize(short), normalize(headsign), ref)
            if k in live_map:
                predicted_epoch = live_map[k]; break

        delay = (predicted_epoch - scheduled_epoch) if predicted_epoch else None
        eta = (predicted_epoch or scheduled_epoch) - int(now_dt.timestamp())
        out.append({
            "route": short, "headsign": headsign, "trip_id": row["trip_id"],
            "scheduled_time": hhmm(dep_sec), "scheduled_epoch": scheduled_epoch,
            "predicted_epoch": predicted_epoch, "delay_sec": delay,
            "live": bool(predicted_epoch), "due": eta <= 60,
        })

    out.sort(key=lambda x: x["predicted_epoch"] or x["scheduled_epoch"])
    return {"stop": {
                "stop_id": stopId, "name": stop.get("stop_name",""), "code": stop.get("stop_code",""),
                "lat": float(stop.get("stop_lat") or 0), "lon": float(stop.get("stop_lon") or 0),
            },
            "now_epoch": int(now_dt.timestamp()), "items": out }

@app.get("/api/trip")
def trip_detail(tripId: str = Query(...)):
    trip = TRIPS.get(tripId)
    if not trip: return JSONResponse({"detail":"trip not found"}, status_code=404)
    rid = trip.get("route_id","")
    route = ROUTES.get(rid, {})
    # collect stops
    stops_for_trip = []
    for sid, rows in STOP_TIMES_BY_STOP.items():
        for r in rows:
            if r["trip_id"] == tripId:
                s = STOPS.get(sid, {})
                stops_for_trip.append({
                    "stop_id": sid, "name": s.get("stop_name",""), "code": s.get("stop_code",""),
                    "lat": float(s.get("stop_lat") or 0), "lon": float(s.get("stop_lon") or 0),
                    "arr": r.get("arrival_time",""), "dep": r.get("departure_time",""),
                    "arr_sec": r.get("_arr_sec",0), "dep_sec": r.get("_dep_sec",0),
                    "seq": int(r.get("stop_sequence") or 0),
                })
    stops_for_trip.sort(key=lambda x: x["seq"])

    shape_pts = []
    sid = trip.get("shape_id")
    if sid and sid in SHAPES: shape_pts = [{"lat":lat,"lon":lon} for (lat,lon) in SHAPES[sid]]
    return {"trip_id": tripId,
            "route": {"route_id": rid, "short_name": route.get("route_short_name",""), "long_name": route.get("route_long_name","")},
            "headsign": trip.get("trip_headsign",""), "stops": stops_for_trip, "shape": shape_pts}

@app.get("/api/vehicles")
def vehicles(route: Optional[str] = Query(None)):
    live = fetch_live()
    if not live.get("ok"):
        return {"ok": False, "vehicles": [], "error": live.get("error")}
    vs = live["vehicles"]
    if route:
        rn = normalize(route)
        vs = [v for v in vs if normalize(v.get("line","")) == rn]
    return {"ok": True, "ts": live["ts"], "vehicles": vs}
