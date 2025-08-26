import os, io, json, time, math, zipfile, csv
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Query, Body, Response, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------
# App & globals
# ---------------------------------------------------------
app = FastAPI(title="Bluestar Bus — API", version="5.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)

STATE: Dict[str, Any] = {
    "build": str(int(time.time())),
    "live_cfg": {"feed_url": os.getenv("LIVE_FEED_URL", "").strip()},
    "gtfs_ready": False,
    "gtfs": {
        "stops": {},        # stop_id -> {stop_id, name, lat, lon}
        "routes": {},       # route_id -> {route_id, route_short_name, route_long_name}
        "trips": {},        # trip_id -> {trip_id, route_id, shape_id, headsign}
        "stop_times": {},   # trip_id -> [ {stop_id, arr, dep, seq} ... ]
        "shapes": {},       # shape_id -> [ {lat, lon, seq} ... ]
        "route2shapes": {}, # route_id -> set(shape_id)
        "index_stop_name": {}
    },
    "live": {"fetched_at": 0.0, "vehicles": []}
}

TZ = timezone.utc

def now_utc() -> datetime:
    return datetime.now(tz=TZ)

def parse_hhmmss(s: str) -> int:
    if not s:
        return 0
    parts = s.split(":")
    while len(parts) < 3:
        parts.append("0")
    h, m, sec = parts[:3]
    return int(h) * 3600 + int(m) * 60 + int(sec)

def parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        # SIRI időpontok ISO formátumban vannak (Z végződés vagy offset)
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return None

def normalize_route(x: Optional[str]) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    for sep in (":", "/"):
        if sep in s:
            s = s.split(sep)[-1]
    if s.startswith("HAA0") and s[4:].isdigit():
        return str(int(s[4:]))
    if s.isdigit():
        return str(int(s))
    return s

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    # méterben
    R = 6371000.0
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2*R*math.asin(math.sqrt(a))

def status_ok():
    return {
        "ok": True,
        "version": app.version,
        "build": STATE["build"],
        "time": now_utc().strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool(STATE["live_cfg"]["feed_url"]),
        "gtfs_dir": "data/gtfs",
        "gtfs_ready": STATE["gtfs_ready"],
        "gtfs_stops": len(STATE["gtfs"]["stops"])
    }

# ---------------------------------------------------------
# Root + index
# ---------------------------------------------------------
@app.get("/", response_class=JSONResponse)
def root():
    return {"detail": "Open /index.html", "docs": "/docs"}

@app.get("/index.html", response_class=PlainTextResponse)
def index_html():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return Response(f.read(), media_type="text/html; charset=utf-8")
    except FileNotFoundError:
        return Response("<h1>index.html missing</h1>", media_type="text/html")

@app.get("/api/status")
def api_status(): return status_ok()

# ---------------------------------------------------------
# Live feed config
# ---------------------------------------------------------
class LiveConfigIn(BaseModel):
    feed_url: str

@app.get("/api/live/config")
def get_live_cfg(): return STATE["live_cfg"]

@app.post("/api/live/config")
def set_live_cfg(cfg: LiveConfigIn):
    STATE["live_cfg"]["feed_url"] = cfg.feed_url.strip()
    return {"ok": True, "feed_url": STATE["live_cfg"]["feed_url"]}

# ---------------------------------------------------------
# GTFS betöltés (upload / URL) + reload + loader
# ---------------------------------------------------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def _extract_zip_to_dir(zip_bytes: bytes, target_dir="data/gtfs") -> Dict[str, Any]:
    ensure_dir(target_dir)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if not name.lower().endswith(".txt"):
                continue
            with z.open(name) as src, open(os.path.join(target_dir, os.path.basename(name)), "wb") as dst:
                dst.write(src.read())
    # jelöljük újratöltésre
    STATE["gtfs_ready"] = False
    G = load_gtfs_if_needed()
    return {"ok": STATE["gtfs_ready"], "stops": len(G["stops"])}

class GtfsUrlIn(BaseModel):
    url: str

@app.post("/api/gtfs/upload")
async def gtfs_upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Please upload a .zip GTFS file.")
    data = await file.read()
    return _extract_zip_to_dir(data)

@app.post("/api/gtfs/load-url")
def gtfs_load_url(inp: GtfsUrlIn):
    import requests
    r = requests.get(inp.url, timeout=60)
    r.raise_for_status()
    return _extract_zip_to_dir(r.content)

def load_gtfs_if_needed() -> Dict[str, Any]:
    if STATE["gtfs_ready"]:
        return STATE["gtfs"]
    base = "data/gtfs"
    need = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]
    if not all(os.path.exists(os.path.join(base, n)) for n in need):
        STATE["gtfs_ready"] = False
        return STATE["gtfs"]

    G = STATE["gtfs"] = {"stops":{}, "routes":{}, "trips":{}, "stop_times":{}, "shapes":{}, "route2shapes":{}, "index_stop_name":{}}

    # stops
    with open(os.path.join(base, "stops.txt"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            sid = r.get("stop_id") or ""
            if not sid: continue
            st = {
                "stop_id": sid,
                "name": r.get("stop_name",""),
                "lat": float(r.get("stop_lat", 0) or 0),
                "lon": float(r.get("stop_lon", 0) or 0)
            }
            G["stops"][sid] = st
            key = st["name"].strip().lower()
            if key: G["index_stop_name"].setdefault(key, []).append(sid)

    # routes
    with open(os.path.join(base, "routes.txt"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rid = r.get("route_id") or ""
            if not rid: continue
            G["routes"][rid] = {
                "route_id": rid,
                "route_short_name": r.get("route_short_name",""),
                "route_long_name": r.get("route_long_name",""),
            }

    # trips
    with open(os.path.join(base, "trips.txt"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            tid = r.get("trip_id") or ""
            if not tid: continue
            rid = r.get("route_id","")
            shp = r.get("shape_id","")
            G["trips"][tid] = {
                "trip_id": tid,
                "route_id": rid,
                "shape_id": shp,
                "headsign": r.get("trip_headsign","") or r.get("trip_short_name","")
            }
            if shp:
                G["route2shapes"].setdefault(rid, set()).add(shp)

    # stop_times
    with open(os.path.join(base, "stop_times.txt"), encoding="utf-8") as f:
        for r in csv.DictReader(f):
            tid = r.get("trip_id") or ""
            if not tid: continue
            G["stop_times"].setdefault(tid, []).append({
                "stop_id": r.get("stop_id",""),
                "arr": r.get("arrival_time",""),
                "dep": r.get("departure_time",""),
                "seq": int(r.get("stop_sequence") or 0)
            })
    for tid, arr in G["stop_times"].items():
        arr.sort(key=lambda x: x["seq"])

    # shapes (opcionális, de jó ha van)
    shp_path = os.path.join(base, "shapes.txt")
    if os.path.exists(shp_path):
        with open(shp_path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                sid = r.get("shape_id") or ""
                if not sid: continue
                STATE["gtfs"]["shapes"].setdefault(sid, []).append({
                    "lat": float(r.get("shape_pt_lat", 0) or 0),
                    "lon": float(r.get("shape_pt_lon", 0) or 0),
                    "seq": int(r.get("shape_pt_sequence") or 0)
                })
        for sid, arr in STATE["gtfs"]["shapes"].items():
            arr.sort(key=lambda x: x["seq"])

    STATE["gtfs_ready"] = True
    return G

@app.post("/api/reload-gtfs")
def reload_gtfs():
    STATE["gtfs_ready"] = False
    G = load_gtfs_if_needed()
    missing = []
    if not G["stops"]: missing.append("stops.txt")
    if not G["routes"]: missing.append("routes.txt")
    if not G["trips"]: missing.append("trips.txt")
    if not G["stop_times"]: missing.append("stop_times.txt")
    if not STATE["gtfs"]["shapes"]: missing.append("shapes.txt")
    return {"ok": len(missing) == 0, "missing": missing, "stops": len(G["stops"])}

# ---------------------------------------------------------
# Live jármű feed (SIRI-VM kompat)
# ---------------------------------------------------------
def fetch_live_raw() -> List[Dict[str, Any]]:
    url = STATE["live_cfg"]["feed_url"]
    if not url:
        return STATE["live"]["vehicles"]
    # kis cache, hogy ne terheljük túl
    if time.time() - STATE["live"]["fetched_at"] < 5 and STATE["live"]["vehicles"]:
        return STATE["live"]["vehicles"]

    import requests
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return STATE["live"]["vehicles"]

    out: List[Dict[str, Any]] = []

    # 1) Egyszerű JSON: {"vehicles":[{lat,lon,route,trip_id,label,...}]}
    if isinstance(data, dict) and "vehicles" in data and isinstance(data["vehicles"], list):
        raw = data["vehicles"]
        for v in raw:
            try:
                lat = float(v.get("lat") or v.get("latitude"))
                lon = float(v.get("lon") or v.get("longitude"))
            except Exception:
                continue
            out.append({
                "lat": lat, "lon": lon,
                "route": normalize_route(v.get("route") or v.get("line") or v.get("line_ref") or ""),
                "trip_id": str(v.get("trip_id") or v.get("journey_id") or ""),
                "label": str(v.get("label") or v.get("id") or ""),
                "timestamp": v.get("timestamp") or v.get("time") or "",
                "stop_id": v.get("stop_id") or "",
                "aimed": v.get("aimed") or "",
                "expected": v.get("expected") or ""
            })

    # 2) SIRI-VM
    elif isinstance(data, dict) and "Siri" in data:
        try:
            vm = data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"][0]["VehicleActivity"]
        except Exception:
            vm = []

        for ent in vm:
            mon = ent.get("MonitoredVehicleJourney", {})
            pos = mon.get("VehicleLocation", {}) or {}
            call = mon.get("MonitoredCall", {}) or {}
            lat = pos.get("Latitude"); lon = pos.get("Longitude")
            try:
                lat = float(lat); lon = float(lon)
            except Exception:
                continue

            aimed = parse_iso(call.get("AimedDepartureTime") or call.get("AimedArrivalTime"))
            expected = parse_iso(call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime"))
            delay_min = None
            if aimed and expected:
                delta = (expected - aimed).total_seconds() / 60.0
                # félperces kerekítés
                delay_min = round(delta * 2) / 2.0

            out.append({
                "lat": lat, "lon": lon,
                "route": normalize_route(mon.get("LineRef")),
                "trip_id": str(mon.get("FramedVehicleJourneyRef", {}).get("DatedVehicleJourneyRef") or ""),
                "label": str(mon.get("VehicleRef") or ""),
                "timestamp": ent.get("RecordedAtTime") or "",
                "stop_id": str(call.get("StopPointRef") or ""),
                "aimed": call.get("AimedDepartureTime") or call.get("AimedArrivalTime") or "",
                "expected": call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime") or "",
                "delay_min": delay_min
            })
    else:
        out = []

    STATE["live"]["vehicles"] = out
    STATE["live"]["fetched_at"] = time.time()
    return out

@app.get("/api/vehicles")
def api_vehicles(trip_id: Optional[str] = None, route: Optional[str] = None):
    V = fetch_live_raw()
    if trip_id:
        tid = str(trip_id).strip()
        V = [v for v in V if v.get("trip_id") == tid]
    elif route:
        rn = normalize_route(route)
        V = [v for v in V if normalize_route(v.get("route")) == rn]
    return {"vehicles": V}

# ---------------------------------------------------------
# Keresés: megállók, viszonylatok
# ---------------------------------------------------------
@app.get("/api/stops/search")
def stops_search(q: str = Query(..., min_length=1)):
    G = load_gtfs_if_needed()
    ql = q.strip().lower()
    res = []
    for sid, st in G["stops"].items():
        if ql in st["name"].lower():
            res.append(st)
            if len(res) >= 30:
                break
    return {"results": res}

@app.get("/api/routes/search")
def routes_search(q: str = Query(..., min_length=1)):
    G = load_gtfs_if_needed()
    qn = normalize_route(q)
    res = []
    for rid, r in G["routes"].items():
        if qn and (normalize_route(r.get("route_short_name")) == qn or normalize_route(rid) == qn):
            res.append({"route_id": rid, **r})
    return {"results": res}

# ---------------------------------------------------------
# Indulások megállóból (lookahead)
#  - due: élő + <=1 perc
#  - delay_min: ha SIRI-ből kiolvasható
# ---------------------------------------------------------
@app.get("/api/departures")
def departures(stop_id: str = Query(...), lookahead_min: int = 60):
    G = load_gtfs_if_needed()
    if stop_id not in G["stops"]:
        return {"departures": []}

    now = now_utc()
    end = now + timedelta(minutes=lookahead_min)
    today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # élő járművek gyors indexe route szerint
    V = fetch_live_raw()
    by_route: Dict[str, List[Dict[str, Any]]] = {}
    for v in V:
        by_route.setdefault(normalize_route(v.get("route")), []).append(v)

    out = []
    for tid, times in G["stop_times"].items():
        # keresd meg ezt a stopot az adott tripben
        for t in times:
            if t["stop_id"] != stop_id:
                continue
            sec = parse_hhmmss(t.get("dep") or t.get("arr"))
            dep_dt = today0 + timedelta(seconds=sec)
            if dep_dt < now - timedelta(minutes=5):
                continue
            if dep_dt > end:
                continue

            trip = G["trips"].get(tid, {})
            route = G["routes"].get(trip.get("route_id", ""), {})
            route_short = route.get("route_short_name", "")
            headsign = trip.get("headsign", "")

            # élő-jel: ha ugyanazon a viszonylaton van jármű és a megállótól < 2km
            live = False
            live_delay = None
            due = False

            cand = by_route.get(normalize_route(route_short), [])
            if cand:
                s = G["stops"][stop_id]
                # legközelebbi jármű a stophoz
                cand_sorted = sorted(
                    cand,
                    key=lambda v: haversine_m(s["lat"], s["lon"], float(v["lat"]), float(v["lon"]))
                )
                if cand_sorted:
                    v0 = cand_sorted[0]
                    dist_m = haversine_m(s["lat"], s["lon"], float(v0["lat"]), float(v0["lon"]))
                    if dist_m <= 2000:  # 2 km-en belül
                        live = True
                        if isinstance(v0.get("delay_min"), (int, float)):
                            live_delay = v0["delay_min"]

            mins = (dep_dt - now).total_seconds() / 60.0
            if live and mins <= 1.0:
                due = True

            out.append({
                "trip_id": tid,
                "route_short": route_short,
                "headsign": headsign,
                "scheduled": dep_dt.isoformat(),
                "minutes": round(mins),
                "live": live,
                "due": due,
                "delay_min": live_delay  # lehet None, ha a feed nem adja
            })

    out.sort(key=lambda d: d["scheduled"])
    return {"departures": out}

# ---------------------------------------------------------
# Trip részletek (shape + megállók + live, delay ha elérhető)
# ---------------------------------------------------------
@app.get("/api/trip")
def trip_detail(trip_id: str = Query(...)):
    G = load_gtfs_if_needed()
    trip = G["trips"].get(trip_id)
    if not trip:
        return {"trip_id": trip_id, "stops": [], "shape": [], "live": {}}

    # stops
    legs = []
    for st in G["stop_times"].get(trip_id, []):
        S = G["stops"].get(st["stop_id"], {})
        legs.append({
            "stop_id": st["stop_id"],
            "name": S.get("name", ""),
            "lat": S.get("lat"),
            "lon": S.get("lon"),
            "time": st.get("dep") or st.get("arr") or ""
        })

    # shape
    shape = []
    if trip.get("shape_id") and trip["shape_id"] in G["shapes"]:
        for p in G["shapes"][trip["shape_id"]]:
            shape.append({"lat": p["lat"], "lon": p["lon"]})

    # live: route alapján (trip_id egyezés ritka a SIRI-ben)
    route_short = G["routes"].get(trip.get("route_id",""), {}).get("route_short_name","")
    V = api_vehicles(route=route_short)["vehicles"]
    live = {"vehicles": V}

    # delay becslés: ha bármelyik jármű ad MonitoredCall aimed/expected-et
    delay_min = None
    for v in V:
        if isinstance(v.get("delay_min"), (int, float)):
            delay_min = v["delay_min"]
            break
    if delay_min is not None:
        live["delay_min"] = delay_min

    return {"trip_id": trip_id, "headsign": trip.get("headsign",""), "stops": legs, "shape": shape, "live": live}

# ---------------------------------------------------------
# Route shape + route live (viszonylat térképhez)
# ---------------------------------------------------------
@app.get("/api/route/shape")
def route_shape(route: str = Query(...)):
    G = load_gtfs_if_needed()
    rn = normalize_route(route)
    # keresünk egy route_id-t, aminek short_name = rn
    rid = None
    for k, r in G["routes"].items():
        if normalize_route(r.get("route_short_name")) == rn or normalize_route(k) == rn:
            rid = k; break
    pts: List[Dict[str, float]] = []
    if rid:
        shapes = list(G["route2shapes"].get(rid, []))
        if shapes:
            sid = shapes[0]  # legegyszerűbb: első shape
            for p in G["shapes"].get(sid, []):
                pts.append({"lat": p["lat"], "lon": p["lon"]})
    return {"route": route, "shape": pts}

@app.get("/api/route/live")
def route_live(route: str = Query(...)):
    V = api_vehicles(route=route)["vehicles"]
    shp = route_shape(route)["shape"]
    return {"route": route, "shape": shp, "vehicles": V}
