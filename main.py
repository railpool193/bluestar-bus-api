import os
import io
import json
import time
import csv
import zipfile
import math
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, UploadFile, File, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import requests
import xml.etree.ElementTree as ET

# ---------------------- App & CORS ----------------------
APP_VERSION = "5.3.0"
BUILD = str(int(time.time()))
TZ_NAME = "Europe/London"
try:
    import zoneinfo
    TZ = zoneinfo.ZoneInfo(TZ_NAME)  # Python 3.9+
except Exception:
    TZ = timezone.utc

app = FastAPI(title="Bluestar Bus — API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Ha a projekt gyökerébe teszed az index.html-t, ezt szolgáljuk ki:
INDEX_PATH = os.path.abspath(os.path.join(os.getcwd(), "index.html"))

# Opcionális statikus fájlok (ha van ./public mappa)
if os.path.isdir("public"):
    app.mount("/open", StaticFiles(directory="public", html=True), name="public")

# ---------------------- Adattár ----------------------
DATA_DIR = os.path.join("data", "gtfs")
os.makedirs(DATA_DIR, exist_ok=True)
LIVE_CFG_FILE = os.path.join("data", "live_config.json")

REQ_FILES = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]
# shapes.txt opcionális, de útvonal rajzhoz kell
SHAPES_FILE = "shapes.txt"

def now_uk():
    try:
        return datetime.now(TZ)
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc)

def sec_since_midnight(dt: Optional[datetime] = None) -> int:
    dt = dt or now_uk()
    return dt.hour*3600 + dt.minute*60 + dt.second

def parse_hms_to_sec(s: str) -> Optional[int]:
    # GTFS-ben lehet 24:xx is -> 86400 felett
    try:
        parts = [int(p) for p in s.strip().split(":")]
        if len(parts) != 3: 
            return None
        return parts[0]*3600 + parts[1]*60 + parts[2]
    except Exception:
        return None

def normalize(s: str) -> str:
    return (s or "").strip().lower()

class GTFSStore:
    def __init__(self):
        self.ready = False
        self.gtfs_dir = DATA_DIR
        # indexek
        self.stops: Dict[str, Dict[str, Any]] = {}
        self.routes: Dict[str, Dict[str, Any]] = {}
        self.trips: Dict[str, Dict[str, Any]] = {}
        self.stop_times_by_stop: Dict[str, List[Dict[str, Any]]] = {}
        self.shapes: Dict[str, List[List[float]]] = {}
        self.route_to_shape: Dict[str, str] = {}
        self.route_short_to_id: Dict[str, str] = {}
        self.lock = threading.Lock()

    # ---- GTFS beolvasás zip-ből ----
    def load_zip_bytes(self, zip_bytes: bytes):
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            names = set(z.namelist())
            missing = [f for f in REQ_FILES if f not in names]
            if missing:
                return {"ok": False, "missing": missing, "stops": 0}
            # Mentés fájlokba (kibontás)
            z.extractall(self.gtfs_dir)
        return self.reload_from_dir()

    def reload_from_dir(self):
        with self.lock:
            # kötelezők
            for f in REQ_FILES:
                if not os.path.isfile(os.path.join(self.gtfs_dir, f)):
                    return {"ok": False, "missing": REQ_FILES, "stops": 0}
            # stops
            self.stops.clear()
            with open(os.path.join(self.gtfs_dir, "stops.txt"), "r", encoding="utf-8-sig") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    sid = row.get("stop_id") or row.get("id")
                    if not sid: 
                        continue
                    self.stops[sid] = {
                        "id": sid,
                        "name": row.get("stop_name") or row.get("name"),
                        "code": row.get("stop_code") or row.get("code"),
                        "lat": float(row.get("stop_lat") or row.get("lat") or 0),
                        "lon": float(row.get("stop_lon") or row.get("lon") or 0),
                    }
            # routes
            self.routes.clear()
            self.route_short_to_id.clear()
            with open(os.path.join(self.gtfs_dir, "routes.txt"), "r", encoding="utf-8-sig") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    rid = row.get("route_id")
                    if not rid: 
                        continue
                    short = row.get("route_short_name") or row.get("short_name") or ""
                    self.routes[rid] = {
                        "id": rid,
                        "short_name": short,
                        "long_name": row.get("route_long_name") or row.get("long_name") or "",
                    }
                    if short:
                        self.route_short_to_id[normalize(short)] = rid
            # trips
            self.trips.clear()
            with open(os.path.join(self.gtfs_dir, "trips.txt"), "r", encoding="utf-8-sig") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    tid = row.get("trip_id")
                    if not tid:
                        continue
                    self.trips[tid] = {
                        "id": tid,
                        "route_id": row.get("route_id"),
                        "service_id": row.get("service_id"),
                        "headsign": row.get("trip_headsign") or row.get("headsign") or "",
                        "direction_id": int(row.get("direction_id") or 0),
                        "shape_id": row.get("shape_id") or "",
                    }
            # stop_times (index megállóra)
            self.stop_times_by_stop.clear()
            with open(os.path.join(self.gtfs_dir, "stop_times.txt"), "r", encoding="utf-8-sig") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    sid = row.get("stop_id")
                    tid = row.get("trip_id")
                    if not sid or not tid:
                        continue
                    arr = parse_hms_to_sec(row.get("arrival_time") or row.get("arrival"))
                    dep = parse_hms_to_sec(row.get("departure_time") or row.get("departure"))
                    self.stop_times_by_stop.setdefault(sid, []).append({
                        "trip_id": tid,
                        "arrival": arr,
                        "departure": dep if dep is not None else arr,
                        "stop_sequence": int(row.get("stop_sequence") or 0),
                    })
            for sid in list(self.stop_times_by_stop.keys()):
                self.stop_times_by_stop[sid].sort(key=lambda x: (x["departure"] or 0, x["stop_sequence"]))
            # shapes (ha van)
            self.shapes.clear()
            shp_path = os.path.join(self.gtfs_dir, SHAPES_FILE)
            if os.path.isfile(shp_path):
                tmp: Dict[str, List[Dict[str, Any]]] = {}
                with open(shp_path, "r", encoding="utf-8-sig") as fh:
                    rdr = csv.DictReader(fh)
                    for row in rdr:
                        sid = row.get("shape_id")
                        if not sid: 
                            continue
                        tmp.setdefault(sid, []).append({
                            "lat": float(row.get("shape_pt_lat") or 0),
                            "lon": float(row.get("shape_pt_lon") or 0),
                            "seq": int(row.get("shape_pt_sequence") or 0),
                        })
                for sid, pts in tmp.items():
                    pts.sort(key=lambda x: x["seq"])
                    self.shapes[sid] = [[p["lon"], p["lat"]] for p in pts]
            # route->shape heurisztika: leggyakoribb shape a route-hoz
            self.route_to_shape.clear()
            cnt: Dict[str, Dict[str, int]] = {}
            for t in self.trips.values():
                rid = t["route_id"]; shp = t.get("shape_id") or ""
                if rid and shp:
                    cnt.setdefault(rid, {}).setdefault(shp, 0)
                    cnt[rid][shp] += 1
            for rid, mp in cnt.items():
                best = max(mp.items(), key=lambda kv: kv[1])[0]
                self.route_to_shape[rid] = best

            self.ready = True
            return {"ok": True, "missing": [], "stops": len(self.stops)}

    # ---- lekérdezések ----
    def search_stops(self, q: str, limit=10):
        qn = normalize(q)
        out = []
        for s in self.stops.values():
            if qn in normalize(s["name"]):
                out.append(s)
            if len(out) >= limit:
                break
        return out

    def search_routes(self, q: str, limit=10):
        qn = normalize(q)
        out = []
        # előnyben a short_name pontos egyezés
        for r in self.routes.values():
            if qn == normalize(r["short_name"]):
                out.append(r)
        if not out:
            for r in self.routes.values():
                if qn in normalize(r["short_name"]) or qn in normalize(r["long_name"]):
                    out.append(r)
                    if len(out) >= limit: break
        # shape hozzáfűzés
        for r in out:
            rid = r["id"]
            shp_id = self.route_to_shape.get(rid)
            if shp_id and shp_id in self.shapes:
                r["shape"] = self.shapes[shp_id]
        return out

    def upcoming_departures(self, stop_id: str, window_min: int = 60):
        if stop_id not in self.stop_times_by_stop:
            return []
        now = now_uk()
        now_s = sec_since_midnight(now)
        end_s = now_s + window_min*60

        items = []
        for st in self.stop_times_by_stop[stop_id]:
            dep_s = st["departure"]
            if dep_s is None:
                continue
            # ma/holnap ablak
            for off in (0, 86400):
                dep = dep_s - now_s + off
                if 0 <= dep <= window_min*60:
                    t = self.trips.get(st["trip_id"], {})
                    rid = t.get("route_id")
                    items.append({
                        "stop_id": stop_id,
                        "stop_time": f"{(dep_s//3600)%24:02d}:{(dep_s%3600)//60:02d}",
                        "route_id": rid,
                        "line": self.routes.get(rid, {}).get("short_name", ""),
                        "headsign": t.get("headsign", ""),
                        "trip_id": st["trip_id"],
                        "departure_in_min": dep/60.0,  # statikus fallback
                    })
        items.sort(key=lambda x: x["departure_in_min"])
        return items


GTFS = GTFSStore()

# ---------------------- Live feed ----------------------
class LiveConfig(BaseModel):
    feed_url: Optional[str] = None

class LiveFeed:
    def __init__(self):
        self.feed_url: Optional[str] = None
        self.last_fetch: float = 0.0
        self.min_period = 10.0  # sec
        self.vehicles: List[Dict[str, Any]] = []  # lat,lon,line,route_short,direction_id,label,delay_min
        # stop+line -> list of updates (eta_sec, delay_sec, direction_id)
        self.stop_line_live: Dict[str, List[Dict[str, Any]]] = {}
        self.lock = threading.Lock()
        self.load_config()

    def config_path(self): return LIVE_CFG_FILE

    def save_config(self):
        os.makedirs(os.path.dirname(self.config_path()), exist_ok=True)
        with open(self.config_path(), "w", encoding="utf-8") as f:
            json.dump({"feed_url": self.feed_url}, f)

    def load_config(self):
        try:
            with open(self.config_path(), "r", encoding="utf-8") as f:
                self.feed_url = json.load(f).get("feed_url")
        except Exception:
            self.feed_url = None

    def ensure_fresh(self):
        if not self.feed_url:
            return
        if time.time() - self.last_fetch < self.min_period:
            return
        with self.lock:
            if time.time() - self.last_fetch < self.min_period:
                return
            try:
                r = requests.get(self.feed_url, timeout=10)
                r.raise_for_status()
                ctype = r.headers.get("Content-Type","").lower()
                if "xml" in ctype or r.text.strip().startswith("<"):
                    self.parse_siri_xml(r.text)
                else:
                    self.parse_siri_json(r.json())
                self.last_fetch = time.time()
            except Exception:
                # hibánál nem borítjuk fel a korábbi adatot
                pass

    # ---- SIRI XML (VehicleMonitoring/StopMonitoring) ----
    def parse_siri_xml(self, text: str):
        root = ET.fromstring(text)
        ns = {"s":"http://www.siri.org.uk/siri"}
        vehicles = []
        stop_line = {}

        # VehicleActivity
        for va in root.findall(".//s:VehicleActivity", ns):
            mj = va.find(".//s:MonitoredVehicleJourney", ns)
            if mj is None: 
                continue
            line = (mj.findtext("s:LineRef", default="", namespaces=ns) or "").strip()
            dirref = mj.findtext("s:DirectionRef", default="", namespaces=ns)
            dest = mj.findtext("s:DestinationName", default="", namespaces=ns)
            vehref = mj.findtext("s:VehicleRef", default="", namespaces=ns)
            lat = mj.findtext(".//s:VehicleLocation/s:Latitude", default="", namespaces=ns)
            lon = mj.findtext(".//s:VehicleLocation/s:Longitude", default="", namespaces=ns)
            try:
                lat = float(lat or 0); lon = float(lon or 0)
            except Exception:
                continue
            delay_min = 0.0
            # MonitoredCall (Expected vs Aimed)
            aimed = mj.findtext(".//s:MonitoredCall/s:AimedArrivalTime", default="", namespaces=ns) or \
                    mj.findtext(".//s:MonitoredCall/s:AimedDepartureTime", default="", namespaces=ns)
            expected = mj.findtext(".//s:MonitoredCall/s:ExpectedArrivalTime", default="", namespaces=ns) or \
                       mj.findtext(".//s:MonitoredCall/s:ExpectedDepartureTime", default="", namespaces=ns)
            stop_ref = mj.findtext(".//s:MonitoredCall/s:StopPointRef", default="", namespaces=ns)

            eta_sec = None
            if expected:
                try:
                    exp = datetime.fromisoformat(expected.replace("Z","+00:00"))
                    eta_sec = max(0, int((exp - now_uk()).total_seconds()))
                except Exception:
                    pass
            if aimed and expected:
                try:
                    a = datetime.fromisoformat(aimed.replace("Z","+00:00"))
                    e = datetime.fromisoformat(expected.replace("Z","+00:00"))
                    delay_min = round((e - a).total_seconds()/60.0, 2)
                except Exception:
                    pass

            vehicles.append({
                "lat": lat, "lon": lon,
                "label": vehref or "",
                "line": line, "route_short": line,
                "direction_id": int(dirref) if (dirref or "").isdigit() else None,
                "delay_min": delay_min,
                "dest": dest or "",
            })
            if stop_ref and line:
                key = f"{stop_ref}|{normalize(line)}"
                stop_line.setdefault(key, []).append({
                    "eta_sec": eta_sec,
                    "delay_sec": None if delay_min is None else int(delay_min*60),
                    "direction_id": int(dirref) if (dirref or "").isdigit() else None,
                })

        self.vehicles = vehicles
        self.stop_line_live = stop_line

    # ---- SIRI JSON (ugyanez JSON-ban) ----
    def parse_siri_json(self, data: Any):
        # Próbáljunk a VehicleActivity tömbhöz eljutni rugalmasan
        va = None
        try:
            va = data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"][0]["VehicleActivity"]
        except Exception:
            # lehet, hogy közvetlen tömb
            va = data.get("VehicleActivity") or []
        vehicles = []
        stop_line = {}
        for item in va or []:
            mj = item.get("MonitoredVehicleJourney", {})
            line = (mj.get("LineRef") or "").strip()
            dirref = mj.get("DirectionRef")
            veh = mj.get("VehicleRef") or ""
            dest = mj.get("DestinationName") or ""
            loc = (mj.get("VehicleLocation") or {})
            lat = float(loc.get("Latitude") or 0)
            lon = float(loc.get("Longitude") or 0)
            mc = mj.get("MonitoredCall") or {}
            stop_ref = mc.get("StopPointRef")
            aimed = mc.get("AimedArrivalTime") or mc.get("AimedDepartureTime")
            expected = mc.get("ExpectedArrivalTime") or mc.get("ExpectedDepartureTime")
            eta_sec = None
            delay_min = 0.0
            if expected:
                try:
                    exp = datetime.fromisoformat(expected.replace("Z","+00:00"))
                    eta_sec = max(0, int((exp - now_uk()).total_seconds()))
                except Exception:
                    pass
            if aimed and expected:
                try:
                    a = datetime.fromisoformat(aimed.replace("Z","+00:00"))
                    e = datetime.fromisoformat(expected.replace("Z","+00:00"))
                    delay_min = round((e - a).total_seconds()/60.0, 2)
                except Exception:
                    pass
            vehicles.append({
                "lat": lat, "lon": lon,
                "label": veh,
                "line": line, "route_short": line,
                "direction_id": int(dirref) if str(dirref).isdigit() else None,
                "delay_min": delay_min,
                "dest": dest,
            })
            if stop_ref and line:
                key = f"{stop_ref}|{normalize(line)}"
                stop_line.setdefault(key, []).append({
                    "eta_sec": eta_sec,
                    "delay_sec": None if delay_min is None else int(delay_min*60),
                    "direction_id": int(dirref) if str(dirref).isdigit() else None,
                })
        self.vehicles = vehicles
        self.stop_line_live = stop_line

LIVE = LiveFeed()

# ---------------------- API: alap ----------------------
@app.get("/")
def root():
    return {"detail": "Open /index.html", "docs": "/docs"}

@app.get("/index.html")
def serve_index():
    if os.path.isfile(INDEX_PATH):
        return FileResponse(INDEX_PATH, media_type="text/html")
    return JSONResponse({"detail":"Missing index.html"}, status_code=404)

@app.get("/api/status")
def api_status():
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": BUILD,
        "time": now_uk().strftime("%H:%M:%S"),
        "tz": TZ_NAME,
        "live_feed_configured": bool(LIVE.feed_url),
        "gtfs_dir": GTFS.gtfs_dir,
        "gtfs_ready": GTFS.ready,
        "gtfs_stops": len(GTFS.stops),
    }

# ---------------------- Live config ----------------------
@app.get("/api/live/config")
def get_live_cfg():
    return {"feed_url": LIVE.feed_url, "last_fetch": LIVE.last_fetch}

@app.post("/api/live/config")
def set_live_cfg(cfg: LiveConfig):
    LIVE.feed_url = (cfg.feed_url or "").strip() or None
    LIVE.save_config()
    return {"ok": True, "feed_url": LIVE.feed_url}

# ---------------------- GTFS feltöltés/letöltés ----------------------
class GtfsUrlIn(BaseModel):
    feed_url: str

@app.post("/api/gtfs/load-url")
def gtfs_load_url(body: GtfsUrlIn):
    url = body.feed_url.strip()
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return GTFS.load_zip_bytes(r.content)

@app.post("/api/gtfs/upload")
def gtfs_upload(file: UploadFile = File(...)):
    content = file.file.read()
    return GTFS.load_zip_bytes(content)

@app.post("/api/reload-gtfs")
def reload_gtfs():
    return GTFS.reload_from_dir()

# ---------------------- Keresők ----------------------
@app.get("/api/stops/search")
def stops_search(q: str = Query(..., min_length=1), limit: int = 10):
    if not GTFS.ready:
        return {"stops": []}
    return {"stops": GTFS.search_stops(q, limit)}

@app.get("/api/routes/search")
def routes_search(q: str = Query(..., min_length=1), limit: int = 10):
    if not GTFS.ready:
        return {"routes": []}
    return {"routes": GTFS.search_routes(q, limit)}

# ---------------------- Departures ----------------------
@app.get("/api/departures")
def departures(stop_id: str, window_min: int = 60):
    if not GTFS.ready:
        return {"departures": []}
    LIVE.ensure_fresh()
    deps = GTFS.upcoming_departures(stop_id, window_min)

    # élő illesztés: stop_ref + line alapján
    for d in deps:
        key = f"{d['stop_id']}|{normalize(d.get('line',''))}"
        live_list = LIVE.stop_line_live.get(key) or []
        # válasszunk legjobban illeszkedőt (legközelebbi ETA)
        live = None
        if live_list:
            # ha van ETA, vegyük a legkisebbet
            live = sorted([x for x in live_list if x.get("eta_sec") is not None], key=lambda x:x["eta_sec"])[0] \
                   if any(x.get("eta_sec") is not None for x in live_list) else live_list[0]
        if live and live.get("eta_sec") is not None:
            d["live"] = True
            d["eta_sec"] = int(live["eta_sec"])
            d["delay_sec"] = live.get("delay_sec")
        else:
            d["live"] = False
    return {"departures": deps}

# ---------------------- Trip részletek + shape ----------------------
@app.get("/api/trip")
def trip_detail(trip_id: str, stop_id: Optional[str] = None):
    if not GTFS.ready:
        return {}
    LIVE.ensure_fresh()
    t = GTFS.trips.get(trip_id)
    if not t:
        return {}
    # stops a triphez (stop_times alapján)
    stops = []
    # megkeressük a trip összes megállóját
    seqs: List[Dict[str, Any]] = []
    for sid, arr in GTFS.stop_times_by_stop.items():
        for st in arr:
            if st["trip_id"] == trip_id:
                seqs.append({"stop_id": sid, **st})
    seqs.sort(key=lambda x: x["stop_sequence"])

    line = GTFS.routes.get(t["route_id"], {}).get("short_name", "")
    for it in seqs:
        s = GTFS.stops.get(it["stop_id"], {})
        row = {
            "seq": it["stop_sequence"],
            "id": it["stop_id"],
            "name": s.get("name",""),
            "code": s.get("code",""),
            "time": f"{(it['departure']//3600)%24:02d}:{(it['departure']%3600)//60:02d}",
            "eta_sec": None
        }
        # live ETA? stop_ref + line
        key = f"{it['stop_id']}|{normalize(line)}"
        live_list = LIVE.stop_line_live.get(key) or []
        if live_list:
            live = sorted([x for x in live_list if x.get("eta_sec") is not None], key=lambda x:x["eta_sec"])[0] \
                   if any(x.get("eta_sec") is not None for x in live_list) else live_list[0]
            row["eta_sec"] = live.get("eta_sec")
        stops.append(row)

    # késés összegzés (ha van)
    delay_sec = None
    key_curr = f"{(stop_id or (stops[0]['id'] if stops else ''))}|{normalize(line)}"
    live_list = LIVE.stop_line_live.get(key_curr) or []
    if live_list and live_list[0].get("delay_sec") is not None:
        delay_sec = live_list[0]["delay_sec"]

    # shape
    shape_coords = []
    shp_id = t.get("shape_id") or GTFS.route_to_shape.get(t.get("route_id") or "", "")
    if shp_id and shp_id in GTFS.shapes:
        shape_coords = GTFS.shapes[shp_id]

    return {
        "trip": {k:t[k] for k in ("id","route_id","direction_id") if k in t},
        "delay_sec": delay_sec,
        "stops": stops,
        "shape": shape_coords
    }

# ---------------------- Vehicles ----------------------
@app.get("/api/vehicles")
def vehicles(route_id: Optional[str] = None):
    if not GTFS.ready:
        return {"vehicles":[]}
    LIVE.ensure_fresh()
    rshort = None
    if route_id:
        # elfogadunk rövid nevet is
        if route_id in GTFS.routes:
            rshort = GTFS.routes[route_id]["short_name"]
        else:
            rshort = route_id
    out = []
    for v in LIVE.vehicles:
        if rshort and normalize(v.get("route_short") or v.get("line") or "") != normalize(rshort):
            continue
        out.append({
            "id": v.get("label") or "",
            "label": v.get("label") or "",
            "lat": v.get("lat"), "lon": v.get("lon"),
            "route_short": v.get("route_short") or v.get("line") or "",
            "direction_id": v.get("direction_id"),
            "delay_min": v.get("delay_min"),
            "dest": v.get("dest",""),
        })
    return {"vehicles": out}
