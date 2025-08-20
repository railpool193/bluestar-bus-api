import io, json, csv, zipfile, time, re
from pathlib import Path
from typing import Dict, Any, List, Optional
from collections import defaultdict, OrderedDict

from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

import httpx, xmltodict

app = FastAPI(title="Bluestar Bus – API", version="4.1.0")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ---------------- CORS + static ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=False), name="static")

# -------------- No-cache middleware --------------
@app.middleware("http")
async def no_cache_mw(request, call_next):
    resp = await call_next(request)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# ===================== Helpers ====================
def _read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))

def _write_json(path: Path, data: Any):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

def gtfs_ok() -> bool:
    return (DATA_DIR / "stops.json").exists() and (DATA_DIR / "schedule.json").exists()

def _find_member(zf: zipfile.ZipFile, name: str) -> Optional[str]:
    lname = name.lower()
    for m in zf.namelist():
        lm = m.lower()
        if lm == lname or lm.endswith("/" + lname):
            return m
    return None

def _hhmm_to_min(t: str) -> int:
    """Accept 'HH:MM' or 'HH:MM:SS' (can be 24+ hours in GTFS)."""
    try:
        parts = t.split(":")
        h, m = int(parts[0]), int(parts[1])
        return h * 60 + m
    except Exception:
        return 0

def norm_route(x: str) -> str:
    """Normalizált vonalszám (pl. 'BLUS:0018' → '18', 'R1' → 'R1')."""
    s = str(x or "").strip().upper()
    if not s:
        return ""
    # próbáljuk a Published short name-et: betűszám kombináció maradhat
    core = re.sub(r"[^A-Z0-9]", "", s)
    if core.isdigit():
        core = str(int(core))
    return core

# ===================== LIVE (BODS) =====================
LIVE_CFG_PATH = DATA_DIR / "live_config.json"

def _get_live_cfg() -> Dict[str, Any]:
    return _read_json(LIVE_CFG_PATH, {"feed_url": ""})

def _set_live_cfg(cfg: Dict[str, Any]):
    _write_json(LIVE_CFG_PATH, cfg or {"feed_url": ""})

class BODSAdapter:
    """
    SIRI-VM kliens a BODS feedhez (JSON vagy XML).
    """
    def __init__(self):
        self.timeout = httpx.Timeout(15.0)
        self.client = httpx.AsyncClient(timeout=self.timeout)

    async def is_available(self) -> bool:
        return bool(_get_live_cfg().get("feed_url"))

    async def _fetch_raw(self) -> Optional[Any]:
        url = _get_live_cfg().get("feed_url", "")
        if not url:
            return None
        try:
            r = await self.client.get(url, headers={"Cache-Control":"no-cache"}, params={"_": int(time.time())})
            r.raise_for_status()
            ct = (r.headers.get("content-type") or "").lower()
            text = r.text.strip()
            if "json" in ct or text.startswith("{") or text.startswith("["):
                return r.json()
            return xmltodict.parse(text)
        except Exception:
            return None

    @staticmethod
    def _as_list(x):
        if x is None:
            return []
        return x if isinstance(x, list) else [x]

    def _parse_vehicles(self, raw) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []

        # JSON
        def pick_json(d):
            vs = []
            try:
                sd = d.get("Siri",{}).get("ServiceDelivery",{})
                vmd = sd.get("VehicleMonitoringDelivery",[])
                if isinstance(vmd, dict): vmd=[vmd]
                for deliv in vmd:
                    for va in deliv.get("VehicleActivity",[]) or []:
                        mj = va.get("MonitoredVehicleJourney",{}) or {}
                        loc = mj.get("VehicleLocation",{}) or {}
                        # néhány feed-ben máshol a lokáció:
                        if not loc and va.get("VehicleLocation"): loc = va.get("VehicleLocation")
                        route = mj.get("LineRef") or mj.get("PublishedLineName") or mj.get("LineName") or ""
                        reg   = mj.get("VehicleRef") or mj.get("VehicleRegistrationMark") or va.get("VehicleRef") or ""
                        vs.append({
                            "lat": float(loc.get("Latitude") or 0),
                            "lon": float(loc.get("Longitude") or 0),
                            "route": route,
                            "bearing": mj.get("Bearing"),
                            "reg": reg,
                            "trip_id": mj.get("DatedVehicleJourneyRef") or mj.get("VehicleJourneyRef") or "",
                            "line_ref": mj.get("LineRef") or "",
                            "dest": mj.get("DestinationName") or mj.get("DestinationShortName") or "",
                        })
            except Exception:
                pass
            return vs

        # XML
        def pick_xml(d):
            vs = []
            try:
                sd = d.get("Siri",{}).get("ServiceDelivery",{})
                vmd = sd.get("VehicleMonitoringDelivery",{})
                acts = BODSAdapter._as_list(vmd.get("VehicleActivity"))
                for va in acts:
                    mj = va.get("MonitoredVehicleJourney",{}) or {}
                    loc = mj.get("VehicleLocation",{}) or va.get("VehicleLocation",{}) or {}
                    def val(obj, k):
                        v = obj.get(k)
                        if isinstance(v, dict) and "#text" in v: return v["#text"]
                        return v
                    route = val(mj,"LineRef") or val(mj,"PublishedLineName") or val(mj,"LineName") or ""
                    reg   = val(mj,"VehicleRef") or val(mj,"VehicleRegistrationMark") or val(va,"VehicleRef") or ""
                    vs.append({
                        "lat": float(val(loc,"Latitude") or 0),
                        "lon": float(val(loc,"Longitude") or 0),
                        "route": route,
                        "bearing": val(mj,"Bearing"),
                        "reg": reg,
                        "trip_id": val(mj,"DatedVehicleJourneyRef") or val(mj,"VehicleJourneyRef") or "",
                        "line_ref": val(mj,"LineRef") or "",
                        "dest": val(mj,"DestinationName") or val(mj,"DestinationShortName") or "",
                    })
            except Exception:
                pass
            return vs

        if not raw:
            return out
        if isinstance(raw, dict) and "Siri" in raw:
            out = pick_json(raw)
        elif isinstance(raw, dict):
            out = pick_xml(raw)
        # csak érvényes koordináta
        return [v for v in out if v.get("lat") and v.get("lon")]

    async def vehicles(self) -> List[Dict[str, Any]]:
        raw = await self._fetch_raw()
        return self._parse_vehicles(raw)

    async def vehicles_by_route(self, route_no: str) -> List[Dict[str, Any]]:
        vs = await self.vehicles()
        target = norm_route(route_no)
        out = []
        for v in vs:
            r = norm_route(v.get("route") or v.get("line_ref"))
            if r == target:
                out.append(v)
        return out

siri_live = BODSAdapter()

# ===================== GTFS builder =====================
def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        req = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
        members = {n: _find_member(zf, n) for n in req}
        missing = [n for n, m in members.items() if m is None]
        if missing:
            raise ValueError(f"Hiányzó GTFS fájlok: {', '.join(missing)}")

        # routes
        routes: Dict[str, Dict[str, str]] = {}
        with zf.open(members["routes.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")):
                routes[row["route_id"]] = {
                    "short": (row.get("route_short_name") or "").strip(),
                    "long": (row.get("route_long_name") or "").strip(),
                }

        # trips
        trips: Dict[str, Dict[str, str]] = {}
        with zf.open(members["trips.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")):
                r = routes.get(row["route_id"], {"short": "", "long": ""})
                trips[row["trip_id"]] = {
                    "route": r["short"] or r["long"],
                    "headsign": (row.get("trip_headsign") or "").strip(),
                }

        # stops.json
        stops: List[Dict[str, str]] = []
        with zf.open(members["stops.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")):
                stops.append({
                    "stop_id": row["stop_id"],
                    "stop_name": (row.get("stop_name") or "").strip(),
                    "stop_code": (row.get("stop_code") or "").strip(),
                })
        _write_json(DATA_DIR / "stops.json", stops)

        # schedule.json & trip_stops.json
        schedule: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        trip_stops: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        with zf.open(members["stop_times.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")):
                tid = row["trip_id"]
                t = (row.get("departure_time") or row.get("arrival_time") or "").strip()
                if not t: 
                    continue
                trip = trips.get(tid)
                if not trip:
                    continue
                schedule[row["stop_id"]].append({
                    "time": t,
                    "route": trip["route"],
                    "destination": trip["headsign"],
                    "trip_id": tid
                })
                trip_stops[tid].append({
                    "seq": int(row.get("stop_sequence") or 0),
                    "stop_id": row["stop_id"],
                    "time": t
                })

        for lst in schedule.values():
            lst.sort(key=lambda x: _hhmm_to_min(x["time"]))
        for lst in trip_stops.values():
            lst.sort(key=lambda x: x["seq"])

        _write_json(DATA_DIR / "schedule.json", schedule)
        _write_json(DATA_DIR / "trip_stops.json", trip_stops)
        _write_json(DATA_DIR / "trips_index.json", trips)

# ===================== API =====================

@app.get("/", include_in_schema=False, response_class=HTMLResponse)
async def root_html():
    html = (BASE_DIR / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)

@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "gtfs": gtfs_ok(),
        "live": await siri_live.is_available(),
        "build": str(int(time.time()))
    }

# LIVE config
@app.get("/api/live/config")
async def get_live_cfg():
    return _get_live_cfg()

@app.post("/api/live/config")
async def set_live_cfg(payload: Dict[str, Any]):
    url = (payload or {}).get("feed_url","").strip()
    if not url:
        _set_live_cfg({"feed_url": ""})
        return {"ok": True}
    if "api_key=" not in url:
        raise HTTPException(status_code=400, detail="Adj meg teljes BODS feed URL-t api_key paraméterrel.")
    _set_live_cfg({"feed_url": url})
    return {"ok": True}

# GTFS upload
@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Kérlek GTFS ZIP fájlt tölts fel.")
    content = await file.read()
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    try:
        _build_from_zip_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"GTFS feldolgozási hiba: {e}")
    return {"status": "uploaded"}

# Stop search
@app.get("/api/stops/search")
async def api_stops_search(q: str = Query(..., min_length=1), limit: int = 20):
    stops = _read_json(DATA_DIR / "stops.json", [])
    ql = q.strip().lower()
    res = [s for s in stops if ql in (s.get("stop_name") or "").lower() or ql == (s.get("stop_code") or "").lower()]
    res.sort(key=lambda s: (len(s.get("stop_name","")), s.get("stop_name","")))
    return res[:limit]

# Next departures (UK/BST független – kliens oldali UK óra alapján jelenítjük)
@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = Query(60, ge=5, le=480), live: bool = True):
    schedule = _read_json(DATA_DIR / "schedule.json", {})
    base = schedule.get(stop_id, [])

    # nincs szerver oldali zóna-feltételezés – csak 24h modulo
    now_struct = time.gmtime()  # stabil
    now_min = (now_struct.tm_hour * 60 + now_struct.tm_min) % (24 * 60)

    upcoming: List[Dict[str, Any]] = []
    window = minutes

    for d in base:
        t = (d.get("time") or "").strip()
        if not t:
            continue
        dep_min = _hhmm_to_min(t) % (24 * 60)
        delta = (dep_min - now_min) % (24 * 60)
        if delta <= window:
            upcoming.append({
                "route": d.get("route"),
                "destination": d.get("destination"),
                "time": t,
                "trip_id": d.get("trip_id"),
                "eta_min": None,
                "delay_min": None,
                "vehicle_reg": None,
                "live": False
            })

    # dedupe
    def k(it): return (str(it.get("route") or ""), str(it.get("destination") or ""), str(it.get("time") or ""))
    dedup = OrderedDict()
    for it in upcoming:
        if k(it) not in dedup or (not dedup[k(it)].get("trip_id") and it.get("trip_id")):
            dedup[k(it)] = it
    upcoming = list(dedup.values())

    # LIVE jelző
    if live and await siri_live.is_available():
        try:
            all_live = await siri_live.vehicles()
            live_routes = defaultdict(list)
            for v in all_live:
                key = norm_route(v.get("route") or v.get("line_ref"))
                if key:
                    live_routes[key].append(v)
            for it in upcoming:
                lr = live_routes.get(norm_route(it.get("route")))
                if lr:
                    it["live"] = True
                    it["vehicle_reg"] = lr[0].get("reg")
        except Exception:
            pass

    upcoming.sort(key=lambda x: (_hhmm_to_min(x.get("time","00:00")) % (24*60)))
    return upcoming[:120]

# Trip details
@app.get("/api/trips/{trip_id}")
async def api_trip_details(trip_id: str, route: str = Query("", description="hint"), time_s: str = Query("", alias="time", description="HH:MM hint")):
    trip_stops = _read_json(DATA_DIR / "trip_stops.json", {})
    stops_idx = { s["stop_id"]: s for s in _read_json(DATA_DIR / "stops.json", []) }
    trips_index = _read_json(DATA_DIR / "trips_index.json", {})

    seq = trip_stops.get(trip_id)
    if not seq:
        target_route = norm_route(route)
        target_min = _hhmm_to_min(time_s) if time_s else None
        candidates = [tid for tid, meta in trips_index.items() if norm_route(meta.get("route")) == target_route]

        best_tid = None
        if candidates:
            if target_min is None:
                best_tid = candidates[0]
            else:
                best_delta = 10**9
                for tid in candidates:
                    calls = trip_stops.get(tid) or []
                    if not calls: 
                        continue
                    dep = calls[0].get("time")
                    if not dep:
                        continue
                    dep_min = _hhmm_to_min(dep) % (24*60)
                    delta = (dep_min - target_min) % (24*60)
                    if delta < best_delta:
                        best_delta, best_tid = delta, tid
        if best_tid:
            seq = trip_stops.get(best_tid)
            trip_id = best_tid

    calls = []
    for r in (seq or []):
        st = stops_idx.get(r["stop_id"])
        calls.append({
            "time": r.get("time"),
            "stop_id": r.get("stop_id"),
            "stop_name": (st or {}).get("stop_name") or r.get("stop_id"),
            "eta_min": None,
            "delay_min": None
        })

    meta = trips_index.get(trip_id) or {}
    return {"trip_id": trip_id, "route": meta.get("route"), "headsign": meta.get("headsign"), "vehicle": None, "calls": calls}

# Route search
@app.get("/api/routes/search")
async def api_route_search(q: str = Query("", description="Járatszám/név"), limit: int = 40):
    schedule = _read_json(DATA_DIR / "schedule.json", {})
    routes = set()
    for lst in schedule.values():
        for it in lst:
            if it.get("route"):
                routes.add(str(it["route"]))
    qn = norm_route(q)
    if qn:
        res = [r for r in routes if norm_route(r).startswith(qn)]
    else:
        res = sorted(routes, key=lambda x: (len(x), x))
    res = sorted(res, key=lambda x: (len(x), x))
    return [{"route": r} for r in res[:limit]]

# Route vehicles (LIVE)
@app.get("/api/routes/{route}/vehicles")
async def api_route_vehicles(route: str):
    if not await siri_live.is_available():
        return []
    try:
        vs = await siri_live.vehicles_by_route(route)
        return vs[:120]
    except Exception:
        return []

# Front file
@app.get("/index.html", include_in_schema=False)
async def index_file():
    return FileResponse(str(BASE_DIR / "index.html"), media_type="text/html")
