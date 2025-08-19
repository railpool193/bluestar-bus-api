import io, json, csv, zipfile, time
from pathlib import Path
from typing import Dict, Any, List, Optional
from collections import defaultdict

from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Bluestar Bus – API", version="3.0.0")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# -------- CORS + statikus --------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=False), name="static")

# -------- No-cache minden válaszra --------
@app.middleware("http")
async def no_cache_mw(request, call_next):
    resp = await call_next(request)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# ===================== SEGÉDEK =====================
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

# ===================== LIVE réteg (mock + hook) =====================
class SiriLiveAdapter:
    """
    Fájl-alapú mock, ugyanebbe az interfészbe később könnyen beköthető a valódi SIRI/AVL.
    Fájlok a data/ mappában:
      - live_available.flag            → ha létezik: live = true
      - live_stop_<STOPID>.json        → élő indulások a megállóban
      - live_route_<ROUTE>.json        → élő járművek a vonalon
      - live_trip_<TRIPID>.json        → élő trip részletek
    """
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    async def is_available(self) -> bool:
        return (self.data_dir / "live_available.flag").exists()

    async def stop_next_departures(self, stop_id: str, minutes: int) -> List[Dict[str, Any]]:
        f = self.data_dir / f"live_stop_{stop_id}.json"
        if not f.exists():
            return []
        return _read_json(f, [])

    async def vehicles_by_route(self, route_no: str) -> List[Dict[str, Any]]:
        f = self.data_dir / f"live_route_{route_no}.json"
        if not f.exists():
            return []
        return _read_json(f, [])

    async def trip_details(self, trip_id: str) -> Dict[str, Any]:
        f = self.data_dir / f"live_trip_{trip_id}.json"
        if not f.exists():
            return {}
        return _read_json(f, {})

siri_live = SiriLiveAdapter(DATA_DIR)

# Opcionális: rendszám → típus mapping
FLEET_MAP: Dict[str, Dict[str, str]] = _read_json(DATA_DIR / "fleet.json", {})
def enrich_vehicle(v: Dict[str, Any]) -> Dict[str, Any]:
    if not v:
        return v
    reg = (v.get("reg") or v.get("vehicle_reg") or "").upper()
    if reg and reg in FLEET_MAP:
        meta = FLEET_MAP[reg]
        v.setdefault("type", meta.get("model") or meta.get("type"))
        v.setdefault("fleet_no", meta.get("fleet_no"))
    return v

# ===================== GTFS feldolgozás =====================
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

        # schedule.json  (stop_id -> list of departures)
        schedule: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        # trip_stops.json (trip_id -> sequence list)
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
            lst.sort(key=lambda x: x["time"])
        for lst in trip_stops.values():
            lst.sort(key=lambda x: x["seq"])

        _write_json(DATA_DIR / "schedule.json", schedule)
        _write_json(DATA_DIR / "trip_stops.json", trip_stops)

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

@app.get("/api/stops/search")
async def api_stops_search(q: str = Query(..., min_length=1), limit: int = 20):
    stops = _read_json(DATA_DIR / "stops.json", [])
    ql = q.strip().lower()
    res = [s for s in stops if ql in (s.get("stop_name") or "").lower() or ql == (s.get("stop_code") or "").lower()]
    res.sort(key=lambda s: (len(s.get("stop_name","")), s.get("stop_name","")))
    return res[:limit]

def _hhmm_to_min(t: str) -> int:
    try:
        h, m, *rest = t.split(":")
        return int(h) * 60 + int(m)
    except:
        return 0

@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = Query(60, ge=5, le=240), live: bool = True):
    """
    Következő indulások a megadott időablakban (perc).
    Ha live=True és elérhető, hozzáadjuk az ETA/delay/vehicle mezőket.
    """
    schedule = _read_json(DATA_DIR / "schedule.json", {})
    base = schedule.get(stop_id, [])
    if not base:
        # ha nincs menetrend, próbáljuk csak live-val (új megálló is lehet)
        if live and await siri_live.is_available():
            live_only = await siri_live.stop_next_departures(stop_id, minutes)
            return live_only
        return []

    now = time.localtime()
    now_min = now.tm_hour * 60 + now.tm_min
    hi = now_min + minutes

    upcoming: List[Dict[str, Any]] = []
    for d in base:
        dep_min = _hhmm_to_min(d["time"]) % (24*60)
        in_min = (dep_min - now_min) % (24*60)
        if in_min <= minutes:
            upcoming.append({
                "route": d["route"],
                "destination": d["destination"],
                "time": d["time"],
                "trip_id": d.get("trip_id"),
                "eta_min": None,
                "delay_min": None,
                "vehicle_reg": None,
                "live": False
            })

    if live and await siri_live.is_available():
        try:
            live_items = await siri_live.stop_next_departures(stop_id, minutes)
            idx = { (L.get("route",""), L.get("headsign",""), L.get("scheduled_time","")): L for L in live_items }
            merged = []
            for it in upcoming:
                key = (it["route"], it["destination"], it["time"])
                L = idx.get(key)
                if L:
                    it["eta_min"]    = L.get("eta_min")
                    it["delay_min"]  = L.get("delay_min")
                    it["vehicle_reg"]= L.get("vehicle_reg")
                    it["trip_id"]    = it["trip_id"] or L.get("trip_id")
                    it["live"]       = True
                merged.append(it)
            # Új live-only elemek is jöhetnek (ha nincs GTFS párjuk)
            for L in live_items:
                key = (L.get("route",""), L.get("headsign",""), L.get("scheduled_time",""))
                if key not in idx:  # csak akkor add, ha tényleg nincs párja
                    merged.append({
                        "route": L.get("route",""),
                        "destination": L.get("headsign",""),
                        "time": L.get("scheduled_time",""),
                        "trip_id": L.get("trip_id"),
                        "eta_min": L.get("eta_min"),
                        "delay_min": L.get("delay_min"),
                        "vehicle_reg": L.get("vehicle_reg"),
                        "live": True
                    })
            upcoming = merged
        except Exception:
            pass

    upcoming.sort(key=lambda x: (not x["live"], x["eta_min"] if x["eta_min"] is not None else 99999, x["time"]))
    return upcoming[:80]

@app.get("/api/trips/{trip_id}")
async def api_trip_details(trip_id: str):
    """Trip részletek: ha van élő, azt adjuk vissza; különben GTFS-ből a megállólista."""
    if await siri_live.is_available():
        try:
            live = await siri_live.trip_details(trip_id)
            if live:
                if "vehicle" in live:
                    live["vehicle"] = enrich_vehicle(live["vehicle"])
                return live
        except Exception:
            pass

    # GTFS fallback
    trip_stops = _read_json(DATA_DIR / "trip_stops.json", {})
    stops_idx = { s["stop_id"]: s for s in _read_json(DATA_DIR / "stops.json", []) }
    seq = trip_stops.get(trip_id, [])
    calls = []
    for r in seq:
        st = stops_idx.get(r["stop_id"])
        calls.append({
            "time": r.get("time"),
            "stop_id": r.get("stop_id"),
            "stop_name": (st or {}).get("stop_name") or r.get("stop_id"),
            "eta_min": None,
            "delay_min": None
        })
    return {"trip_id": trip_id, "route": None, "headsign": None, "vehicle": None, "calls": calls}

@app.get("/api/routes/search")
async def api_route_search(q: str = Query("", description="Járatszám/név"), limit: int = 30):
    schedule = _read_json(DATA_DIR / "schedule.json", {})
    routes = set()
    for lst in schedule.values():
        for it in lst:
            if it.get("route"):
                routes.add(it["route"])
    res = sorted([r for r in routes if q.strip().lower() in str(r).lower()], key=lambda x: (len(str(x)), str(x)))
    return [{"route": r} for r in res[:limit]]

@app.get("/api/routes/{route}/vehicles")
async def api_route_vehicles(route: str):
    if not await siri_live.is_available():
        return []
    try:
        vs = await siri_live.vehicles_by_route(route)
        return [enrich_vehicle(v) for v in vs][:100]
    except Exception:
        return []

# ===================== FRONT =====================
@app.get("/index.html", include_in_schema=False)
async def index_file():
    return FileResponse(str(BASE_DIR / "index.html"), media_type="text/html")
