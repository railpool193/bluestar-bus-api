import io, json, csv, zipfile, time
from pathlib import Path
from typing import Dict, Any, List, Optional
from collections import defaultdict

from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Bluestar Bus – API", version="2.1.0")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
# --- LIVE: SIRI/AVL adapter (mock + hook) -----------------
import json, asyncio
from pathlib import Path

# Feltételezem, hogy DATA_DIR már nálad létezik. Ha nem:
# DATA_DIR = Path(__file__).parent / "data"
# DATA_DIR.mkdir(parents=True, exist_ok=True)

class SiriLiveAdapter:
    """Ezt később könnyen kicserélheted valódi SIRI/AVL hívásokra."""
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    async def is_available(self) -> bool:
        # Valódi esetben itt lehetne egy ping a szolgáltatásra.
        return (self.data_dir / "live_available.flag").exists()

    async def vehicles_by_route(self, route_no: str) -> list[dict]:
        """
        Vissza: [{reg,type,lat,lon,bearing,stop_name,trip_id,headsign}, ...]
        """
        f = self.data_dir / f"live_route_{route_no}.json"
        if not f.exists():
            return []
        return json.loads(f.read_text(encoding="utf-8"))

    async def stop_next_departures(self, stop_id: str, mins: int) -> list[dict]:
        """
        Vissza: [{trip_id,route,headsign,scheduled_time,eta_min,delay_min,vehicle_reg}, ...]
        """
        f = self.data_dir / f"live_stop_{stop_id}.json"
        if not f.exists():
            return []
        return json.loads(f.read_text(encoding="utf-8"))

    async def trip_details(self, trip_id: str) -> dict:
        """
        Vissza: {trip_id, route, headsign, calls:[{time,stop_id,stop_name,eta_min,delay_min}],
                 vehicle:{reg,type}}
        """
        f = self.data_dir / f"live_trip_{trip_id}.json"
        if not f.exists():
            return {}
        return json.loads(f.read_text(encoding="utf-8"))

# Globális példány:
siri_live = SiriLiveAdapter(DATA_DIR)
DATA_DIR.mkdir(exist_ok=True)

# ---- CORS + cache OFF az indexhez ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# -------- Helpers --------
def _read_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))

def _write_json(path: Path, data: Any):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

def gtfs_files_exist() -> bool:
    return (DATA_DIR / "stops.json").exists() and (DATA_DIR / "schedule.json").exists()

def vehicles_map() -> Dict[str, Dict[str, str]]:
    """
    Opcionális mapping: rendszám -> { "model": "...", "fleet_no": "..." }
    Fájl: data/vehicles.json  (szabadon bővíthető)
    """
    return _read_json(DATA_DIR / "vehicles.json", {})

# ----- EGY NAGYON EGYSZERŰ „élő réteg” -----
# Itt hagyunk egy csatlakozási pontot a SIRI/RT-hez. Ha van háttér
# folyamatod, ami ide kiír JSON-t (pl. data/live_state.json), a frontend azonnal használja.
class LiveLayer:
    def __init__(self, path: Path):
        self.path = path

    async def is_available(self) -> bool:
        return self.path.exists()

    def _state(self):
        return _read_json(self.path, {
            "updated": 0,
            # stop_id -> list of {time(mins), route, destination, trip_id, live: true, vehicle: {reg, model}, progress%}
            "live_departures": {},
            # route_short_name -> list of vehicles {lat, lon, bearing, dir, reg, model, next_stop, when}
            "route_vehicles": {}
        })

    async def departures_for_stop(self, stop_id: str) -> List[Dict[str, Any]]:
        return self._state().get("live_departures", {}).get(stop_id, [])

    async def vehicles_for_route(self, route_short: str) -> List[Dict[str, Any]]:
        return self._state().get("route_vehicles", {}).get(route_short, [])

siri_live = LiveLayer(DATA_DIR / "live_state.json")

# -------- GTFS feldolgozás (feltöltés után) --------
def _find_member(zf: zipfile.ZipFile, name: str) -> Optional[str]:
    lname = name.lower()
    for m in zf.namelist():
        if m.lower().endswith("/" + lname) or m.lower() == lname:
            return m
    return None

def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        req = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
        members = {n: _find_member(zf, n) for n in req}
        missing = [n for n, m in members.items() if m is None]
        if missing:
            raise ValueError(f"Hiányzó GTFS fájlok: {', '.join(missing)}")

        # stops.json  (id + name + optional: code)
        stops = []
        with zf.open(members["stops.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                stops.append({
                    "stop_id": row["stop_id"],
                    "stop_name": row.get("stop_name", "").strip(),
                    "stop_code": row.get("stop_code", "").strip(),
                })
        _write_json(DATA_DIR / "stops.json", stops)

        # routes: route_id -> short/long
        routes = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = {
                    "short": row.get("route_short_name") or "",
                    "long": row.get("route_long_name") or "",
                }

        # trips: trip_id -> {route_short, headsign}
        trips = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                r = routes.get(row["route_id"], {"short": "", "long": ""})
                trips[row["trip_id"]] = {
                    "route": r["short"] or r["long"],
                    "headsign": (row.get("trip_headsign") or "").strip(),
                }

        # schedule: stop_id -> list of departures (HH:MM:SS, route, dest, trip_id)
        schedule = defaultdict(list)
        # trip_stops: trip_id -> list of {seq, stop_id, time}
        trip_stops = defaultdict(list)
        with zf.open(members["stop_times.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
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

        # sort lists
        for lst in schedule.values():
            lst.sort(key=lambda x: x["time"])
        for lst in trip_stops.values():
            lst.sort(key=lambda x: x["seq"])

        _write_json(DATA_DIR / "schedule.json", schedule)
        _write_json(DATA_DIR / "trip_stops.json", trip_stops)

    (DATA_DIR / "gtfs_loaded.flag").write_text("ok", encoding="utf-8")


# --------- API ENDPOINTS ---------
@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "gtfs": gtfs_files_exist(),
        "live": await siri_live.is_available(),
        "build": str(int(time.time()))
    }

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    content = await file.read()
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    _build_from_zip_bytes(content)
    return {"status": "uploaded"}

@app.get("/api/stops/search")
async def stops_search(q: str = Query("", description="Megálló neve vagy kódja")):
    stops = _read_json(DATA_DIR / "stops.json", [])
    ql = q.strip().lower()
    if not ql:
        return []
    res = [
        s for s in stops
        if ql in s["stop_name"].lower() or (s.get("stop_code") or "").lower().startswith(ql)
    ]
    # max 20 találat
    return res[:20]

def _hhmm_to_minutes(t: str) -> int:
    try:
        h, m, *_ = t.split(":")
        return int(h) * 60 + int(m)
    except:
        return 0

@app.get("/api/stops/{stop_id}/next_departures")
async def next_departures(stop_id: str, minutes: int = 60):
    schedule = _read_json(DATA_DIR / "schedule.json", {})
    base = schedule.get(stop_id, [])

    # időablak szűrés: a mai nap perceihez képest
    now = time.localtime()
    now_min = now.tm_hour * 60 + now.tm_min
    upper = now_min + minutes

    upcoming = []
    for d in base:
        dep_min = _hhmm_to_minutes(d["time"]) % (24*60)
        # egyszerű: „mosttól + ablak” (átfordulást nagyvonalúan engedjük)
        if now_min <= dep_min <= upper or (upper >= 24*60 and dep_min <= (upper % (24*60))):
            minutes_left = (dep_min - now_min) % (24*60)
            upcoming.append({
                "route": d["route"],
                "destination": d["destination"],
                "time": d["time"],
                "in_minutes": minutes_left,
                "trip_id": d["trip_id"],
                "live": False,
                "vehicle": None
            })

    # élő overlay
    if await siri_live.is_available():
        live = await siri_live.departures_for_stop(stop_id)
        # felülírjuk/összefésüljük (azonos trip vagy route+dest+közeli idő)
        def sig(item): return (item["trip_id"] or "", item["route"], item["destination"])
        by_sig = {sig(x): x for x in upcoming}
        for ld in live:
            key = (ld.get("trip_id") or "", ld.get("route"), ld.get("destination"))
            if key in by_sig:
                by_sig[key]["in_minutes"] = ld.get("time", by_sig[key]["in_minutes"])
                by_sig[key]["live"] = True
                by_sig[key]["vehicle"] = ld.get("vehicle")
            else:
                # új élő elem is jöhet
                add = {
                    "route": ld.get("route",""),
                    "destination": ld.get("destination",""),
                    "time": "", "in_minutes": ld.get("time", 0),
                    "trip_id": ld.get("trip_id"), "live": True,
                    "vehicle": ld.get("vehicle")
                }
                upcoming.append(add)

    # rendezés: élő előre, majd ETA
    upcoming.sort(key=lambda x: (not x["live"], x["in_minutes"]))
    return upcoming[:50]

@app.get("/api/trips/{trip_id}")
async def trip_details(trip_id: str):
    trip_stops = _read_json(DATA_DIR / "trip_stops.json", {})
    stops = {s["stop_id"]: s for s in _read_json(DATA_DIR / "stops.json", [])}
    seq = trip_stops.get(trip_id, [])
    for r in seq:
        st = stops.get(r["stop_id"])
        r["stop_name"] = st["stop_name"] if st else r["stop_id"]
    return {
        "trip_id": trip_id,
        "stops": seq
    }

@app.get("/api/routes/search")
async def route_search(q: str = Query("", description="Járatszám / név")):
    schedule = _read_json(DATA_DIR / "schedule.json", {})
    routes = set()
    for lst in schedule.values():
        for it in lst:
            if it.get("route"):
                routes.add(it["route"])
    ql = q.strip().lower()
    res = [r for r in routes if ql in str(r).lower()]
    res.sort(key=lambda x: (len(str(x)), str(x)))
    return res[:30]

@app.get("/api/routes/{route}/vehicles")
async def route_vehicles(route: str):
    # élő állapotból
    result = []
    if await siri_live.is_available():
        result = await siri_live.vehicles_for_route(route)
    # gazdagítás a vehicles.json-nal (ha élőben nincs model mező)
    vmap = vehicles_map()
    for v in result:
        if not v.get("model"):
            reg = (v.get("reg") or "").upper()
            meta = vmap.get(reg)
            if meta:
                v["model"] = meta.get("model")
                v["fleet_no"] = meta.get("fleet_no")
    return result

# ---- index.html kiszolgálása (cache kontroll) ----
@app.get("/", response_class=HTMLResponse)
async def index_html():
    html = (BASE_DIR / "index.html").read_text(encoding="utf-8")
    # no-cache fejlécek
    return HTMLResponse(content=html, headers={
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    })
