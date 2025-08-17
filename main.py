from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import io, json, zipfile, time, re

app = FastAPI(title="Bluestar Bus – API", version="1.2.3")
BUILD_ID = str(int(time.time()))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# -------------------- segéd --------------------
def gtfs_files_exist() -> bool:
    return (DATA_DIR / "stops.json").exists() and (DATA_DIR / "schedule.json").exists()

def _find_member(zf: zipfile.ZipFile, name: str):
    lname = name.lower()
    for m in zf.namelist():
        ml = m.lower()
        if ml == lname or ml.endswith("/" + lname):
            return m
    return None

def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    import csv
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        req = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
        members = {n: _find_member(zf, n) for n in req}
        missing = [n for n, m in members.items() if m is None]
        if missing:
            raise ValueError("Hiányzó GTFS fájlok: " + ", ".join(missing))

        # stops.json
        stops = []
        with zf.open(members["stops.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig", errors="replace"))
            for r in reader:
                stops.append({"stop_id": r["stop_id"], "stop_name": (r.get("stop_name") or "").strip()})
        (DATA_DIR / "stops.json").write_text(json.dumps(stops, ensure_ascii=False), encoding="utf-8")

        # routes
        routes = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig", errors="replace"))
            for r in reader:
                routes[r["route_id"]] = r.get("route_short_name") or r.get("route_long_name") or ""

        # trips
        trips = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig", errors="replace"))
            for r in reader:
                trips[r["trip_id"]] = {
                    "route": routes.get(r["route_id"], ""),
                    "headsign": (r.get("trip_headsign") or "").strip()
                }

        # schedule.json
        from collections import defaultdict
        schedule = defaultdict(list)
        with zf.open(members["stop_times.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig", errors="replace"))
            for r in reader:
                trip = trips.get(r["trip_id"])
                if not trip:
                    continue
                t = (r.get("departure_time") or r.get("arrival_time") or "").strip()
                if not t:
                    continue
                schedule[r["stop_id"]].append({
                    "time": t,
                    "route": trip["route"],
                    "destination": trip["headsign"]
                })
        # json a defaultdict-ból
        schedule = {k: v for k, v in schedule.items()}
        (DATA_DIR / "schedule.json").write_text(json.dumps(schedule, ensure_ascii=False), encoding="utf-8")

# -------------------- UI --------------------
@app.get("/", response_class=HTMLResponse)
async def ui_root():
    html = (BASE_DIR / "index.html").read_text(encoding="utf-8").replace("{{BUILD_ID}}", BUILD_ID)
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )

# -------------------- API --------------------
@app.get("/api/status")
async def api_status():
    return {"status": "ok", "gtfs": gtfs_files_exist(), "live": False, "build": BUILD_ID}

# normalizáló a „biztos találatért”
_norm_re = re.compile(r"[^a-z0-9]+")
def _norm(s: str) -> str:
    return _norm_re.sub(" ", (s or "").lower()).strip()

@app.get("/api/stops/search")
async def api_search_stops(q: str = Query(min_length=2)):
    if not gtfs_files_exist():
        return []
    stops = json.loads((DATA_DIR / "stops.json").read_text(encoding="utf-8"))
    if not isinstance(stops, list) or not stops:
        return []
    qn = _norm(q)
    # részsztring vagy szóeleji egyezés
    hits = [s for s in stops if qn in _norm(s["stop_name"])]
    if not hits:
        hits = [s for s in stops if _norm(s["stop_name"]).startswith(qn)]
    return hits[:20]

@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = 60):
    if not gtfs_files_exist():
        return []
    schedule = json.loads((DATA_DIR / "schedule.json").read_text(encoding="utf-8"))
    rows = schedule.get(stop_id, [])
    return rows[:50]

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    content = await file.read()
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    _build_from_zip_bytes(content)
    return {"status": "uploaded"}

# -------------------- DEBUG (ideiglenes) --------------------
@app.get("/api/debug/stops/count")
async def dbg_count():
    if not (DATA_DIR / "stops.json").exists():
        return {"exists": False, "count": 0}
    stops = json.loads((DATA_DIR / "stops.json").read_text(encoding="utf-8"))
    return {"exists": True, "count": len(stops)}

@app.get("/api/debug/stops/sample")
async def dbg_sample(n: int = 5):
    if not (DATA_DIR / "stops.json").exists():
        return []
    stops = json.loads((DATA_DIR / "stops.json").read_text(encoding="utf-8"))
    return stops[:max(0, min(50, n))]
