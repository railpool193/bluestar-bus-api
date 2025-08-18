import io, json, zipfile, csv
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI(title="Bluestar Bus – API", version="2.1.0")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

INDEX_FILE = BASE_DIR / "index.html"

# ---------- Cache-tiltás minden válaszra ----------
@app.middleware("http")
async def add_no_cache_headers(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# ---------- Statikus / gyökér ----------
@app.get("/", include_in_schema=False)
async def ui_root():
    return FileResponse(str(INDEX_FILE), media_type="text/html")

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static"), html=False), name="static")

# ---------- Segédfüggvények ----------
def gtfs_files_exist() -> bool:
    return (DATA_DIR / "stops.json").exists() and (DATA_DIR / "schedule.json").exists()

def read_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _find_member(zf: zipfile.ZipFile, name: str) -> Optional[str]:
    lname = name.lower()
    for m in zf.namelist():
        if m.lower().endswith("/" + lname) or m.lower() == lname:
            return m
    return None

def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    """GTFS feldolgozás: stops.json + schedule.json létrehozása (egyszerű, gyors)."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        req = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
        members = {n: _find_member(zf, n) for n in req}
        missing = [n for n, m in members.items() if m is None]
        if missing:
            raise ValueError(f"Hiányzó GTFS fájlok a ZIP-ben: {', '.join(missing)}")

        # stops.json
        stops: List[Dict[str, Any]] = []
        with zf.open(members["stops.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                stops.append({
                    "stop_id": row["stop_id"],
                    "stop_name": (row.get("stop_name") or "").strip(),
                })
        (DATA_DIR / "stops.json").write_text(json.dumps(stops, ensure_ascii=False), encoding="utf-8")

        # routes map
        routes: Dict[str, str] = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = row.get("route_short_name") or row.get("route_long_name") or ""

        # trips map
        trips: Dict[str, Dict[str, str]] = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trips[row["trip_id"]] = {
                    "route": routes.get(row["route_id"], ""),
                    "headsign": (row.get("trip_headsign") or "").strip(),
                }

        # schedule.json: stop_id -> list of {time, route, destination}
        schedule = defaultdict(list)
        with zf.open(members["stop_times.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trip = trips.get(row["trip_id"])
                if not trip:
                    continue
                t = (row.get("departure_time") or row.get("arrival_time") or "").strip()
                if not t:
                    continue
                schedule[row["stop_id"]].append({
                    "time": t,  # HH:MM:SS
                    "route": trip["route"],
                    "destination": trip["headsign"],
                })

        (DATA_DIR / "schedule.json").write_text(json.dumps(schedule, ensure_ascii=False), encoding="utf-8")

# ---------- API ----------
@app.get("/api/status")
async def api_status():
    build = os.environ.get("RAILWAY_BUILD", "") or str(int(datetime.utcnow().timestamp()))
    live_available = bool(os.environ.get("LIVE_ENABLED", ""))  # ha lesz SIRI, itt kapcsolható
    return {"status": "ok", "gtfs": gtfs_files_exist(), "live": live_available, "build": build}

@app.get("/api/stops/search")
async def api_search_stops(q: str = Query(..., min_length=1), limit: int = 20):
    stops = read_json(DATA_DIR / "stops.json", [])
    ql = q.lower()
    res = [s for s in stops if ql in (s.get("stop_name") or "").lower()]
    res = res[:limit]
    return res

def _to_today_dt(hms: str) -> datetime:
    # HH:MM[:SS] -> datetime ma (UTC+0 feltételezés; a kijelzéshez elég)
    parts = [int(p) for p in hms.split(":")]
    while len(parts) < 3:
        parts.append(0)
    now = datetime.utcnow()
    base = datetime(now.year, now.month, now.day, 0, 0, 0)
    return base + timedelta(hours=parts[0], minutes=parts[1], seconds=parts[2])

@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = 60):
    schedule: Dict[str, List[Dict[str, Any]]] = read_json(DATA_DIR / "schedule.json", {})
    items = schedule.get(stop_id, [])
    now = datetime.utcnow()
    window = now + timedelta(minutes=minutes)

    out = []
    for it in items:
        dt = _to_today_dt(it["time"])
        # kezeli a 24+ órás időket is (pl. 25:10:00 -> másnap)
        if dt < now:
            # ha múltban van, de +24h beleesik az ablakba, toljuk 24h-val
            dt_plus = dt + timedelta(days=1)
            if dt_plus <= window:
                due = int((dt_plus - now).total_seconds() // 60)
                out.append({**it, "time": dt_plus.strftime("%H:%M"), "due": due})
            continue
        if dt <= window:
            due = int((dt - now).total_seconds() // 60)
            out.append({**it, "time": dt.strftime("%H:%M"), "due": due})

    out.sort(key=lambda x: (x["due"], x["time"]))
    return out

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    content = await file.read()
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    _build_from_zip_bytes(content)
    return {"status": "uploaded"}
