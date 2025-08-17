import io, json, zipfile, csv, os
from pathlib import Path
from collections import defaultdict
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse

app = FastAPI(title="Bluestar Bus – API", version="1.2.2")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# --------- GTFS segédek ---------
def gtfs_files_exist() -> bool:
    return (DATA_DIR / "stops.json").exists() and (DATA_DIR / "schedule.json").exists()

def _find_member(zf: zipfile.ZipFile, name: str) -> str | None:
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
            raise ValueError(f"Hiányzó GTFS fájl(ok): {', '.join(missing)}")

        # stops.json
        stops = []
        with zf.open(members["stops.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                stops.append({
                    "stop_id": row["stop_id"],
                    "stop_name": (row.get("stop_name") or "").strip(),
                })
        (DATA_DIR / "stops.json").write_text(json.dumps(stops, ensure_ascii=False), encoding="utf-8")

        # routes
        routes = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = row.get("route_short_name") or row.get("route_long_name") or ""

        # trips
        trips = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trips[row["trip_id"]] = {
                    "route": routes.get(row["route_id"], ""),
                    "headsign": (row.get("trip_headsign") or "").strip()
                }

        # schedule.json  (stop_id -> list[ {time, route, destination} ])
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
                    "time": t,
                    "route": trip["route"],
                    "destination": trip["headsign"]
                })

        (DATA_DIR / "schedule.json").write_text(json.dumps(schedule, ensure_ascii=False), encoding="utf-8")

# --------- API végpontok ---------
# UI gyökér
@app.get("/", response_class=HTMLResponse)
async def ui_root():
    # ugyanabból a mappából szolgáljuk ki az index.html-t
    return FileResponse(BASE_DIR / "index.html")

@app.get("/api/status")
async def api_status():
    # ha lesz SIRI live, itt lehet True
    live_available = False
    return {"status": "ok", "gtfs": gtfs_files_exist(), "live": live_available}

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    content = await file.read()
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    try:
        _build_from_zip_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "uploaded"}

@app.get("/api/stops/search")
async def search_stops(q: str):
    if not gtfs_files_exist():
        return []
    ql = q.strip().lower()
    if not ql:
        return []
    stops = json.loads((DATA_DIR / "stops.json").read_text(encoding="utf-8"))
    out = [s for s in stops if ql in (s.get("stop_name","").lower())]
    return out[:20]

def _hhmm_to_minutes(hhmm: str) -> int:
    try:
        h, m = hhmm.split(":")[:2]
        return int(h) * 60 + int(m)
    except:
        return 999999

@app.get("/api/stops/{stop_id}/next_departures")
async def next_departures(stop_id: str, window: int = 60):
    if not gtfs_files_exist():
        return {"stop": None, "departures": []}

    # stop név
    stop_name = None
    for s in json.loads((DATA_DIR / "stops.json").read_text(encoding="utf-8")):
        if s["stop_id"] == stop_id:
            stop_name = s["stop_name"]
            break

    schedule = json.loads((DATA_DIR / "schedule.json").read_text(encoding="utf-8"))
    items = schedule.get(stop_id, [])

    # mostani idő
    from datetime import datetime
    now = datetime.utcnow()  # GTFS-ben nincs zóna; elég demo célra
    now_min = now.hour * 60 + now.minute
    end_min = now_min + max(0, int(window))

    deps = []
    for it in items:
        tmin = _hhmm_to_minutes(it["time"])
        # egyszerű ablak: azonos napon lévő, és most<=t<most+window
        if now_min <= tmin < end_min:
            deps.append({
                "route": it["route"],
                "destination": it["destination"],
                "time": f"{tmin//60:02d}:{tmin%60:02d}",
                "live": None  # később SIRI-rel feltölthető
            })
    deps.sort(key=lambda d: d["time"])
    return {"stop": {"stop_id": stop_id, "stop_name": stop_name}, "departures": deps}
