from __future__ import annotations
import io
import json
import csv
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# -------------------------
# Alap beállítások / mappák
# -------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# statikus (index.html)
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Bluestar Bus – API", version="1.2.3")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# kiszolgáljuk a /static-ot (nem kötelező, de hasznos)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# -------------------------
# Hasznos segédfüggvények
# -------------------------
def _stops_path() -> Path:
    return DATA_DIR / "stops.json"


def _schedule_path() -> Path:
    return DATA_DIR / "schedule.json"


def gtfs_files_exist() -> bool:
    return _stops_path().exists() and _schedule_path().exists()


def _find_member(zf: zipfile.ZipFile, name: str) -> str | None:
    """GTFS fájlt megkeresi gyökérben/al-mappában (case-insensitive)."""
    lname = name.lower()
    for m in zf.namelist():
        if m.lower().endswith("/" + lname) or m.lower() == lname:
            return m
    return None


def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    """
    GTFS feldolgozás: létrehozza a data/stops.json és data/schedule.json fájlokat.
    A schedule minimál: stop_id -> list[ {time, route, destination} ]
    """
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
                    "stop_name": (row.get("stop_name") or "").strip()
                })
        _stops_path().write_text(json.dumps(stops, ensure_ascii=False), encoding="utf-8")

        # routes map
        routes: Dict[str, str] = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = (
                    row.get("route_short_name")
                    or row.get("route_long_name")
                    or ""
                ).strip()

        # trips map
        trips: Dict[str, Dict[str, str]] = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trips[row["trip_id"]] = {
                    "route": routes.get(row["route_id"], ""),
                    "headsign": (row.get("trip_headsign") or "").strip()
                }

        # schedule.json
        schedule: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        with zf.open(members["stop_times.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                t = (row.get("departure_time") or row.get("arrival_time") or "").strip()
                if not t:
                    continue
                tr = trips.get(row["trip_id"])
                if not tr:
                    continue
                schedule[row["stop_id"]].append({
                    "time": t,  # HH:MM:SS
                    "route": tr["route"],
                    "destination": tr["headsign"],
                })

        # idő szerint sorba
        for sid in schedule:
            schedule[sid].sort(key=lambda x: x["time"])

        _schedule_path().write_text(json.dumps(schedule, ensure_ascii=False), encoding="utf-8")

    (DATA_DIR / "gtfs_loaded.flag").write_text("ok", encoding="utf-8")


def _load_json(p: Path, default):
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def _hms_to_seconds(hms: str) -> int:
    # GTFS-ben előfordulhat 24+ óra (pl. 25:10:00). Kezeljük.
    parts = [int(x) for x in hms.split(":")]
    h, m, s = (parts + [0, 0, 0])[:3]
    return h * 3600 + m * 60 + s


# -------------------------
# API végpontok
# -------------------------
@app.get("/", response_class=HTMLResponse)
async def ui_root():
    # index.html-t visszaadjuk
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return HTMLResponse("<h1>Bluestar Bus</h1><p>Hiányzó index.html</p>", status_code=200)
    return HTMLResponse(index_path.read_text(encoding="utf-8"))


@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "gtfs": gtfs_files_exist(),
        "live": False,  # később beköthető SIRI/RTPI
        "build": str(int(datetime.utcnow().timestamp()))
    }


@app.get("/api/stops/search")
async def api_stops_search(q: str):
    """Stop-keresés névrészletre (case-insensitive)."""
    ql = q.strip().lower()
    if not ql:
        return []
    stops = _load_json(_stops_path(), [])
    return [s for s in stops if ql in (s.get("stop_name") or "").lower()][:50]


@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = 60):
    """
    Következő indulások egyszerű (napi ismétlődő) logika szerint.
    A GTFS dátum-/naptárkezelést nem vállaljuk – cél: gyors demo.
    """
    schedule = _load_json(_schedule_path(), {})
    dep_list = schedule.get(stop_id, [])
    if not dep_list:
        return []

    # Most (UTC) -> UK idő eltolás nélkül is elég demónak: seconds since midnight UTC
    now = datetime.utcnow()
    now_sec = now.hour * 3600 + now.minute * 60 + now.second
    end_sec = now_sec + max(1, minutes) * 60

    # Mai ablak
    today = [d for d in dep_list if now_sec <= _hms_to_seconds(d["time"]) <= end_sec]

    # ha nincs, megpróbálunk "átcsúszó" (pl. 24+ órás) időket is
    if not today and any(_hms_to_seconds(d["time"]) > 24 * 3600 for d in dep_list):
        today = [d for d in dep_list if now_sec <= _hms_to_seconds(d["time"]) % (24 * 3600) <= end_sec]

    # visszaadjuk HH:MM formátumban
    out = []
    for d in today[:50]:
        sec = _hms_to_seconds(d["time"]) % (24 * 3600)
        hh = sec // 3600
        mm = (sec % 3600) // 60
        out.append({
            "route": d.get("route", ""),
            "destination": d.get("destination", ""),
            "time": f"{hh:02d}:{mm:02d}",
            "live": None  # később SIRI-vel kitölthető
        })
    return out


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    """GTFS ZIP feltöltés + azonnali feldolgozás."""
    content = await file.read()
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    _build_from_zip_bytes(content)
    return {"status": "uploaded"}
