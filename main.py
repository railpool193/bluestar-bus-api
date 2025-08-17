from __future__ import annotations

# ---------- standard lib ----------
import io
import os
import json
import csv
import zipfile
from pathlib import Path
from datetime import datetime, timedelta

# ---------- third-party ----------
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

# =================================
# Alap beállítások / mappák
# =================================
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

INDEX_FILE = BASE_DIR / "index.html"

app = FastAPI(title="Bluestar Bus – API", version="1.2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =================================
# Live (SIRI) „kapcsoló”
#   – igazi bekötéshez add meg a környezeti változót:
#       SIRI_STOP_MONITORING_URL
#     ha üres, a „Live” jelzés piros marad.
# =================================
SIRI_STOP_MONITORING_URL = os.getenv("SIRI_STOP_MONITORING_URL", "").strip()


class SiriLive:
    """Egyszerű keret; most csak az elérhetőséget jelzi.
    Később ide tudjuk bekötni a valódi SIRI StopMonitoring hívást.
    """

    def is_available(self) -> bool:
        return bool(SIRI_STOP_MONITORING_URL)


siri_live = SiriLive()

# =================================
# Segédek GTFS-hez
# =================================
def gtfs_files_exist() -> bool:
    return (DATA_DIR / "stops.json").exists() and (DATA_DIR / "schedule.json").exists()


def _find_member(zf: zipfile.ZipFile, name: str) -> str | None:
    """Keres egy adott GTFS fájlt a zip gyökerében vagy almappában (case-insensitive)."""
    lname = name.lower()
    for m in zf.namelist():
        mm = m.lower()
        if mm == lname or mm.endswith("/" + lname):
            return m
    return None


def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    """GTFS zip feldolgozása: stops.json + schedule.json előállítása."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        required = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
        members = {n: _find_member(zf, n) for n in required}
        missing = [n for n, m in members.items() if m is None]
        if missing:
            raise ValueError(f"Hiányzó GTFS fájlok a ZIP-ben: {', '.join(missing)}")

        # ---- stops.json ----
        stops = []
        with zf.open(members["stops.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                stops.append(
                    {
                        "stop_id": row["stop_id"],
                        "stop_name": (row.get("stop_name") or "").strip(),
                    }
                )
        (DATA_DIR / "stops.json").write_text(
            json.dumps(stops, ensure_ascii=False), encoding="utf-8"
        )

        # ---- schedule.json ----
        # route_id -> route_short_name (vagy long, ha üres)
        routes: dict[str, str] = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = (
                    row.get("route_short_name")
                    or row.get("route_long_name")
                    or ""
                ).strip()

        # trip_id -> (route_short_name, headsign)
        trips: dict[str, dict[str, str]] = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trips[row["trip_id"]] = {
                    "route": routes.get(row["route_id"], ""),
                    "headsign": (row.get("trip_headsign") or "").strip(),
                }

        # stop_id -> list of {"time": "HH:MM:SS", "route": "...", "destination": "..."}
        from collections import defaultdict

        schedule: dict[str, list[dict[str, str]]] = defaultdict(list)
        with zf.open(members["stop_times.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trip = trips.get(row["trip_id"])
                if not trip:
                    continue
                t = (row.get("departure_time") or row.get("arrival_time") or "").strip()
                if not t:
                    continue
                schedule[row["stop_id"]].append(
                    {
                        "time": t,  # HH:MM(:SS)
                        "route": trip["route"],
                        "destination": trip["headsign"],
                    }
                )

        (DATA_DIR / "schedule.json").write_text(
            json.dumps(schedule, ensure_ascii=False),
            encoding="utf-8",
        )

    # jelzőfájl – opcionális
    (DATA_DIR / "gtfs_loaded.flag").write_text("ok", encoding="utf-8")


def _load_stops() -> list[dict]:
    p = DATA_DIR / "stops.json"
    if not p.exists():
        return []
    return json.loads(p.read_text(encoding="utf-8"))


def _load_schedule() -> dict[str, list[dict]]:
    p = DATA_DIR / "schedule.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _to_today_iso(time_hms: str) -> str:
    """HH:MM(:SS) -> mai nap ISO (helyi idő alapján)"""
    parts = [int(x) for x in time_hms.split(":")]
    now = datetime.now()
    hh, mm = parts[0], parts[1]
    ss = parts[2] if len(parts) > 2 else 0
    dt = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        hours=hh, minutes=mm, seconds=ss
    )
    # ha már elmúlt, hagyjuk benne – a front úgyis a következő 60 percet kérdezi
    return dt.isoformat(timespec="seconds")


# =================================
# Végpontok
# =================================
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ui_root():
    # index.html-t szolgáljuk ki
    if INDEX_FILE.exists():
        return HTMLResponse(INDEX_FILE.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Missing index.html</h1>", status_code=200)


@app.get("/index.html", response_class=FileResponse, include_in_schema=False)
async def index_html():
    return FileResponse(INDEX_FILE)


@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "gtfs": gtfs_files_exist(),
        "live": siri_live.is_available(),
    }


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    """GTFS ZIP feltöltés + azonnali feldolgozás."""
    content = await file.read()
    # opcionális: mentsük le a kapott zipet
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    # feldolgozás
    try:
        _build_from_zip_bytes(content)
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=400)
    return {"status": "uploaded"}


@app.get("/api/stops/search")
async def api_stops_search(q: str = Query("", min_length=2)):
    """Megállók keresése névrészletre."""
    if not gtfs_files_exist():
        return []
    ql = q.strip().lower()
    results = [
        s for s in _load_stops() if ql in (s.get("stop_name") or "").lower()
    ]
    # visszaadjuk mindet; a front rendezi/limitelheti
    return results


@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = 60):
    """Következő indulások a kiválasztott megállóból a következő X percben."""
    if not gtfs_files_exist():
        return {"stop_id": stop_id, "minutes": minutes, "results": []}

    sched = _load_schedule().get(stop_id, [])
    if not sched:
        return {"stop_id": stop_id, "minutes": minutes, "results": []}

    now = datetime.now()
    window_end = now + timedelta(minutes=minutes)

    out = []
    for item in sched:
        # HH:MM(:SS) -> mai nap
        t_iso = _to_today_iso(item["time"])
        t_dt = datetime.fromisoformat(t_iso)
        if now <= t_dt <= window_end:
            out.append(
                {
                    "route": item.get("route") or "",
                    "destination": item.get("destination") or "",
                    "time_iso": t_iso,
                    "is_live": False,  # majd SIRI bekötésnél jelöljük True-ra
                }
            )

    # egyszerű rendezés idő szerint
    out.sort(key=lambda x: x["time_iso"])

    return {"stop_id": stop_id, "minutes": minutes, "results": out}
