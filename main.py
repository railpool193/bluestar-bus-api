from __future__ import annotations

# ── standard lib
import io
import csv
import json
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

# ── fastapi
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

# ── app init
app = FastAPI(title="Bluestar Bus – API", version="1.2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ha kell szigorítani, itt tedd
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── fájl elérési utak
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
INDEX_FILE = BASE_DIR / "index.html"

STOPS_JSON = DATA_DIR / "stops.json"
SCHEDULE_JSON = DATA_DIR / "schedule.json"


# ========== Segédfüggvények ====================================================

def gtfs_files_exist() -> bool:
    return STOPS_JSON.exists() and SCHEDULE_JSON.exists()


def _find_member(zf: zipfile.ZipFile, name: str) -> str | None:
    """GTFS fájl keresése a zip-ben, gyökérben vagy almappában (case-insensitive)."""
    lname = name.lower()
    for m in zf.namelist():
        ml = m.lower()
        if ml == lname or ml.endswith("/" + lname):
            return m
    return None


def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    """
    GTFS feldolgozás:
    - stops.json: [ {stop_id, stop_name} ]
    - schedule.json: { stop_id: [ {time, route, destination} ] }
    Egyszerűsített menetrend (calendar-t most nem kezeljük).
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        required = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
        members: Dict[str, str | None] = {n: _find_member(zf, n) for n in required}
        missing = [n for n, m in members.items() if m is None]
        if missing:
            raise ValueError("Hiányzó GTFS fájlok a ZIP-ben: " + ", ".join(missing))

        # --- stops.json ---
        stops: List[Dict[str, str]] = []
        with zf.open(members["stops.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                stops.append({
                    "stop_id": row["stop_id"],
                    "stop_name": (row.get("stop_name") or "").strip()
                })
        STOPS_JSON.write_text(json.dumps(stops, ensure_ascii=False), encoding="utf-8")

        # --- routes táblázat: route_id -> short/long name ---
        routes: Dict[str, str] = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = (
                    row.get("route_short_name")
                    or row.get("route_long_name")
                    or ""
                ).strip()

        # --- trips táblázat: trip_id -> (route_name, headsign) ---
        trips: Dict[str, Dict[str, str]] = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trips[row["trip_id"]] = {
                    "route": routes.get(row["route_id"], ""),
                    "headsign": (row.get("trip_headsign") or "").strip()
                }

        # --- stop_times -> schedule.json ---
        from collections import defaultdict
        schedule: Dict[str, List[Dict[str, str]]] = defaultdict(list)
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
                    "destination": trip["headsign"]
                })

        SCHEDULE_JSON.write_text(json.dumps(schedule, ensure_ascii=False), encoding="utf-8")


def _now_seconds() -> int:
    """Aktuális idő másodpercben (napon belül), helyi idő alapján."""
    now = datetime.now()
    return now.hour * 3600 + now.minute * 60 + now.second


def _time_str_to_seconds(t: str) -> int:
    """GTFS HH:MM(:SS) -> másodperc."""
    parts = [int(p) for p in t.split(":")]
    if len(parts) == 2:
        h, m = parts
        s = 0
    else:
        h, m, s = parts[:3]
    # GTFS-ben lehet 24+ óra is (pl. 25:10:00), ezért nem modulozzuk itt, csak számolunk
    return h * 3600 + m * 60 + s


# ========== UI gyökér ===========================================================

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ui_root():
    """
    Az index.html kiszolgálása. Cache TILTÁS, hogy mindig a frisset kapd.
    """
    if INDEX_FILE.exists():
        content = INDEX_FILE.read_text(encoding="utf-8")
    else:
        content = "<h1>Missing index.html</h1>"
    return HTMLResponse(content=content, headers={"Cache-Control": "no-store, max-age=0"})


# ========== API =================================================================

@app.get("/api/status")
async def api_status():
    """
    Egyszerű „egészségjelentés”.
    - gtfs: betöltve-e a feldolgozott JSON
    - live: itt most helyben False; ha később bekötjük a SIRI-t, innen jelezzük.
    """
    return {"status": "ok", "gtfs": gtfs_files_exist(), "live": False}


@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    """
    GTFS ZIP feltöltése és azonnali feldolgozása.
    A feldolgozás után a /api/status `gtfs: true`-t fog mutatni.
    """
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="ZIP fájlt adj meg.")

    content = await file.read()
    # opcionális: mentsük el a legutóbbi ZIP-et
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)

    try:
        _build_from_zip_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Feldolgozási hiba: {e}") from e

    return {"status": "uploaded"}


@app.get("/api/stops/search")
async def api_stop_search(q: str = Query(..., min_length=2, description="Megálló név (részlet is lehet)")):
    """
    Megálló-keresés (case-insensitive részszó).
    Válasz: [ { stop_id, stop_name }, ... ]
    """
    if not gtfs_files_exist():
        return []

    stops = json.loads(STOPS_JSON.read_text(encoding="utf-8"))
    ql = q.strip().lower()
    results = [s for s in stops if ql in (s.get("stop_name", "").lower())]
    # limitáljuk mondjuk 30-ra
    return results[:30]


@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=360)):
    """
    Következő indulások az adott megállóból, az elkövetkező `minutes` percben.
    Egyszerűsített (napi) szűrés az időpontokra, dátum-kezelés nélkül.
    """
    if not gtfs_files_exist():
        return {"stop_id": stop_id, "minutes": minutes, "results": []}

    schedule: Dict[str, List[Dict[str, str]]] = json.loads(SCHEDULE_JSON.read_text(encoding="utf-8"))
    entries = schedule.get(stop_id, [])

    now_s = _now_seconds()
    horizon = now_s + minutes * 60

    # átváltjuk a belső időket másodpercre és szűrjük
    out: List[Dict[str, Any]] = []
    for e in entries:
        tsec = _time_str_to_seconds(e["time"])
        # ha a GTFS-ben 24+ óra szerepel, igazítsuk egy napon belüli „folytatott időhöz”
        # egyszerű megközelítés: ha tsec < 24h és now közelében vagyunk, működik;
        # 24+ óráknál engedjük át, mert horizontig így is eljut.
        if now_s <= tsec <= horizon:
            out.append({
                "route": e.get("route", ""),
                "destination": e.get("destination", ""),
                "time_iso": (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                             + timedelta(seconds=tsec)).isoformat(timespec="minutes"),
                "is_live": False  # ide köthetjük később a valósidejűséget
            })

    # rendezzük idő szerint és limitáljuk ésszerűen
    out.sort(key=lambda x: x["time_iso"])
    return {"stop_id": stop_id, "minutes": minutes, "results": out[:100]}
    

# ========== Lokális futtatás ====================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
