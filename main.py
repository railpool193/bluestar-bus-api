# main.py
from __future__ import annotations

import csv
import io
import json
import os
import re
import shutil
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import httpx
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

# ──────────────────────────────────────────────────────────────────────────────
# Konfiguráció / mappák
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR: Path = Path(__file__).resolve().parent
DATA_DIR: Path = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
GTFS_DIR: Path = BASE_DIR / "gtfs"
STATIC_DIR: Path = BASE_DIR / "static"

DATA_DIR.mkdir(parents=True, exist_ok=True)
GTFS_DIR.mkdir(parents=True, exist_ok=True)

STOPS_INDEX = DATA_DIR / "stops_index.json"

# Live (SIRI/BODS) env
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()
BODS_BASE_URL = os.getenv("BODS_BASE_URL", "https://data.bus-data.dft.gov.uk/api/v1").rstrip("/")
BODS_FEED_ID = os.getenv("BODS_FEED_ID", "").strip()

# Egyszerű cache a SIRI feedhez (kb. 20–30 mp elég)
_siri_cache: Dict[str, Tuple[float, Any]] = {}
_SIRI_TTL_SEC = 25.0

# ──────────────────────────────────────────────────────────────────────────────
# App & statikus fájlok
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Bluestar Bus – API", version="1.1.0")
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


# ──────────────────────────────────────────────────────────────────────────────
# Segédfüggvények
# ──────────────────────────────────────────────────────────────────────────────
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_hms_to_seconds(hms: str) -> int:
    """GTFS HH:MM:SS → másodperc (24+ órát is enged pl. 25:10:00)."""
    m = re.match(r"^(\d+):(\d{2}):(\d{2})$", hms)
    if not m:
        return -1
    h, m_, s = map(int, m.groups())
    return h * 3600 + m_ * 60 + s


def _load_csv_from_gtfs(name: str) -> List[Dict[str, str]]:
    """Beolvas egy .txt (CSV) GTFS fájlt a kitömörített mappából."""
    file_path = GTFS_DIR / name
    if not file_path.exists():
        return []
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def _build_stops_index() -> None:
    """Egyszerű keresőindex a megállókhoz."""
    stops = _load_csv_from_gtfs("stops.txt")
    index = [
        {"stop_id": s.get("stop_id", "").strip(), "stop_name": s.get("stop_name", "").strip()}
        for s in stops
        if s.get("stop_id") and s.get("stop_name")
    ]
    STOPS_INDEX.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")


def _gtfs_loaded() -> bool:
    return STOPS_INDEX.exists() and any(GTFS_DIR.glob("*.txt"))


# ──────────────────────────────────────────────────────────────────────────────
# API – státusz
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/api/status")
def status():
    return {
        "status": "ok",
        "gtfs_loaded": _gtfs_loaded(),
        "siri_configured": bool(BODS_API_KEY and BODS_FEED_ID),
    }


# ──────────────────────────────────────────────────────────────────────────────
# GTFS feltöltés (multipart/form-data: file=<gtfs.zip>)
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/api/upload")
async def upload_gtfs(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Adj meg egy GTFS .zip fájlt.")

    # ideiglenesen mentsük
    tmp_zip = DATA_DIR / "gtfs_upload.zip"
    with tmp_zip.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    # ürítsük a GTFS_DIR-t és bontsuk ki
    for p in GTFS_DIR.glob("*"):
        if p.is_file():
            p.unlink()
        else:
            shutil.rmtree(p, ignore_errors=True)

    with zipfile.ZipFile(tmp_zip, "r") as z:
        z.extractall(GTFS_DIR)

    # index újraépítése
    _build_stops_index()

    return {"status": "ok", "method": "upload", "message": "GTFS betöltve az adatbázisba."}


# ──────────────────────────────────────────────────────────────────────────────
# Megálló keresés
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/api/stops/search")
def search_stops(q: str):
    if len(q or "") < 2:
        return []
    if not STOPS_INDEX.exists():
        return []
    items = json.loads(STOPS_INDEX.read_text(encoding="utf-8"))
    ql = q.lower()
    return [it for it in items if ql in it["stop_name"].lower()][:20]


# ──────────────────────────────────────────────────────────────────────────────
# Következő indulások (egyszerű, “hézagos” GTFS logika – naptárat nem szűrünk)
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/api/stops/{stop_id}/next_departures")
def next_departures(stop_id: str, minutes: int = 60):
    if not _gtfs_loaded():
        return {"stop_id": stop_id, "minutes": minutes, "results": []}

    stop_times = _load_csv_from_gtfs("stop_times.txt")
    trips = _load_csv_from_gtfs("trips.txt")
    routes = _load_csv_from_gtfs("routes.txt")

    trip_to_route: Dict[str, str] = {t["trip_id"]: t.get("route_id", "") for t in trips if "trip_id" in t}
    route_names: Dict[str, str] = {}
    for r in routes:
        name = r.get("route_short_name") or r.get("route_long_name") or r.get("route_id", "")
        route_names[r.get("route_id", "")] = name

    # Mostani idő (helyi zóna helyett UTC; a GTFS időpontok napon belüliek)
    now = _now_utc()
    now_sec = now.hour * 3600 + now.minute * 60 + now.second
    window = minutes * 60

    results: List[Dict[str, Any]] = []
    for st in stop_times:
        if st.get("stop_id") != stop_id:
            continue
        arr = st.get("departure_time") or st.get("arrival_time") or ""
        sec = _parse_hms_to_seconds(arr)
        if sec < 0:
            continue
        # csak a következő "window" percen belüliek
        if 0 <= sec - now_sec <= window:
            trip_id = st.get("trip_id", "")
            route_id = trip_to_route.get(trip_id, "")
            route = route_names.get(route_id, route_id)
            # destination-nek megpróbáljuk a "stop_headsign"/"trip_headsign" mezőt használni
            dest = st.get("stop_headsign") or next((t.get("trip_headsign")
                                                    for t in trips if t.get("trip_id") == trip_id and t.get("trip_headsign")), "")
            dt_iso = (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=sec)).isoformat()
            results.append(
                {"route": route, "destination": dest or "", "time_iso": dt_iso, "is_live": False}
            )

    results.sort(key=lambda x: x["time_iso"])
    return {"stop_id": stop_id, "minutes": minutes, "results": results[:30]}


# ──────────────────────────────────────────────────────────────────────────────
# Live (SIRI VM / BODS) – egyszerű feed letöltés + gyors cache
# ──────────────────────────────────────────────────────────────────────────────
async def _fetch_siri_vm() -> Any:
    if not (BODS_API_KEY and BODS_FEED_ID):
        raise HTTPException(status_code=503, detail="Live feed nincs konfigurálva.")

    cache_key = "vm"
    now = time.time()
    if cache_key in _siri_cache:
        ts, payload = _siri_cache[cache_key]
        if now - ts < _SIRI_TTL_SEC:
            return payload

    url = f"{BODS_BASE_URL}/datafeed/{BODS_FEED_ID}?api_key={BODS_API_KEY}"
    # BODS XML-t ad vissza; nekünk elég a nyers text, és kliensoldalt jelzünk, ha live érkezik
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        xml_text = resp.text

    _siri_cache[cache_key] = (now, xml_text)
    return xml_text


@app.get("/api/live/{stop_id}")
async def live_for_stop(stop_id: str):
    """
    Visszaad egy nagyon egyszerű jelzést:
    - ha van érvényes SIRI VM feed és sikerült lekérni, 'available': True
    - a konkrét stop_id szerinti szűrést itt nem erőltetjük (feed kompatibilitás miatt),
      a frontend csak a 'is_live' flaget használja az indulásoknál.
    """
    if not (BODS_API_KEY and BODS_FEED_ID):
        return {"stop_id": stop_id, "available": False}

    try:
        payload = await _fetch_siri_vm()
        return {"stop_id": stop_id, "available": bool(payload)}
    except Exception as e:
        # ne dobjuk fel a teljes hibát – legyen false
        return {"stop_id": stop_id, "available": False, "error": str(e)}


# ──────────────────────────────────────────────────────────────────────────────
# Egyszerű health
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/healthz")
def healthz():
    return {"ok": True}
