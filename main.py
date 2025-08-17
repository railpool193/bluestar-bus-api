import os
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict
from pathlib import Path
import json

# --- ÚJ: live modul ---
import siri_live

app = FastAPI(title="Bluestar Bus – API", version="1.2.1")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# statikus UI (index.html a gyökérre)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


# ===== Segédfüggvények a GTFS adatainkhoz =====

_STOPS_PATH = DATA_DIR / "stops.json"
# A te korábbi feldolgozód hozta létre: next_departures számolásához menetrendi cache
_SCHEDULE_PATH = DATA_DIR / "schedule.json"

_STOPS: List[Dict] = []
_SCHEDULE: Dict[str, List[Dict]] = {}  # stop_id -> [{route,destination,time_iso}...]

def _load_gtfs_cache():
    global _STOPS, _SCHEDULE
    if _STOPS_PATH.exists():
        _STOPS = json.loads(_STOPS_PATH.read_text(encoding="utf-8"))
    if _SCHEDULE_PATH.exists():
        _SCHEDULE = json.loads(_SCHEDULE_PATH.read_text(encoding="utf-8"))

_load_gtfs_cache()


# ===== UI gyökér =====
@app.get("/", response_class=HTMLResponse)
async def ui_root():
    # a /static/index.html-t szolgáljuk ki gyökér alatt
    idx = BASE_DIR / "static" / "index.html"
    if not idx.exists():
        return HTMLResponse("<h1>UI hiányzik</h1>", status_code=500)
    return FileResponse(str(idx))


# ===== API =====
@app.get("/api/status")
async def api_status():
    gtfs_loaded = bool(_STOPS)
    # --- ÚJ: live státusz hívás biztonságosan
    try:
        live_ok = siri_live.is_live_available()
    except Exception:
        live_ok = False
    return {"status": "ok", "gtfs": gtfs_loaded, "live": live_ok}


@app.get("/api/stops/search")
async def search_stops(q: str):
    qn = (q or "").strip().lower()
    if len(qn) < 2:
        return []
    # egyszerű névrészlet keresés
    res = [
        {"stop_id": s["stop_id"], "stop_name": s["stop_name"]}
        for s in _STOPS
        if qn in s["stop_name"].lower()
    ]
    # maximum 15 találat
    return res[:15]


@app.get("/api/stops/{stop_id}/next_departures")
async def next_departures(stop_id: str, minutes: int = 60):
    # Menetrendi (GTFS-ből előállított) lista
    scheduled = _SCHEDULE.get(stop_id, [])

    # --- ÚJ: live overlay
    live_items: List[Dict] = []
    try:
        live_items = siri_live.get_live_departures(stop_id, limit=30)
    except Exception:
        live_items = []

    # egyszerű összeolvasztás: ha azonos járat+cél és közeli idő (±2 perc), akkor jelöld is_live=True
    def key(d: Dict) -> tuple:
        return (d.get("route", ""), d.get("destination", ""))

    from datetime import datetime, timezone
    def to_dt(iso: str):
        try:
            if iso.endswith("Z"):
                return datetime.fromisoformat(iso[:-1]).replace(tzinfo=timezone.utc)
            return datetime.fromisoformat(iso)
        except Exception:
            return None

    live_by_key = {}
    for li in live_items:
        live_by_key.setdefault(key(li), []).append(li)

    merged = []
    for s in scheduled:
        s_dt = to_dt(s.get("time_iso", ""))
        k = key(s)
        flag = False
        if s_dt and k in live_by_key:
            for li in live_by_key[k]:
                l_dt = to_dt(li["time_iso"])
                if not l_dt:
                    continue
                # 2 perc közelség: live-nek tekintjük
                if abs((l_dt - s_dt).total_seconds()) <= 120:
                    flag = True
                    break
        merged.append({
            "route": s.get("route", ""),
            "destination": s.get("destination", ""),
            "time_iso": s.get("time_iso", ""),
            "is_live": flag
        })

    # ha nincs menetrend, de van live, akkor listázzuk a live tételeket
    if not merged and live_items:
        merged = live_items

    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "results": merged[:40]
    }


# ===== GTFS feltöltés (a meglévő feldolgozódhoz igazítva) =====
@app.post("/api/upload")
async def upload_gtfs(file: UploadFile = File(...)):
    """
    Ez a végpont marad a korábbihoz illeszkedően.
    Feltételezzük, hogy a háttér feldolgozód a feltöltött ZIP-ből legenerálja a
    data/stops.json és data/schedule.json fájlokat.
    """
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="ZIP fájl szükséges")

    tmp = DATA_DIR / "upload_gtfs.zip"
    with tmp.open("wb") as f:
        f.write(await file.read())

    # --- Itt nálatok fut a feldolgozás (meglévő kód), ami a két JSON-t létrehozza ---
    # Ha nálad ez egy külön script/funkció, hívd meg itt.
    # Példa: process_gtfs_zip(tmp, DATA_DIR)
    # Most csak jelzünk, és feltételezzük, hogy a pipeline megcsinálja.

    # újratöltjük a memóriát
    _load_gtfs_cache()

    return {"status": "uploaded"}
