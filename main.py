import os
import csv
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from datetime import datetime, timedelta

from gtfs_utils import get_next_departures  # a korábbi GTFS logikád
import siri_live  # a korábbi “élő” adat logikád

app = FastAPI(title="Bluestar Bus API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
STOPS_FILE = DATA_DIR / "stops.txt"

# ---------------------------
# Megálló-adatbázis (GTFS/stops.txt)
# ---------------------------
STOPS: List[Dict[str, Any]] = []

def _norm(s: str) -> str:
    return " ".join(s.lower().strip().split())

def _load_stops() -> None:
    global STOPS
    STOPS = []
    if not STOPS_FILE.exists():
        return
    with open(STOPS_FILE, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            # A GTFS tipikusan: stop_id, stop_name, stop_lat, stop_lon, ...
            STOPS.append({
                "stop_id": row.get("stop_id", "").strip(),
                "name": row.get("stop_name", "").strip(),
                "lat": row.get("stop_lat"),
                "lon": row.get("stop_lon"),
                "loc": row.get("stop_desc") or row.get("platform_code") or "",
            })
_load_stops()

# ---------------------------
# Root + egészségügy
# ---------------------------
@app.get("/", response_class=JSONResponse)
def root():
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "health": "/health",
            "ck_next_60": "/vincents-walk/ck?minutes=60",
            "cm_next_60": "/vincents-walk/cm?minutes=60",
            "generic_example": "/next_departures/1980SN12619E?minutes=60",
        },
    }

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat() + "Z"}

# ---------------------------
# Megálló-keresés (kis/nagybetű független, több találatot ad vissza)
# ---------------------------
@app.get("/search_stop")
def search_stop(
    name: str = Query(..., description="Megálló neve (részlet is lehet)"),
    limit: int = Query(15, ge=1, le=50),
):
    if not STOPS:
        raise HTTPException(status_code=503, detail="Nincs GTFS stops.txt betöltve.")
    q = _norm(name)
    if len(q) < 2:
        return {"query": name, "results": []}

    results = []
    for s in STOPS:
        if q in _norm(s["name"]):
            results.append({
                "stop_id": s["stop_id"],
                "name": s["name"],
                "lat": s["lat"],
                "lon": s["lon"],
            })
        if len(results) >= limit:
            break
    return {"query": name, "count": len(results), "results": results}

# ---------------------------
# Következő indulások (GTFS)
# ---------------------------
@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    try:
        deps = get_next_departures(stop_id, minutes=minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": deps}
    except TypeError:
        # korábbi hibád: get_next_departures nem várt "minutes"-t → most már várt,
        # de ha egy régi verzió futna, próbáljuk meg paraméter nélkül:
        deps = get_next_departures(stop_id)
        # opcionális: szűkítsük minutes ablakra, ha a régi függvény sokat ad vissza
        # (ha a régi nem ad időt, ezt el is hagyhatod)
        return {"stop_id": stop_id, "minutes": minutes, "departures": deps}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build departures ({type(e).__name__}): {e}")

# ---------------------------
# Vincents Walk gyorslinkek (ha használod)
# ---------------------------
@app.get("/vincents-walk/ck")
def vw_ck(minutes: int = 60):
    return next_departures("1980SN12619E", minutes=minutes)

@app.get("/vincents-walk/cm")
def vw_cm(minutes: int = 60):
    return next_departures("1980SN12619W", minutes=minutes)

# ---------------------------
# Egyszerű index.html
# ---------------------------
@app.get("/index.html", response_class=HTMLResponse)
def serve_index():
    idx = ROOT / "index.html"
    if idx.exists():
        return idx.read_text(encoding="utf-8")
    return HTMLResponse(
        '<!doctype html><meta charset="utf-8"><title>Bluestar Bus</title>'
        '<p>Hiányzik az <code>index.html</code> a gyökérből.</p>',
        status_code=200,
    )
