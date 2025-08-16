# main.py
from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

from gtfs_utils import get_next_departures  # def get_next_departures(stop_id: str, minutes: int) -> List[Dict]

app = FastAPI(title="Bluestar Bus API", version="1.1")

# ---- CORS (ha külön domainről hívod a frontendet) ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # ha szigorúbb kell: ["https://sajatdomain.hu"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- index.html kiszolgálás ----
BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"

@app.get("/", response_class=HTMLResponse)
def root():
    if INDEX_FILE.exists():
        return FileResponse(str(INDEX_FILE), media_type="text/html; charset=utf-8")
    return JSONResponse(
        {
            "message": "Bluestar Bus API",
            "links": {
                "docs": "/docs",
                "health": "/health",
                "ck_next_60": "/vincents-walk/ck?minutes=60",
                "cm_next_60": "/vincents-walk/cm?minutes=60",
                "generic_example": "/next_departures/1980SN12619E?minutes=60",
            },
        }
    )

@app.get("/index.html", response_class=HTMLResponse)
def index_html():
    if INDEX_FILE.exists():
        return FileResponse(str(INDEX_FILE), media_type="text/html; charset=utf-8")
    return JSONResponse({"message": "Bluestar Bus API - nincs index.html"})

@app.get("/health")
def health():
    return {"status": "ok"}

# ---- GTFS megállók betöltése ----
STOPS_TXT = BASE_DIR / "data" / "stops.txt"
stops_index: List[Dict[str, str]] = []

def _norm(s: str) -> str:
    return s.lower().strip()

def _load_stops():
    global stops_index
    stops_index = []
    if not STOPS_TXT.exists():
        return
    with STOPS_TXT.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = (row.get("stop_id") or "").strip()
            sname = (row.get("stop_name") or "").strip()
            if sid and sname:
                stops_index.append({"stop_id": sid, "stop_name": sname, "_norm": _norm(sname)})

_load_stops()

# ---- Egyszerű TTL cache a /search_stops válaszokra ----
# Kulcs: (term, limit)  ->  (timestamp, results)
_search_cache: Dict[Tuple[str, int], Tuple[float, List[Dict[str, str]]]] = {}
SEARCH_TTL_SECONDS = 24 * 60 * 60  # 24 óra

def _cached_search(term: str, limit: int) -> List[Dict[str, str]]:
    now = time.time()
    key = (term, limit)
    hit = _search_cache.get(key)
    if hit and now - hit[0] < SEARCH_TTL_SECONDS:
        return hit[1]
    # új lekérdezés
    t = _norm(term)
    starts, contains = [], []
    for st in stops_index:
        nm_norm = st["_norm"]
        if t in nm_norm:
            item = {"stop_id": st["stop_id"], "stop_name": st["stop_name"]}
            (starts if nm_norm.startswith(t) else contains).append(item)
    results = (starts + contains)[:limit]
    _search_cache[key] = (now, results)
    # méret féken tartása (nagyon egyszerű LRU-szerű takarítás)
    if len(_search_cache) > 1000:
        # legöregebb 200 elem törlése
        for _ in range(200):
            oldest = min(_search_cache.items(), key=lambda kv: kv[1][0])[0]
            _search_cache.pop(oldest, None)
    return results

# ---- Megálló-kereső ----
@app.get("/search_stops", response_model=List[Dict[str, str]])
def search_stops(
    q: Optional[str] = Query(None, description="Megálló neve (részlet)"),
    name: Optional[str] = Query(None, description="Alias a q helyett"),
    limit: int = Query(20, ge=1, le=50),
):
    term = (q or name or "").strip()
    if not term or not stops_index:
        return []
    return _cached_search(term, limit)

# ---- Indulások ----
@app.get("/next_departures/{stop_id}")
def api_next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=240)):
    try:
        # minutes pozíciós arg, hogy biztosan kompatibilis legyen
        deps = get_next_departures(stop_id, minutes)
    except TypeError as e:
        return JSONResponse(status_code=500, content={"detail": f"Failed to build departures (TypeError): {e}"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build departures: {e}")
    return {"stop_id": stop_id, "minutes": minutes, "departures": deps or []}

# ---- Kényelmi aliasok ----
VW_CK = os.getenv("VW_CK_STOP_ID", "1980HAA13371")
VW_CM = os.getenv("VW_CM_STOP_ID", "1980SN12619E")

@app.get("/vincents-walk/ck")
def vw_ck(minutes: int = Query(60, ge=1, le=240)):
    return api_next_departures(VW_CK, minutes)

@app.get("/vincents-walk/cm")
def vw_cm(minutes: int = Query(60, ge=1, le=240)):
    return api_next_departures(VW_CM, minutes)
