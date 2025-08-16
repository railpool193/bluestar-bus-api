from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import List, Dict

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# ⚠️ Ezt a modult nálad már használjuk – fontos, hogy a függvény
# minutes paramétert is fogadjon: def get_next_departures(stop_id: str, minutes: int = 60)
from siri_live import get_next_departures

APP_ROOT = Path(__file__).resolve().parent
DATA_DIR = APP_ROOT / "data"
STOPS_FILE = DATA_DIR / "stops.txt"

app = FastAPI(
    title="Bluestar Bus API",
    version="1.1",
    description="FastAPI backend for Bluestar timetables and live departures.",
)

# Ha a kis UI-t más domainekről is hívnád:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # szűkítheted, ha szeretnéd
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GTFS stop index a kereséshez -------------------------------------------------

_stops_index: List[Dict[str, str]] = []


def _load_stops_index() -> None:
    """Betölti a data/stops.txt fájlból a megállók alapadatait."""
    global _stops_index
    _stops_index = []

    if not STOPS_FILE.exists():
        # Nem végzetes: a többi endpoint működik, csak a kereső nem.
        print(f"[WARN] stops.txt nem található: {STOPS_FILE}")
        return

    with STOPS_FILE.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # GTFS: legalább stop_id, stop_name legyen
            stop_id = (row.get("stop_id") or "").strip()
            stop_name = (row.get("stop_name") or "").strip()
            locality = (row.get("stop_desc") or row.get("locality") or "").strip()
            stop_code = (row.get("stop_code") or "").strip()

            if stop_id and stop_name:
                _stops_index.append(
                    {
                        "stop_id": stop_id,
                        "stop_name": stop_name,
                        "locality": locality,
                        "stop_code": stop_code,
                    }
                )

    print(f"[INFO] Betöltött megállók száma: {len(_stops_index)}")


# induláskor egyszer betöltjük
_load_stops_index()

# --- Gyökér / index ---------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
def root():
    """
    Mini UI: index.html kiszolgálása.
    """
    index_path = APP_ROOT / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    # fallback – ha nincs index, adjunk egy barátságos JSON-t
    return JSONResponse(
        {
            "message": "Bluestar Bus API",
            "links": {
                "docs": "/docs",
                "health": "/health",
                "ck_next_60": "/vincents-walk/ck?minutes=60",
                "cm_next_60": "/vincents-walk/cm?minutes=60",
                "generic_example": "/next_departures/1980SN12619E?minutes=60",
                "search_example": "/search_stops?q=walk",
            },
        }
    )


@app.get("/index.html")
def index_html():
    """
    Ha kifejezetten /index.html-nek hívod, itt is kiszolgáljuk.
    """
    index_path = APP_ROOT / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(index_path)


# --- Egyszerű állapotjelzés -------------------------------------------------------


@app.get("/health")
def health():
    return {"status": "ok"}


# --- Megálló-kereső a GTFS stops.txt alapján -------------------------------------


@app.get("/search_stops")
def search_stops(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100)):
    """
    Egyszerű név szerinti keresés a GTFS `stops.txt` fájlban.

    Példa:
      /search_stops?q=vincent
    """
    if not _stops_index:
        raise HTTPException(status_code=503, detail="Stops index not available")

    q_norm = q.lower().strip()
    results = []
    for s in _stops_index:
        text = f"{s['stop_name']} {s['locality']} {s['stop_code']}".lower()
        if q_norm in text:
            results.append(s)
            if len(results) >= limit:
                break
    return {"query": q, "count": len(results), "results": results}


# --- Valós idejű vagy GTFS-alapú következő indulások ------------------------------


@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=180)):
    """
    Következő indulások egy megállóból, `minutes` perces ablakban.

    Fontos: a siri_live.get_next_departures-nek átadjuk a minutes paramétert is.
    """
    try:
        departures = get_next_departures(stop_id, minutes=minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": departures}
    except Exception as e:
        # Látható hibaüzenet (mint eddig)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build departures ({type(e).__name__}): {e}",
        )


# --- Kényelmi végpontok Vincents Walk-hoz (ahogy eddig is volt) -------------------

# Állítsd a saját fix megállóazonosítóidra:
STOP_VINCENTS_WALK_CK = "1980SN12619E"
STOP_VINCENTS_WALK_CM = "1980SN12619W"


@app.get("/vincents-walk/ck")
def vincents_walk_ck(minutes: int = Query(60, ge=1, le=180)):
    return next_departures(STOP_VINCENTS_WALK_CK, minutes=minutes)


@app.get("/vincents-walk/cm")
def vincents_walk_cm(minutes: int = Query(60, ge=1, le=180)):
    return next_departures(STOP_VINCENTS_WALK_CM, minutes=minutes)


@app.get("/vincents-walk")
def vincents_walk(minutes: int = Query(60, ge=1, le=180)):
    """
    Ha nem adsz irányt, visszaadhatjuk pl. az egyik oldalt (vagy később mindkettőt).
    Most a CK oldalra mutat kényelmi okból.
    """
    return next_departures(STOP_VINCENTS_WALK_CK, minutes=minutes)
