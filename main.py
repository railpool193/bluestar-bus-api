# main.py
import os
import csv
from pathlib import Path
from functools import lru_cache
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

# Ha van saját segédfájlod GTFS-hez, használjuk azt
# (pozicionális paraméterekkel hívjuk, hogy ne legyen "unexpected keyword" hiba)
try:
    from gtfs_utils import get_next_departures as gtfs_get_next_departures  # type: ignore
except Exception:
    gtfs_get_next_departures = None  # fallback, ha nincs


APP_DIR = Path(__file__).parent.resolve()
DATA_DIR = APP_DIR / "data"
STOPS_PATH = DATA_DIR / "stops.txt"
INDEX_HTML = APP_DIR / "index.html"

app = FastAPI(
    title="Bluestar Bus API",
    version="1.0",
    docs_url="/docs",
    redoc_url=None,
)


# ---------------------------
# Helpers: megállók betöltése
# ---------------------------
@lru_cache(maxsize=1)
def _load_stops() -> List[Dict[str, str]]:
    """Betölti egyszer a stops.txt-t és cache-eli."""
    if not STOPS_PATH.exists():
        return []
    stops: List[Dict[str, str]] = []
    with STOPS_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # minimális védelem: csak a szükséges kulcsok
            stops.append(
                {
                    "stop_id": row.get("stop_id", ""),
                    "stop_name": row.get("stop_name", ""),
                }
            )
    return stops


def _search_stops_by_name(query: str, limit: int = 20) -> List[Dict[str, str]]:
    """Egyszerű, kis/nagybetűt nem érzékeny részszavas keresés a stop_name mezőben."""
    q = (query or "").strip().lower()
    if not q:
        return []
    results = []
    for s in _load_stops():
        if q in s["stop_name"].lower():
            results.append(s)
            if len(results) >= limit:
                break
    return results


# -------------
# Root + health
# -------------
@app.get("/")
def root():
    """Ha van index.html, azt szolgáljuk ki. Egyébként JSON linkek."""
    if INDEX_HTML.exists():
        return FileResponse(str(INDEX_HTML), media_type="text/html")
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
    return {"status": "ok"}


# ----------------
# Stop-keresés API
# ----------------
@app.get("/search_stop")
def search_stop(name: str = Query(..., description="Megálló neve (vagy részlete)"), limit: int = Query(20, ge=1, le=50)):
    """
    Megálló keresése a GTFS `stops.txt` alapján.
    Visszaad: stop_id, stop_name.
    """
    try:
        if not STOPS_PATH.exists():
            raise HTTPException(status_code=503, detail="Nincs GTFS stops.txt a /data mappában.")
        matches = _search_stops_by_name(name, limit=limit)
        return matches
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {e}")


# --------------------------
# Indulások (GTFS / segédfgv)
# --------------------------
@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=180)):
    """
    Következő indulások a megadott időablakban.
    A `gtfs_utils.get_next_departures(stop_id, minutes)` függvényt hívjuk,
    POZICIONÁLIS paraméterekkel (így nincs 'unexpected keyword' hiba).
    """
    try:
        if gtfs_get_next_departures is None:
            raise HTTPException(status_code=503, detail="GTFS segédfüggvény (gtfs_utils.get_next_departures) nem elérhető.")

        # FONTOS: pozicionális hívás
        data = gtfs_get_next_departures(stop_id, minutes)  # type: ignore

        # Válasz forma egységesítése
        resp = {
            "stop_id": stop_id,
            "minutes": minutes,
            "departures": data if isinstance(data, list) else data.get("departures", []),
        }
        return resp
    except HTTPException:
        raise
    except Exception as e:
        # Hiba esetén egységes JSON
        return JSONResponse(
            status_code=500,
            content={"detail": f"Failed to build departures: {e}"},
        )


# ----------------------------------------------------
# (Opcionális) régi demó linkek – ha használtátok őket
# ----------------------------------------------------
@app.get("/vincents-walk/ck")
def vw_ck(minutes: int = Query(60, ge=1, le=180)):
    """Shortcut: Vincent Walk (CK) stop – állítsd be a helyes stop_id-t, ha szeretnéd használni."""
    stop_id = "1980SN12619E"  # példa; cseréld arra a megállóra, amit szeretnél
    return next_departures(stop_id=stop_id, minutes=minutes)


@app.get("/vincents-walk/cm")
def vw_cm(minutes: int = Query(60, ge=1, le=180)):
    """Shortcut: Vincent Walk (CM) stop – állítsd be a helyes stop_id-t, ha szeretnéd használni."""
    stop_id = "1980SN12619E"  # példa; cseréld arra a megállóra, amit szeretnél
    return next_departures(stop_id=stop_id, minutes=minutes)
