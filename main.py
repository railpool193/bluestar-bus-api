# main.py
from __future__ import annotations
import os
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# --- Saját segédfüggvényeid / meglévő logika ---
# Feltételezem, hogy ezek már a repo-ban vannak és működnek:
# - gtfs_utils.get_next_departures(stop_id: str, minutes: int) -> Dict[str, Any]
from gtfs_utils import get_next_departures

APP_DIR = Path(__file__).parent.resolve()
DATA_DIR = APP_DIR / "data"
INDEX_HTML = APP_DIR / "index.html"

app = FastAPI(title="Bluestar Bus API", version="1.1")

# Opcionális: statikus fájlok (ha lesz külön assets könyvtárad)
# app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

# -------------------------------------------------------------------------
# GTFS megállók beolvasása (data/stops.txt) – 1x induláskor
# -------------------------------------------------------------------------
stops_index: List[Dict[str, str]] = []

def _load_stops() -> None:
    """Betölti a data/stops.txt-t memória-indexbe (stop_id, stop_name, lat, lon)."""
    global stops_index
    stops_index = []

    stops_path = DATA_DIR / "stops.txt"
    if not stops_path.exists():
        # Nem halunk el, de jelezzük a logban és a keresés üres lesz
        print(f"[WARN] Nem találom a stops.txt fájlt: {stops_path}")
        return

    # UTF-8-BOM barát olvasás (sok GTFS ilyen)
    with stops_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stop_id = (row.get("stop_id") or "").strip()
            stop_name = (row.get("stop_name") or "").strip()
            lat = (row.get("stop_lat") or "").strip()
            lon = (row.get("stop_lon") or "").strip()
            if stop_id and stop_name:
                stops_index.append({
                    "stop_id": stop_id,
                    "stop_name": stop_name,
                    "lat": lat,
                    "lon": lon,
                })

    print(f"[INFO] Betöltött megállók: {len(stops_index)}")

# Betöltjük induláskor
_load_stops()

# -------------------------------------------------------------------------
# Root / index
# -------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    if INDEX_HTML.exists():
        return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))
    # Ha nincs index.html, adjunk barátságos szöveget
    return HTMLResponse(
        '<pre>{"message":"Bluestar Bus API - nincs index.html"}</pre>',
        status_code=200
    )

@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}

# -------------------------------------------------------------------------
# Név szerinti megálló-keresés
# -------------------------------------------------------------------------
@app.get("/search_stops")
def search_stops(
    q: str = Query(..., min_length=2, description="Megálló neve (részlet is lehet)"),
    limit: int = Query(15, ge=1, le=50)
) -> Dict[str, Any]:
    """
    Részszavas, nem kis-/nagybetű érzékeny keresés stop_name alapján.
    Visszaad: { results: [ {stop_id, stop_name, lat, lon}, ... ] }
    """
    if not stops_index:
        # Nincs betöltve a stops.txt – inkább jelezzük egyértelműen
        raise HTTPException(status_code=503, detail="Megálló-adatbázis nem elérhető (stops.txt hiányzik).")

    term = q.lower().strip()
    results: List[Dict[str, str]] = []

    for s in stops_index:
        if term in s["stop_name"].lower():
            results.append(s)
            if len(results) >= limit:
                break

    return {"results": results}

# -------------------------------------------------------------------------
# Következő indulások – már meglévő logikád felhasználásával
# -------------------------------------------------------------------------
@app.get("/next_departures/{stop_id}")
def api_next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=240)) -> Dict[str, Any]:
    """
    Általános végpont: következő indulások a GTFS alapján (és ahol van, élő adatok jelzése).
    A get_next_departures meglévő függvényedet használja.
    """
    try:
        data = get_next_departures(stop_id, minutes)  # <- ezt már megírtad korábban
        # Biztonsági fallback
        if not isinstance(data, dict):
            data = {"stop_id": stop_id, "minutes": minutes, "departures": []}
        return data
    except Exception as e:
        # Barátságos hiba a kliensnek
        raise HTTPException(status_code=500, detail=f"Failed to build departures: {type(e).__name__}: {e}")

# -------------------------------------------------------------------------
# A két fix Vincents Walk gombhoz hagyjuk meg a rövidítéseket (ha használtad)
# -------------------------------------------------------------------------
# Ezekhez csak beírjuk a megfelelő stop_id-t – a konkrét kódodban már működtek

VINCENTS_WALK_CK = "1980SN12619E"   # példa, nálad már be volt drótozva
VINCENTS_WALK_CM = "1980SN12619F"   # példa, ha van "másik oldal" id

@app.get("/vincents-walk/ck")
def vw_ck(minutes: int = Query(60, ge=1, le=240)) -> Dict[str, Any]:
    return api_next_departures(VINCENTS_WALK_CK, minutes)

@app.get("/vincents-walk/cm")
def vw_cm(minutes: int = Query(60, ge=1, le=240)) -> Dict[str, Any]:
    return api_next_departures(VINCENTS_WALK_CM, minutes)

# -------------------------------------------------------------------------
# Opcionális: index.html direkt kiszolgálása (ha /index.html-re is szeretnéd)
# -------------------------------------------------------------------------
@app.get("/index.html", response_class=HTMLResponse)
def serve_index() -> HTMLResponse:
    if INDEX_HTML.exists():
        return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))
    return HTMLResponse('<pre>{"detail":"Not Found"}</pre>', status_code=404)
