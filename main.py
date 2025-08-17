# main.py
import os
import csv
import asyncio
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# --- SIRI élő lekérdező import ---
SIRI_CONFIGURED = True
_siri_import_error: Optional[str] = None

try:
    # feltételezzük, hogy siri_live.py ugyanebben a mappában van
    # és tartalmaz egy async függvényt: get_next_departures(stop_id: str, minutes: int)
    from siri_live import get_next_departures  # type: ignore
except Exception as e:
    SIRI_CONFIGURED = False
    _siri_import_error = f"{e}"
    async def get_next_departures(*args, **kwargs):  # fallback hiba esetére
        raise HTTPException(status_code=500, detail=f"SIRI not available: {_siri_import_error}")

app = FastAPI(
    title="Bluestar Bus – API",
    version="0.1.0",
)

# CORS (ha a frontend külön domainen lenne; ártani nem árt)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GTFS megállók betöltése kereséshez ---
STOPS: List[dict] = []
GTFS_LOADED = False

def _load_stops() -> None:
    global STOPS, GTFS_LOADED
    candidates = [
        "gtfs/stops.txt",
        "data/gtfs/stops.txt",
        "stops.txt",
        "/app/gtfs/stops.txt",
    ]
    for p in candidates:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                # Elvárt oszlopok: stop_id, stop_name
                for row in reader:
                    sid = row.get("stop_id", "").strip()
                    sname = row.get("stop_name", "").strip()
                    if not sid or not sname:
                        continue
                    STOPS.append({
                        "stop_id": sid,
                        "stop_name": sname,
                        "norm": sname.lower(),
                    })
            GTFS_LOADED = True
            break

_load_stops()

# --- API ENDPOINTS ---

@app.get("/api/status", summary="Status")
async def status():
    """Egyszerű health/status végpont."""
    return {
        "status": "ok",
        "gtfs_loaded": GTFS_LOADED,
        "siri_configured": SIRI_CONFIGURED,
    }

@app.get("/api/stops/search", summary="Search Stops")
async def search_stops(
    q: str = Query(..., description="Megálló neve (részlet is lehet)"),
    limit: int = Query(10, ge=1, le=50),
):
    if not GTFS_LOADED:
        raise HTTPException(status_code=503, detail="GTFS stops not loaded")
    qn = q.strip().lower()
    if not qn:
        return {"query": q, "results": []}

    # nagyon egyszerű részszó-keresés
    results = []
    for s in STOPS:
        if qn in s["norm"]:
            results.append({"stop_id": s["stop_id"], "stop_name": s["stop_name"]})
            if len(results) >= limit:
                break

    return {"query": q, "results": results}

@app.get("/api/stops/{stop_id}/next_departures", summary="Next Departures")
async def api_next_departures(
    stop_id: str,
    minutes: int = Query(60, ge=5, le=720, description="Időablak percekben"),
):
    """
    Következő indulások a megadott megállóhoz.
    - Először próbál élő SIRI (BODS) adatból
    - Ha nincs élő, a backend a menetrendi (GTFS) fallbacket adhatja vissza
    """
    # hívjuk a siri_live implementációt (async)
    results = await get_next_departures(stop_id=stop_id, minutes=minutes)
    # Várt visszaadott forma: {"stop_id": "...", "minutes": n, "results": [{route, destination, time_iso, is_live}, ...]}
    return results

# --- Statikus fájlok (frontend) kiszolgálása ---
# A mountingot LEGUTOLJÁRA tedd, hogy az /api/* útvonalakat ne írja felül!
app.mount("/", StaticFiles(directory="static", html=True), name="static")


# --- Uvicorn belépési pont (helyi futtatáshoz) ---
if __name__ == "__main__":
    import uvicorn
    # Railway a PORT env változót adja; lokálban 8000
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=os.environ.get("RELOAD", "0") == "1")
