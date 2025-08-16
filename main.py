# main.py
import os
import logging
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# --- Loggolás ---------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bluestar.main")

# --- Külső modulok betöltése (GTFS + SIRI) ---------------------------------
GTFS_OK = False
SIRI_OK = False
GTFS_ERR = None
SIRI_ERR = None

try:
    import gtfs_utils as gtfs  # saját modulod
    # ha a modulban van init betöltés, ez elég; különben itt is lehetne cache-elni
    GTFS_OK = True
    log.info("GTFS modul betöltve.")
except Exception as e:
    GTFS_ERR = repr(e)
    log.exception("GTFS modul betöltése sikertelen.")

try:
    import siri_live as siri  # saját modulod
    SIRI_OK = True
    log.info("SIRI modul betöltve.")
except Exception as e:
    SIRI_ERR = repr(e)
    log.exception("SIRI modul betöltése sikertelen.")

# --- FastAPI app ------------------------------------------------------------
app = FastAPI(title="Bluestar Bus API", version="1.0.0")

# CORS (ha később külön domainről kérdezed az API-t)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ha szigorítanád: ["https://sajat-domain"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Statikus fájlok (index.html, CSS, JS)
STATIC_DIR = os.path.join(os.getcwd(), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    log.info("Static dir mounted: %s", STATIC_DIR)
else:
    log.warning("Nincs 'static' mappa. A / csak JSON fallback-et ad.")

# --- Gyökér: index.html kiszolgálása ---------------------------------------
@app.get("/", include_in_schema=False)
def root():
    """
    Ha van static/index.html, azt szolgáljuk ki.
    Egyébként egy rövid JSON-t adunk linkekkel.
    """
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return JSONResponse(
        {
            "message": "Bluestar Bus API",
            "links": {
                "docs": "/docs",
                "openapi": "/openapi.json",
                "health": "/health",
                "search_example": "/search_stops?q=vincent",
                "generic_example": "/next_departures/1980SN12619E?minutes=60",
            },
        }
    )

# --- Healthcheck ------------------------------------------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "gtfs_loaded": GTFS_OK,
        "siri_available": SIRI_OK,
        "gtfs_error": GTFS_ERR,
        "siri_error": SIRI_ERR,
    }

# --- Megálló-kereső (GTFS) -------------------------------------------------
@app.get("/search_stops")
def search_stops(q: str = Query(..., min_length=2, description="Megálló név (részlet)")) -> Dict[str, Any]:
    """
    Név szerinti megálló-keresés a GTFS 'stops.txt' alapján.
    Visszaad: display_name, stop_id, stop_code (ha van).
    """
    if not GTFS_OK:
        raise HTTPException(status_code=503, detail=f"GTFS nem elérhető: {GTFS_ERR}")

    try:
        results = gtfs.search_stops(q)  # elvárt, hogy list[dict] legyen
        # Frontend-barát forma
        out: List[Dict[str, Any]] = []
        for r in results:
            out.append(
                {
                    "display_name": r.get("display_name") or r.get("name") or r.get("stop_name"),
                    "stop_id": r.get("stop_id"),
                    "stop_code": r.get("stop_code", None),
                }
            )
        return {"query": q, "results": out}
    except Exception as e:
        log.exception("Hiba a megálló-keresésben")
        raise HTTPException(status_code=500, detail=f"Hiba a keresésben: {e}")

# --- Következő indulások (SIRI) --------------------------------------------
@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=240)) -> Dict[str, Any]:
    """
    Következő indulások egy megállóból.
    A `siri_live.get_next_departures(stop_id, minutes)` függvényt hívja,
    és továbbítja a visszaadott listát.
    """
    if not SIRI_OK:
        # Ne dőljünk el – adjunk üres listát inkább
        log.warning("SIRI nem elérhető, üres lista tér vissza.")
        return {"stop_id": stop_id, "minutes": minutes, "departures": []}

    try:
        deps = siri.get_next_departures(stop_id=stop_id, minutes=minutes)
        # Elvárt elem forma (példa):
        # {
        #   "route": "17",
        #   "destination": "City Centre",
        #   "time": "15:42",
        #   "is_live": True,           # élő adat?
        #   "delay_min": 2             # ha van késés
        # }
        return {"stop_id": stop_id, "minutes": minutes, "departures": deps}
    except Exception as e:
        log.exception("Hiba az indulások építése közben")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build departures ({e.__class__.__name__}): {e}",
        )

# --- Lokális futtatás (Railway is ezt használja PORT környezeti változóval) -
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=bool(os.getenv("RELOAD", "")))
