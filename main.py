# main.py
import os
import csv
from pathlib import Path
from typing import List, Dict, Any

from fastapi import FastAPI, Query, Path as FPath
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse

# Menetrendi indulások (GTFS) – a projektben már benne van
from gtfs_utils import get_next_departures as _gtfs_next

APP_NAME = "Bluestar Bus API"

app = FastAPI(title=APP_NAME, version="1.0")

# --- CORS: UI-ból kényelmes legyen hívni ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GTFS stops betöltése kereséshez ---
STOPS_PATH = Path("data") / "stops.txt"
_stops: List[Dict[str, str]] = []  # {"stop_id":..., "stop_name":...}

def _load_stops() -> None:
    global _stops
    _stops = []
    if STOPS_PATH.exists():
        with STOPS_PATH.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sid = (row.get("stop_id") or "").strip()
                sname = (row.get("stop_name") or "").strip()
                if sid and sname:
                    _stops.append({"stop_id": sid, "stop_name": sname})

# induláskor egyszer betöltjük
_load_stops()

# --- Segéd: menetrendi indulások wrapper (biztosan pozicionális átadás!) ---
def get_next_departures_wrapper(stop_id: str, minutes: int) -> List[Dict[str, Any]]:
    """
    Biztonságos wrapper: a gtfs_utils.get_next_departures hívása
    pozicionális paraméterrel, hogy ne legyen 'unexpected keyword' hiba.
    """
    try:
        # ❶ Elsőként próbáljuk a (stop_id, minutes) pozicionális hívást
        return _gtfs_next(stop_id, minutes)
    except TypeError:
        # ❷ Ha a modul csak 1 paramétert vár, akkor hívjuk úgy,
        #    és ha listát ad vissza, nem vágjuk meg (UI időszűrést is tud)
        return _gtfs_next(stop_id)

# -----------------------------
#           ENDPOINTS
# -----------------------------

@app.get("/", response_class=JSONResponse)
def root():
    return {
        "message": f"{APP_NAME}",
        "links": {
            "docs": "/docs",
            "health": "/health",
            # példák
            "ck_next_60": "/vincents-walk/ck?minutes=60",
            "cm_next_60": "/vincents-walk/cm?minutes=60",
            "generic_example": "/next_departures/1980SN12619E?minutes=60",
        },
    }

@app.get("/health", response_class=JSONResponse)
def health():
    return {"status": "ok", "stops_loaded": len(_stops)}

# --- Megálló keresés név szerint ---
@app.get("/search_stop", response_class=JSONResponse)
def search_stop(name: str = Query("", description="Megálló neve (részlet is lehet)")):
    """
    Rész-egyezés, kis/nagybetű független. Ha nincs találat -> üres lista.
    """
    q = (name or "").strip().lower()
    if not q:
        return []

    try:
        results = []
        for s in _stops:
            sname = s["stop_name"].strip().lower()
            if q in sname:
                results.append({"stop_id": s["stop_id"], "stop_name": s["stop_name"]})
        # opcionálisan szűkítsünk 20 találatra, hogy ne legyen túl hosszú
        return results[:20]
    except Exception as e:
        # Ne dőljön el a kliens – adjunk vissza üres listát és naplózzuk
        print("Megálló-keresési hiba:", e)
        return []

# --- Következő indulások egy megállóból ---
@app.get("/next_departures/{stop_id}", response_class=JSONResponse)
def next_departures(
    stop_id: str = FPath(..., description="GTFS stop_id"),
    minutes: int = Query(60, ge=1, le=240, description="Előretekintési idő percekben (1–240)"),
):
    """
    Menetrendi alapú (GTFS) indulások. A gtfs_utils függvényhívás
    mindig pozicionális paramétert kap, így nem lehet 'unexpected keyword' hiba.
    """
    try:
        deps = get_next_departures_wrapper(stop_id, minutes)
        # Biztonság kedvéért, ha None jönne:
        if deps is None:
            deps = []
        return {"stop_id": stop_id, "minutes": minutes, "departures": deps}
    except Exception as e:
        # Kliensbarát üzenet, a részletek a logban
        print("Failed to build departures:", repr(e))
        return JSONResponse(
            status_code=500,
            content={"detail": f"Failed to build departures ({type(e).__name__}): {str(e)}"},
        )

# --- Kényelmi végpontok (fix megállók) ---
@app.get("/vincents-walk/ck", response_class=JSONResponse)
def vw_ck(minutes: int = Query(60, ge=1, le=240)):
    return next_departures("1980SN12619E", minutes)  # példa: CK oldali oszlop

@app.get("/vincents-walk/cm", response_class=JSONResponse)
def vw_cm(minutes: int = Query(60, ge=1, le=240)):
    return next_departures("1980SN12619W", minutes)  # példa: CM oldali oszlop

@app.get("/vincents-walk", response_class=JSONResponse)
def vw_both(minutes: int = Query(60, ge=1, le=240)):
    # mindkét irány összefűzve (ha kell)
    a = next_departures("1980SN12619E", minutes)
    b = next_departures("1980SN12619W", minutes)
    # a és b JSONResponse is lehet – szedjük ki a tartalmát
    def _content(resp):
        return resp if isinstance(resp, dict) else resp.body  # FastAPI JSONResponse sajátos
    return {"ck": _content(a), "cm": _content(b)}

# --- index.html kiszolgálása, ha van ---
@app.get("/index.html")
def index_html():
    if Path("index.html").exists():
        return FileResponse("index.html", media_type="text/html; charset=utf-8")
    return JSONResponse({"message": f"{APP_NAME} - nincs index.html"})

# --- Render / Railway kompatibilis futtatás ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=bool(os.getenv("RELOAD", "")),
    )
