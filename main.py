# main.py
from __future__ import annotations

import os
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# A saját modulod, ami a GTFS fájlokból számolja a következő indulásokat
from gtfs_utils import get_next_departures


# ======= Konfigurálható, kényelmi stop ID-k (környezeti változóval is felülírhatók) =======
# TIPP: állítsd be Railway-en, hogy ne kelljen a kódban módosítani:
# VW_CK_STOP_ID, VW_CM_STOP_ID
VW_CK_STOP_ID = os.getenv("VW_CK_STOP_ID", "1980SN12619E")  # Vincents Walk (CK) – cseréld valós ID-ra, ha más
VW_CM_STOP_ID = os.getenv("VW_CM_STOP_ID", "1980SN12620E")  # Vincents Walk (CM) – cseréld valós ID-ra, ha más

# Megengedett időablak (percben), hogy ne kérjünk véletlenül túl nagy intervallumot
MIN_MINUTES = 1
MAX_MINUTES = 720

app = FastAPI(
    title="Bluestar Bus API",
    version="2.0.0",
    contact={"name": "Bluestar Bus API"},
)


# ==========================
#        Segédfüggvény
# ==========================
def _validate_minutes(value: int) -> int:
    if value is None:
        raise HTTPException(status_code=400, detail="Missing 'minutes' query param")
    if not isinstance(value, int):
        raise HTTPException(status_code=400, detail="'minutes' must be integer")
    if value < MIN_MINUTES or value > MAX_MINUTES:
        raise HTTPException(
            status_code=400,
            detail=f"'minutes' must be between {MIN_MINUTES} and {MAX_MINUTES}",
        )
    return value


def _json_departures(stop_id: str, minutes: int) -> JSONResponse:
    try:
        if not stop_id:
            raise HTTPException(status_code=400, detail="Missing 'stop_id'")
        minutes = _validate_minutes(minutes)
        deps: List[Dict[str, Any]] = get_next_departures(stop_id, minutes=minutes)
        return JSONResponse(content={"stop_id": stop_id, "minutes": minutes, "departures": deps})
    except HTTPException:
        # már kidolgozott, “szép” hiba
        raise
    except Exception as e:
        # ideiglenes diagnosztika – ha gond van, a Railway HTTP logban látszik a részletes traceback
        raise HTTPException(status_code=500, detail=f"Failed to build departures: {e}")


# ==========================
#           Routes
# ==========================

@app.get("/")
def index() -> Dict[str, Any]:
    """Kezdőoldal: gyors linkek és példa hívások."""
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "health": "/health",
            "ck_next_60": f"/vincents-walk/ck?minutes=60",
            "cm_next_60": f"/vincents-walk/cm?minutes=60",
            "generic_example": f"/next_departures/{VW_CK_STOP_ID}?minutes=60",
        },
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60) -> JSONResponse:
    """Általános végpont: következő indulások tetszőleges megállóhoz."""
    return _json_departures(stop_id, minutes)


@app.get("/siri-live")
def siri_live(stop_id: str, minutes: int = 60) -> JSONResponse:
    """
    Alias a next_departures-hoz, kényelmes query paramokkal:
    /siri-live?stop_id=1980SN12619E&minutes=60
    """
    return _json_departures(stop_id, minutes)


# ---------- Vincents Walk kényelmi végpontok ----------
@app.get("/vincents-walk/ck")
def vincents_walk_ck(minutes: int = 60) -> JSONResponse:
    return _json_departures(VW_CK_STOP_ID, minutes)


@app.get("/vincents-walk/cm")
def vincents_walk_cm(minutes: int = 60) -> JSONResponse:
    return _json_departures(VW_CM_STOP_ID, minutes)


# (Opcionális) Egy összesítő, ami mindkét oldalt adja egyszerre:
@app.get("/vincents-walk")
def vincents_walk(minutes: int = 60) -> Dict[str, Any]:
    minutes = _validate_minutes(minutes)
    ck = get_next_departures(VW_CK_STOP_ID, minutes=minutes)
    cm = get_next_departures(VW_CM_STOP_ID, minutes=minutes)
    return {"minutes": minutes, "ck": {"stop_id": VW_CK_STOP_ID, "departures": ck},
            "cm": {"stop_id": VW_CM_STOP_ID, "departures": cm}}


# Helyi futtatáshoz (nem szükséges Railway-en)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
