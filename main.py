from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, Any
import uvicorn

from gtfs_utils import get_next_departures

app = FastAPI(title="Bluestar Bus API", version="1.0")

# --- Állandó buszmegálló ID-k (példák) ---
VW_CK_STOP_ID = "1980SN12619E"  # Vincent’s Walk (ck)
VW_CM_STOP_ID = "1980SN12620E"  # Vincent’s Walk (cm)


# Segédfüggvény a minutes ellenőrzésére
def _validate_minutes(minutes: int) -> int:
    if minutes <= 0 or minutes > 180:
        raise HTTPException(status_code=400, detail="Minutes must be between 1 and 180")
    return minutes


# Egyedi JSON válasz építése
def _json_departures(stop_id: str, minutes: int) -> JSONResponse:
    try:
        minutes = _validate_minutes(minutes)
        deps = get_next_departures(stop_id, minutes)   # <-- csak pozicionális paraméter
        return JSONResponse(content={"stop_id": stop_id, "minutes": minutes, "departures": deps})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- API végpontok ---

@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "health": "/health",
            "ck_next_60": "/vincents-walk/ck?minutes=60",
            "cm_next_60": "/vincents-walk/cm?minutes=60",
            "generic_example": f"/next_departures/{VW_CK_STOP_ID}?minutes=60",
        },
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60) -> JSONResponse:
    return _json_departures(stop_id, minutes)


@app.get("/vincents-walk/ck")
def vincents_walk_ck(minutes: int = 60) -> JSONResponse:
    return _json_departures(VW_CK_STOP_ID, minutes)


@app.get("/vincents-walk/cm")
def vincents_walk_cm(minutes: int = 60) -> JSONResponse:
    return _json_departures(VW_CM_STOP_ID, minutes)


@app.get("/vincents-walk")
def vincents_walk(minutes: int = 60) -> Dict[str, Any]:
    minutes = _validate_minutes(minutes)
    ck = get_next_departures(VW_CK_STOP_ID, minutes)   # <-- itt is pozicionális
    cm = get_next_departures(VW_CM_STOP_ID, minutes)   # <-- itt is pozicionális
    return {
        "minutes": minutes,
        "ck": {"stop_id": VW_CK_STOP_ID, "departures": ck},
        "cm": {"stop_id": VW_CM_STOP_ID, "departures": cm},
    }


# --- Lokális futtatás ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
