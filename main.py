# main.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any
import os

# A saját GTFS segédfüggvényeid
from gtfs_utils import get_next_departures

app = FastAPI(
    title="Bluestar Bus API",
    version="1.0",
    description="FastAPI backend that serves upcoming departures from Bluestar GTFS data.",
)

# Ha a későbbiekben külön domain-ről jön a frontend, jól jöhet a CORS:
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # igény szerint szűkítsd
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Hasznos konstansok / shortcut stop id-k (Vincents Walk) ---
STOP_VINCENTS_WALK_CK = "1980SN12619E"   # Vincents Walk (CK oldal)
STOP_VINCENTS_WALK_CM = "1980SN12619W"   # Vincents Walk (CM oldal) – ha más, cseréld erre a helyes ID-t


# ---------------------------
# Frontend (index.html) kiszolgálása
# ---------------------------
@app.get("/", include_in_schema=False)
def serve_index_html():
    """
    A gyökéren az index.html-t szolgáljuk ki (egyszerű frontend).
    """
    file_path = os.path.join(os.path.dirname(__file__), "index.html")
    if not os.path.exists(file_path):
        # Ha valamiért nincs meg az index.html, adjunk vissza egy barátságos JSON-t.
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
                "note": "index.html not found next to main.py – serving JSON index instead.",
            }
        )
    return FileResponse(file_path)


# (Opcionális) JSON index külön végponton, ha szeretnéd megtartani:
@app.get("/api", tags=["Index"])
def api_index():
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


# ---------------------------
# Health
# ---------------------------
@app.get("/health", tags=["Health"])
def health() -> Dict[str, Any]:
    return {"status": "ok"}


# ---------------------------
# Generic GTFS lekérdezés
# ---------------------------
@app.get("/next_departures/{stop_id}", tags=["Next Departures"])
def next_departures(
    stop_id: str,
    minutes: Optional[int] = Query(60, ge=1, le=240, description="Előretekintés percekben"),
):
    """
    Általános végpont: bármely GTFS stop_id-hez kiadja a következő indulásokat `minutes` perces ablakban.
    """
    try:
        deps = get_next_departures(stop_id, minutes=minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": deps}
    except TypeError as e:
        # pl. ha a get_next_departures régi szignatúrája nem fogad 'minutes' kulcsot
        raise HTTPException(status_code=500, detail=f"Failed to build departures (TypeError): {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build departures: {e}")


# ---------------------------
# Vincents Walk shortcutok
# ---------------------------
@app.get("/vincents-walk/ck", tags=["Vincents Walk"])
def vincents_walk_ck(minutes: Optional[int] = Query(60, ge=1, le=240)):
    """
    Vincents Walk – CK oldal (shortcut).
    """
    return next_departures(STOP_VINCENTS_WALK_CK, minutes=minutes)


@app.get("/vincents-walk/cm", tags=["Vincents Walk"])
def vincents_walk_cm(minutes: Optional[int] = Query(60, ge=1, le=240)):
    """
    Vincents Walk – CM oldal (shortcut).
    """
    return next_departures(STOP_VINCENTS_WALK_CM, minutes=minutes)


@app.get("/vincents-walk", tags=["Vincents Walk"])
def vincents_walk(minutes: Optional[int] = Query(60, ge=1, le=240)):
    """
    Vincents Walk összesítve – mindkét oldal egy válaszban.
    """
    try:
        ck = get_next_departures(STOP_VINCENTS_WALK_CK, minutes=minutes)
        cm = get_next_departures(STOP_VINCENTS_WALK_CM, minutes=minutes)
        return {
            "minutes": minutes,
            "ck": {"stop_id": STOP_VINCENTS_WALK_CK, "departures": ck},
            "cm": {"stop_id": STOP_VINCENTS_WALK_CM, "departures": cm},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build Vincents Walk response: {e}")
