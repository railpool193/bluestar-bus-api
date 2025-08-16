from fastapi import FastAPI, HTTPException, Query, Path
from typing import Any, Dict

# A saját segédfüggvényed
from gtfs_utils import get_next_departures

app = FastAPI(
    title="Bluestar Bus API",
    version="1.0.0",
    contact={"name": "Bluestar Bus API"},
)


# ---- belső segédfüggvény ----------------------------------------------------
def _call_next_departures(stop_id: str, minutes: int):
    """
    A gtfs_utils.get_next_departures() wrapper-je:
    1) Először pozíciós paraméterrel próbálja (stop_id, minutes)
    2) Ha az TypeError-t dob, megpróbálja window_minutes kulcsszóval
    3) Ha bármi más hiba van, HTTP 500-at dob részletes üzenettel
    """
    try:
        # Leggyakoribb: a függvény a második paramétert pozíciósan várja
        return get_next_departures(stop_id, minutes)
    except TypeError:
        # Alternatíva: név szerint 'window_minutes' a paraméter
        try:
            return get_next_departures(stop_id, window_minutes=minutes)
        except Exception as e2:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to build departures (2nd attempt): {e2}",
            )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build departures: {e}",
        )


# ---- alap / egészség ---------------------------------------------------------
@app.get("/")
def root() -> Dict[str, Any]:
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
def health() -> Dict[str, str]:
    return {"status": "ok"}


# ---- általános végpont -------------------------------------------------------
@app.get("/next_departures/{stop_id}")
def next_departures(
    stop_id: str = Path(..., description="GTFS stop_id"),
    minutes: int = Query(60, ge=1, le=240, description="Időablak percekben"),
) -> Dict[str, Any]:
    deps = _call_next_departures(stop_id, minutes)
    return {"stop_id": stop_id, "minutes": minutes, "departures": deps}


# ---- kényelmi aliasok a Vincents Walk megállókhoz ----------------------------
# CK irány (példa stop_id: 1980SN12619E)
@app.get("/vincents-walk/ck")
def vincents_walk_ck(
    minutes: int = Query(60, ge=1, le=240, description="Időablak percekben"),
) -> Dict[str, Any]:
    stop_id = "1980SN12619E"
    deps = _call_next_departures(stop_id, minutes)
    return {"stop_id": stop_id, "minutes": minutes, "departures": deps}


# CM irány (ha másik stop_id kell)
@app.get("/vincents-walk/cm")
def vincents_walk_cm(
    minutes: int = Query(60, ge=1, le=240, description="Időablak percekben"),
) -> Dict[str, Any]:
    # Itt azt a stop_id-t add meg, amelyik a másik irány
    stop_id = "1980SN12619W"
    deps = _call_next_departures(stop_id, minutes)
    return {"stop_id": stop_id, "minutes": minutes, "departures": deps}


# Összesített/általános Vincents Walk (ha csak egy alap stop-ot akarsz)
@app.get("/vincents-walk")
def vincents_walk_any(
    minutes: int = Query(60, ge=1, le=240, description="Időablak percekben"),
) -> Dict[str, Any]:
    stop_id = "1980SN12619E"
    deps = _call_next_departures(stop_id, minutes)
    return {"stop_id": stop_id, "minutes": minutes, "departures": deps}
