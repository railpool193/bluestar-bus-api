# main.py
from __future__ import annotations

from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from gtfs_utils import (
    load_gtfs,
    search_stops_by_name,
    map_to_stop_code,
    sibling_stop_codes_by_name,
    _GTFS,  # csak gyors eléréshez a fallbackben
)
import siri_live  # a te SIRI modulod (get_next_departures, is_available)

app = FastAPI(title="Bluestar Bus API")

# ---- CORS (frontend miatt) ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Startup: GTFS betöltés ----
load_gtfs("data")


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "health": "/health",
            "search_example": "/stops/search?q=hanover",
            "generic_example": "/next_departures/1980SN12619A?minutes=60",
        },
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "gtfs_loaded": True,
        "siri_available": siri_live.is_available(),
    }


@app.get("/stops/search")
def stops_search(q: str = Query(..., min_length=2)) -> Dict[str, Any]:
    try:
        return {"query": q, "results": search_stops_by_name(q, limit=12)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")


@app.get("/next_departures/{stop_ref}")
def next_departures(stop_ref: str, minutes: int = 60) -> Dict[str, Any]:
    """
    stop_ref lehet stop_code (ATCO/NaPTAN) vagy stop_id is.
    1) stop_ref -> stop_code mappelés
    2) SIRI lekérés
    3) ha nincs adat, testvér-megállók végigpróbálása ugyanazon stop_name alatt
    """
    # 1) mappelés stop_code-ra
    code = map_to_stop_code(stop_ref)
    if not code:
        raise HTTPException(status_code=404, detail=f"Stop not found: {stop_ref}")

    # 2) első próbálkozás
    try:
        base = siri_live.get_next_departures(code, minutes=minutes)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SIRI error: {e}")

    if base and base.get("departures"):
        return {
            "stop_ref": code,
            "minutes": minutes,
            "departures": base["departures"],
            "live_source": base.get("live_source", "siri"),
        }

    # 3) Fallback: ugyanazon stop_name összes testvér kódja
    df = _GTFS["stops"]
    row = None
    if "stop_code" in df.columns:
        m = df[df["stop_code"] == code]
        if not m.empty:
            row = m.iloc[0]
    if row is None:
        m2 = df[df["stop_id"] == code]
        if not m2.empty:
            row = m2.iloc[0]

    if row is not None:
        name = row["stop_name"]
        siblings = sibling_stop_codes_by_name(name)
        for sib in siblings:
            if sib == code:
                continue
            try:
                alt = siri_live.get_next_departures(sib, minutes=minutes)
            except Exception:
                continue
            if alt and alt.get("departures"):
                return {
                    "stop_ref": sib,
                    "minutes": minutes,
                    "departures": alt["departures"],
                    "live_source": alt.get("live_source", "siri"),
                    "note": f"No live data for {code}, using sibling stop {sib}.",
                }

    # Semmi nem jött
    return {
        "stop_ref": code,
        "minutes": minutes,
        "departures": [],
        "live_source": "siri",
        "note": "No live data found for this stop (and siblings) in the given window.",
    }


# --- Opcionális: nyers SIRI debug végpont ---
@app.get("/debug/siri_raw/{stop_code}")
def siri_raw(stop_code: str, minutes: int = 60) -> Dict[str, Any]:
    try:
        return siri_live.get_next_departures(stop_code, minutes=minutes)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SIRI error: {e}")


# Lokális futtatáshoz:
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
