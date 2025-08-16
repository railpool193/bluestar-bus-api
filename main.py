# main.py
import os
import csv
from pathlib import Path
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

import siri_live  # a saját modulod

app = FastAPI(title="Bluestar Bus API")

# CORS engedélyezés
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# stops.txt betöltése induláskor
# -----------------------------
STOPS: List[Dict[str, str]] = []

@app.on_event("startup")
def load_stops():
    global STOPS
    STOPS = []
    stops_path = Path("data/stops.txt")
    if stops_path.exists():
        with stops_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stop_id = (row.get("stop_id") or "").strip()
                stop_name = (row.get("stop_name") or "").strip()
                if stop_id and stop_name:
                    STOPS.append({"id": stop_id, "name": stop_name})
        print(f"Betöltve {len(STOPS)} megálló a stops.txt-ből")
    else:
        print("WARNING: data/stops.txt nem található")

def _score(q: str, name: str) -> int:
    ql = q.casefold()
    nl = name.casefold()
    pos = nl.find(ql)
    if pos < 0:
        return 10_000
    return pos + len(name)

@app.get("/search_stops")
def search_stops(q: str, limit: int = 15):
    """Egyszerű megállókereső a GTFS stops.txt alapján"""
    if not q or len(q.strip()) < 2:
        return []
    q = q.strip()
    items = sorted(
        (s for s in STOPS if q.casefold() in s["name"].casefold()),
        key=lambda s: _score(q, s["name"])
    )
    return items[: max(1, min(50, limit))]

# -----------------------------
# élő indulások
# -----------------------------
@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    """Következő indulások egy megállóból"""
    try:
        deps = siri_live.get_next_departures(stop_id, minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": deps}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build departures ({type(e).__name__}): {e}")

# -----------------------------
# debug segéd
# -----------------------------
@app.get("/debug/peek_stops")
def debug_peek_stops(limit: int = 5):
    """Kipróbálni, milyen StopPointRef értékek jönnek a feedben"""
    doc = siri_live._get_cached_doc()
    va_list = siri_live._iter_vehicle_activities(doc)[: max(1, min(50, limit))]
    refs = []
    for va in va_list:
        mvj = va.get("MonitoredVehicleJourney", {}) or {}
        mc = mvj.get("MonitoredCall") or {}
        if mc.get("StopPointRef"):
            refs.append(mc.get("StopPointRef"))
        onward = mvj.get("OnwardCalls") or {}
        onward_calls = onward.get("OnwardCall") or []
        if isinstance(onward_calls, dict):
            onward_calls = [onward_calls]
        for oc in onward_calls[:3]:
            if oc.get("StopPointRef"):
                refs.append(oc.get("StopPointRef"))
    return list(dict.fromkeys(refs))[:30]

# -----------------------------
# index.html kiszolgálás
# -----------------------------
@app.get("/")
def root():
    index_path = Path("static/index.html")
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "Bluestar Bus API – nincs index.html"}
