# main.py
import os
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Opcionális modulok (amennyiben nálad már léteznek) ---
# Mindkettő "best-effort" használat; ha nincs / másként nevezett,
# akkor továbbra is működik a backend menetrendi adatokkal.
gtfs_utils = None
siri_live = None
try:
    import gtfs_utils as _gtfs  # a te repo-dból
    gtfs_utils = _gtfs
except Exception:
    gtfs_utils = None

try:
    import siri_live as _siri  # a te repo-dból
    siri_live = _siri
except Exception:
    siri_live = None

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
INDEX_FILE = APP_DIR / "index.html"

app = FastAPI(title="Bluestar Bus API", version="1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ------------- GTFS stops betöltés egyszer -------------
_STOPS: List[Dict[str, str]] = []
_STOPS_IDX_READY = False


def _load_stops() -> None:
    global _STOPS, _STOPS_IDX_READY
    if _STOPS_IDX_READY:
        return
    stops_txt = DATA_DIR / "stops.txt"
    if not stops_txt.exists():
        _STOPS = []
        _STOPS_IDX_READY = True
        return

    with stops_txt.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        _STOPS = []
        for row in reader:
            # fontos mezők normalizálása
            _STOPS.append(
                {
                    "stop_id": row.get("stop_id", "").strip(),
                    "stop_name": row.get("stop_name", "").strip(),
                    "stop_lat": row.get("stop_lat", ""),
                    "stop_lon": row.get("stop_lon", ""),
                }
            )
    _STOPS_IDX_READY = True


def search_stops_by_name(q: str, limit: int = 8) -> List[Dict[str, str]]:
    """Egyszerű, kis/nagybetűt nem érzékeny tartalmazás alapú keresés."""
    _load_stops()
    if not q:
        return []
    qq = q.lower().strip()
    results = [s for s in _STOPS if qq in s["stop_name"].lower()]
    # rendezés: rövidebb név, majd ABC
    results.sort(key=lambda s: (len(s["stop_name"]), s["stop_name"]))
    return results[:limit]


# ------------------- Élő + menetrendi egyesítés -------------------

def _normalize_live_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Különböző szerkezetekből normalizál egy "live" rekordot.
    Várt kimenet: route, destination, departure_time, realtime=True
    """
    if not item:
        return None

    # lehetséges kulcsnevek
    route = item.get("route") or item.get("line") or item.get("line_name")
    dest = item.get("destination") or item.get("direction") or item.get("dest")
    # idő: preferált "expected", ha nincs, akkor "departure_time"/"aimed"
    dep = (
        item.get("expected_departure_time")
        or item.get("expected_time")
        or item.get("departure_time")
        or item.get("aimed_departure_time")
        or item.get("time")
    )

    if not (route and dest and dep):
        return None

    return {
        "route": str(route),
        "destination": str(dest),
        "departure_time": str(dep),
        "realtime": True,
    }


def _fetch_live(stop_id: str, minutes: int) -> List[Dict[str, Any]]:
    """Best-effort lekérés a siri_live modulból több névvel/struktúrával."""
    if siri_live is None:
        return []

    # támogatott hívásnevek:
    candidates = [
        ("get_departures", {"stop_id": stop_id, "minutes": minutes}),
        ("get_next_departures", {"stop_id": stop_id, "minutes": minutes}),
        ("get_next_departures", {"stop_id": stop_id}),  # régi szignatúra
        ("build_departures", {"stop_id": stop_id, "minutes": minutes}),
    ]
    for func_name, kwargs in candidates:
        func = getattr(siri_live, func_name, None)
        if callable(func):
            try:
                data = func(**kwargs)
                # data lehet lista vagy dict {"departures": [...]}
                if isinstance(data, dict) and "departures" in data:
                    data = data["departures"]
                if not isinstance(data, list):
                    continue
                out = []
                for it in data:
                    norm = _normalize_live_item(it)
                    if norm:
                        out.append(norm)
                return out
            except TypeError:
                # nem passzoló szignatúra → próbáljuk a következőt
                continue
            except Exception:
                # ha bármi gond van, lépünk tovább a fallbackre
                return []
    return []


def _normalize_sched_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Menetrendi rekord normalizálása."""
    if not item:
        return None
    route = item.get("route") or item.get("line") or item.get("line_name")
    dest = item.get("destination") or item.get("headsign") or item.get("dest")
    dep = item.get("departure_time") or item.get("time")
    if not (route and dest and dep):
        return None
    return {
        "route": str(route),
        "destination": str(dest),
        "departure_time": str(dep),
        "realtime": bool(item.get("realtime", False)),  # ha netán már benne van
    }


def _fetch_schedule(stop_id: str, minutes: int) -> List[Dict[str, Any]]:
    """Menetrendi (GTFS) következő indulások lekérése, ha van hozzá util."""
    if gtfs_utils is None:
        return []

    # több lehetséges publikus segédfüggvény támogatása
    candidates = [
        ("get_next_departures", {"stop_id": stop_id, "minutes": minutes}),
        ("next_departures", {"stop_id": stop_id, "minutes": minutes}),
        ("get_departures", {"stop_id": stop_id, "minutes": minutes}),
    ]
    for func_name, kwargs in candidates:
        func = getattr(gtfs_utils, func_name, None)
        if callable(func):
            try:
                data = func(**kwargs)
                if isinstance(data, dict) and "departures" in data:
                    data = data["departures"]
                if not isinstance(data, list):
                    continue
                out = []
                for it in data:
                    norm = _normalize_sched_item(it)
                    if norm:
                        out.append(norm)
                return out
            except Exception:
                return []
    return []


def _merge_departures(schedule: List[Dict[str, Any]],
                      live: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Egyszerű egyesítés:
    - élő rekord felülírja / jelzi ugyanazt az indulást
    - kulcs: (route, destination, departure_time)
    """
    merged: Dict[tuple, Dict[str, Any]] = {}
    for it in schedule:
        key = (it["route"], it["destination"], it["departure_time"])
        merged[key] = dict(it)

    for it in live:
        key = (it["route"], it["destination"], it["departure_time"])
        if key in merged:
            # ha menetrendiben megvan ugyanez az idő → jelöljük élőnek
            merged[key]["realtime"] = True
        else:
            merged[key] = dict(it)

    # vissza listává, idő szerinti ABC (hh:mm alaknál működik)
    out = list(merged.values())
    out.sort(key=lambda x: (x.get("departure_time", ""), x.get("route", "")))
    return out


# ---------------------- Endpoints ----------------------

@app.get("/")
def root():
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
def health():
    return {"ok": True}


@app.get("/index.html")
def index_html():
    if INDEX_FILE.exists():
        return FileResponse(INDEX_FILE)
    return JSONResponse({"message": "Bluestar Bus API - nincs index.html"}, status_code=200)


@app.get("/search_stops")
def api_search_stops(q: str = Query(..., min_length=2, description="Megálló neve"),
                     limit: int = 8):
    try:
        results = search_stops_by_name(q, limit=limit)
        # front-end kényelmi label
        for r in results:
            r["label"] = f'{r["stop_name"]} ({r["stop_id"]})'
        return {"query": q, "results": results}
    except Exception as e:
        raise HTTPException(500, f"Search failed: {e}")


@app.get("/next_departures/{stop_id}")
def api_next_departures(stop_id: str, minutes: int = 60):
    try:
        sched = _fetch_schedule(stop_id, minutes)
        live = _fetch_live(stop_id, minutes)
        departures = _merge_departures(sched, live)
        return {"stop_id": stop_id, "minutes": minutes, "departures": departures}
    except Exception as e:
        raise HTTPException(500, f"Failed to build departures: {e}")


# --- A két kényelmi végpont (ha korábban használtad) ---
@app.get("/vincents-walk/ck")
def next_ck(minutes: int = 60):
    return api_next_departures("1980SN12619E", minutes)


@app.get("/vincents-walk/cm")
def next_cm(minutes: int = 60):
    return api_next_departures("1980HAA13371", minutes)
