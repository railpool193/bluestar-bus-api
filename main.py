from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pathlib import Path
import uvicorn

from gtfs_utils import get_next_departures  # saját függvényed, ami GTFS-ből adatokhoz nyúl

app = FastAPI(title="Bluestar Bus API", version="1.0.0")


# ----------------------------
# Gyökér oldal → index.html
# ----------------------------
@app.get("/", include_in_schema=False, response_class=HTMLResponse)
def serve_index():
    html_path = Path(__file__).with_name("index.html")
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return html_path.read_text(encoding="utf-8")


# ----------------------------
# Healthcheck
# ----------------------------
@app.get("/health")
def health_check():
    return {"status": "ok"}


# ----------------------------
# Következő indulások
# ----------------------------
@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    try:
        departures = get_next_departures(stop_id, minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": departures}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build departures ({type(e).__name__}): {e}")


# ----------------------------
# Példa egy konkrét megállóra
# ----------------------------
@app.get("/vincents-walk/ck")
def vincents_walk_ck(minutes: int = 60):
    stop_id = "1980SN12619E"  # Vincents Walk CK
    try:
        departures = get_next_departures(stop_id, minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": departures}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build departures ({type(e).__name__}): {e}")


# ----------------------------
# Csak lokális fejlesztéshez
# ----------------------------
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
# main.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse
from typing import List, Dict, Optional
import os
import csv
import re
import unicodedata
from datetime import datetime, timedelta

from gtfs_utils import get_next_departures   # meglévő helper: GTFS-ből számol
from siri_live import fetch_siri_departures  # <--- győződj meg róla, hogy ez a modul létezik

app = FastAPI(title="Bluestar Bus API", version="1.1.0")

# --------- Helper: ékezet- és írásjel-mentesítés a kereséshez ----------
_norm_table = dict.fromkeys(i for i in range(0x110000)
                            if unicodedata.category(chr(i)).startswith('M'))
def _normalize(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).translate(_norm_table)
    s = re.sub(r"[^A-Za-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()

# --------- STOPS cache betöltés ----------
_STOPS: List[Dict] = []
def _load_stops():
    global _STOPS
    if _STOPS:
        return
    path = os.path.join("data", "stops.txt")
    if not os.path.exists(path):
        return
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            _STOPS.append({
                "stop_id": row.get("stop_id"),
                "name": row.get("stop_name") or "",
                "desc": row.get("stop_desc") or "",
                "lat": float(row.get("stop_lat") or 0),
                "lon": float(row.get("stop_lon") or 0),
                "norm": _normalize(row.get("stop_name") or ""),
            })

_load_stops()

# --------- ÚJ: Megálló-kereső endpoint ----------
@app.get("/stops")
def search_stops(
    q: str = Query(..., min_length=2, max_length=50),
    limit: int = Query(10, ge=1, le=50)
):
    if not _STOPS:
        raise HTTPException(500, "Stops DB not loaded.")
    qn = _normalize(q)
    matches = [s for s in _STOPS if qn in s["norm"]]
    # egyszerű rangsor: rövidebb név, elején egyezés előnyben
    def score(s):
        name = s["name"]
        pos = _normalize(name).find(qn)
        return (pos if pos >= 0 else 999, len(name))
    matches.sort(key=score)
    return [
        {
            "stop_id": s["stop_id"],
            "name": s["name"],
            "desc": s["desc"],
            "lat": s["lat"],
            "lon": s["lon"],
        }
        for s in matches[:limit]
    ]

# --------- ÚJ: GTFS + Live összeolvasztás ----------
def _hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")

def merge_with_live(stop_id: str, minutes: int):
    """
    Visszaadja a következő indulásokat:
    - GTFS (menetrendi) lista
    - Ha elérhető, SIRI 'élő' lista -> jelölés + predicted_time
    """
    # GTFS (meglévő helpered)
    sched = get_next_departures(stop_id, minutes=minutes)  # [{route,destination,departure_time},...]

    # Élő adatok (ha van BODS konfiguráció)
    live = []
    try:
        live = fetch_siri_departures(stop_id)  # [{route,destination,departure_time or predicted_time},...]
    except Exception:
        live = []

    # map: (route, hh:mm) -> predicted_hh:mm
    live_index = {}
    for it in live:
        route = (it.get("route") or "").strip()
        # Veszünk egy időt: predicted_time, vagy departure_time; HH:MM
        pt = it.get("predicted_time") or it.get("departure_time")
        if not route or not pt:
            continue
        # kerekítés HH:MM
        m = re.match(r"^(\d{2}):(\d{2})", pt)
        if not m:
            continue
        live_index[(route, f"{m.group(1)}:{m.group(2)}")] = pt

    # jelölés a menetrendi listán
    enriched = []
    for row in sched:
        rt = (row.get("route") or "").strip()
        tt = (row.get("departure_time") or "").strip()[:5]  # HH:MM
        pred = live_index.get((rt, tt))
        enriched.append({
            **row,
            "live": bool(pred),                # élő jelölés
            "predicted_time": pred or None,    # ha tudjuk, mutatjuk
        })
    return enriched

# --------- MÓDOSÍTOTT: meglévő endpointot bővítjük live flaggel ----------
@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    try:
        data = merge_with_live(stop_id, minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": data}
    except TypeError as e:
        raise HTTPException(400, f"Failed to build departures (TypeError): {e}")
    except Exception as e:
        raise HTTPException(500, f"Failed to build departures: {e}")

# ------- (maradhatnak a Vincents Walk shortcut endpointjaid, stb.) -------

# ------- Root/Index marad változatlanul -------
@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html><head><meta http-equiv="refresh" content="0; url=/index.html"/></head>
    <body>OK</body></html>
    """
