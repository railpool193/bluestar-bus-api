from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from typing import List, Dict
import os
import csv
import re
import unicodedata
from datetime import datetime

# ---- Saját modulok (már megvannak a repódban) ----
try:
    from gtfs_utils import get_next_departures
except Exception as e:
    raise RuntimeError(f"gtfs_utils import error: {e}")

# siri_live opcionális: ha nincs/hibás, az API akkor is megy 'élő' jel nélkül
def _empty_live(_stop_id: str) -> List[Dict]:
    return []

try:
    from siri_live import fetch_siri_departures  # elvárt: List[Dict] route/destination/(predicted_)time
except Exception:
    fetch_siri_departures = _empty_live


app = FastAPI(title="Bluestar Bus API", version="1.1.0")


# =============== Segédfüggvények / cache ===================

# ékezet/írásjel normalizálás kereséshez
_norm_table = dict.fromkeys(
    i for i in range(0x110000) if unicodedata.category(chr(i)).startswith("M")
)
def _normalize(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).translate(_norm_table)
    s = re.sub(r"[^A-Za-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()

# STOPS betöltés cache-be
_STOPS: List[Dict] = []
def _load_stops():
    """GTFS stops.txt beolvasása a data/ mappából."""
    global _STOPS
    if _STOPS:
        return
    path = os.path.join("data", "stops.txt")
    if not os.path.exists(path):
        return
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                _STOPS.append(
                    {
                        "stop_id": row.get("stop_id"),
                        "name": row.get("stop_name") or "",
                        "desc": row.get("stop_desc") or "",
                        "lat": float(row.get("stop_lat") or 0),
                        "lon": float(row.get("stop_lon") or 0),
                        "norm": _normalize(row.get("stop_name") or ""),
                    }
                )
            except Exception:
                continue

_load_stops()


# =============== Index / Health ===================

@app.get("/", response_class=HTMLResponse)
def root():
    return """
    <html><head><meta http-equiv="refresh" content="0; url=/index.html"/></head>
    <body>OK</body></html>
    """

@app.get("/index.html")
def serve_index():
    path = os.path.join(os.getcwd(), "index.html")
    if not os.path.exists(path):
        raise HTTPException(404, "index.html not found")
    return FileResponse(path, media_type="text/html")

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat(timespec="seconds")}


# =============== Megálló-kereső ===================

@app.get("/stops")
def search_stops(
    q: str = Query(..., min_length=2, max_length=50),
    limit: int = Query(10, ge=1, le=50),
):
    if not _STOPS:
        raise HTTPException(500, "Stops DB not loaded (data/stops.txt not found).")

    qn = _normalize(q)
    hits = [s for s in _STOPS if qn in s["norm"]]

    def score(s):
        pos = _normalize(s["name"]).find(qn)
        return (pos if pos >= 0 else 999, len(s["name"]))

    hits.sort(key=score)
    return [
        {
            "stop_id": s["stop_id"],
            "name": s["name"],
            "desc": s["desc"],
            "lat": s["lat"],
            "lon": s["lon"],
        }
        for s in hits[:limit]
    ]


# =============== GTFS + LIVE összeolvasztás ===================

def merge_with_live(stop_id: str, minutes: int) -> List[Dict]:
    sched = get_next_departures(stop_id, minutes=minutes)

    try:
        live_list = fetch_siri_departures(stop_id) or []
    except Exception:
        live_list = []

    live_index = {}
    time_re = re.compile(r"^(\d{2}):(\d{2})")
    for it in live_list:
        route = (it.get("route") or "").strip()
        t = it.get("predicted_time") or it.get("departure_time")
        if not route or not t:
            continue
        m = time_re.match(t)
        if not m:
            continue
        hhmm = f"{m.group(1)}:{m.group(2)}"
        live_index[(route, hhmm)] = hhmm

    enriched = []
    for row in sched:
        rt = (row.get("route") or "").strip()
        sched_time = (row.get("departure_time") or "").strip()[:5]
        pred = live_index.get((rt, sched_time))
        enriched.append(
            {
                **row,
                "live": bool(pred),
                "predicted_time": pred if pred else None,
            }
        )
    return enriched


# =============== API: Következő indulások ===================

@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    try:
        data = merge_with_live(stop_id, minutes)
        return {"stop_id": stop_id, "minutes": minutes, "departures": data}
    except TypeError as e:
        raise HTTPException(400, f"Failed to build departures (TypeError): {e}")
    except Exception as e:
        raise HTTPException(500, f"Failed to build departures: {e}")


# =============== Gyorslinkek ===================

@app.get("/vincents-walk/ck")
def vw_ck(minutes: int = 60):
    return next_departures("1980SN12619E", minutes)

@app.get("/vincents-walk/cm")
def vw_cm(minutes: int = 60):
    return next_departures("1980SN12619W", minutes)

@app.get("/vincents-walk")
def vw(minutes: int = 60):
    return next_departures("1980SN12619E", minutes)


# =============== Lokális futtatás ===================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
