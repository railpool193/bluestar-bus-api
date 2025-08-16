from fastapi import FastAPI
from gtfs_utils import get_next_departures

app = FastAPI(title="Bluestar Bus API", version="0.2.0")

# --- Gyökér: linkek a hasznos végpontokra ---
@app.get("/")
def index():
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "health": "/health",
            "ck_next_60": "/vincents-walk/ck?minutes=60",
            "cm_next_60": "/vincents-walk/cm?minutes=60",
            "generic_example": "/next_departures/1980SN12619E?minutes=60"
        }
    }

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    departures = get_next_departures(stop_id, minutes_ahead=minutes)
    return {"stop_id": stop_id, "minutes": minutes, "departures": departures}

# ---- Vincent's Walk gyors végpontok ----
CK = "1980SN12619E"   # Vincent's Walk [CK]
CM = "1980HAA13371"   # Vincent's Walk [CM]

@app.get("/vincents-walk/ck")
def vincents_walk_ck(minutes: int = 60):
    return {"stop_id": CK, "minutes": minutes,
            "departures": get_next_departures(CK, minutes_ahead=minutes)}

@app.get("/vincents-walk/cm")
def vincents_walk_cm(minutes: int = 60):
    return {"stop_id": CM, "minutes": minutes,
            "departures": get_next_departures(CM, minutes_ahead=minutes)}

@app.get("/vincents-walk")
def vincents_walk(minutes: int = 60):
    return {
        "minutes": minutes,
        "ck": get_next_departures(CK, minutes_ahead=minutes),
        "cm": get_next_departures(CM, minutes_ahead=minutes)
    }
# main.py (kiegészítés)
from fastapi import FastAPI, Query
from siri_live import get_live_json, LiveDataError

# ... a meglévő app = FastAPI(...) megvan

@app.get("/")
def index():
    return {"service": "Bluestar Bus API", "endpoints": ["/health", "/next_departures/{stop_id}", "/live", "/live?line=18"]}

@app.get("/live")
def live(line: str | None = Query(default=None, description="Opcionális vonalszűrő, pl. 18 vagy 19a")):
    """
    Élő járműpozíciók a BODS SIRI-VM feedből.
    Opcionálisan szűrhető vonalra (?line=18).
    """
    try:
        data = get_live_json(line_filter=line)
        return {"count": len(data), "line": line, "vehicles": data}
    except LiveDataError as e:
        return {"error": str(e)}
import os
import requests

feed_id = os.getenv("BODS_FEED_ID")
api_key = os.getenv("BODS_API_KEY")

url = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{feed_id}/?api_key={api_key}"

print("Fetching:", url)

resp = requests.get(url, timeout=30)
resp.raise_for_status()

# Ez ZIP fájl (benne XML)
with open("avl_feed.zip", "wb") as f:
    f.write(resp.content)

print("Feed downloaded: avl_feed.zip")
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from gtfs_utils import get_next_departures

app = FastAPI(title="Bluestar Bus API")

@app.get("/siri-live")
def siri_live(stop_id: str = Query(...), minutes: int = 60):
    """Alias: /siri-live?stop_id=...&minutes=...  ->  next_departures"""
    deps = get_next_departures(stop_id, minutes=minutes)
    return JSONResponse(
        content={"stop_id": stop_id, "minutes": minutes, "departures": deps}
    )
