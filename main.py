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
