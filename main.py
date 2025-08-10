
from fastapi import FastAPI
from gtfs_utils import get_next_departures

app = FastAPI(title="Bluestar Bus API", version="0.1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    """Return departures within the next `minutes` for a given GTFS stop_id."""
    departures = get_next_departures(stop_id, minutes_ahead=minutes)
    return {"stop_id": stop_id, "minutes": minutes, "departures": departures}
