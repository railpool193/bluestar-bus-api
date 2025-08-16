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
