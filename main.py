# main.py
# ----------------------
# FastAPI app, szinkron endpointokkal (nincs await szinkron függvényre),
# és egyszerű statikus index.html kiszolgálással.

import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from siri_live import get_next_departures, SiriLiveError  # a régi névhez tartozó alias


app = FastAPI(title="Bluestar Bus – Live API")

# CORS – ha kell máshonnan is hívni
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# --- Egyszerű index kiszolgálás (a repo gyökerében lévő index.html) ---
ROOT = Path(__file__).resolve().parent
INDEX = ROOT / "index.html"

if (ROOT / "static").exists():
    app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")


@app.get("/", response_class=FileResponse)
def serve_index():
    if INDEX.exists():
        return FileResponse(str(INDEX))
    # ha nincs index.html, adjunk egyszerű választ
    return PlainTextResponse("Bluestar Bus – Live API", media_type="text/plain")


# --- Egyszerű health/status ---
@app.get("/health")
def health():
    return {
        "status": "ok",
        "gtfs_loaded": True,          # ha van külön GTFS betöltés, itt jelezheted
        "siri_available": True,
        "gtfs_error": None,
        "siri_error": None,
    }


# --- Élő indulások adott megállóra (SIRI VM alapján) ---
@app.get("/api/siri/next_departures/{stop_id}")
def next_departures(
    stop_id: str,
    minutes: int = Query(60, ge=1, le=360),
):
    try:
        results = get_next_departures(
            stop_id=stop_id,
            minutes=minutes,
            api_key=os.getenv("BODS_API_KEY"),
            feed_id=os.getenv("BODS_FEED_ID"),
        )
        return {"query": {"stop_id": stop_id, "minutes": minutes}, "results": results}
    except SiriLiveError as e:
        # SIRI/BODS oldali vagy feldolgozási hiba
        raise HTTPException(status_code=502, detail=str(e))


# --- Opcionális: egyszerű státusz, hogy az env be van-e állítva ---
@app.get("/api/status")
def api_status():
    return {
        "BODS_API_KEY": bool(os.getenv("BODS_API_KEY")),
        "BODS_FEED_ID": bool(os.getenv("BODS_FEED_ID")),
    }
