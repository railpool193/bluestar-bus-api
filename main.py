import os
import csv
import io
import zipfile
import sqlite3
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Query, HTTPException, Request, Body
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx

import siri_live
import gtfs

APP_TITLE = "Bluestar Bus – API"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "gtfs.sqlite")

app = FastAPI(title=APP_TITLE, version="0.1.0", docs_url="/", openapi_url="/openapi.json")

# templates
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static"), html=True), name="static")


# --------- helpers ---------
def db_exists() -> bool:
    return os.path.exists(DB_PATH)

def siri_configured() -> bool:
    return bool(os.environ.get("BODS_SIRI_FEED_ID")) and bool(os.environ.get("BODS_API_KEY"))

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --------- pages ---------
@app.get("/ui", response_class=HTMLResponse)
async def ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# --------- api ---------
@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "gtfs_loaded": db_exists(),
        "siri_configured": siri_configured(),
    }


@app.get("/api/stops/search")
async def search_stops(q: str = Query(..., min_length=1, description="Megálló részlete (pl. 'vincent')"), limit: int = 12):
    if not db_exists():
        # üres lista, de jelezzük, mi hiányzik
        return {"query": q, "results": [], "hint": "GTFS database not found"}
    rows = gtfs.search_stops(DB_PATH, q, limit)
    return {
        "query": q,
        "results": [
            {
                "stop_id": r["stop_id"],
                "name": r["stop_name"],
                "code": r.get("stop_code"),
                "lat": r.get("stop_lat"),
                "lon": r.get("stop_lon"),
            }
            for r in rows
        ],
    }


@app.get("/api/stops/{stop_id}/next_departures")
async def next_departures(stop_id: str, minutes: int = 60):
    minutes = max(5, min(minutes, 240))

    # 1) próbáljuk SIRI-t (ha be van állítva)
    live_results = []
    used_source = None
    if siri_configured():
        try:
            live_results = await siri_live.get_next_departures(stop_id, minutes)
            if live_results:
                used_source = "live"
        except Exception:
            # ne dőljön el az endpoint – csendben visszaesünk GTFS-re
            live_results = []

    # 2) GTFS menetrend fallback
    schedule_results = []
    if db_exists():
        schedule_results = gtfs.get_scheduled_departures(DB_PATH, stop_id, minutes)

    # ha van élő, előre tesszük; különben menetrend
    results = []
    for it in live_results:
        results.append({
            "route": it["route"],
            "destination": it["destination"],
            "time_iso": it["time_iso"],
            "is_live": True
        })
    for it in schedule_results:
        # ne duplikáljunk ugyanarra az időpontra
        key = (it["route"], it["destination"], it["time_iso"])
        if not any((r["route"], r["destination"], r["time_iso"]) == key for r in results):
            results.append({
                "route": it["route"],
                "destination": it["destination"],
                "time_iso": it["time_iso"],
                "is_live": False
            })

    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "results": results
    }


# --- admin: GTFS betöltés ---
@app.post("/api/admin/load_gtfs")
async def load_gtfs(url: str = Body(embed=True, media_type="application/json")):
    """
    Töltsd be a GTFS zipet URL-ről (pl. BODS 'Timetables - GTFS' letöltési link).
    """
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=400, detail="Invalid URL")

    # letöltés
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        content = resp.content

    # bontás és import
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        gtfs.import_from_zip_to_sqlite(zf, DB_PATH)

    return {"status": "ok", "db_path": DB_PATH}
