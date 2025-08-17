import os
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
from siri_live import get_live_departures
from gtfs_utils import get_gtfs

app = FastAPI(title="Bluestar Bus – API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

@app.get("/api/status")
def status():
    gtfs = get_gtfs()
    return {
        "status": "ok",
        "gtfs_loaded": bool(gtfs.stops),
        "siri_configured": bool(os.getenv("BODS_API_KEY") and os.getenv("BODS_FEED_ID"))
    }

@app.get("/api/stops/search")
def search_stops(name: str = Query(..., min_length=2)):
    gtfs = get_gtfs()
    res = gtfs.search_stops(name)
    if not res:
        raise HTTPException(status_code=404, detail="Not Found")
    return {"query": name, "results": res}

@app.get("/api/stops/{stop_id}/next_departures")
async def next_departures(stop_id: str, minutes: int = 60):
    # 1) élő
    live = await get_live_departures(stop_id, minutes)
    # 2) menetrendi fallback (ha nincs élő vagy nagyon kevés)
    gtfs = get_gtfs()
    scheduled = gtfs.scheduled_departures(stop_id, minutes)

    # ha van élő, az legyen elöl; külön címkézve jönnek
    results: List[Dict] = []
    if live:
        results.extend(live)
        # egészítsük ki menetrendivel az ablak végéig, de ne duplikáljunk
        live_keys = {(x["route"], x["destination"], x["time_iso"]) for x in live}
        for s in scheduled:
            k = (s["route"], s["destination"], s["time_iso"])
            if k not in live_keys:
                results.append(s)
    else:
        results = scheduled

    results.sort(key=lambda x: x["time_iso"])
    return {"stop_id": stop_id, "minutes": minutes, "results": results}
