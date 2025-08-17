import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import uvicorn

# ------- opcionális: GTFS megálló-kereső (egyszerű, memóriás példa) -------
# Ha nincs GTFS-ed betöltve, ez csak "látszat" keresőt ad.
# Ha korábban volt saját GTFS loadered, ide illesztheted.
STOPS: List[Dict[str, str]] = []   # töltsd fel saját adataiddal (stop_name, stop_id)

def search_stops_local(q: str) -> List[Dict[str, str]]:
    ql = q.strip().lower()
    if not ql:
        return []
    hits = [s for s in STOPS if ql in s.get("stop_name","").lower()]
    return hits[:10]

# ---------------- SIRI élő ----------------
import siri_live

app = FastAPI(title="Bluestar Bus API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ha szeretnéd, szűkítsd a domainjeidre
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "gtfs_loaded": bool(STOPS),
        "siri_available": True,
        "gtfs_error": None,
        "siri_error": None,
    }

@app.get("/", response_class=HTMLResponse)
async def root():
    # statikus index.html kiszolgálása (ha a fájl a projekt gyökerében van)
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read(), status_code=200)
    except Exception:
        return HTMLResponse("<h1>Bluestar Bus API</h1>", status_code=200)

@app.get("/search_stops")
async def search_stops(name: str = Query(..., min_length=2)):
    try:
        results = search_stops_local(name)
        # Válasz egységes formában
        out = [
            {
                "display_name": f"{s.get('stop_name','')} ({s.get('stop_id','')})",
                "stop_id": s.get("stop_id","")
            }
            for s in results
        ]
        return {"query": name, "results": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/siri/next_departures/{stop_id}")
async def siri_next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=480)):
    try:
        data = await siri_live.get_next_departures(stop_id, minutes=minutes)
        return {"stop_id": stop_id, "minutes": minutes, "results": data}
    except Exception as e:
        # továbbítjuk a hiba részleteit, hogy a frontend ki tudja írni
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # Railway/Render esetén a PORT környezeti változót érdemes használni
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
