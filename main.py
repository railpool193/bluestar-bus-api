import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import siri_live

APP_TITLE = "Bluestar Bus API"

app = FastAPI(title=APP_TITLE)

# CORS – a böngészős UI miatt
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- STATIC UI (index.html) ----
@app.get("/", include_in_schema=False)
def serve_index():
    # Ha a fájl a projekt gyökerében van, így visszaadjuk
    if os.path.exists("index.html"):
        return FileResponse("index.html", media_type="text/html")
    return JSONResponse(
        {"message": APP_TITLE, "links": {"health": "/health", "example": "/next_departures/1980SN12619A?minutes=60"}}
    )

# ---- HEALTH ----
@app.get("/health")
def health():
    ok, err = siri_live.health_check()
    return {
        "status": "ok" if ok else "error",
        "gtfs_loaded": True,          # ha van külön GTFS modulod, itt jelezheted a valós állapotot
        "siri_available": ok,
        "gtfs_error": None,
        "siri_error": err,
    }

# ---- STOP KERESÉS (helykitöltő – ha van GTFS keresőd, ide drótozd be) ----
@app.get("/search_stops")
def search_stops(q: str):
    return {"query": q, "results": siri_live.search_stops(q)}

# ---- KÖVETKEZŐ INDULÁSOK ----
@app.get("/next_departures/{stop_id}")
def next_departures(stop_id: str, minutes: int = 60):
    """
    Visszaadja a megálló élő indulásait a megadott időablakban.
    Soha ne dőljön el 500-as hibával általános esetben — részletes hibaüzenetet ad vissza.
    """
    try:
        stop_id = (stop_id or "").strip().upper()
        if not stop_id:
            raise HTTPException(status_code=422, detail="Missing stop_id")

        minutes = max(1, int(minutes))
        results = siri_live.get_live_departures(stop_id, minutes=minutes)
        return {"stop_id": stop_id, "minutes": minutes, "results": results}

    except siri_live.SiriAuthError as e:
        raise HTTPException(status_code=502, detail=f"SIRI auth error: {e}")

    except siri_live.SiriNoData:
        # Nincs adat az időablakban → üres tömb 200-zal, hogy a UI tudja kezelni
        return {"stop_id": stop_id, "minutes": minutes, "results": []}

    except HTTPException:
        raise
    except Exception as e:
        # napló
        print("[/next_departures] ERROR:", repr(e))
        raise HTTPException(status_code=500, detail=f"Server error: {e}")
