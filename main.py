from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import gtfs
import siri_live
import logging

app = FastAPI()

# Logging beállítás
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bluestar")

# Statikus fájlok (frontend)
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


@app.get("/departures")
async def departures(
    stop_id: str = Query(..., description="Stop ID, pl. 1980SN12619E"),
    minutes: int = Query(60, description="Időablak percekben")
):
    logger.info(f"Indulások lekérése: stop_id={stop_id}, minutes={minutes}")

    try:
        departures = gtfs.get_next_departures(stop_id, minutes=minutes)

        # Debug log - indulások részletes listázása
        if not departures:
            logger.warning(f"Nincs indulás a megadott ID-re: {stop_id}")
        else:
            for dep in departures:
                logger.info(
                    f"Indulás: route={dep.get('route')} "
                    f"dest={dep.get('destination')} "
                    f"time={dep.get('time')} "
                    f"live={dep.get('live')}"
                )

        return JSONResponse(
            content={"stop_id": stop_id, "minutes": minutes, "departures": departures}
        )

    except Exception as e:
        logger.error(f"Hiba az indulások lekérésekor: {e}")
        return JSONResponse(
            content={"error": str(e)}, status_code=500
        )


@app.get("/search")
async def search_stops(query: str):
    """Megálló keresése név szerint"""
    try:
        results = gtfs.search_stops(query)
        logger.info(f"Keresés '{query}' → {len(results)} találat")
        return results
    except Exception as e:
        logger.error(f"Hiba keresés közben: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/")
async def root():
    return {"message": "Bluestar Bus API – nincs index.html"}
