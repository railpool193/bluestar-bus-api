from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import gtfs_utils
import siri_live

app = FastAPI()

# HTML sablonok betöltése
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "gtfs_loaded": gtfs_utils.is_loaded(),
        "siri_available": siri_live.is_available()
    }

@app.get("/search_stop")
async def search_stop(query: str):
    results = gtfs_utils.search_stop(query)
    return {"query": query, "results": results}

@app.get("/next_departures/{stop_id}")
async def next_departures(stop_id: str, minutes: int = 60):
    results = await siri_live.get_next_departures(stop_id, minutes)
    return {"query": stop_id, "results": results}
