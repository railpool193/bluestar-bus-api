from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import shutil
import zipfile
import os
import sqlite3
import pandas as pd
import siri_live

app = FastAPI(title="Bluestar Bus – API", version="1.2.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_FILE = "gtfs.db"


def init_db(gtfs_path: str):
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    conn = sqlite3.connect(DB_FILE)

    with zipfile.ZipFile(gtfs_path, "r") as zip_ref:
        zip_ref.extractall("gtfs")

    # stops.txt
    if os.path.exists("gtfs/stops.txt"):
        stops = pd.read_csv("gtfs/stops.txt")
        stops.to_sql("stops", conn, if_exists="replace", index=False)

    conn.close()


@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "gtfs": os.path.exists(DB_FILE),
        "live": siri_live.is_live_available()
    }


@app.post("/api/upload")
async def upload_gtfs(file: UploadFile = File(...)):
    file_location = f"temp_{file.filename}"
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    init_db(file_location)
    os.remove(file_location)
    return {"status": "uploaded"}


@app.get("/api/stops/search")
async def search_stops(q: str = Query(..., min_length=2)):
    if not os.path.exists(DB_FILE):
        return []

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT stop_id, stop_name FROM stops WHERE stop_name LIKE ? LIMIT 20",
        (f"%{q}%",)
    )
    rows = cur.fetchall()
    conn.close()

    return [{"stop_id": r[0], "stop_name": r[1]} for r in rows]


@app.get("/api/departures/{stop_id}")
async def get_departures(stop_id: str):
    departures = siri_live.get_live_departures(stop_id)
    return {"stop_id": stop_id, "departures": departures}


# --- UI bekötése ---
app.mount("/", StaticFiles(directory="static", html=True), name="static")
