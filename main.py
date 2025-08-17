from fastapi import FastAPI, File, UploadFile, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import zipfile
import io
import csv
import os
import re

app = FastAPI(title="Bluestar Bus – API", version="1.2.0")

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DB init ---
DB_FILE = "gtfs.db"

def get_db():
    return sqlite3.connect(DB_FILE)

def init_db():
    if not os.path.exists(DB_FILE):
        conn = get_db()
        c = conn.cursor()
        c.execute("CREATE TABLE stops (stop_id TEXT PRIMARY KEY, stop_name TEXT)")
        c.execute("CREATE TABLE trips (trip_id TEXT PRIMARY KEY, route_id TEXT)")
        c.execute("""
            CREATE TABLE stop_times (
                trip_id TEXT,
                arrival_time TEXT,
                departure_time TEXT,
                stop_id TEXT
            )
        """)
        conn.commit()
        conn.close()

init_db()

# --- MODELS ---
class SearchResult(BaseModel):
    stop_id: str
    stop_name: str

class Departure(BaseModel):
    route: str
    departure_time: str
    live: str | None = None

# --- HELPERS ---
def _norm(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()

# --- ROUTES ---
@app.get("/api/status")
def api_status():
    return {"status": "ok", "gtfs": os.path.exists(DB_FILE)}

@app.post("/api/upload")
async def upload_gtfs(file: UploadFile = File(...)):
    # töröljük a régi DB-t
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    init_db()
    conn = get_db()
    c = conn.cursor()

    content = await file.read()
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        # stops.txt
        with z.open("stops.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                c.execute("INSERT INTO stops (stop_id, stop_name) VALUES (?, ?)",
                          (row["stop_id"], row["stop_name"]))
        # trips.txt
        with z.open("trips.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                c.execute("INSERT INTO trips (trip_id, route_id) VALUES (?, ?)",
                          (row["trip_id"], row["route_id"]))
        # stop_times.txt
        with z.open("stop_times.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                c.execute("INSERT INTO stop_times (trip_id, arrival_time, departure_time, stop_id) VALUES (?, ?, ?, ?)",
                          (row["trip_id"], row["arrival_time"], row["departure_time"], row["stop_id"]))

    conn.commit()
    conn.close()
    return {"status": "uploaded"}

@app.get("/api/stops/search", response_model=list[SearchResult])
def api_search_stops(q: str = Query(..., min_length=2, description="Stop név/id részlete")):
    conn = get_db()
    c = conn.cursor()
    q_raw = q.strip()
    q_like = f"%{q_raw.lower()}%"

    rows = list(c.execute(
        """
        SELECT stop_id, stop_name
        FROM stops
        WHERE lower(stop_name) LIKE :q
           OR lower(stop_id)  LIKE :q
        LIMIT 50
        """,
        {"q": q_like}
    ))

    # fallback: normalizált rész-keresés
    if not rows:
        qn = _norm(q_raw)
        if qn:
            all_rows = list(c.execute("SELECT stop_id, stop_name FROM stops"))
            qr = []
            for sid, sname in all_rows:
                sn = _norm(sname)
                if qn in sn:
                    qr.append((sid, sname))
                elif not qn.endswith("s") and (qn + "s") in sn:
                    qr.append((sid, sname))
            rows = qr[:50]

    conn.close()
    return [{"stop_id": r[0], "stop_name": r[1]} for r in rows]

@app.get("/api/departures/{stop_id}", response_model=list[Departure])
def api_departures(stop_id: str):
    conn = get_db()
    c = conn.cursor()
    rows = list(c.execute(
        """
        SELECT t.route_id, s.departure_time
        FROM stop_times s
        JOIN trips t ON s.trip_id = t.trip_id
        WHERE s.stop_id = ?
        ORDER BY s.departure_time ASC
        LIMIT 20
        """,
        (stop_id,)
    ))
    conn.close()

    # fake "live" adat jelzés
    departures = []
    for r in rows:
        departures.append({"route": r[0], "departure_time": r[1], "live": "–"})
    return departures
