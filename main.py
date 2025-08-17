# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

app = FastAPI(title="Bluestar Bus – API", version="1.0.1")

# --- CORS (frontend -> API) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # ha szeretnéd szigorítani, ide írd a domain(eke)t
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Static frontend (index.html a / alatt) ---
# Tegyél egy 'static' mappát a projekt gyökerébe (benne index.html, style.css stb.)
app.mount("/", StaticFiles(directory="static", html=True), name="static")


# --- (Opcionális) kis segítség végpont a fejlesztéshez ---
@app.get("/api/ui", summary="Simple Ui Hint")
def ui_hint():
    return JSONResponse(
        {
            "hint": "A frontend a gyökéren érhető el (/) – próbáld megnyitni a kezdőoldalt.",
            "routes": {
                "search": "/api/stops/search?q=vincent",
                "next_departures": "/api/stops/1980SN12618A/next_departures",
                "status": "/api/status",
            },
        }
    )

# MEGJEGYZÉS:
# A tényleges domain/railway URL-en a gyökér (/) mostantól a static/index.html-t szolgálja ki,
# az API végpontok pedig továbbra is /api/... alatt érhetők el változatlanul.from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse

app = FastAPI(title="Bluestar Bus – API", version="1.0.1")

# ---- statikus fájlok kiszolgálása ----
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# /static/... útvonalon menjen minden statikus asset (css, js, képek)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Főoldal: adja vissza a static/index.html-t
@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(STATIC_DIR / "index.html")

import io
import os
import csv
import zipfile
import sqlite3
import tempfile
import datetime as dt
from typing import Optional, List

import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, AnyHttpUrl

# ---- Beállítások
DB_PATH = os.environ.get("GTFS_DB_PATH", "/data/gtfs.sqlite")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

app = FastAPI(title="Bluestar Bus – API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Segédfüggvények DB-hez
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def db_init(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta(
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stops(
            stop_id TEXT PRIMARY KEY,
            stop_name TEXT,
            stop_lat REAL,
            stop_lon REAL,
            searchable_name TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS routes(
            route_id TEXT PRIMARY KEY,
            route_short_name TEXT,
            route_long_name TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trips(
            trip_id TEXT PRIMARY KEY,
            route_id TEXT,
            trip_headsign TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stop_times(
            trip_id TEXT,
            arrival_time TEXT,
            departure_time TEXT,
            stop_id TEXT
        );
    """)
    conn.commit()

def time_to_seconds(t: str) -> Optional[int]:
    # GTFS-ben 24+ órát is enged (pl. 25:10:00), ezt is támogatjuk
    try:
        parts = t.split(":")
        if len(parts) != 3:
            return None
        h, m, s = map(int, parts)
        return h*3600 + m*60 + s
    except Exception:
        return None

def seconds_to_hhmm(ss: int) -> str:
    h = ss // 3600
    m = (ss % 3600) // 60
    return f"{h:02d}:{m:02d}"

def set_meta(conn, k, v):
    conn.execute("REPLACE INTO meta(k,v) VALUES (?,?)", (k, v))
    conn.commit()

def get_meta(conn, k, default=None):
    cur = conn.execute("SELECT v FROM meta WHERE k=?", (k,))
    row = cur.fetchone()
    return row["v"] if row else default

# ---- Modellek
class LoadGtfsRequest(BaseModel):
    url: AnyHttpUrl

class SearchResult(BaseModel):
    stop_id: str
    stop_name: str

class Departure(BaseModel):
    route: str
    destination: Optional[str]
    time_iso: str
    is_live: bool = False

class NextDeparturesResponse(BaseModel):
    stop_id: str
    minutes: int
    results: List[Departure]

# ---- Endpontok
@app.get("/api/status")
def status():
    conn = get_conn()
    db_init(conn)
    loaded = bool(get_meta(conn, "gtfs_loaded", "0") == "1")
    return {"status": "ok", "gtfs_loaded": loaded, "siri_configured": False}

@app.get("/api/stops/search", response_model=List[SearchResult])
def search_stops(q: str):
    q = q.strip()
    if not q:
        return []
    conn = get_conn()
    cur = conn.execute(
        """
        SELECT stop_id, stop_name
        FROM stops
        WHERE searchable_name LIKE ?
        ORDER BY stop_name
        LIMIT 50
        """,
        (f"%{q.lower()}%",),
    )
    return [{"stop_id": r["stop_id"], "stop_name": r["stop_name"]} for r in cur.fetchall()]

@app.get("/api/stops/{stop_id}/next_departures", response_model=NextDeparturesResponse)
def next_departures(stop_id: str, minutes: int = 60):
    now = dt.datetime.utcnow()
    # csak az időt használjuk (napfüggetlen), demó célra elegendő
    now_secs = now.hour*3600 + now.minute*60 + now.second
    end_secs = now_secs + minutes*60

    conn = get_conn()
    cur = conn.execute("""
        SELECT st.departure_time, r.route_short_name AS route, COALESCE(t.trip_headsign, r.route_long_name) AS dest
        FROM stop_times st
        JOIN trips t ON t.trip_id = st.trip_id
        LEFT JOIN routes r ON r.route_id = t.route_id
        WHERE st.stop_id = ?
    """, (stop_id,))
    rows = cur.fetchall()

    results = []
    for r in rows:
        secs = time_to_seconds(r["departure_time"])
        if secs is None:
            continue
        # ablakba eső indulások (egyszerű, napfüggetlen logika)
        if now_secs <= secs <= end_secs:
            dep_time = dt.datetime.combine(now.date(), dt.time()) + dt.timedelta(seconds=secs)
            results.append(Departure(
                route=r["route"] or "",
                destination=r["dest"],
                time_iso=dep_time.isoformat(),
                is_live=False
            ))
    # idő szerint rendezés és limit
    results.sort(key=lambda d: d.time_iso)
    return NextDeparturesResponse(stop_id=stop_id, minutes=minutes, results=results[:50])

# ---- GTFS betöltés: URL vagy feltöltött fájl
@app.post("/api/admin/load_gtfs")
async def load_gtfs_json(payload: Optional[LoadGtfsRequest] = None, file: UploadFile = File(None)):
    """
    Két mód:
    - JSON: {"url": "https://.../gtfs.zip"}
    - multipart/form-data: file=<gtfs.zip>
    """
    # Honnan jön a zip?
    zip_bytes: bytes
    source = ""
    if payload and payload.url:
        source = "url"
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                r = await client.get(str(payload.url), follow_redirects=True)
                r.raise_for_status()
                zip_bytes = r.content
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"GTFS letöltési hiba: {e}")
    elif file is not None:
        source = "upload"
        try:
            zip_bytes = await file.read()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Fájlolvasási hiba: {e}")
    else:
        raise HTTPException(status_code=400, detail="Adj meg egy 'url'-t JSON-ban VAGY tölts fel egy 'file' ZIP-et.")

    # Ellenőrzés: zip-e?
    if not zipfile.is_zipfile(io.BytesIO(zip_bytes)):
        raise HTTPException(status_code=400, detail="A kapott tartalom nem ZIP.")

    # Ideiglenes kicsomagolás
    try:
        with tempfile.TemporaryDirectory() as td:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                z.extractall(td)

            # Szükséges fájlok
            stops_p = None
            routes_p = None
            trips_p = None
            stop_times_p = None
            for name in os.listdir(td):
                low = name.lower()
                p = os.path.join(td, name)
                if low == "stops.txt":
                    stops_p = p
                elif low == "routes.txt":
                    routes_p = p
                elif low == "trips.txt":
                    trips_p = p
                elif low == "stop_times.txt":
                    stop_times_p = p

            if not all([stops_p, routes_p, trips_p, stop_times_p]):
                raise HTTPException(status_code=400, detail="Hiányzó GTFS fájl(ok): szükséges stops.txt, routes.txt, trips.txt, stop_times.txt")

            # DB init + feltöltés
            conn = get_conn()
            db_init(conn)
            cur = conn.cursor()
            # ürítjük a táblákat
            cur.execute("DELETE FROM stops")
            cur.execute("DELETE FROM routes")
            cur.execute("DELETE FROM trips")
            cur.execute("DELETE FROM stop_times")
            conn.commit()

            # stops
            with open(stops_p, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = []
                for r in reader:
                    stop_id = r.get("stop_id")
                    stop_name = r.get("stop_name") or ""
                    stop_lat = r.get("stop_lat") or None
                    stop_lon = r.get("stop_lon") or None
                    if not stop_id:
                        continue
                    rows.append((stop_id, stop_name, float(stop_lat) if stop_lat else None,
                                 float(stop_lon) if stop_lon else None, stop_name.lower()))
                cur.executemany("INSERT OR REPLACE INTO stops VALUES (?,?,?,?,?)", rows)

            # routes
            with open(routes_p, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = []
                for r in reader:
                    route_id = r.get("route_id")
                    if not route_id:
                        continue
                    rows.append((
                        route_id,
                        r.get("route_short_name"),
                        r.get("route_long_name"),
                    ))
                cur.executemany("INSERT OR REPLACE INTO routes VALUES (?,?,?)", rows)

            # trips
            with open(trips_p, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = []
                for r in reader:
                    trip_id = r.get("trip_id")
                    route_id = r.get("route_id")
                    if not trip_id:
                        continue
                    rows.append((
                        trip_id,
                        route_id,
                        r.get("trip_headsign"),
                    ))
                cur.executemany("INSERT OR REPLACE INTO trips VALUES (?,?,?)", rows)

            # stop_times
            batch = []
            with open(stop_times_p, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    trip_id = r.get("trip_id")
                    stop_id = r.get("stop_id")
                    dep = r.get("departure_time") or r.get("arrival_time")
                    arr = r.get("arrival_time") or dep
                    if not (trip_id and stop_id and (dep or arr)):
                        continue
                    batch.append((trip_id, arr or "", dep or "", stop_id))
                    if len(batch) >= 20_000:
                        cur.executemany("INSERT INTO stop_times VALUES (?,?,?,?)", batch)
                        batch = []
            if batch:
                cur.executemany("INSERT INTO stop_times VALUES (?,?,?,?)", batch)

            conn.commit()
            set_meta(conn, "gtfs_loaded", "1")

        return {
            "status": "ok",
            "method": source,
            "message": "GTFS betöltve az adatbázisba.",
        }
    except HTTPException:
        raise
    except Exception as e:
        # Részletes hiba a logban, a kliensnek biztonságos üzenet
        print("GTFS load error:", repr(e))
        raise HTTPException(status_code=500, detail="GTFS feldolgozási hiba. Nézd meg a szerver logokat.")

# ---- Egyszerű UI (opcionális)
@app.get("/api/ui")
def simple_ui_hint():
    return {"open": "/docs vagy /redoc a böngészőben a teszteléshez."}
