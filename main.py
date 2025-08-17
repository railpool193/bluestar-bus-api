# main.py
from __future__ import annotations

import io
import os
import sys
import csv
import zipfile
import sqlite3
import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import uvicorn
import httpx
from fastapi import FastAPI, UploadFile, File, Form, Query, Path as FPath, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

APP_TITLE = "Bluestar Bus – API"
app = FastAPI(title=APP_TITLE, version="1.1.0")

# ----- Paths & DB -----
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "gtfs.sqlite"

# ----- Live (BODS/SIRI) env -----
BODS_BASE_URL = os.getenv("BODS_BASE_URL", "").rstrip("/")
BODS_API_KEY = os.getenv("BODS_API_KEY", "")
BODS_FEED_ID = os.getenv("BODS_FEED_ID", "")
BODS_PRODUCER = os.getenv("BODS_PRODUCER", "")

def siri_configured() -> bool:
    return all([BODS_BASE_URL, BODS_API_KEY, BODS_FEED_ID])

# Simple in-process cache for SIRI results (reduce rate)
_siri_cache: Dict[str, Tuple[float, Any]] = {}  # key: "vm", value: (ts, xml_root)
SIRI_TTL_SEC = 20.0

# ----- CORS -----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----- Utils -----
def db_conn() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def gtfs_loaded() -> bool:
    return DB_PATH.exists()

def parse_time_hhmmss(t: str) -> int:
    # returns seconds from midnight; supports 24+ hour times like 25:10:00
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s

def today_service_ids(cur: sqlite3.Cursor, day: dt.date) -> set:
    wd = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][day.weekday()]
    ids = set()
    for row in cur.execute(f"""
      SELECT service_id FROM calendar
      WHERE start_date <= ? AND end_date >= ? AND {wd}=1
    """, (int(day.strftime("%Y%m%d")), int(day.strftime("%Y%m%d")))):
        ids.add(row["service_id"])

    # calendar_dates exceptions
    add = set()
    remove = set()
    for row in cur.execute("SELECT service_id, date, exception_type FROM calendar_dates WHERE date=?", (int(day.strftime("%Y%m%d")),)):
        if row["exception_type"] == 1:
            add.add(row["service_id"])
        elif row["exception_type"] == 2:
            remove.add(row["service_id"])

    ids.difference_update(remove)
    ids.update(add)
    return ids

# ----- GTFS Loader -----
def load_gtfs_from_bytes(zip_bytes: bytes) -> None:
    # (re)create DB
    if DB_PATH.exists():
        DB_PATH.unlink()
    con = db_conn()
    cur = con.cursor()

    # Create tables
    cur.executescript("""
    CREATE TABLE stops(
      stop_id TEXT PRIMARY KEY,
      stop_name TEXT,
      stop_code TEXT,
      stop_lat REAL,
      stop_lon REAL
    );
    CREATE TABLE routes(
      route_id TEXT PRIMARY KEY,
      route_short_name TEXT,
      route_long_name TEXT
    );
    CREATE TABLE trips(
      trip_id TEXT PRIMARY KEY,
      route_id TEXT,
      service_id TEXT,
      trip_headsign TEXT
    );
    CREATE TABLE stop_times(
      trip_id TEXT,
      arrival_time TEXT,
      departure_time TEXT,
      stop_id TEXT,
      stop_sequence INTEGER
    );
    CREATE TABLE calendar(
      service_id TEXT PRIMARY KEY,
      monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER,
      friday INTEGER, saturday INTEGER, sunday INTEGER,
      start_date INTEGER, end_date INTEGER
    );
    CREATE TABLE calendar_dates(
      service_id TEXT,
      date INTEGER,
      exception_type INTEGER
    );
    CREATE INDEX idx_stop_times_stop ON stop_times(stop_id);
    CREATE INDEX idx_stop_times_seq ON stop_times(stop_id, stop_sequence);
    """)
    con.commit()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        def load_csv(filename: str) -> List[Dict[str,str]]:
            with zf.open(filename) as f:
                text = io.TextIOWrapper(f, encoding="utf-8-sig")
                return list(csv.DictReader(text))

        # Required
        for tbl, fname in [
            ("stops","stops.txt"),
            ("routes","routes.txt"),
            ("trips","trips.txt"),
            ("stop_times","stop_times.txt"),
        ]:
            if fname not in zf.namelist():
                raise ValueError(f"Hiányzó fájl a GTFS-ben: {fname}")

        # Insert data
        rows = load_csv("stops.txt")
        cur.executemany("INSERT INTO stops(stop_id,stop_name,stop_code,stop_lat,stop_lon) VALUES (?,?,?,?,?)",
            [(r.get("stop_id"), r.get("stop_name"), r.get("stop_code"), r.get("stop_lat"), r.get("stop_lon")) for r in rows])

        rows = load_csv("routes.txt")
        cur.executemany("INSERT INTO routes(route_id,route_short_name,route_long_name) VALUES (?,?,?)",
            [(r.get("route_id"), r.get("route_short_name"), r.get("route_long_name")) for r in rows])

        rows = load_csv("trips.txt")
        cur.executemany("INSERT INTO trips(trip_id,route_id,service_id,trip_headsign) VALUES (?,?,?,?)",
            [(r.get("trip_id"), r.get("route_id"), r.get("service_id"), r.get("trip_headsign")) for r in rows])

        rows = load_csv("stop_times.txt")
        cur.executemany("INSERT INTO stop_times(trip_id,arrival_time,departure_time,stop_id,stop_sequence) VALUES (?,?,?,?,?)",
            [(r.get("trip_id"), r.get("arrival_time"), r.get("departure_time"), r.get("stop_id"), int(r.get("stop_sequence", "0") or 0)) for r in rows])

        if "calendar.txt" in zf.namelist():
            rows = load_csv("calendar.txt")
            cur.executemany("""
              INSERT INTO calendar(service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date)
              VALUES (?,?,?,?,?,?,?,?,?,?)
            """, [(r.get("service_id"),
                   int(r.get("monday",0) or 0), int(r.get("tuesday",0) or 0), int(r.get("wednesday",0) or 0),
                   int(r.get("thursday",0) or 0), int(r.get("friday",0) or 0), int(r.get("saturday",0) or 0),
                   int(r.get("sunday",0) or 0), int(r.get("start_date",0) or 0), int(r.get("end_date",0) or 0))
                  for r in rows])

        if "calendar_dates.txt" in zf.namelist():
            rows = load_csv("calendar_dates.txt")
            cur.executemany("INSERT INTO calendar_dates(service_id,date,exception_type) VALUES (?,?,?)",
                            [(r.get("service_id"), int(r.get("date",0) or 0), int(r.get("exception_type",0) or 0)) for r in rows])

    con.commit()
    con.close()

# ----- Schemas -----
class LoadGtfsRequest(BaseModel):
    url: str

# ----- Routes -----

@app.get("/api/status")
def api_status():
    return {
        "status": "ok",
        "gtfs_loaded": gtfs_loaded(),
        "siri_configured": siri_configured(),
    }

# Serve static index
STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/", response_class=HTMLResponse)
def root():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return index.read_text(encoding="utf-8")
    return "<h1>Bluestar Bus API</h1><p>UI még nincs feltöltve (static/index.html).</p>"

# ---- GTFS UPLOAD (multipart) ----
@app.post("/api/upload")
async def upload_gtfs(file: UploadFile = File(...)):
    content = await file.read()
    try:
        load_gtfs_from_bytes(content)
    except Exception as e:
        return JSONResponse(status_code=400, content={"status":"error","message":str(e)})
    return {"status":"ok","method":"upload","message":"GTFS betöltve az adatbázisba."}

# ---- GTFS LOAD (admin: URL vagy multipart) ----
@app.post("/api/admin/load_gtfs")
async def admin_load_gtfs(
    request: Request,
    file: Optional[UploadFile] = File(None)
):
    # multipart with file
    if file is not None:
        content = await file.read()
        try:
            load_gtfs_from_bytes(content)
            return {"status":"ok","method":"upload","message":"GTFS betöltve az adatbázisba."}
        except Exception as e:
            return JSONResponse(status_code=400, content={"status":"error","message":str(e)})

    # JSON body with url
    try:
        body = await request.json()
        url = body.get("url")
    except Exception:
        url = None

    if url:
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                r = await client.get(url)
                r.raise_for_status()
            load_gtfs_from_bytes(r.content)
            return {"status":"ok","method":"url","message":"GTFS letöltve és betöltve."}
        except Exception as e:
            return JSONResponse(status_code=400, content={"status":"error","message":str(e)})

    return JSONResponse(status_code=400, content={"detail":"Adj meg egy 'url'-t JSON-ban VAGY tölts fel egy 'file' ZIP-et."})

# ---- STOP SEARCH ----
@app.get("/api/stops/search")
def search_stops(q: str = Query(..., min_length=2)):
    if not gtfs_loaded():
        return []
    con = db_conn()
    cur = con.cursor()
    like = f"%{q.lower()}%"
    rows = cur.execute("""
      SELECT stop_id, stop_name FROM stops
      WHERE lower(stop_name) LIKE ?
      ORDER BY stop_name LIMIT 50
    """, (like,)).fetchall()
    con.close()
    return [{"stop_id": r["stop_id"], "stop_name": r["stop_name"]} for r in rows]

# ---- NEXT DEPARTURES ----
@app.get("/api/stops/{stop_id}/next_departures")
def next_departures(
    stop_id: str = FPath(...),
    minutes: int = Query(60, ge=5, le=240)
):
    if not gtfs_loaded():
        return {"stop_id": stop_id, "minutes": minutes, "results": []}

    now = dt.datetime.now()
    today = now.date()
    now_sec = now.hour*3600 + now.minute*60 + now.second
    until_sec = now_sec + minutes*60

    con = db_conn()
    cur = con.cursor()
    service_ids = today_service_ids(cur, today)
    if not service_ids:
        con.close()
        return {"stop_id": stop_id, "minutes": minutes, "results": []}

    placeholders = ",".join(["?"]*len(service_ids))

    # pick all stop_times for this stop, join to trips/routes
    rows = cur.execute(f"""
      SELECT st.trip_id, st.departure_time, t.trip_headsign, t.route_id, r.route_short_name
      FROM stop_times st
      JOIN trips t ON t.trip_id = st.trip_id
      JOIN routes r ON r.route_id = t.route_id
      WHERE st.stop_id = ?
        AND t.service_id IN ({placeholders})
      ORDER BY st.departure_time
    """, (stop_id, *service_ids)).fetchall()

    results = []
    for r in rows:
        try:
            sec = parse_time_hhmmss(r["departure_time"])
        except Exception:
            continue
        # handle after-midnight times (>= 24:00:00)
        # We allow times up to +6 hours next day
        abs_sec = sec
        if abs_sec < now_sec:
            # today already passed; if within window crossing midnight, also consider next day's early trips
            if sec <= 6*3600:
                abs_sec += 24*3600
            else:
                continue

        if now_sec <= abs_sec <= until_sec:
            when = (now.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(seconds=sec))
            if sec >= 24*3600:
                when = when + dt.timedelta(days=1)
            results.append({
                "route": r["route_short_name"] or "",
                "destination": r["trip_headsign"] or "",
                "time_iso": when.isoformat(timespec="minutes"),
                "is_live": False
            })

    con.close()
    return {"stop_id": stop_id, "minutes": minutes, "results": results[:50]}

# ---- LIVE (SIRI) ----
@app.get("/api/live/{stop_id}")
async def live_for_stop(stop_id: str = FPath(...)):
    if not siri_configured():
        return {"stop_id": stop_id, "results": []}

    # cache
    now = dt.datetime.now().timestamp()
    cache_key = "vm"
    cached = _siri_cache.get(cache_key)
    if cached and now - cached[0] < SIRI_TTL_SEC:
        xml_root = cached[1]
    else:
        try:
            params = {
                "api_key": BODS_API_KEY,
                "format": "SIRI",
                "operatorRef": BODS_PRODUCER or None,
                "feedId": BODS_FEED_ID
            }
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(f"{BODS_BASE_URL}/datafeed/{BODS_FEED_ID}", params=params)
                resp.raise_for_status()
            import xml.etree.ElementTree as ET
            xml_root = ET.fromstring(resp.text)
            _siri_cache[cache_key] = (now, xml_root)
        except Exception:
            return {"stop_id": stop_id, "results": []}

    # parse minimal SIRI-VM → id, aimed/expected time, line, dest, stopRef
    import xml.etree.ElementTree as ET
    ns = {"s": "http://www.siri.org.uk/siri"}

    results: List[Dict[str, str]] = []
    for mvj in xml_root.findall(".//s:MonitoredVehicleJourney", ns):
        stop_ref = (mvj.findtext("s:MonitoredCall/s:StopPointRef", default="", namespaces=ns) or "").strip()
        if stop_ref != stop_id:
            continue
        line = (mvj.findtext("s:LineRef", default="", namespaces=ns) or "").strip()
        dest = (mvj.findtext("s:DestinationName", default="", namespaces=ns) or "").strip()
        t_expected = (mvj.findtext("s:MonitoredCall/s:ExpectedDepartureTime", default="", namespaces=ns)
                      or mvj.findtext("s:MonitoredCall/s:AimedDepartureTime", default="", namespaces=ns) or "").strip()
        if not t_expected:
            continue
        results.append({
            "route": line,
            "destination": dest,
            "time_iso": t_expected[:16],  # YYYY-MM-DDTHH:MM
            "is_live": True
        })

    # sort & cap
    results.sort(key=lambda x: x["time_iso"])
    return {"stop_id": stop_id, "results": results[:50]}

# ----- run local -----
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
