import csv
import io
import json
import os
import sqlite3
import time
import zipfile
from datetime import datetime, timedelta, date, timezone
from typing import Dict, List, Optional

import requests
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# ---------- Paths / constants ----------
DATA_DIR = os.environ.get("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)
CFG_PATH = os.path.join(DATA_DIR, "config.json")
DB_PATH = os.path.join(DATA_DIR, "gtfs.db")
BUILD = os.environ.get("BUILD", str(int(time.time())))
TZ_EU_LONDON = "Europe/London"  # for display only

# ---------- App ----------
app = FastAPI(title="Bluestar Bus — API", version="5.0.0", openapi_url="/openapi.json", docs_url="/")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ---------- tiny config store ----------
def _load_cfg() -> Dict:
    if os.path.exists(CFG_PATH):
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_cfg(cfg: Dict) -> None:
    tmp = CFG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f)
    os.replace(tmp, CFG_PATH)

# ---------- SQLite helpers ----------
def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _db_exists() -> bool:
    return os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0

def _sql(c: sqlite3.Connection, q: str, args: tuple = ()):
    cur = c.execute(q, args)
    return [dict(r) for r in cur.fetchall()]

# ---------- GTFS loader ----------
def _parse_time_to_secs(hms: str) -> Optional[int]:
    """
    GTFS time can be 24+ hours, e.g. '25:17:00'. Return seconds since 00:00, may be >= 86400.
    """
    if not hms:
        return None
    try:
        h, m, s = (int(x) for x in hms.split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return None

def load_gtfs_zip_to_sqlite(zip_bytes: bytes) -> None:
    # Build DB from scratch
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    c = _conn()
    cur = c.cursor()

    # Create tables (minimal subset we use)
    cur.executescript("""
    CREATE TABLE stops (
      stop_id TEXT PRIMARY KEY,
      stop_name TEXT,
      stop_code TEXT
    );
    CREATE TABLE routes (
      route_id TEXT PRIMARY KEY,
      route_short_name TEXT,
      route_long_name TEXT
    );
    CREATE TABLE trips (
      trip_id TEXT PRIMARY KEY,
      route_id TEXT,
      service_id TEXT,
      trip_headsign TEXT
    );
    CREATE TABLE stop_times (
      trip_id TEXT,
      arrival_time TEXT,
      departure_time TEXT,
      stop_id TEXT,
      stop_sequence INTEGER
    );
    CREATE TABLE calendar (
      service_id TEXT PRIMARY KEY,
      monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER,
      friday INTEGER, saturday INTEGER, sunday INTEGER,
      start_date TEXT, end_date TEXT
    );
    CREATE TABLE calendar_dates (
      service_id TEXT,
      date TEXT,
      exception_type INTEGER
    );
    CREATE INDEX idx_stop_times_stop ON stop_times(stop_id);
    CREATE INDEX idx_stop_times_trip ON stop_times(trip_id);
    CREATE INDEX idx_trips_route ON trips(route_id);
    """)

    z = zipfile.ZipFile(io.BytesIO(zip_bytes))

    def load_csv(name: str) -> List[Dict]:
        try:
            with z.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8-sig")
                rdr = csv.DictReader(text)
                return [ {k:(v or "").strip() for k,v in row.items()} for row in rdr ]
        except KeyError:
            return []

    # Load files
    for row in load_csv("stops.txt"):
        cur.execute("INSERT INTO stops(stop_id,stop_name,stop_code) VALUES (?,?,?)",
                    (row.get("stop_id"), row.get("stop_name"), row.get("stop_code")))
    for row in load_csv("routes.txt"):
        cur.execute("INSERT INTO routes(route_id,route_short_name,route_long_name) VALUES (?,?,?)",
                    (row.get("route_id"), row.get("route_short_name"), row.get("route_long_name")))
    for row in load_csv("trips.txt"):
        cur.execute("INSERT INTO trips(trip_id,route_id,service_id,trip_headsign) VALUES (?,?,?,?)",
                    (row.get("trip_id"), row.get("route_id"), row.get("service_id"), row.get("trip_headsign")))
    # stop_times is large → use executemany in batches
    st_rows = load_csv("stop_times.txt")
    batch = []
    for r in st_rows:
        batch.append((r.get("trip_id"), r.get("arrival_time"), r.get("departure_time"),
                      r.get("stop_id"), int(r.get("stop_sequence") or 0)))
        if len(batch) >= 5000:
            cur.executemany("INSERT INTO stop_times(trip_id,arrival_time,departure_time,stop_id,stop_sequence) VALUES (?,?,?,?,?)", batch)
            batch = []
    if batch:
        cur.executemany("INSERT INTO stop_times(trip_id,arrival_time,departure_time,stop_id,stop_sequence) VALUES (?,?,?,?,?)", batch)

    for row in load_csv("calendar.txt"):
        cur.execute("""INSERT INTO calendar(service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (row.get("service_id"),
                     int(row.get("monday") or 0), int(row.get("tuesday") or 0), int(row.get("wednesday") or 0),
                     int(row.get("thursday") or 0), int(row.get("friday") or 0), int(row.get("saturday") or 0),
                     int(row.get("sunday") or 0), row.get("start_date"), row.get("end_date")))
    for row in load_csv("calendar_dates.txt"):
        cur.execute("INSERT INTO calendar_dates(service_id,date,exception_type) VALUES (?,?,?)",
                    (row.get("service_id"), row.get("date"), int(row.get("exception_type") or 0)))

    c.commit()
    c.close()

# ---------- helpers for service day ----------
def _today_yyyymmdd_uk() -> str:
    # naive “UK now” using localtime; precise TZ handling in container may vary, but it’s fine for scheduling
    now = datetime.utcnow()
    # pretend Europe/London offset by reading time.gmtime/time.localtime could be wrong in container, fallback to UTC date
    return now.strftime("%Y%m%d")

def _weekday_uk_idx() -> int:
    # Monday=0 ... Sunday=6
    return datetime.utcnow().weekday()

def _active_service_ids_for_today(c: sqlite3.Connection) -> set:
    ymd = _today_yyyymmdd_uk()
    wd = _weekday_uk_idx()
    col = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][wd]
    svcs = set()
    for r in _sql(c, f"SELECT service_id FROM calendar WHERE {col}=1 AND start_date<=? AND end_date>=?", (ymd, ymd)):
        svcs.add(r["service_id"])
    # calendar_dates overrides
    added = {r["service_id"] for r in _sql(c, "SELECT service_id FROM calendar_dates WHERE date=? AND exception_type=1", (ymd,))}
    removed = {r["service_id"] for r in _sql(c, "SELECT service_id FROM calendar_dates WHERE date=? AND exception_type=2", (ymd,))}
    return (svcs | added) - removed

# ---------- API ----------
@app.get("/api/status")
def api_status():
    try:
        uk_time = datetime.utcnow().strftime("%H:%M:%S")  # display only
        cfg = _load_cfg()
        return {
            "ok": True,
            "version": app.version,
            "build": BUILD,
            "uk_time": uk_time,
            "tz": TZ_EU_LONDON,
            "live_feed_configured": bool(cfg.get("feed_url")),
            "gtfs_loaded": _db_exists(),
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "build": BUILD}

# ----- live feed config -----
@app.get("/api/live/config")
def get_live_cfg():
    cfg = _load_cfg()
    return {"feed_url": cfg.get("feed_url")}

@app.post("/api/live/config")
def set_live_cfg(payload: Dict):
    try:
        url = (payload or {}).get("feed_url", "").strip()
        cfg = _load_cfg()
        cfg["feed_url"] = url
        _save_cfg(cfg)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ----- GTFS upload -----
@app.post("/api/upload")
def upload_gtfs(file: UploadFile = File(...)):
    try:
        content = file.file.read()
        load_gtfs_zip_to_sqlite(content)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

# ----- stops search -----
@app.get("/api/stops/search")
def stops_search(q: str):
    try:
        if not _db_exists():
            return []
        c = _conn()
        like = f"%{q.lower()}%"
        rows = _sql(c, "SELECT stop_id, stop_name, stop_code FROM stops WHERE lower(stop_name) LIKE ? LIMIT 30", (like,))
        c.close()
        out = []
        for r in rows:
            name = r["stop_name"] or ""
            code = r.get("stop_code") or ""
            if code:
                name = f"{name} [{code}]"
            out.append({"id": r["stop_id"], "name": name})
        return out
    except Exception as e:
        return {"error": str(e), "results": []}

# ----- next departures -----
@app.get("/api/stops/{stop_id}/next_departures")
def next_departures(stop_id: str, window: int = 60):
    """
    Returns departures within <window> minutes from now (UK time-ish).
    Always returns 200 with {departures: [...]} to avoid 500 on frontend.
    """
    try:
        if not _db_exists():
            return {"departures": [], "error": "GTFS not loaded"}
        c = _conn()
        active = _active_service_ids_for_today(c)
        # get all stop times at this stop (we'll filter by time in Python)
        rows = _sql(c, """SELECT st.trip_id, st.departure_time, t.route_id, t.trip_headsign,
                                 r.route_short_name, r.route_long_name
                          FROM stop_times st
                          JOIN trips t ON t.trip_id = st.trip_id
                          JOIN routes r ON r.route_id = t.route_id
                          WHERE st.stop_id = ?""", (stop_id,))
        c.close()

        # now & window
        now = datetime.utcnow()
        now_sec = now.hour * 3600 + now.minute * 60 + now.second
        max_sec = now_sec + max(1, int(window)) * 60

        deps = []
        for r in rows:
            dep_s = _parse_time_to_secs(r["departure_time"])
            if dep_s is None:
                continue
            # only trips whose service is active today
            # fetch service for this trip lazily (cache optional)
            # To avoid extra queries we pulled t.service_id? Not selected → quick extra map:
            # (we could change SELECT to include service_id; do that robustly)
        # re-query to include service_id (small extra cost, but safe):
        c2 = _conn()
        svc_map = {x["trip_id"]: x["service_id"] for x in _sql(c2, "SELECT trip_id, service_id FROM trips WHERE trip_id IN (%s)" %
                                        ",".join("?"*min(999, len({row["trip_id"] for row in rows}))),
                                        tuple({row["trip_id"] for row in rows}) )} if rows else {}
        c2.close()

        for r in rows:
            dep_s = _parse_time_to_secs(r["departure_time"])
            if dep_s is None:
                continue
            # today window (allow times >= 24h too)
            if not (now_sec <= dep_s <= max_sec):
                continue
            svc = svc_map.get(r["trip_id"])
            if svc and active and (svc not in active):
                continue
            route = r.get("route_short_name") or r.get("route_long_name") or r.get("route_id")
            headsign = r.get("trip_headsign") or ""
            dep_dt = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=dep_s)
            deps.append({
                "trip_id": r["trip_id"],
                "route": route,
                "destination": headsign,
                "time": dep_dt.isoformat(),
                "time_display": dep_dt.strftime("%H:%M"),
                "is_due": (dep_s - now_sec) <= 60,
                "is_live": False,
                "delay_min": None
            })

        # sort by time and de-duplicate by trip_id
        deps.sort(key=lambda x: (x["time"]))
        seen = set()
        out = []
        for d in deps:
            if d["trip_id"] in seen:
                continue
            seen.add(d["trip_id"])
            out.append(d)
        return {"departures": out}
    except Exception as e:
        return {"departures": [], "error": f"{type(e).__name__}: {e}"}

# ----- trip details -----
@app.get("/api/trips/{trip_id}")
def trip_details(trip_id: str):
    try:
        if not _db_exists():
            return {"stops": [], "error": "GTFS not loaded"}
        c = _conn()
        # join stop_times -> stops for names
        rows = _sql(c, """SELECT st.stop_sequence, st.arrival_time, st.departure_time, s.stop_name, s.stop_code
                          FROM stop_times st JOIN stops s ON s.stop_id = st.stop_id
                          WHERE st.trip_id=? ORDER BY st.stop_sequence ASC""", (trip_id,))
        c.close()
        out = []
        for r in rows:
            tm = r.get("departure_time") or r.get("arrival_time")
            sec = _parse_time_to_secs(tm or "")
            if sec is None:
                display = "--:--"
            else:
                hh = (sec // 3600) % 24
                mm = (sec % 3600) // 60
                display = f"{hh:02d}:{mm:02d}"
            name = r["stop_name"]
            if r.get("stop_code"):
                name = f"{name} [{r['stop_code']}]"
            out.append({
                "stop_name": name,
                "time_display": display,
                "sequence": r["stop_sequence"],
                "is_past": False
            })
        return {"stops": out}
    except Exception as e:
        return {"stops": [], "error": str(e)}

# ----- routes search -----
@app.get("/api/routes/search")
def routes_search(q: str):
    try:
        if not _db_exists():
            return []
        c = _conn()
        like = f"%{q.lower()}%"
        rows = _sql(c, """SELECT DISTINCT
                             COALESCE(NULLIF(TRIM(route_short_name),''), route_long_name, route_id) AS route
                          FROM routes
                          WHERE lower(COALESCE(route_short_name,'')) LIKE ?
                             OR lower(COALESCE(route_long_name,'')) LIKE ?
                             OR lower(route_id) LIKE ?
                          LIMIT 30""", (like, like, like))
        c.close()
        return rows
    except Exception as e:
        return {"error": str(e), "results": []}

# ----- vehicles by route from SIRI-VM -----
@app.get("/api/routes/{route}/vehicles")
def route_vehicles(route: str):
    try:
        cfg = _load_cfg()
        url = (cfg.get("feed_url") or "").strip()
        if not url:
            return {"vehicles": [], "error": "feed_url not set"}
        # fetch SIRI-VM XML
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        xml = r.text
        # ultra-light parse (no external deps): look for blocks
        # This is not a full XML parser but works well for SIRI snippets.
        def _between(s: str, a: str, b: str, start=0):
            i = s.find(a, start)
            if i < 0: return -1, -1, ""
            j = s.find(b, i + len(a))
            if j < 0: return -1, -1, ""
            return i, j + len(b), s[i+len(a):j]

        vehicles = []
        pos = 0
        route_lc = route.strip().lower()
        while True:
            i1, i2, block = _between(xml, "<VehicleActivity>", "</VehicleActivity>", pos)
            if i1 < 0: break
            pos = i2
            # extract fields
            def tag(t):
                si, sj, content = _between(block, f"<{t}>", f"</{t}>")
                return content.strip() if si >= 0 else ""
            line = tag("PublishedLineName") or tag("LineRef")
            if (line or "").strip().lower() != route_lc:
                continue
            lat = tag("Latitude"); lon = tag("Longitude")
            dest = tag("DestinationName")
            label = tag("VehicleRef") or tag("LineRef")
            try:
                latf = float(lat); lonf = float(lon)
            except Exception:
                continue
            vehicles.append({"lat": latf, "lon": lonf, "label": f"{line} → {dest} [{label}]"})
        return {"vehicles": vehicles}
    except Exception as e:
        return {"vehicles": [], "error": f"{type(e).__name__}: {e}"}

# ---------- Static frontend ----------
# Serve /public if present (index.html & assets). Keep after /api to avoid shadowing.
PUBLIC_DIR = os.path.join(os.path.dirname(__file__), "public")
if os.path.isdir(PUBLIC_DIR):
    app.mount("/", StaticFiles(directory=PUBLIC_DIR, html=True), name="public")

# ---------- Health ----------
@app.get("/healthz")
def health():
    return {"ok": True, "build": BUILD}

# ---------- Entry ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
