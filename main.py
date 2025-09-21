"""
Simple bus timetable and live vehicle API using FastAPI.

This application serves as a drop‑in replacement for the original
Bluestar bus API. It exposes JSON endpoints for searching stops and routes,
returning upcoming departures for a stop, retrieving a single trip's
stop list, reporting on live vehicle positions, managing GTFS uploads
and downloads, and configuring a BODS SIRI‑VM live feed.  A small
web frontend is served from the ``static`` directory.

The focus of this implementation is clarity and robustness.  It
validates incoming data, handles missing GTFS tables gracefully and
performs minimal calendar/date logic to decide when a trip is running.
Live data is fetched lazily and cached for a short period to avoid
excess network usage.  All timestamps are normalised to UTC and
presented in the configured time zone.

To run the application locally:

    pip install -r requirements.txt
    uvicorn main:app --reload --port 8000

The default time zone is Europe/London.  This can be overridden by
setting the ``TZ`` environment variable.  See the accompanying
``index.html`` for a basic user interface.
"""

import io
import json
import os
import time
import zipfile
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any

import pandas as pd
import pytz
import httpx
import xmltodict
from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration and state

APP_VERSION = "1.0.0"

# Directories for storing GTFS and uploaded files.  These are created
# relative to the current working directory.  You can mount a volume on
# ``data`` to persist uploads across restarts.
DATA_DIR = os.path.abspath("data")
GTFS_DIR = os.path.join(DATA_DIR, "gtfs")
GTFS_ZIP = os.path.join(DATA_DIR, "gtfs.zip")

# Create directories if they don't exist.
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(GTFS_DIR, exist_ok=True)
os.makedirs("static", exist_ok=True)

# Time zone configuration.  Defaults to Europe/London.  You can override
# this by exporting ``TZ`` in your environment.
TZ_NAME = os.environ.get("TZ", "Europe/London")
try:
    TZ = pytz.timezone(TZ_NAME)
except Exception:
    TZ = pytz.timezone("Europe/London")

# Live feed refresh interval default (seconds).
DEFAULT_REFRESH_SEC = 30


class LiveConfig(BaseModel):
    """Configuration for the live SIRI‑VM feed."""

    feed_url: Optional[str] = None  # BODS SIRI‑VM URL
    refresh_sec: int = DEFAULT_REFRESH_SEC


class AppState:
    """Container for all mutable application state.

    This object is mutated from within request handlers and helper
    functions.  It keeps the loaded GTFS tables, summary metadata,
    previously fetched live vehicle data and the current live feed
    configuration.  All timestamps in this state are expressed as
    seconds since the epoch (UTC).
    """

    def __init__(self):
        self.build: int = int(time.time())
        self.live_cfg: LiveConfig = LiveConfig()
        self.gtfs_ready: bool = False
        self.gtfs_meta: Dict[str, Any] = {}
        self.gtfs_tables: Dict[str, pd.DataFrame] = {}
        self.vehicles: List[Dict[str, Any]] = []
        self.vehicles_ts: float = 0.0  # epoch seconds of last fetch


# Instantiate global state
STATE = AppState()

# ---------------------------------------------------------------------------
# Utility functions

def now_utc() -> datetime:
    """Return current time in UTC with tzinfo attached."""
    return datetime.utcnow().replace(tzinfo=pytz.utc)

def now_local() -> datetime:
    """Return current time in the configured time zone."""
    return now_utc().astimezone(TZ)

def parse_hhmmss_to_today(hhmmss: str, local_day: date) -> datetime:
    """Convert an HH:MM:SS string to a datetime on the given day in TZ.

    If the hour component is >=24, it is rolled over into the next day.
    This handles GTFS times that extend past midnight.
    """
    parts = hhmmss.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid HH:MM:SS value: {hhmmss}")
    h, m, s = map(int, parts)
    day_offset = h // 24
    h = h % 24
    dt = datetime(local_day.year, local_day.month, local_day.day, h, m, s, tzinfo=TZ)
    dt += timedelta(days=day_offset)
    return dt

def service_is_running(trip_row: pd.Series, dt_local: datetime) -> bool:
    """Determine if a trip is operating on the given day.

    This implements GTFS calendar.txt and calendar_dates.txt rules.  If
    calendar.txt is present, the service must be active on the
    corresponding weekday between start_date and end_date.  calendar_dates
    modifies the service by explicitly adding or removing dates.
    """
    service_id = trip_row.get("service_id")
    day = dt_local.date()

    cal = STATE.gtfs_tables.get("calendar")
    ok_calendar = True
    if cal is not None and not cal.empty:
        c = cal[cal["service_id"] == service_id]
        ok_calendar = False
        if not c.empty:
            c = c.iloc[0]
            start = pd.to_datetime(c["start_date"], format="%Y%m%d").date()
            end = pd.to_datetime(c["end_date"], format="%Y%m%d").date()
            if start <= day <= end:
                weekday = [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ][day.weekday()]
                ok_calendar = bool(int(c[weekday]))

    dates = STATE.gtfs_tables.get("calendar_dates")
    if dates is not None and not dates.empty:
        d = dates[dates["service_id"] == service_id]
        if not d.empty:
            d = d.copy()
            d["date"] = pd.to_datetime(d["date"], format="%Y%m%d").dt.date
            same = d[d["date"] == day]
            if not same.empty:
                ex = int(same.iloc[0]["exception_type"])
                return ex == 1
    return ok_calendar

async def fetch_live_if_needed() -> None:
    """Fetch SIRI‑VM live vehicle data if the cache is stale.

    Live data is only downloaded if a feed URL is configured and the
    previous fetch is older than ``refresh_sec`` seconds.  Any
    exceptions during download or parsing are silently ignored and
    the previous cache is retained.
    """
    cfg = STATE.live_cfg
    if not cfg.feed_url:
        return
    now_ts = time.time()
    if now_ts - STATE.vehicles_ts < cfg.refresh_sec:
        return
    # download
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(cfg.feed_url)
            resp.raise_for_status()
            text = resp.text
    except Exception:
        return
    # parse XML
    try:
        payload = xmltodict.parse(text)
    except Exception:
        return
    vehicles: List[Dict[str, Any]] = []
    try:
        deliveries = (
            payload.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("VehicleMonitoringDelivery", [])
        )
        # deliveries can be a list or dict; normalise to list
        if isinstance(deliveries, dict):
            deliveries = [deliveries]
        for delivery in deliveries:
            acts = delivery.get("VehicleActivity", [])
            if isinstance(acts, dict):
                acts = [acts]
            for act in acts:
                mvj = act.get("MonitoredVehicleJourney", {}) or {}
                when_str = act.get("RecordedAtTime") or act.get("ValidUntilTime")
                try:
                    ts = datetime.fromisoformat(when_str.replace("Z", "+00:00")).astimezone(pytz.utc)
                except Exception:
                    ts = now_utc()
                mc = mvj.get("MonitoredCall") or {}
                onward = (mvj.get("OnwardCalls") or {}).get("OnwardCall") or []
                exp_dep = (
                    mc.get("ExpectedDepartureTime")
                    or (onward[0].get("ExpectedDepartureTime") if onward else None)
                )
                aimed_dep = (
                    mc.get("AimedDepartureTime")
                    or (onward[0].get("AimedDepartureTime") if onward else None)
                )
                def _p(s: Optional[str]) -> Optional[datetime]:
                    if not s:
                        return None
                    try:
                        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(pytz.utc)
                    except Exception:
                        return None
                vehicles.append({
                    "line": str(mvj.get("PublishedLineName") or mvj.get("LineRef") or "").strip(),
                    "destination": (mvj.get("DestinationName") or "").strip(),
                    "vehicle_ref": str(mvj.get("VehicleRef") or mvj.get("VehicleId") or "").strip(),
                    "lat": (mvj.get("VehicleLocation") or {}).get("Latitude"),
                    "lon": (mvj.get("VehicleLocation") or {}).get("Longitude"),
                    "bearing": mvj.get("Bearing"),
                    "timestamp_utc": ts.isoformat(),
                    "aimed_dep_utc": _p(aimed_dep).isoformat() if _p(aimed_dep) else None,
                    "expected_dep_utc": _p(exp_dep).isoformat() if _p(exp_dep) else None,
                })
    except Exception:
        pass
    # compute delay minutes if both aimed and expected are present
    for v in vehicles:
        try:
            a = v.get("aimed_dep_utc")
            e = v.get("expected_dep_utc")
            if a and e:
                a_dt = datetime.fromisoformat(a)
                e_dt = datetime.fromisoformat(e)
                v["delay_min"] = int(round((e_dt - a_dt).total_seconds() / 60.0))
            else:
                v["delay_min"] = None
        except Exception:
            v["delay_min"] = None
    STATE.vehicles = vehicles
    STATE.vehicles_ts = now_ts

def load_gtfs_from_dir(gtfs_dir: str) -> None:
    """Load GTFS tables from a directory into dataframes and update state.

    Required tables are ``stops``, ``routes``, ``trips`` and
    ``stop_times``.  Optional tables include ``shapes``, ``calendar`` and
    ``calendar_dates``.  Missing required tables raise a runtime
    error.
    """
    required = ["stops", "routes", "trips", "stop_times"]
    tables: Dict[str, pd.DataFrame] = {}
    for name in required + ["shapes", "calendar", "calendar_dates"]:
        path = os.path.join(gtfs_dir, f"{name}.txt")
        if os.path.exists(path):
            df = pd.read_csv(path, dtype=str)
            tables[name] = df
        else:
            tables[name] = pd.DataFrame()
    missing = [t for t in required if tables[t].empty]
    if missing:
        raise RuntimeError(f"Missing GTFS tables: {', '.join(missing)}")
    STATE.gtfs_tables.clear()
    STATE.gtfs_tables.update(tables)
    STATE.gtfs_ready = True
    STATE.gtfs_meta = {
        "gtfs_stops": len(tables["stops"]),
        "gtfs_routes": len(tables["routes"]),
        "tz": TZ_NAME,
    }

def extract_and_load_gtfs(zip_bytes: bytes) -> None:
    """Extract a GTFS zip file into ``GTFS_DIR`` and load the tables."""
    # purge existing contents
    for fname in os.listdir(GTFS_DIR):
        try:
            os.remove(os.path.join(GTFS_DIR, fname))
        except Exception:
            pass
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        zf.extractall(GTFS_DIR)
    load_gtfs_from_dir(GTFS_DIR)

def ensure_gtfs_loaded() -> None:
    """Ensure that GTFS tables are loaded, else raise a 503 error."""
    if not STATE.gtfs_ready:
        raise HTTPException(status_code=503, detail="GTFS data not loaded")

def build_live_by_route() -> Dict[str, Dict[str, Any]]:
    """Return a mapping of route -> freshest live record.

    Only records newer than 90 seconds and with a line name are
    considered.  Per route, the record with the latest timestamp is
    chosen.  Returned values include expected departure, aimed
    departure, delay, destination and timestamp.
    """
    best: Dict[str, Dict[str, Any]] = {}
    now = now_utc()
    for v in STATE.vehicles:
        route = (v.get("line") or "").strip()
        if not route:
            continue
        try:
            ts = datetime.fromisoformat(v["timestamp_utc"])
        except Exception:
            continue
        if (now - ts).total_seconds() > 90:
            continue
        cur = best.get(route)
        if (not cur) or (ts > datetime.fromisoformat(cur["ts"])):
            best[route] = {
                "ts": ts.isoformat(),
                "expected": v.get("expected_dep_utc"),
                "aimed": v.get("aimed_dep_utc"),
                "delay_min": v.get("delay_min"),
                "destination": v.get("destination"),
            }
    return best

def departure_rows_for_stop(stop_id: str, window_min: int) -> List[Dict[str, Any]]:
    """Compute upcoming departures for a stop.

    This function assumes GTFS tables are loaded.  It searches all
    trips that stop at the given ``stop_id`` and collects departures
    occurring within the next ``window_min`` minutes.  Live data is
    merged when available to adjust departure times and delays.
    """
    trips_df = STATE.gtfs_tables.get("trips")
    stop_times_df = STATE.gtfs_tables.get("stop_times")
    routes_df = STATE.gtfs_tables.get("routes")
    if trips_df is None or stop_times_df is None:
        return []
    now_local_dt = now_local()
    end_local_dt = now_local_dt + timedelta(minutes=max(1, min(window_min, 480)))
    # Build lookup for route_id -> display name
    route_names: Dict[str, str] = {}
    if routes_df is not None and not routes_df.empty:
        for _, row in routes_df.iterrows():
            rid = row.get("route_id") or ""
            short = (row.get("route_short_name") or "").strip()
            longn = (row.get("route_long_name") or "").strip()
            route_names[rid] = short or longn or rid
    # Precompute live by route
    live = build_live_by_route()
    rows: List[Dict[str, Any]] = []
    # Group stop_times by stop_id
    grouped = stop_times_df[stop_times_df["stop_id"] == stop_id]
    for _, st_row in grouped.iterrows():
        dep_str = st_row.get("departure_time") or st_row.get("arrival_time") or ""
        if not dep_str:
            continue
        try:
            dep_local = parse_hhmmss_to_today(dep_str, now_local_dt.date())
        except Exception:
            continue
        dep_utc = dep_local.astimezone(pytz.utc)
        # skip if departure already more than a minute in the past
        if dep_local < now_local_dt - timedelta(minutes=1):
            continue
        # skip if departure after window
        if dep_local > end_local_dt:
            continue
        trip_id = st_row.get("trip_id")
        if trip_id is None:
            continue
        trip_row = trips_df[trips_df["trip_id"] == trip_id]
        if trip_row.empty:
            continue
        trip_meta = trip_row.iloc[0]
        # check if service is running today
        if not service_is_running(trip_meta, now_local_dt):
            continue
        route_id = trip_meta.get("route_id") or ""
        route = route_names.get(route_id, route_id)
        headsign = (trip_meta.get("trip_headsign") or "").strip()
        # use live if available
        li = live.get(route)
        dep_use = dep_utc
        is_live = False
        delay_min = None
        if li and li.get("expected"):
            try:
                exp = datetime.fromisoformat(li["expected"])
                # if difference within 2 hours, override
                if abs((exp - dep_utc).total_seconds()) < 2 * 3600:
                    dep_use = exp
                    is_live = True
                    delay_min = li.get("delay_min")
            except Exception:
                pass
        mins = int((dep_use - now_utc()).total_seconds() // 60)
        is_due = is_live and (-1 <= mins <= 0)
        rows.append({
            "route": route or "–",
            "destination": headsign or (li or {}).get("destination") or "–",
            "time_iso": dep_use.isoformat(),
            "time_display": "Due" if is_due else dep_use.astimezone(TZ).strftime("%H:%M"),
            "is_live": is_live,
            "is_due": is_due,
            "delay_min": delay_min,
            "trip_id": trip_id,
        })
    # deduplicate on route + destination + time_iso
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for r in rows:
        key = (r["route"], r["destination"], r["time_iso"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    # sort: due (live and within a minute) first, then by time
    uniq.sort(key=lambda r: (not r["is_due"], r["time_iso"]))
    return uniq

# ---------------------------------------------------------------------------
# FastAPI app and endpoints

app = FastAPI(title="Bluestar Replacement API", version=APP_VERSION, docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (index.html and related assets)
app.mount("/", StaticFiles(directory="static", html=True), name="static")

@app.get("/api/status")
async def api_status() -> Dict[str, Any]:
    """Return basic status information about the service."""
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": STATE.build,
        "time": now_local().strftime("%H:%M:%S"),
        "tz": TZ_NAME,
        "live_feed_configured": bool(STATE.live_cfg.feed_url),
        "gtfs_ready": STATE.gtfs_ready,
        "gtfs_stops": STATE.gtfs_meta.get("gtfs_stops", 0),
    }

@app.get("/api/live/config")
async def api_get_live_config() -> Dict[str, Any]:
    """Get the current live feed configuration."""
    return STATE.live_cfg.dict()

@app.post("/api/live/config")
async def api_set_live_config(cfg: LiveConfig) -> Dict[str, Any]:
    """Set the live feed URL and refresh interval."""
    if not cfg.feed_url:
        raise HTTPException(status_code=400, detail="feed_url is required")
    if cfg.refresh_sec < 5 or cfg.refresh_sec > 3600:
        raise HTTPException(status_code=400, detail="refresh_sec must be between 5 and 3600 seconds")
    STATE.live_cfg = cfg
    # reset live cache
    STATE.vehicles_ts = 0.0
    return {"ok": True}

@app.post("/api/gtfs/upload")
async def api_gtfs_upload(file: UploadFile = File(...)) -> Dict[str, Any]:
    """Upload a GTFS zip file.

    The uploaded file is extracted and parsed.  The GTFS must include
    stops.txt, routes.txt, trips.txt and stop_times.txt.  Returns the
    number of stops on success.
    """
    data = await file.read()
    try:
        extract_and_load_gtfs(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    # persist the uploaded file for later reload
    try:
        with open(GTFS_ZIP, "wb") as f:
            f.write(data)
    except Exception:
        pass
    return {"ok": True, "gtfs_stops": STATE.gtfs_meta.get("gtfs_stops", 0)}

class GtfsUrlIn(BaseModel):
    url: str

@app.post("/api/gtfs/load-url")
async def api_gtfs_load_url(body: GtfsUrlIn) -> Dict[str, Any]:
    """Download a GTFS zip from a URL and load it."""
    if not body.url:
        raise HTTPException(status_code=400, detail="url is required")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(body.url)
            resp.raise_for_status()
            data = resp.content
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download GTFS: {e}")
    try:
        extract_and_load_gtfs(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    # persist to file
    try:
        with open(GTFS_ZIP, "wb") as f:
            f.write(data)
    except Exception:
        pass
    return {"ok": True, "gtfs_stops": STATE.gtfs_meta.get("gtfs_stops", 0)}

@app.post("/api/reload-gtfs")
async def api_reload_gtfs() -> Dict[str, Any]:
    """Reload the previously uploaded GTFS file or directory.

    If a zip file exists at ``GTFS_ZIP``, it is extracted and loaded.
    Otherwise, the current ``GTFS_DIR`` contents are reloaded.  Throws
    a 400 if neither is available.
    """
    if os.path.exists(GTFS_ZIP):
        try:
            with open(GTFS_ZIP, "rb") as f:
                data = f.read()
            extract_and_load_gtfs(data)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    elif any(fname.endswith(".txt") for fname in os.listdir(GTFS_DIR)):
        try:
            load_gtfs_from_dir(GTFS_DIR)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    else:
        raise HTTPException(status_code=400, detail="No GTFS to reload")
    return {"ok": True, "gtfs_stops": STATE.gtfs_meta.get("gtfs_stops", 0)}

@app.get("/api/stops/search")
async def api_stops_search(q: str = "") -> List[Dict[str, str]]:
    """Search for stops by name.  Returns at most 50 matches."""
    ensure_gtfs_loaded()
    ql = (q or "").strip().lower()
    stops = STATE.gtfs_tables.get("stops")
    if stops is None or stops.empty:
        return []
    res: List[Dict[str, str]] = []
    if not ql:
        return res
    for _, row in stops.iterrows():
        name = (row.get("stop_name") or "").strip()
        if ql in name.lower():
            res.append({"id": row.get("stop_id"), "name": name})
            if len(res) >= 50:
                break
    return res

@app.get("/api/routes/search")
async def api_routes_search(q: str = "") -> List[Dict[str, str]]:
    """Search for routes by short or long name.  Returns at most 50."""
    ensure_gtfs_loaded()
    ql = (q or "").strip().lower()
    routes = STATE.gtfs_tables.get("routes")
    if routes is None or routes.empty:
        return []
    res: List[Dict[str, str]] = []
    if not ql:
        return res
    for _, row in routes.iterrows():
        short = (row.get("route_short_name") or "").strip()
        longn = (row.get("route_long_name") or "").strip()
        rid = row.get("route_id") or ""
        display = short or longn or rid
        if ql in display.lower():
            res.append({"route": display})
            if len(res) >= 50:
                break
    return res

@app.get("/api/departures")
async def api_departures(stop_id: str, window: int = 90) -> Dict[str, Any]:
    """Get upcoming departures from a stop."""
    ensure_gtfs_loaded()
    window = max(1, min(window, 480))
    # fetch live if necessary
    await fetch_live_if_needed()
    rows = departure_rows_for_stop(stop_id, window)
    return {"departures": rows}

@app.get("/api/trip")
async def api_trip(trip_id: str) -> Dict[str, Any]:
    """Get full stop list and times for a trip."""
    ensure_gtfs_loaded()
    trips = STATE.gtfs_tables.get("trips")
    stop_times = STATE.gtfs_tables.get("stop_times")
    stops = STATE.gtfs_tables.get("stops")
    if trips is None or stop_times is None or stops is None:
        raise HTTPException(status_code=404, detail="Trip not found")
    trip_row = trips[trips["trip_id"] == trip_id]
    if trip_row.empty:
        raise HTTPException(status_code=404, detail="Trip not found")
    meta = trip_row.iloc[0]
    route_id = meta.get("route_id") or ""
    route_display = (
        (meta.get("route_short_name") or "").strip()
        or (meta.get("route_long_name") or "").strip()
        or route_id
    )
    headsign = (meta.get("trip_headsign") or "").strip()
    rows: List[Dict[str, Any]] = []
    now_dt = now_local()
    for _, st_row in stop_times[stop_times["trip_id"] == trip_id].iterrows():
        dep_str = st_row.get("departure_time") or st_row.get("arrival_time") or ""
        try:
            dep_local = parse_hhmmss_to_today(dep_str, now_dt.date())
        except Exception:
            continue
        stop_id_row = st_row.get("stop_id")
        stop_name = ""
        try:
            stop_name = (
                stops[stops["stop_id"] == stop_id_row].iloc[0].get("stop_name")
                or ""
            ).strip()
        except Exception:
            stop_name = stop_id_row or ""
        rows.append({
            "stop_id": stop_id_row,
            "stop_name": stop_name,
            "time_iso": dep_local.astimezone(pytz.utc).isoformat(),
            "time_display": dep_local.strftime("%H:%M"),
            "is_past": dep_local < now_dt,
            "is_live": False,
            "is_due": False,
            "delay_min": None,
        })
    return {"route": route_display, "headsign": headsign, "stops": rows}

@app.get("/api/vehicles")
async def api_vehicles(route: Optional[str] = None) -> Dict[str, Any]:
    """Get current live vehicles.  Optionally filter by route."""
    # fetch live if necessary
    await fetch_live_if_needed()
    now_dt = now_utc()
    items: Dict[str, Dict[str, Any]] = {}
    for v in STATE.vehicles:
        if route and str(v.get("line") or "").strip() != str(route).strip():
            continue
        lat = v.get("lat")
        lon = v.get("lon")
        if not lat or not lon:
            continue
        try:
            ts = datetime.fromisoformat(v["timestamp_utc"])
        except Exception:
            continue
        # ignore stale points (older than 60s)
        if (now_dt - ts).total_seconds() > 60:
            continue
        vref = v.get("vehicle_ref") or ""
        if not vref:
            continue
        # only keep the freshest record for each vehicle
        cur = items.get(vref)
        if (not cur) or (ts > datetime.fromisoformat(cur["timestamp"])):
            items[vref] = {
                "vehicle_ref": vref,
                "lat": lat,
                "lon": lon,
                "bearing": v.get("bearing"),
                "timestamp": ts.isoformat(),
                "label": f"{v.get('line', '').strip()} · {v.get('destination', '').strip()}".strip(),
            }
    return {"items": list(items.values()), "ts": STATE.vehicles_ts}

@app.get("/api/route-shape")
def api_route_shape(route: str) -> Dict[str, Any]:
    """Return the shape (list of [lat, lon]) for the given route.

    The route parameter is matched against short or long route names
    in routes.txt.  The first trip with a matching route_id is used
    to determine the shape_id.  If no shape or route is available
    the shape list will be empty.
    """
    ensure_gtfs_loaded()
    shapes_df = STATE.gtfs_tables.get("shapes")
    trips_df = STATE.gtfs_tables.get("trips")
    routes_df = STATE.gtfs_tables.get("routes")
    # no shapes available
    if shapes_df is None or shapes_df.empty:
        return {"shape": []}
    rparam = (route or "").strip()
    route_id = None
    # map route name to route_id using short/long names
    if routes_df is not None and not routes_df.empty:
        for _, row in routes_df.iterrows():
            short = (row.get("route_short_name") or "").strip()
            longn = (row.get("route_long_name") or "").strip()
            rid = row.get("route_id") or ""
            if rparam == short or rparam == longn:
                route_id = rid
                break
    # fallback: treat the input as route_id
    if not route_id:
        route_id = rparam
    shape_id = None
    # find first trip for this route_id with shape_id defined
    if trips_df is not None and not trips_df.empty:
        for _, row in trips_df.iterrows():
            if (row.get("route_id") or "") == route_id:
                shape_id = row.get("shape_id")
                if shape_id:
                    break
    if not shape_id:
        return {"shape": []}
    seg = shapes_df[shapes_df["shape_id"] == shape_id]
    if seg.empty:
        return {"shape": []}
    # order points by sequence if available
    if "shape_pt_sequence" in seg.columns:
        try:
            seg = seg.copy()
            seg["shape_pt_sequence"] = seg["shape_pt_sequence"].astype(float)
            seg = seg.sort_values("shape_pt_sequence")
        except Exception:
            pass
    coords: List[List[float]] = []
    for _, r in seg.iterrows():
        try:
            lat = float(r.get("shape_pt_lat"))
            lon = float(r.get("shape_pt_lon"))
            coords.append([lat, lon])
        except Exception:
            continue
    return {"shape": coords}
