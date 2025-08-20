import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz
import requests
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse

# -------------------------------------------------------------------
# Alap beállítások
# -------------------------------------------------------------------

APP_VERSION = "4.3.0"
BUILD = str(int(datetime.now(timezone.utc).timestamp()))
UK_TZ = pytz.timezone("Europe/London")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"            # ide kerül a statikus/kinyert GTFS
CACHE_DIR = BASE_DIR / "cache"          # ide a feldolgozott/gyorsítótár
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# FONTOS: a / gyökér a frontend, a Swagger a /docs alatt
app = FastAPI(title="Bluestar Bus – API", version=APP_VERSION, docs_url="/docs", redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

# Frontend kiszolgálás
@app.get("/", include_in_schema=False)
def root_html():
    idx = BASE_DIR / "index.html"
    if not idx.exists():
        return HTMLResponse("<h1>Frontend hiányzik (index.html)</h1>", status_code=200)
    return HTMLResponse(idx.read_text(encoding="utf-8"))

@app.get("/index.html", include_in_schema=False)
def index_file():
    return FileResponse(str(BASE_DIR / "index.html"), media_type="text/html")

# -------------------------------------------------------------------
# Kicsi utilok
# -------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def fmt_hhmm(dt_utc: datetime) -> str:
    """UTC -> UK helyi idő HH:MM"""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    local = dt_utc.astimezone(UK_TZ)
    return local.strftime("%H:%M")

def _read_json(path: Path, default=None):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def _status_ok():
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": BUILD,
        "uk_time": _now_utc().astimezone(UK_TZ).strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool(get_live_cfg().get("feed_url")),
    }

# -------------------------------------------------------------------
# Live feed beállítás (BODS SIRI-VM endpoint)
# -------------------------------------------------------------------

LIVE_CFG_PATH = CACHE_DIR / "live_cfg.json"

def get_live_cfg() -> Dict[str, str]:
    return _read_json(LIVE_CFG_PATH, default={"feed_url": ""})

def set_live_cfg(payload: Dict[str, str]):
    if not payload or "feed_url" not in payload:
        raise HTTPException(400, "feed_url is required")
    _write_json(LIVE_CFG_PATH, {"feed_url": payload["feed_url"]})

@app.get("/api/status")
def api_status():
    return JSONResponse(_status_ok())

@app.get("/api/live/config")
def api_get_live_cfg():
    return JSONResponse(get_live_cfg())

@app.post("/api/live/config")
def api_set_live_cfg(cfg: Dict[str, str]):
    set_live_cfg(cfg)
    return JSONResponse({"ok": True})

# -------------------------------------------------------------------
# GTFS statikus adatok – loader
# Előállított fájlok:
#   - stops.json: [{id,name}]
#   - routes.json: [{route}]
#   - trip_index.json: { TRIP_ID: {"route":"17","headsign":"..."} }
#   - trip_stops.json: { TRIP_ID: [ {"stop_id":"...","stop_name":"...","dep_utc":"...","stop_sequence":N}, ...] }
# -------------------------------------------------------------------

STOPS_JSON = DATA_DIR / "stops.json"
ROUTES_JSON = DATA_DIR / "routes.json"
TRIP_INDEX_JSON = DATA_DIR / "trip_index.json"
TRIP_STOPS_JSON = DATA_DIR / "trip_stops.json"

def require_data():
    if not all(p.exists() for p in [STOPS_JSON, ROUTES_JSON, TRIP_INDEX_JSON, TRIP_STOPS_JSON]):
        raise HTTPException(503, "Static data not uploaded/processed yet.")

@app.post("/api/upload")
def api_upload(file: UploadFile = File(...)):
    raw = file.file.read()
    z = zipfile.ZipFile(io.BytesIO(raw))
    import csv

    # stops
    stops = []
    with z.open("stops.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            stops.append({"id": row["stop_id"], "name": (row.get("stop_name") or "").strip()})
    _write_json(STOPS_JSON, stops)

    # routes
    routes_raw = []
    with z.open("routes.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            name = row.get("route_short_name") or row.get("route_long_name") or row.get("route_id")
            routes_raw.append({"route": str(name).strip()})
    seen, routes = set(), []
    for r in routes_raw:
        if r["route"] and r["route"] not in seen:
            seen.add(r["route"])
            routes.append(r)
    routes.sort(key=lambda x: (len(x["route"]), x["route"]))
    _write_json(ROUTES_JSON, routes)

    # trips
    trips = {}
    with z.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            trips[row["trip_id"]] = {
                "route": str(row.get("route_id") or "").strip(),
                "headsign": (row.get("trip_headsign") or "").strip(),
            }

    # stop_times -> CSAK indulási idő
    def parse_hhmmss(s: str) -> timedelta:
        h, m, s2 = s.split(":")
        return timedelta(hours=int(h), minutes=int(m), seconds=int(s2))

    trip_stops: Dict[str, List[Dict[str, Any]]] = {}
    with z.open("stop_times.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            tid = row["trip_id"]
            dep = (row.get("departure_time") or "").strip()
            if not dep:
                continue
            base_date = _now_utc().astimezone(UK_TZ).date()
            dep_local = datetime.combine(base_date, datetime.min.time()).replace(tzinfo=UK_TZ) + parse_hhmmss(dep)
            dep_utc = dep_local.astimezone(timezone.utc)
            trip_stops.setdefault(tid, []).append({
                "stop_id": row["stop_id"],
                "stop_name": "",
                "dep_utc": dep_utc.isoformat(),
                "stop_sequence": int(row.get("stop_sequence") or 0)
            })

    # stop nevek hozzárendelése
    name_map = {s["id"]: s["name"] for s in stops}
    for tid, segs in trip_stops.items():
        for s in segs:
            s["stop_name"] = name_map.get(s["stop_id"], s["stop_id"])
        segs.sort(key=lambda x: x["stop_sequence"])

    _write_json(TRIP_INDEX_JSON, trips)
    _write_json(TRIP_STOPS_JSON, trip_stops)

    return {"status": "uploaded"}

# -------------------------------------------------------------------
# Megálló kereső
# -------------------------------------------------------------------

@app.get("/api/stops/search")
def api_stops_search(q: str):
    require_data()
    ql = q.strip().lower()
    items = _read_json(STOPS_JSON, [])
    return JSONResponse([s for s in items if ql in s["name"].lower()][:20])

# -------------------------------------------------------------------
# Élő cache + normalizálás
# -------------------------------------------------------------------

LIVE_CACHE_PATH = CACHE_DIR / "siri_vm.json"
LIVE_CACHE_MAX_AGE = 20  # mp

def fetch_live() -> Dict[str, Any]:
    cfg = get_live_cfg()
    url = cfg.get("feed_url")
    cache = _read_json(LIVE_CACHE_PATH, default=None)

    # friss cache elég
    if cache:
        try:
            age = _now_utc() - datetime.fromisoformat(cache["ts"])
            if age.total_seconds() <= LIVE_CACHE_MAX_AGE:
                return cache
        except Exception:
            pass

    if not url:
        empty = {"records": [], "ts": _now_utc().isoformat()}
        _write_json(LIVE_CACHE_PATH, empty)
        return empty

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        payload = r.json()
    except Exception:
        return cache or {"records": [], "ts": _now_utc().isoformat()}

    records = []
    try:
        deliveries = payload.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
        for d in deliveries:
            for va in d.get("VehicleActivity", []):
                mvj = va.get("MonitoredVehicleJourney", {}) or {}
                vt = va.get("RecordedAtTime") or va.get("ValidUntilTime")
                try:
                    ts = datetime.fromisoformat((vt or "").replace("Z", "+00:00"))
                except Exception:
                    ts = _now_utc()
                loc = mvj.get("VehicleLocation") or {}
                line = (mvj.get("PublishedLineName") or mvj.get("LineRef") or "") or ""
                vref = (mvj.get("VehicleRef") or mvj.get("VehicleId") or "") or ""

                # csak ha van azonosítható jármű + pozíció
                if not vref or loc.get("Latitude") is None or loc.get("Longitude") is None:
                    continue

                records.append({
                    "line": str(line).strip(),
                    "vehicle_ref": str(vref).strip(),
                    "destination": (mvj.get("DestinationName") or "").strip(),
                    "bearing": mvj.get("Bearing"),
                    "lat": loc.get("Latitude"),
                    "lon": loc.get("Longitude"),
                    "timestamp_utc": ts.replace(tzinfo=timezone.utc).isoformat(),
                    # ha lenne per-stop ETA:
                    "expected_departure_utc": (
                        ((mvj.get("MonitoredCall") or {}).get("ExpectedDepartureTime"))
                        or (((mvj.get("OnwardCalls") or {}).get("OnwardCall") or [{}])[0].get("ExpectedDepartureTime"))
                    ),
                })
    except Exception:
        pass

    cache = {"records": records, "ts": _now_utc().isoformat()}
    _write_json(LIVE_CACHE_PATH, cache)
    return cache

# route-szintű Live jelzés (ExpectedDepartureTime nélkül is)
def live_lookup_for_stop(stop_id: str) -> Dict[str, Dict[str, Any]]:
    live = fetch_live()["records"]
    now = _now_utc()
    best: Dict[str, Dict[str, Any]] = {}
    for r in live:
        route = (r.get("line") or "").strip()
        if not route:
            continue
        exp = r.get("expected_departure_utc")
        exp_dt = None
        if exp:
            try:
                exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                exp_dt = None
        try:
            ts = datetime.fromisoformat(r["timestamp_utc"])
        except Exception:
            ts = now
        # csak friss activity (<= 120 mp)
        if (now - ts).total_seconds() > 120:
            continue
        cur = best.get(route)
        cand = {"is_live": True, "expected_dep_utc": exp_dt, "ts": ts}
        if not cur or ts > cur["ts"]:
            best[route] = cand
    return best

# -------------------------------------------------------------------
# Következő indulások – csak indulás, HH:MM, Due/live jelzés
# -------------------------------------------------------------------

@app.get("/api/stops/{stop_id}/next_departures")
def api_next_departures(stop_id: str, window: int = 60):
    """
    Visszaadja a következő indulásokat adott megállóból (csak indulás, HH:MM).
    'Due' ha élő és ~1 percen belül.
    """
    require_data()

    trips: Dict[str, Dict[str, Any]] = _read_json(TRIP_INDEX_JSON, {})
    trip_stops: Dict[str, List[Dict[str, Any]]] = _read_json(TRIP_STOPS_JSON, {})

    now = _now_utc()
    end = now + timedelta(minutes=max(1, min(window, 480)))
    live_by_route = live_lookup_for_stop(stop_id)

    rows: List[Dict[str, Any]] = []
    for tid, meta in trips.items():
        for s in trip_stops.get(tid, []):
            if s["stop_id"] != stop_id:
                continue
            dep = datetime.fromisoformat(s["dep_utc"])
            if dep < now - timedelta(minutes=1) or dep > end:
                continue
            route = str(meta.get("route") or "").strip()
            headsign = meta.get("headsign") or ""
            live_info = live_by_route.get(route)
            is_live = bool(live_info and live_info.get("is_live"))
            dep_use = live_info.get("expected_dep_utc") if (is_live and live_info.get("expected_dep_utc")) else dep
            mins_to = int((dep_use - now).total_seconds() // 60)
            is_due = is_live and -1 <= mins_to <= 0

            rows.append({
                "route": route or headsign or "–",
                "destination": headsign or "–",
                "time_iso": dep_use.isoformat(),
                "time_display": "Due" if is_due else fmt_hhmm(dep_use),
                "is_live": is_live,
                "is_due": is_due,
                "trip_id": tid,
            })

    # egyszerű dedupe (route+dest+time_display)
    seen = set()
    clean = []
    for r in rows:
        key = (r["route"], r["destination"], r["time_display"])
        if key in seen:
            continue
        seen.add(key)
        clean.append(r)

    clean.sort(key=lambda r: (not r["is_due"], r["time_iso"]))
    return {"departures": clean}

# -------------------------------------------------------------------
# Trip részletek – csak indulás
# -------------------------------------------------------------------

@app.get("/api/trips/{trip_id}")
def api_trip_details(trip_id: str):
    require_data()
    trips: Dict[str, Dict[str, Any]] = _read_json(TRIP_INDEX_JSON, {})
    trip_stops: Dict[str, List[Dict[str, Any]]] = _read_json(TRIP_STOPS_JSON, {})
    meta = trips.get(trip_id)
    if not meta:
        raise HTTPException(404, "Trip not found")

    segments = trip_stops.get(trip_id, [])
    now = _now_utc()
    out = []
    for s in segments:
        dep = datetime.fromisoformat(s["dep_utc"])
        out.append({
            "stop_name": s["stop_name"],
            "time_iso": dep.isoformat(),
            "time_display": fmt_hhmm(dep),
            "is_past": dep < now,
            "is_live": False,
            "is_due": False,
        })
    return {"route": meta.get("route") or "", "headsign": meta.get("headsign") or "", "stops": out}

# -------------------------------------------------------------------
# Vonal kereső + járművek (csak ténylegesen közlekedők)
# -------------------------------------------------------------------

@app.get("/api/routes/search")
def api_routes_search(q: str):
    require_data()
    ql = q.strip().lower()
    items = _read_json(ROUTES_JSON, [])
    return JSONResponse([r for r in items if ql in r["route"].lower()][:20])

@app.get("/api/routes/{route}/vehicles")
def api_route_vehicles(route: str):
    """
    Csak a megadott vonal valósan közlekedő járművei:
      - vehicle_ref KÖTELEZŐ
      - csak friss rekord (<= 30 mp)
      - vehicle_ref szerinti dedupe (legfrissebb állapot marad)
    """
    live = fetch_live()["records"]
    now = _now_utc()
    fresh_limit = 30  # mp

    by_vehicle: Dict[str, Dict[str, Any]] = {}
    for r in live:
        if str(r.get("line", "")).strip().lower() != str(route).strip().lower():
            continue
        vref = (r.get("vehicle_ref") or "").strip()
        if not vref:
            continue
        lat, lon = r.get("lat"), r.get("lon")
        if lat is None or lon is None:
            continue
        try:
            ts = datetime.fromisoformat(r["timestamp_utc"])
        except Exception:
            continue
        if (now - ts).total_seconds() > fresh_limit:
            continue
        cur = by_vehicle.get(vref)
        if not cur or ts > datetime.fromisoformat(cur["timestamp"]):
            by_vehicle[vref] = {
                "vehicle_ref": vref,
                "lat": lat,
                "lon": lon,
                "bearing": r.get("bearing"),
                "timestamp": ts.isoformat(),
                "label": f'{route} · {r.get("destination","")}'.strip(),
            }

    return {"vehicles": list(by_vehicle.values())}
