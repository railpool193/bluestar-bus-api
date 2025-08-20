import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytz
import requests
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

# -------------------------------------------------------------------
# Alap beállítások
# -------------------------------------------------------------------

APP_VERSION = "4.2.1"
BUILD = str(int(datetime.now(timezone.utc).timestamp()))
UK_TZ = pytz.timezone("Europe/London")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"            # ide kerül a statikus/kinyert GTFS
CACHE_DIR = BASE_DIR / "cache"          # ide a feldolgozott/gyorsítótár
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Bluestar Bus – API", version=APP_VERSION, docs_url="/", redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

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
# GTFS statikus adatok – egyszerűsített loader
# Elvárt előfeldolgozott fájlok (feltöltés után generáljuk):
#   - stops.json: [{id,name}]
#   - routes.json: [{route,name}]
#   - trip_index.json: { "TRIP_ID": {"route": "17", "headsign": "..."} , ...}
#   - trip_stops.json: { "TRIP_ID": [ {"stop_id": "...", "stop_name":"...", "dep_utc":"2025-08-20T07:30:00Z"}, ...] }
# -------------------------------------------------------------------

STOPS_JSON = DATA_DIR / "stops.json"
ROUTES_JSON = DATA_DIR / "routes.json"
TRIP_INDEX_JSON = DATA_DIR / "trip_index.json"
TRIP_STOPS_JSON = DATA_DIR / "trip_stops.json"

def require_data():
    if not all(p.exists() for p in [STOPS_JSON, ROUTES_JSON, TRIP_INDEX_JSON, TRIP_STOPS_JSON]):
        raise HTTPException(503, "Static data not uploaded/processed yet.")

# -------------------------------------------------------------------
# /api/upload – fogad egy GTFS zip-et és előkészít
# -------------------------------------------------------------------

@app.post("/api/upload")
def api_upload(file: UploadFile = File(...)):
    raw = file.file.read()
    z = zipfile.ZipFile(io.BytesIO(raw))
    import csv

    # stops
    stops = []
    with z.open("stops.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            stops.append({"id": row["stop_id"], "name": row.get("stop_name", "").strip()})
    _write_json(STOPS_JSON, stops)

    # routes
    routes = []
    with z.open("routes.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            route_id = str(row.get("route_short_name") or row.get("route_long_name") or row.get("route_id") or "").strip()
            routes.append({"route": route_id})
    uniq_routes = []
    seen = set()
    for r in routes:
        k = r["route"].strip()
        if k and k not in seen:
            seen.add(k)
            uniq_routes.append({"route": k})
    _write_json(ROUTES_JSON, sorted(uniq_routes, key=lambda x: (len(x["route"]), x["route"])))

    # trips
    trips: Dict[str, Dict[str, Any]] = {}
    with z.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            trips[row["trip_id"]] = {
                "route": str(row.get("route_id", "")).strip(),
                "headsign": (row.get("trip_headsign") or "").strip(),
            }

    # idő konvert segéd
    def parse_hhmmss(s: str) -> timedelta:
        h, m, s2 = s.split(":")
        return timedelta(hours=int(h), minutes=int(m), seconds=int(s2))

    # stop_times – csak indulási idő
    trip_stops: Dict[str, List[Dict[str, Any]]] = {}
    with z.open("stop_times.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            tid = row["trip_id"]
            dep = row.get("departure_time")
            if not dep:
                continue
            base_date = _now_utc().astimezone(UK_TZ).date()
            dep_delta = parse_hhmmss(dep)  # kezeli a 24+ órát is
            dep_local = datetime.combine(base_date, datetime.min.time()).replace(tzinfo=UK_TZ) + dep_delta
            dep_utc = dep_local.astimezone(timezone.utc)

            trip_stops.setdefault(tid, []).append({
                "stop_id": row["stop_id"],
                "stop_name": "",  # később pótoljuk
                "dep_utc": dep_utc.isoformat(),
                "stop_sequence": int(row.get("stop_sequence") or 0),
            })

    # stop nevek hozzárendelése és sorbarakás
    stop_name_map = {s["id"]: s["name"] for s in stops}
    for tid, arr in trip_stops.items():
        for it in arr:
            it["stop_name"] = stop_name_map.get(it["stop_id"], it["stop_id"])
        arr.sort(key=lambda x: x["stop_sequence"])

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
    res = [s for s in items if ql in s["name"].lower()][:20]
    return JSONResponse(res)

# -------------------------------------------------------------------
# Élő feed – egyszerű cache + normalizálás
# -------------------------------------------------------------------

LIVE_CACHE_PATH = CACHE_DIR / "siri_vm.json"
LIVE_CACHE_MAX_AGE = 20  # mp – UI 10–20s körül frissít

def fetch_live() -> Dict[str, Any]:
    cfg = get_live_cfg()
    url = cfg.get("feed_url")
    if not url:
        return {"records": [], "ts": _now_utc().isoformat()}

    cache = _read_json(LIVE_CACHE_PATH, default=None)
    if cache:
        age = _now_utc() - datetime.fromisoformat(cache["ts"])
        if age.total_seconds() <= LIVE_CACHE_MAX_AGE:
            return cache

    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        payload = r.json()
    except Exception:
        if cache:
            return cache
        return {"records": [], "ts": _now_utc().isoformat()}

    records = []
    try:
        deliveries = payload.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
        for d in deliveries:
            for va in d.get("VehicleActivity", []):
                mvj = va.get("MonitoredVehicleJourney", {})
                vt = va.get("RecordedAtTime") or va.get("ValidUntilTime")
                try:
                    ts = datetime.fromisoformat((vt or "").replace("Z", "+00:00"))
                except Exception:
                    ts = _now_utc()

                exp_dep = (
                    ((mvj.get("MonitoredCall") or {}).get("ExpectedDepartureTime"))
                    or ((mvj.get("OnwardCalls") or {}).get("OnwardCall") or [{}])[0].get("ExpectedDepartureTime")
                )

                records.append({
                    "line": str(mvj.get("PublishedLineName") or mvj.get("LineRef") or "").strip(),
                    "vehicle_ref": str(mvj.get("VehicleRef") or mvj.get("VehicleId") or "").strip(),
                    "destination": (mvj.get("DestinationName") or "").strip(),
                    "bearing": mvj.get("Bearing"),
                    "lat": (mvj.get("VehicleLocation") or {}).get("Latitude"),
                    "lon": (mvj.get("VehicleLocation") or {}).get("Longitude"),
                    "timestamp_utc": ts.replace(tzinfo=timezone.utc).isoformat(),
                    "expected_departure_utc": exp_dep,
                })
    except Exception:
        pass

    cache = {"records": records, "ts": _now_utc().isoformat()}
    _write_json(LIVE_CACHE_PATH, cache)
    return cache

def live_lookup_for_stop(stop_id: str) -> Dict[str, Dict[str, Any]]:
    """Visszaadja route-szinten a legfrissebb élő ind. időt (ha van)."""
    live = fetch_live()["records"]
    now = _now_utc()
    best: Dict[str, Dict[str, Any]] = {}

    for r in live:
        exp = r.get("expected_departure_utc")
        exp_dt = None
        if exp:
            try:
                exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                exp_dt = None

        route = r.get("line", "").strip()
        if not route:
            continue

        ts = datetime.fromisoformat(r["timestamp_utc"])
        if (now - ts).total_seconds() > 120:
            continue

        cur = best.get(route)
        cand = {"is_live": True, "expected_dep_utc": exp_dt, "ts": ts}
        if not cur or ts > cur["ts"]:
            best[route] = cand

    return best

# -------------------------------------------------------------------
# Következő indulások (csak indulási idő, HH:MM, Due flag)
# -------------------------------------------------------------------

@app.get("/api/stops/{stop_id}/next_departures")
def api_next_departures(stop_id: str, window: int = 60):
    """
    Visszaadja a következő indulásokat adott megállóból (csak indulási idő, HH:MM).
    'Due' ha élő és esedékes (<= 0 perc és >= -1 perc).
    """
    require_data()

    trips: Dict[str, Dict[str, Any]] = _read_json(TRIP_INDEX_JSON, {})
    trip_stops: Dict[str, List[Dict[str, Any]]] = _read_json(TRIP_STOPS_JSON, {})

    now = _now_utc()
    end = now + timedelta(minutes=max(1, min(window, 480)))

    live_by_route = live_lookup_for_stop(stop_id)
    rows: List[Dict[str, Any]] = []

    for tid, meta in trips.items():
        segments = trip_stops.get(tid) or []
        for s in segments:
            if s["stop_id"] != stop_id:
                continue
            dep = datetime.fromisoformat(s["dep_utc"])
            if dep < now - timedelta(minutes=1):
                continue
            if dep > end:
                continue

            route = str(meta.get("route") or "").strip()
            headsign = meta.get("headsign") or ""
            live_info = live_by_route.get(route)
            is_live = bool(live_info and live_info.get("is_live", False))

            dep_use = dep
            if is_live and live_info.get("expected_dep_utc"):
                dep_use = live_info["expected_dep_utc"]

            mins_to = int((dep_use - now).total_seconds() // 60)
            is_due = is_live and mins_to <= 0 and mins_to >= -1

            rows.append({
                "route": route or headsign or "–",
                "destination": headsign or "–",
                "time_iso": dep_use.isoformat(),
                "time_display": "Due" if is_due else fmt_hhmm(dep_use),
                "is_live": is_live,
                "is_due": is_due,
                "trip_id": tid,
            })

    # rendezés: due elöl, aztán idő szerint
    rows.sort(key=lambda r: (not r["is_due"], r["time_iso"]))
    return {"departures": rows}

# -------------------------------------------------------------------
# Trip részletek – csak indulás, színezéshez flag-ek
# -------------------------------------------------------------------

@app.get("/api/trips/{trip_id}")
def api_trip_details(trip_id: str):
    require_data()
    trips: Dict[str, Dict[str, Any]] = _read_json(TRIP_INDEX_JSON, {})
    trip_stops: Dict[str, List[Dict[str, Any]]] = _read_json(TRIP_STOPS_JSON, {})
    meta = trips.get(trip_id)
    if not meta:
        raise HTTPException(404, "Trip not found")

    segments = trip_stops.get(trip_id) or []
    now = _now_utc()

    out = []
    for s in segments:
        dep = datetime.fromisoformat(s["dep_utc"])
        is_past = dep < now
        out.append({
            "stop_name": s["stop_name"],
            "time_iso": dep.isoformat(),
            "time_display": fmt_hhmm(dep),
            "is_past": is_past,
            "is_live": False,
            "is_due": False,
        })

    return {
        "route": meta.get("route") or "",
        "headsign": meta.get("headsign") or "",
        "stops": out
    }

# -------------------------------------------------------------------
# Vonal kereső és járművek
# -------------------------------------------------------------------

@app.get("/api/routes/search")
def api_routes_search(q: str):
    require_data()
    ql = q.strip().lower()
    items = _read_json(ROUTES_JSON, [])
    res = [r for r in items if ql in r["route"].lower()][:20]
    return JSONResponse(res)

@app.get("/api/routes/{route}/vehicles")
def api_route_vehicles(route: str):
    """
    Csak a megadott vonal járművei:
      - legfrissebb állapot VehicleRef szerint
      - csak friss (<= 60 mp) rekordok
    """
    live = fetch_live()["records"]
    now = _now_utc()
    fresh_limit = 60  # mp

    by_vehicle: Dict[str, Dict[str, Any]] = {}
    for r in live:
        if str(r.get("line", "")).strip() != str(route).strip():
            continue
        if not r.get("lat") or not r.get("lon"):
            continue
        ts = datetime.fromisoformat(r["timestamp_utc"])
        if (now - ts).total_seconds() > fresh_limit:
            continue
        vref = r.get("vehicle_ref") or ""
        if not vref:
            continue
        cur = by_vehicle.get(vref)
        if not cur or ts > datetime.fromisoformat(cur["timestamp"]):
            by_vehicle[vref] = {
                "vehicle_ref": vref,
                "lat": r["lat"],
                "lon": r["lon"],
                "bearing": r.get("bearing"),
                "timestamp": ts.isoformat(),
                "label": f'{route} · {r.get("destination","")}'.strip(),
            }

    return {"vehicles": list(by_vehicle.values())}

# -------------------------------------------------------------------
# Egyszerű UI a gyökér alatt (teszteléshez)
# -------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def home_page():
    return HTMLResponse("<h1>Bluestar Bus API</h1><p>OK</p>")
