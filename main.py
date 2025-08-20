import io
import json
import zipfile
import csv
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytz
import requests
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

# ------------------------------------------------------------
# Alap beállítások
# ------------------------------------------------------------
APP_VERSION = "4.4.0"
BUILD = str(int(datetime.now(timezone.utc).timestamp()))
UK_TZ = pytz.timezone("Europe/London")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"    # statikus/kinyert GTFS
CACHE_DIR = BASE_DIR / "cache"  # cache + live cfg
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(
    title="Bluestar Bus – API",
    version=APP_VERSION,
    docs_url="/api",
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

# ------------------------------------------------------------
# Utilok
# ------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def hhmm_from_utc(dt_utc: datetime) -> str:
    """UTC -> UK helyi idő HH:MM (24 órás, DST helyesen)."""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(UK_TZ).strftime("%H:%M")

def read_json(p: Path, default=None):
    if not p.exists():
        return default
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_json(p: Path, data: Any):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def status_payload():
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": BUILD,
        "uk_time": now_utc().astimezone(UK_TZ).strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool(get_live_cfg().get("feed_url")),
    }

# ------------------------------------------------------------
# Live feed (BODS SIRI-VM) beállítás
# ------------------------------------------------------------
LIVE_CFG_PATH = CACHE_DIR / "live_cfg.json"

def get_live_cfg() -> Dict[str, str]:
    return read_json(LIVE_CFG_PATH, {"feed_url": ""})

def set_live_cfg(cfg: Dict[str, str]):
    if not cfg or "feed_url" not in cfg:
        raise HTTPException(400, "feed_url is required")
    write_json(LIVE_CFG_PATH, {"feed_url": cfg["feed_url"]})

@app.get("/api/status")
def api_status():
    return JSONResponse(status_payload())

@app.get("/api/live/config")
def api_live_get():
    return JSONResponse(get_live_cfg())

@app.post("/api/live/config")
def api_live_set(cfg: Dict[str, str]):
    set_live_cfg(cfg)
    return {"ok": True}

# ------------------------------------------------------------
# GTFS: elvárt előállított fájlok
# ------------------------------------------------------------
STOPS_JSON       = DATA_DIR / "stops.json"       # [{id,name}]
ROUTES_JSON      = DATA_DIR / "routes.json"      # [{route}]
TRIP_INDEX_JSON  = DATA_DIR / "trip_index.json"  # {trip_id:{route_id,route,headsign}}
TRIP_STOPS_JSON  = DATA_DIR / "trip_stops.json"  # {trip_id:[{stop_id,stop_name,dep_utc,seq}]}

def require_data():
    need = [STOPS_JSON, ROUTES_JSON, TRIP_INDEX_JSON, TRIP_STOPS_JSON]
    if not all(p.exists() for p in need):
        raise HTTPException(503, "Static data not uploaded/processed yet.")

# ------------------------------------------------------------
# /api/upload – GTFS feldolgozás (csak indulási idők)
# ------------------------------------------------------------
@app.post("/api/upload")
def api_upload(file: UploadFile = File(...)):
    raw = file.file.read()
    z = zipfile.ZipFile(io.BytesIO(raw))

    # --- stops
    stops: List[Dict[str, str]] = []
    with z.open("stops.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            stops.append({"id": row["stop_id"], "name": (row.get("stop_name") or "").strip()})
    write_json(STOPS_JSON, stops)
    stop_name_map = {s["id"]: s["name"] for s in stops}

    # --- routes → route_id -> display (short előnyben)
    route_map: Dict[str, str] = {}
    with z.open("routes.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            rid = row["route_id"]
            short = (row.get("route_short_name") or "").strip()
            longn = (row.get("route_long_name") or "").strip()
            route_map[rid] = short or longn or rid
    # keresőhöz egyedi lista
    routes_list = [{"route": v} for v in sorted(set(route_map.values()), key=lambda x: (len(x), x))]
    write_json(ROUTES_JSON, routes_list)

    # --- trips (route rövid névvel)
    trips: Dict[str, Dict[str, str]] = {}
    with z.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            rid = row.get("route_id", "")
            trips[row["trip_id"]] = {
                "route_id": rid,
                "route": route_map.get(rid, rid),   # pl. "17"
                "headsign": (row.get("trip_headsign") or "").strip(),
            }

    # --- stop_times → CSAK departure_time → dep_utc (DST helyes)
    def parse_hhmmss(s: str) -> timedelta:
        h, m, s2 = s.split(":")
        return timedelta(hours=int(h), minutes=int(m), seconds=int(s2))

    trip_stops: Dict[str, List[Dict[str, Any]]] = {}
    today_local = now_utc().astimezone(UK_TZ).date()  # a GTFS napi idők ehhez a naphoz képest

    with z.open("stop_times.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            tid = row["trip_id"]
            dep = (row.get("departure_time") or "").strip()
            if not dep:
                continue
            # >24:00:00 is ok → timedelta kezeli
            dep_local = datetime.combine(today_local, datetime.min.time()).replace(tzinfo=UK_TZ) + parse_hhmmss(dep)
            dep_utc = dep_local.astimezone(timezone.utc)

            trip_stops.setdefault(tid, []).append({
                "stop_id": row["stop_id"],
                "stop_name": stop_name_map.get(row["stop_id"], row["stop_id"]),
                "dep_utc": dep_utc.isoformat(),
                "seq": int(row.get("stop_sequence") or 0),
            })

    for arr in trip_stops.values():
        arr.sort(key=lambda x: x["seq"])

    write_json(TRIP_INDEX_JSON, trips)
    write_json(TRIP_STOPS_JSON, trip_stops)

    return {"status": "uploaded"}

# ------------------------------------------------------------
# Megálló kereső
# ------------------------------------------------------------
@app.get("/api/stops/search")
def api_stops_search(q: str):
    require_data()
    ql = q.strip().lower()
    items = read_json(STOPS_JSON, [])
    return [s for s in items if ql in (s["name"] or "").lower()][:20]

# ------------------------------------------------------------
# Élő adat (SIRI-VM) cache + normalizálás
# ------------------------------------------------------------
LIVE_CACHE_PATH = CACHE_DIR / "siri_vm.json"
LIVE_CACHE_MAX_AGE = 20  # mp

def fetch_live() -> Dict[str, Any]:
    cfg = get_live_cfg()
    url = cfg.get("feed_url")
    if not url:
        return {"records": [], "ts": now_utc().isoformat()}

    cache = read_json(LIVE_CACHE_PATH)
    if cache:
        age = now_utc() - datetime.fromisoformat(cache["ts"])
        if age.total_seconds() <= LIVE_CACHE_MAX_AGE:
            return cache

    try:
        resp = requests.get(url, timeout=12)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        # hiba esetén régi cache vagy üres
        return cache or {"records": [], "ts": now_utc().isoformat()}

    records: List[Dict[str, Any]] = []
    try:
        deliveries = payload.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
        for d in deliveries or []:
            for va in (d.get("VehicleActivity") or []):
                mvj = va.get("MonitoredVehicleJourney", {}) or {}
                vt = va.get("RecordedAtTime") or va.get("ValidUntilTime") or ""
                try:
                    ts = datetime.fromisoformat(vt.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    ts = now_utc()

                # koordináta szűrés UK környékére
                loc = mvj.get("VehicleLocation") or {}
                lat = loc.get("Latitude"); lon = loc.get("Longitude")
                try:
                    lat = float(lat) if lat is not None else None
                    lon = float(lon) if lon is not None else None
                except Exception:
                    lat = lon = None
                if not (lat and lon):
                    continue
                if not (49 <= lat <= 61 and -8 <= lon <= 2):
                    continue

                # expected dep (ha van)
                exp_dep = (
                    (mvj.get("MonitoredCall") or {}).get("ExpectedDepartureTime")
                    or ((mvj.get("OnwardCalls") or {}).get("OnwardCall") or [{}])[0].get("ExpectedDepartureTime")
                )

                records.append({
                    "line": str(mvj.get("PublishedLineName") or mvj.get("LineRef") or "").strip(),
                    "vehicle_ref": str(mvj.get("VehicleRef") or mvj.get("VehicleId") or "").strip(),
                    "destination": (mvj.get("DestinationName") or "").strip(),
                    "bearing": mvj.get("Bearing"),
                    "lat": lat,
                    "lon": lon,
                    "timestamp_utc": ts.isoformat(),
                    "expected_departure_utc": exp_dep,
                })
    except Exception:
        pass

    out = {"records": records, "ts": now_utc().isoformat()}
    write_json(LIVE_CACHE_PATH, out)
    return out

def live_lookup_by_route() -> Dict[str, Dict[str, Any]]:
    """Legfrissebb élő infó route-szinten (≤120 mp)."""
    live = fetch_live()["records"]
    now = now_utc()
    best: Dict[str, Dict[str, Any]] = {}
    for r in live:
        route = (r.get("line") or "").strip()
        if not route:
            continue
        try:
            ts = datetime.fromisoformat(r["timestamp_utc"])
        except Exception:
            continue
        if (now - ts).total_seconds() > 120:
            continue

        exp_dt = None
        exp = r.get("expected_departure_utc")
        if exp:
            try:
                exp_dt = datetime.fromisoformat(exp.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                exp_dt = None

        cand = {"ts": ts, "expected_dep_utc": exp_dt, "is_live": True}
        cur = best.get(route)
        if not cur or ts > cur["ts"]:
            best[route] = cand
    return best

# ------------------------------------------------------------
# Következő indulások (csak indulás, HH:MM, Due flag ha live)
# ------------------------------------------------------------
@app.get("/api/stops/{stop_id}/next_departures")
def api_next_departures(stop_id: str, window: int = 60):
    require_data()
    trips = read_json(TRIP_INDEX_JSON, {})
    trip_stops = read_json(TRIP_STOPS_JSON, {})

    now = now_utc()
    end = now + timedelta(minutes=max(1, min(window, 480)))

    live_by_route = live_lookup_by_route()

    rows: List[Dict[str, Any]] = []
    for tid, meta in trips.items():
        route = (meta.get("route") or "").strip()
        headsign = meta.get("headsign") or ""
        for s in trip_stops.get(tid, []):
            if s["stop_id"] != stop_id:
                continue
            dep = datetime.fromisoformat(s["dep_utc"])
            if dep < now - timedelta(minutes=1):
                continue
            if dep > end:
                continue

            dep_use = dep
            is_live = False
            live_info = live_by_route.get(route)
            if live_info:
                is_live = True
                if live_info.get("expected_dep_utc"):
                    dep_use = live_info["expected_dep_utc"]

            mins = int((dep_use - now).total_seconds() // 60)
            is_due = is_live and (-1 <= mins <= 0)

            rows.append({
                "route": route or headsign or "–",
                "destination": headsign or "–",
                "time_iso": dep_use.isoformat(),
                "time_display": "Due" if is_due else hhmm_from_utc(dep_use),
                "is_live": is_live,
                "is_due": is_due,
                "trip_id": tid,
            })

    rows.sort(key=lambda r: (not r["is_due"], r["time_iso"]))
    return {"departures": rows}

# ------------------------------------------------------------
# Trip részletek – csak indulási idők (múlt szürkézéshez flag)
# ------------------------------------------------------------
@app.get("/api/trips/{trip_id}")
def api_trip_details(trip_id: str):
    require_data()
    trips = read_json(TRIP_INDEX_JSON, {})
    trip_stops = read_json(TRIP_STOPS_JSON, {})

    meta = trips.get(trip_id)
    if not meta:
        raise HTTPException(404, "Trip not found")

    now = now_utc()
    out = []
    for s in trip_stops.get(trip_id, []):
        dep = datetime.fromisoformat(s["dep_utc"])
        out.append({
            "stop_name": s["stop_name"],
            "time_iso": dep.isoformat(),
            "time_display": hhmm_from_utc(dep),
            "is_past": dep < now,
            "is_live": False,
            "is_due": False,
        })

    return {
        "route": meta.get("route") or "",
        "headsign": meta.get("headsign") or "",
        "stops": out
    }

# ------------------------------------------------------------
# Vonal kereső & járművek (csak friss, adott route, deduplikált)
# ------------------------------------------------------------
@app.get("/api/routes/search")
def api_routes_search(q: str):
    require_data()
    ql = q.strip().lower()
    items = read_json(ROUTES_JSON, [])
    return [r for r in items if ql in (r["route"] or "").lower()][:20]

@app.get("/api/routes/{route}/vehicles")
def api_route_vehicles(route: str):
    live = fetch_live()["records"]
    now = now_utc()
    fresh_limit = 60  # mp

    by_vehicle: Dict[str, Dict[str, Any]] = {}
    for r in live:
        if (r.get("line") or "").strip() != route.strip():
            continue
        lat, lon = r.get("lat"), r.get("lon")
        if not (lat and lon):
            continue
        try:
            ts = datetime.fromisoformat(r["timestamp_utc"])
        except Exception:
            continue
        if (now - ts).total_seconds() > fresh_limit:
            continue

        vref = r.get("vehicle_ref") or ""
        if not vref:
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

# ------------------------------------------------------------
# Index (statikus UI) – a gyökér a frontend HTML-t adja
# ------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index_html():
    return (BASE_DIR / "index.html").read_text(encoding="utf-8")
