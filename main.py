import io
import json
import re
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pytz
import requests
import xmltodict
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

APP_VERSION = "5.0.0"
BUILD = str(int(datetime.now(timezone.utc).timestamp()))
UK_TZ = pytz.timezone("Europe/London")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = BASE_DIR / "cache"
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Bluestar Bus – API", version=APP_VERSION, docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_headers=["*"], allow_methods=["*"],
)

def _now_utc() -> datetime: return datetime.now(timezone.utc)
def fmt_hhmm(dt_utc: datetime) -> str:
    if dt_utc.tzinfo is None: dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(UK_TZ).strftime("%H:%M")
def _read_json(p: Path, default=None): 
    if not p.exists(): return default
    with p.open("r", encoding="utf-8") as f: return json.load(f)
def _write_json(p: Path, data: Any):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False)

def _status_ok():
    return {
        "ok": True, "version": APP_VERSION, "build": BUILD,
        "uk_time": _now_utc().astimezone(UK_TZ).strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool(get_live_cfg().get("feed_url")),
    }

def _get(d, path, default=None):
    cur = d
    for part in path.split("."):
        if isinstance(cur, list):
            try: cur = cur[int(part)]; continue
            except Exception: cur = cur[0] if cur else default
        if isinstance(cur, dict) and part in cur: cur = cur[part]
        else: return default
    return cur

# -------- Live feed config --------
LIVE_CFG_PATH = CACHE_DIR / "live_cfg.json"
def get_live_cfg() -> Dict[str, str]: return _read_json(LIVE_CFG_PATH, {"feed_url": ""})
def set_live_cfg(payload: Dict[str, str]):
    if not payload or "feed_url" not in payload: raise HTTPException(400, "feed_url is required")
    _write_json(LIVE_CFG_PATH, {"feed_url": payload["feed_url"]})

@app.get("/api/status") def api_status(): return JSONResponse(_status_ok())
@app.get("/api/live/config") def api_get_live_cfg(): return JSONResponse(get_live_cfg())
@app.post("/api/live/config") def api_set_live_cfg(cfg: Dict[str, str]): set_live_cfg(cfg); return {"ok": True}

# -------- Static data --------
STOPS_JSON = DATA_DIR / "stops.json"
ROUTES_JSON = DATA_DIR / "routes.json"
TRIP_INDEX_JSON = DATA_DIR / "trip_index.json"
TRIP_STOPS_JSON = DATA_DIR / "trip_stops.json"
def require_data():
    if not all(p.exists() for p in [STOPS_JSON, ROUTES_JSON, TRIP_INDEX_JSON, TRIP_STOPS_JSON]):
        raise HTTPException(503, "Static data not uploaded/processed yet.")

# -------- Upload GTFS ZIP --------
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
            name = row.get("route_short_name") or row.get("route_long_name") or row.get("route_id")
            routes.append({"route": str(name).strip()})
    seen = set(); uniq = []
    for r in routes:
        k = r["route"]
        if k not in seen:
            seen.add(k); uniq.append({"route": k})
    _write_json(ROUTES_JSON, sorted(uniq, key=lambda x: (len(x["route"]), x["route"])))

    # trips
    trips = {}
    with z.open("trips.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            trips[row["trip_id"]] = {
                "route": row.get("route_id", ""),
                "headsign": row.get("trip_headsign", ""),
            }

    def parse_hhmmss(s: str) -> timedelta:
        h, m, s2 = s.split(":"); return timedelta(hours=int(h), minutes=int(m), seconds=int(s2))

    trip_stops: Dict[str, List[Dict[str, Any]]] = {}
    with z.open("stop_times.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            tid = row["trip_id"]; dep = row.get("departure_time")
            if not dep: continue
            base_date = _now_utc().astimezone(UK_TZ).date()
            dep_delta = parse_hhmmss(dep)
            dep_local = datetime.combine(base_date, datetime.min.time()).replace(tzinfo=UK_TZ) + dep_delta
            dep_utc = dep_local.astimezone(timezone.utc)
            trip_stops.setdefault(tid, []).append({
                "stop_id": row["stop_id"], "stop_name": "", "dep_utc": dep_utc.isoformat(),
                "stop_sequence": int(row.get("stop_sequence") or 0),
            })

    stop_name_map = {s["id"]: s["name"] for s in stops}
    for tid, arr in trip_stops.items():
        for it in arr:
            it["stop_name"] = stop_name_map.get(it["stop_id"], it["stop_id"])
        arr.sort(key=lambda x: x["stop_sequence"])

    _write_json(TRIP_INDEX_JSON, trips)
    _write_json(TRIP_STOPS_JSON, trip_stops)
    return {"status": "uploaded"}

# -------- Search endpoints --------
@app.get("/api/stops/search")
def api_stops_search(q: str):
    require_data()
    items = _read_json(STOPS_JSON, [])
    ql = q.strip().lower()
    return [s for s in items if ql in s["name"].lower()][:20]

@app.get("/api/routes/search")
def api_routes_search(q: str):
    require_data()
    items = _read_json(ROUTES_JSON, [])
    ql = q.strip().lower()
    res = [r for r in items if ql in r["route"].lower()][:20]
    for r in res: r["route"] = norm_line(r["route"])
    return res

# -------- Live (SIRI-VM XML/JSON) --------
LIVE_CACHE_PATH = CACHE_DIR / "siri_vm.json"
LIVE_CACHE_MAX_AGE = 20
LINE_PREFIX_RE = re.compile(r"^(blus:|bluestar:)", re.IGNORECASE)

def norm_line(x: str) -> str:
    s = (x or "").strip()
    return LINE_PREFIX_RE.sub("", s)

def fetch_live() -> Dict[str, Any]:
    cfg = get_live_cfg(); url = cfg.get("feed_url")
    if not url: return {"records": [], "ts": _now_utc().isoformat()}

    cache = _read_json(LIVE_CACHE_PATH)
    if cache:
        age = _now_utc() - datetime.fromisoformat(cache["ts"])
        if age.total_seconds() <= LIVE_CACHE_MAX_AGE: return cache

    def _as_utc_iso(x):
        if not x: return None
        try: return datetime.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except Exception: return None

    recs: List[Dict[str, Any]] = []
    try:
        r = requests.get(url, timeout=12); r.raise_for_status()
        ctype = (r.headers.get("Content-Type") or "").lower()
        text = r.text.strip()

        if "xml" in ctype or text.startswith("<"):
            doc = xmltodict.parse(text)
            deliveries = _get(doc, "Siri.ServiceDelivery.VehicleMonitoringDelivery") or []
            if isinstance(deliveries, dict): deliveries = [deliveries]
            for d in deliveries:
                vas = d.get("VehicleActivity") or []
                if isinstance(vas, dict): vas = [vas]
                for va in vas:
                    mvj = va.get("MonitoredVehicleJourney") or {}
                    recs.append({
                        "line": mvj.get("PublishedLineName") or mvj.get("LineRef") or "",
                        "vehicle_ref": mvj.get("VehicleRef") or "",
                        "destination": mvj.get("DestinationName") or "",
                        "bearing": mvj.get("Bearing"),
                        "lat": _get(mvj, "VehicleLocation.Latitude"),
                        "lon": _get(mvj, "VehicleLocation.Longitude"),
                        "timestamp_utc": _as_utc_iso(va.get("RecordedAtTime") or d.get("ResponseTimestamp")) or _now_utc().isoformat(),
                        "expected_departure_utc": _as_utc_iso(
                            _get(mvj, "MonitoredCall.ExpectedDepartureTime")
                            or _get(mvj, "OnwardCalls.OnwardCall.0.ExpectedDepartureTime")
                        ),
                    })
        else:
            payload = r.json()
            deliveries = payload.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
            for d in deliveries:
                for va in d.get("VehicleActivity", []):
                    mvj = va.get("MonitoredVehicleJourney", {})
                    loc = mvj.get("VehicleLocation") or {}
                    onward = (mvj.get("OnwardCalls") or {}).get("OnwardCall") or [{}]
                    recs.append({
                        "line": mvj.get("PublishedLineName") or mvj.get("LineRef") or "",
                        "vehicle_ref": mvj.get("VehicleRef") or mvj.get("VehicleId") or "",
                        "destination": mvj.get("DestinationName") or "",
                        "bearing": mvj.get("Bearing"),
                        "lat": loc.get("Latitude"), "lon": loc.get("Longitude"),
                        "timestamp_utc": _as_utc_iso(va.get("RecordedAtTime") or d.get("ResponseTimestamp")) or _now_utc().isoformat(),
                        "expected_departure_utc": _as_utc_iso((mvj.get("MonitoredCall") or {}).get("ExpectedDepartureTime") or onward[0].get("ExpectedDepartureTime")),
                    })
        out=[]
        for r in recs:
            try:
                lat = float(r["lat"]) if r.get("lat") is not None else None
                lon = float(r["lon"]) if r.get("lon") is not None else None
            except Exception:
                lat = lon = None
            out.append({
                "line": norm_line(r.get("line") or ""),
                "vehicle_ref": (r.get("vehicle_ref") or "").strip(),
                "destination": (r.get("destination") or "").replace("_"," ").strip(),
                "bearing": r.get("bearing"),
                "lat": lat, "lon": lon,
                "timestamp_utc": r.get("timestamp_utc") or _now_utc().isoformat(),
                "expected_departure_utc": r.get("expected_departure_utc"),
            })
        recs = out
    except Exception:
        if cache: return cache
        return {"records": [], "ts": _now_utc().isoformat()}

    cache = {"records": recs, "ts": _now_utc().isoformat()}
    _write_json(LIVE_CACHE_PATH, cache)
    return cache

def live_lookup_for_stop(stop_id: str) -> Dict[str, Dict[str, Any]]:
    live = fetch_live()["records"]; now = _now_utc(); best={}
    for r in live:
        route = norm_line(r.get("line","")); if not route: continue
        try: ts = datetime.fromisoformat(r["timestamp_utc"])
        except Exception: continue
        if (now - ts).total_seconds() > 120: continue
        exp = r.get("expected_departure_utc")
        exp_dt = None
        if exp:
            try: exp_dt = datetime.fromisoformat(exp.replace("Z","+00:00")).astimezone(timezone.utc)
            except Exception: exp_dt = None
        cand = {"is_live": True, "expected_dep_utc": exp_dt, "ts": ts}
        cur = best.get(route)
        if not cur or ts > cur["ts"]: best[route] = cand
    return best

# -------- Next departures (+ delay/early) --------
@app.get("/api/stops/{stop_id}/next_departures")
def api_next_departures(stop_id: str, window: int = 60):
    require_data()
    trips = _read_json(TRIP_INDEX_JSON, {})
    trip_stops = _read_json(TRIP_STOPS_JSON, {})
    now = _now_utc(); end = now + timedelta(minutes=max(1, min(window, 480)))
    live_by_route = live_lookup_for_stop(stop_id)

    rows=[]
    for tid, meta in trips.items():
        for s in (trip_stops.get(tid) or []):
            if s["stop_id"] != stop_id: continue
            sched_dep = datetime.fromisoformat(s["dep_utc"])
            if sched_dep < now - timedelta(minutes=1): continue
            if sched_dep > end: continue

            route = norm_line(str(meta.get("route") or "").strip())
            headsign = meta.get("headsign") or ""
            live_info = live_by_route.get(route)
            is_live = bool(live_info and live_info.get("is_live"))
            dep_use = sched_dep
            delay_min = 0
            if is_live and live_info.get("expected_dep_utc"):
                dep_use = live_info["expected_dep_utc"]
                delay_min = int(round((dep_use - sched_dep).total_seconds()/60.0))

            mins_to = int((dep_use - now).total_seconds() // 60)
            is_due = is_live and mins_to <= 0 and mins_to >= -1

            rows.append({
                "route": route or headsign or "–",
                "destination": headsign or "–",
                "time_iso": dep_use.isoformat(),
                "time_display": "Due" if is_due else fmt_hhmm(dep_use),
                "is_live": is_live, "is_due": is_due,
                "delay_min": delay_min,  # + late / - early
                "sched_time_iso": sched_dep.isoformat(),
                "trip_id": tid,
            })

    # dedupe (prefer Due / latest live)
    uniq={}
    for r in rows:
        key=(r["route"], r["destination"], r["time_display"])
        cur=uniq.get(key)
        if not cur or (r["is_due"] and not cur["is_due"]): uniq[key]=r
    rows=list(uniq.values())
    rows.sort(key=lambda r: (not r["is_due"], r["time_iso"]))
    return {"departures": rows}

# -------- Trip details (schedule only) --------
@app.get("/api/trips/{trip_id}")
def api_trip_details(trip_id: str):
    require_data()
    trips = _read_json(TRIP_INDEX_JSON, {})
    trip_stops = _read_json(TRIP_STOPS_JSON, {})
    meta = trips.get(trip_id)
    if not meta: raise HTTPException(404, "Trip not found")

    now = _now_utc(); out=[]
    for s in (trip_stops.get(trip_id) or []):
        dep = datetime.fromisoformat(s["dep_utc"])
        out.append({
            "stop_name": s["stop_name"], "time_iso": dep.isoformat(),
            "time_display": fmt_hhmm(dep), "is_past": dep < now,
            "is_live": False, "is_due": False
        })
    return {"route": norm_line(meta.get("route") or ""), "headsign": meta.get("headsign") or "", "stops": out}

# -------- Vehicles by route --------
@app.get("/api/routes/{route}/vehicles")
def api_route_vehicles(route: str):
    live = fetch_live()["records"]
    now = _now_utc(); want = norm_line(route)
    by_vehicle={}
    for r in live:
        if norm_line(r.get("line","")) != want: continue
        if not r.get("lat") or not r.get("lon"): continue
        ts = datetime.fromisoformat(r["timestamp_utc"])
        if (now - ts).total_seconds() > 120: continue
        vref = r.get("vehicle_ref") or ""; if not vref: continue
        cur = by_vehicle.get(vref)
        if not cur or ts > datetime.fromisoformat(cur["timestamp"]):
            by_vehicle[vref] = {
                "vehicle_ref": vref, "lat": r["lat"], "lon": r["lon"], "bearing": r.get("bearing"),
                "timestamp": ts.isoformat(), "label": f'{want} · {r.get("destination","")}'.strip(),
            }
    return {"vehicles": list(by_vehicle.values())}

# -------- index.html --------
@app.get("/", response_class=HTMLResponse)
def root_page():
    index = BASE_DIR / "index.html"
    if index.exists(): return HTMLResponse(index.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Bluestar Bus API</h1><p>index.html not found.</p>")
