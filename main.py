import io
import json
import zipfile
import csv
import time
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import defaultdict

import requests
import pytz
try:
    import xmltodict  # XML SIRI-VM támogatás
except Exception:
    xmltodict = None

from fastapi import FastAPI, File, HTTPException, UploadFile, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------
# Alapok
# ---------------------------------------------------------
APP_VERSION = "5.0.0"
BUILD = str(int(time.time()))
UK_TZ = pytz.timezone("Europe/London")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"          # statikus/kinyert GTFS JSON
CACHE_DIR = BASE_DIR / "cache"        # élő cache, konfigurációk
DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

STOPS_JSON = DATA_DIR / "stops.json"
ROUTES_JSON = DATA_DIR / "routes.json"
TRIP_INDEX_JSON = DATA_DIR / "trip_index.json"   # trip_id -> {route, headsign, service_id}
TRIP_STOPS_JSON = DATA_DIR / "trip_stops.json"   # trip_id -> [{stop_id, stop_name, dep_utc, seq}]
SERVICE_DAYS_JSON = DATA_DIR / "service_days.json"  # service_id -> {"wdays":[0..6], "dates_add":[], "dates_rm":[]}

LIVE_CFG_PATH = CACHE_DIR / "live_cfg.json"      # {"feed_url": "..."}
LIVE_CACHE_PATH = CACHE_DIR / "siri_vm.json"     # {"records":[...], "ts": "..."}
LIVE_CACHE_MAX_AGE = 20  # mp

# ---------------------------------------------------------
# FastAPI
# ---------------------------------------------------------
app = FastAPI(title="Bluestar Bus – API", version=APP_VERSION, docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

# no-cache mindenre
@app.middleware("http")
async def no_cache_mw(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# statikus (ha használsz /static mappát képekhez, CSS-hez)
STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=False), name="static")

# ---------------------------------------------------------
# Segédek
# ---------------------------------------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _uk_today() -> date:
    return _now_utc().astimezone(UK_TZ).date()

def _fmt_hhmm_from_utc(dt_utc: datetime) -> str:
    """UTC -> UK helyi HH:MM (24h)"""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(UK_TZ).strftime("%H:%M")

def _read_json(path: Path, default=None):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _write_json(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def _ok_status():
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": BUILD,
        "uk_time": _now_utc().astimezone(UK_TZ).strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool(get_live_cfg().get("feed_url")),
        "gtfs_loaded": all(p.exists() for p in [STOPS_JSON, ROUTES_JSON, TRIP_INDEX_JSON, TRIP_STOPS_JSON]),
    }

def _require_data():
    if not all(p.exists() for p in [STOPS_JSON, ROUTES_JSON, TRIP_INDEX_JSON, TRIP_STOPS_JSON]):
        raise HTTPException(503, "GTFS data not uploaded/processed yet.")

# ---------------------------------------------------------
# LIVE feed (BODS SIRI-VM) + cache
# ---------------------------------------------------------
def get_live_cfg() -> Dict[str, str]:
    return _read_json(LIVE_CFG_PATH, default={"feed_url": ""})

def set_live_cfg(payload: Dict[str, str]):
    if not payload or "feed_url" not in payload:
        raise HTTPException(400, "feed_url is required")
    _write_json(LIVE_CFG_PATH, {"feed_url": payload["feed_url"].strip()})

def _parse_vm_json(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """SIRI-VM JSON → normalizált rekordok listája."""
    out: List[Dict[str, Any]] = []
    try:
        deliveries = payload.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
        if isinstance(deliveries, dict):
            deliveries = [deliveries]
        for d in deliveries or []:
            for va in d.get("VehicleActivity", []) or []:
                mvj = va.get("MonitoredVehicleJourney", {}) or {}
                rec_time = va.get("RecordedAtTime") or va.get("ValidUntilTime")
                ts = None
                try:
                    ts = datetime.fromisoformat(str(rec_time).replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    ts = _now_utc()

                loc = mvj.get("VehicleLocation", {}) or {}
                exp_dep = None
                # több helyen is szerepelhet becsült indulás:
                mc = mvj.get("MonitoredCall") or {}
                oc = (mvj.get("OnwardCalls") or {}).get("OnwardCall")
                if isinstance(oc, list) and oc:
                    oc = oc[0]
                exp_dep = mc.get("ExpectedDepartureTime") or (oc or {}).get("ExpectedDepartureTime")

                out.append({
                    "line": str(mvj.get("PublishedLineName") or mvj.get("LineRef") or "").strip(),
                    "vehicle_ref": str(mvj.get("VehicleRef") or mvj.get("VehicleId") or "").strip(),
                    "destination": (mvj.get("DestinationName") or "").strip(),
                    "bearing": mvj.get("Bearing"),
                    "lat": loc.get("Latitude"),
                    "lon": loc.get("Longitude"),
                    "timestamp_utc": ts.isoformat(),
                    "expected_departure_utc": exp_dep,
                })
    except Exception:
        pass
    return out

def _xml_get(obj, key):
    v = obj.get(key)
    if isinstance(v, dict) and "#text" in v:
        return v["#text"]
    return v

def _parse_vm_xml(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """SIRI-VM XML(xmltodict) → normalizált rekordok."""
    out: List[Dict[str, Any]] = []
    try:
        siri = payload.get("Siri", {}) or {}
        sd = siri.get("ServiceDelivery", {}) or {}
        vmd = sd.get("VehicleMonitoringDelivery", {}) or {}
        vas = vmd.get("VehicleActivity") or []
        if isinstance(vas, dict):
            vas = [vas]
        for va in vas:
            mvj = va.get("MonitoredVehicleJourney", {}) or {}
            loc = mvj.get("VehicleLocation", {}) or {}
            rec_time = _xml_get(va, "RecordedAtTime") or _xml_get(va, "ValidUntilTime")
            try:
                ts = datetime.fromisoformat(str(rec_time).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                ts = _now_utc()

            mc = mvj.get("MonitoredCall") or {}
            oc = (mvj.get("OnwardCalls") or {}).get("OnwardCall")
            if isinstance(oc, list) and oc:
                oc = oc[0]
            exp_dep = _xml_get(mc, "ExpectedDepartureTime") or (oc and _xml_get(oc, "ExpectedDepartureTime"))

            out.append({
                "line": str(_xml_get(mvj, "PublishedLineName") or _xml_get(mvj, "LineRef") or "").strip(),
                "vehicle_ref": str(_xml_get(mvj, "VehicleRef") or _xml_get(mvj, "VehicleId") or "").strip(),
                "destination": (_xml_get(mvj, "DestinationName") or "").strip(),
                "bearing": _xml_get(mvj, "Bearing"),
                "lat": _xml_get(loc, "Latitude"),
                "lon": _xml_get(loc, "Longitude"),
                "timestamp_utc": ts.isoformat(),
                "expected_departure_utc": exp_dep,
            })
    except Exception:
        pass
    return out

def fetch_live() -> Dict[str, Any]:
    """BODS SIRI-VM letöltés + 20 mp cache. Vissza: {"records":[...], "ts":"..."}"""
    cfg = get_live_cfg()
    url = cfg.get("feed_url", "").strip()
    now = _now_utc()

    # ha nincs beállítva, üres
    if not url:
        return {"records": [], "ts": now.isoformat()}

    # friss cache?
    cache = _read_json(LIVE_CACHE_PATH, default=None)
    if cache:
        try:
            age = now - datetime.fromisoformat(cache["ts"])
            if age.total_seconds() <= LIVE_CACHE_MAX_AGE:
                return cache
        except Exception:
            pass

    # letöltés
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        content_type = (r.headers.get("content-type") or "").lower()
        records: List[Dict[str, Any]] = []

        if "json" in content_type or r.text.strip().startswith("{"):
            payload = r.json()
            records = _parse_vm_json(payload)
        elif xmltodict is not None:
            payload = xmltodict.parse(r.text)
            records = _parse_vm_xml(payload)
        else:
            records = []
    except Exception:
        # hiba esetén visszaadjuk a régi cache-t, ha van
        if cache:
            return cache
        return {"records": [], "ts": now.isoformat()}

    pack = {"records": records, "ts": now.isoformat()}
    _write_json(LIVE_CACHE_PATH, pack)
    return pack

# ---------------------------------------------------------
# GTFS feldolgozás – calendar + calendar_dates figyelembevétele
# ---------------------------------------------------------
def _parse_time_hhmmss_to_uk_utc(today_local: date, hhmmss: str) -> datetime:
    """GTFS HH:MM:SS (akár >24:00:00) → UK helyi alapú datetime → UTC"""
    h, m, s = [int(x) for x in hhmmss.split(":")]
    extra_days, hour = divmod(h, 24)
    base_local = datetime.combine(today_local, datetime.min.time()).replace(tzinfo=UK_TZ)
    dep_local = base_local + timedelta(days=extra_days, hours=hour, minutes=m, seconds=s)
    return dep_local.astimezone(timezone.utc)

def _weekday_index(d: date) -> int:
    # hétfő=0 ... vasárnap=6 (GTFS-hez jó)
    return d.weekday()

def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    """GTFS beolvasása és a szükséges gyorsító JSON-ok építése."""
    today_local = _uk_today()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = set(zf.namelist())

        def open_csv(name):
            if name not in names:
                raise ValueError(f"Hiányzó GTFS fájl: {name}")
            return csv.DictReader(io.TextIOWrapper(zf.open(name), encoding="utf-8-sig"))

        # -------- stops
        stops = []
        for row in open_csv("stops.txt"):
            stops.append({"id": row["stop_id"], "name": (row.get("stop_name") or "").strip()})
        _write_json(STOPS_JSON, stops)
        stop_name_map = {s["id"]: s["name"] for s in stops}

        # -------- calendar (alap napok)
        service_days: Dict[str, Dict[str, Any]] = {}
        if "calendar.txt" in names:
            for row in open_csv("calendar.txt"):
                service_id = row["service_id"]
                wdays = []
                if row.get("monday") == "1": wdays.append(0)
                if row.get("tuesday") == "1": wdays.append(1)
                if row.get("wednesday") == "1": wdays.append(2)
                if row.get("thursday") == "1": wdays.append(3)
                if row.get("friday") == "1": wdays.append(4)
                if row.get("saturday") == "1": wdays.append(5)
                if row.get("sunday") == "1": wdays.append(6)
                service_days[service_id] = {"wdays": wdays, "dates_add": [], "dates_rm": []}

        # -------- calendar_dates (kivételek)
        if "calendar_dates.txt" in names:
            for row in open_csv("calendar_dates.txt"):
                service_id = row["service_id"]
                dt = row.get("date")  # YYYYMMDD
                if not dt or len(dt) != 8:
                    continue
                y, m, d = int(dt[:4]), int(dt[4:6]), int(dt[6:8])
                if service_id not in service_days:
                    service_days[service_id] = {"wdays": [], "dates_add": [], "dates_rm": []}
                if row.get("exception_type") == "1":
                    service_days[service_id]["dates_add"].append(date(y, m, d))
                elif row.get("exception_type") == "2":
                    service_days[service_id]["dates_rm"].append(date(y, m, d))

        _write_json(SERVICE_DAYS_JSON, {k: {
            "wdays": v["wdays"],
            "dates_add": [d.isoformat() for d in v["dates_add"]],
            "dates_rm": [d.isoformat() for d in v["dates_rm"]],
        } for k, v in service_days.items()})

        # -------- routes
        route_names: Dict[str, str] = {}
        for row in open_csv("routes.txt"):
            rid = row["route_id"]
            nm = (row.get("route_short_name") or row.get("route_long_name") or rid).strip()
            route_names[rid] = nm

        # listázott, deduplikált routes a keresőhöz
        uniq_routes = sorted({v.strip() for v in route_names.values() if v.strip()}, key=lambda x: (len(x), x))
        _write_json(ROUTES_JSON, [{"route": r} for r in uniq_routes])

        # -------- trips
        trips: Dict[str, Dict[str, Any]] = {}
        for row in open_csv("trips.txt"):
            tid = row["trip_id"]
            rid = row["route_id"]
            trips[tid] = {
                "route": route_names.get(rid, rid),
                "headsign": (row.get("trip_headsign") or "").strip(),
                "service_id": row.get("service_id") or "",
            }

        # -------- stop_times (ONLY departure_time)
        trip_stops: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in open_csv("stop_times.txt"):
            tid = row["trip_id"]
            dep = (row.get("departure_time") or "").strip()
            if not dep:  # érkezés nem érdekel
                continue
            seq = int(row.get("stop_sequence") or 0)
            dep_utc = _parse_time_hhmmss_to_uk_utc(today_local, dep)
            sid = row["stop_id"]

            trip_stops[tid].append({
                "stop_id": sid,
                "stop_name": stop_name_map.get(sid, sid),
                "dep_utc": dep_utc.isoformat(),
                "seq": seq,
            })

        # sorba rendez
        for tid, arr in trip_stops.items():
            arr.sort(key=lambda x: x["seq"])

        _write_json(TRIP_INDEX_JSON, trips)
        _write_json(TRIP_STOPS_JSON, trip_stops)

# ---------------------------------------------------------
# Szolgáltatások aktív-e MA? (calendar + dates)
# ---------------------------------------------------------
def _service_active_today(service_id: str) -> bool:
    sd = _read_json(SERVICE_DAYS_JSON, {})
    info = sd.get(service_id)
    if not info:
        # ha nincs naptár info: tekintsük aktívnak (sok feed így jön)
        return True
    # dátumlistákat visszaalakítjuk
    today = _uk_today()
    wdays = set(info.get("wdays") or [])
    add = {date.fromisoformat(x) for x in (info.get("dates_add") or [])}
    rm = {date.fromisoformat(x) for x in (info.get("dates_rm") or [])}
    if today in rm:
        return False
    if today in add:
        return True
    return _weekday_index(today) in wdays

# ---------------------------------------------------------
# API – státusz + index
# ---------------------------------------------------------
@app.get("/api/status")
def api_status():
    return JSONResponse(_ok_status())

@app.get("/", include_in_schema=False)
def root_html():
    # az index.html legyen a repo gyökerében
    idx = BASE_DIR / "index.html"
    if not idx.exists():
        return HTMLResponse("<h1>Bluestar Bus API</h1><p>index.html not found.</p>", status_code=200)
    return FileResponse(str(idx), media_type="text/html")

@app.get("/index.html", include_in_schema=False)
def index_html():
    return root_html()

# ---------------------------------------------------------
# API – LIVE config
# ---------------------------------------------------------
@app.get("/api/live/config")
def api_get_live_cfg():
    return JSONResponse(get_live_cfg())

@app.post("/api/live/config")
def api_set_live_cfg(cfg: Dict[str, str]):
    set_live_cfg(cfg)
    return JSONResponse({"ok": True})

# ---------------------------------------------------------
# API – GTFS UPLOAD
# ---------------------------------------------------------
@app.post("/api/upload")
def api_upload(file: UploadFile = File(...)):
    raw = file.file.read()
    try:
        _build_from_zip_bytes(raw)
    except KeyError as e:
        raise HTTPException(400, f"GTFS missing file: {e}")
    except Exception as e:
        raise HTTPException(400, f"GTFS processing error: {e}")
    return {"status": "uploaded"}

# ---------------------------------------------------------
# API – STOP kereső
# ---------------------------------------------------------
@app.get("/api/stops/search")
def api_stops_search(q: str):
    _require_data()
    ql = q.strip().lower()
    items = _read_json(STOPS_JSON, [])
    res = [s for s in items if ql in (s["name"] or "").lower()]
    return JSONResponse(res[:30])

# ---------------------------------------------------------
# API – Következő indulások (csak indulás!)
#  - naptár szerint MA aktív trip-ek
#  - élő ExpectedDepartureTime, ha elérhető (route-szinten)
#  - Due: ha élő és 0..-1 perc között
# ---------------------------------------------------------
def _live_lookup_by_route() -> Dict[str, Dict[str, Any]]:
    live = fetch_live()["records"]
    now = _now_utc()
    best: Dict[str, Dict[str, Any]] = {}
    for r in live:
        route = (r.get("line") or "").strip()
        if not route:
            continue
        try:
            ts = datetime.fromisoformat(r["timestamp_utc"])
        except Exception:
            ts = now
        # 2 percnél régebbi aktivitást ne tekintsük „élőnek”
        if (now - ts).total_seconds() > 120:
            continue

        exp = r.get("expected_departure_utc")
        exp_dt = None
        if exp:
            try:
                exp_dt = datetime.fromisoformat(str(exp).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                exp_dt = None

        cand = {"is_live": True, "expected_dep_utc": exp_dt, "ts": ts}
        cur = best.get(route)
        if not cur or ts > cur["ts"]:
            best[route] = cand
    return best

@app.get("/api/stops/{stop_id}/next_departures")
def api_next_departures(stop_id: str, window: int = 60):
    _require_data()
    trips = _read_json(TRIP_INDEX_JSON, {})
    trip_stops = _read_json(TRIP_STOPS_JSON, {})

    now = _now_utc()
    end = now + timedelta(minutes=max(1, min(window, 480)))

    live_by_route = _live_lookup_by_route()
    rows: List[Dict[str, Any]] = []

    for tid, meta in trips.items():
        # csak ma aktív szolgáltatás
        sid = meta.get("service_id") or ""
        if not _service_active_today(sid):
            continue

        segments = trip_stops.get(tid) or []
        # keresett megálló és indulási idő ablak
        for s in segments:
            if s["stop_id"] != stop_id:
                continue
            dep = datetime.fromisoformat(s["dep_utc"])
            if dep < now - timedelta(minutes=1) or dep > end:
                continue

            route = (meta.get("route") or "").strip()
            headsign = (meta.get("headsign") or "").strip()
            live_info = live_by_route.get(route)
            is_live = bool(live_info and live_info.get("is_live"))

            dep_use = dep
            if is_live and live_info.get("expected_dep_utc"):
                dep_use = live_info["expected_dep_utc"]

            mins_to 
