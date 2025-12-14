import os
import csv
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

import pytz
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

# httpx opcionális – ha nincs telepítve, live adat nem lesz, de nem dől el az app
try:
    import httpx
except Exception:
    httpx = None

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bluestar")

# ------------------------- Konfig -------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
UK_TZ = pytz.timezone("Europe/London")

ALLOWED_OPERATORS = {"blus", "unil"}  # Bluestar / Unilink

GOOGLE_MAPS_API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

LIVE_TTL = int(os.getenv("LIVE_TTL", "20"))
LIVE_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}

def now_uk() -> datetime:
    return datetime.now(UK_TZ)

def midnight_uk(dt: Optional[datetime] = None) -> datetime:
    dt = dt or now_uk()
    return UK_TZ.localize(datetime(dt.year, dt.month, dt.day, 0, 0, 0))

def operator_ok(op: str) -> bool:
    return (op or "").strip().lower()[:4] in ALLOWED_OPERATORS

def gtfs_sec(hhmmss: str) -> int:
    try:
        h, m, s = (hhmmss or "00:00:00").split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except Exception:
        return 0

def sec_to_today(sec: int) -> datetime:
    base = midnight_uk()
    days = sec // 86400
    rem = sec % 86400
    return base + timedelta(days=days, seconds=rem)

def fmt_hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")

def mins_from_now(dt: datetime) -> int:
    return int(round((dt - now_uk()).total_seconds() / 60))

def parse_iso(dt_str: str) -> Optional[datetime]:
    try:
        if not dt_str:
            return None
        ds = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ds).astimezone(UK_TZ)
    except Exception:
        return None

# ------------------------- SIRI URL autodetekció + header -------------------------
def _first_truthy(*vals):
    for v in vals:
        if v:
            return v
    return ""

def _guess_url(kind: str) -> str:
    cands = []
    for k, v in os.environ.items():
        kk = k.upper()
        vv = str(v or "").strip()
        if not vv.startswith(("http://", "https://")):
            continue
        if kind == "vm" and any(s in kk for s in ("VEHICLE", "VM")):
            cands.append(vv)
        if kind == "sm" and any(s in kk for s in ("STOP", "SM")):
            cands.append(vv)
    return cands[0] if cands else ""

def _build_extra_headers() -> Dict[str, str]:
    h: Dict[str, str] = {}
    if os.getenv("SIRI_KEY_HEADER") and os.getenv("SIRI_KEY_VALUE"):
        h[os.getenv("SIRI_KEY_HEADER")] = os.getenv("SIRI_KEY_VALUE")
    if os.getenv("SIRI_HEADER_NAME") and os.getenv("SIRI_HEADER_VALUE"):
        h[os.getenv("SIRI_HEADER_NAME")] = os.getenv("SIRI_HEADER_VALUE")
    if os.getenv("OCP_APIM_SUBSCRIPTION_KEY"):
        h["Ocp-Apim-Subscription-Key"] = os.getenv("OCP_APIM_SUBSCRIPTION_KEY")
    if os.getenv("X_API_KEY"):
        h["X-API-Key"] = os.getenv("X_API_KEY")
    return h

SIRI_VM_URL_RAW = _first_truthy(
    os.getenv("SIRI_API_VEHICLE_MONITORING"),
    os.getenv("SIRI_VM_URL"),
    _guess_url("vm"),
)
SIRI_SM_URL_RAW = _first_truthy(
    os.getenv("SIRI_STOP_MONITORING"),
    os.getenv("SIRI_SM_URL"),
    _guess_url("sm"),
)
EXTRA_HEADERS = _build_extra_headers()

def _format_vm_url(line_ref: str) -> Tuple[str, Dict[str, str]]:
    u = SIRI_VM_URL_RAW or ""
    if not u:
        return "", {}
    if "{line_ref}" in u:
        return u.replace("{line_ref}", str(line_ref)), {}
    return u, {"LineRef": line_ref}

def _format_sm_url(stop_id: str) -> Tuple[str, Dict[str, str]]:
    u = SIRI_SM_URL_RAW or ""
    if not u:
        return "", {}
    if "{stop_id}" in u:
        return u.replace("{stop_id}", str(stop_id)), {}
    # query param verzió
    return u, {"MonitoringRef": stop_id, "MaximumStopVisits": "10"}

def cache_get(key: Tuple[str, str]):
    v = LIVE_CACHE.get(key)
    if not v:
        return None
    if v["exp"] < datetime.utcnow().timestamp():
        LIVE_CACHE.pop(key, None)
        return None
    return v["val"]

def cache_set(key: Tuple[str, str], val):
    LIVE_CACHE[key] = {"val": val, "exp": datetime.utcnow().timestamp() + LIVE_TTL}

async def http_get_json(url: str, params: Optional[Dict[str, str]] = None):
    if not url or httpx is None:
        return None
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(url, params=params or {}, headers=EXTRA_HEADERS)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        log.warning("live request failed: %s", e)
        return None

async def fetch_live_vm(route_short: str) -> List[Dict[str, Any]]:
    if not route_short:
        return []
    ck = ("vm", route_short.lower())
    cached = cache_get(ck)
    if cached is not None:
        return cached

    out: List[Dict[str, Any]] = []
    url, params = _format_vm_url(route_short)
    data = await http_get_json(url, params=params) if url else None

    try:
        deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", []) or []
        for d in deliveries:
            for a in (d.get("VehicleActivity") or []):
                j = a.get("MonitoredVehicleJourney", {}) or {}
                line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                op = (j.get("OperatorRef") or "").strip()
                if line and route_short.lower() != str(line).lower():
                    continue
                if op and not operator_ok(op):
                    continue
                loc = j.get("VehicleLocation") or {}
                lat = loc.get("Latitude"); lon = loc.get("Longitude")
                if lat is None or lon is None:
                    continue
                out.append({
                    "lat": float(lat),
                    "lon": float(lon),
                    "fleet": str(j.get("VehicleRef") or a.get("VehicleRef") or ""),
                    "line": str(line),
                    "operator": (op or "").lower()[:4],
                })
    except Exception as e:
        log.warning("parse VM failed: %s", e)
        out = []

    cache_set(ck, out)
    return out

async def fetch_live_sm(stop_code_or_id: str) -> List[Dict[str, Any]]:
    ck = ("sm", stop_code_or_id)
    cached = cache_get(ck)
    if cached is not None:
        return cached

    items: List[Dict[str, Any]] = []
    url, params = _format_sm_url(stop_code_or_id)
    data = await http_get_json(url, params=params) if url else None

    try:
        deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("StopMonitoringDelivery", []) or []
        for d in deliveries:
            for v in (d.get("MonitoredStopVisit") or []):
                j = v.get("MonitoredVehicleJourney", {}) or {}
                line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                op = (j.get("OperatorRef") or "").strip()
                if op and not operator_ok(op):
                    continue
                call = j.get("MonitoredCall") or {}
                aimed = call.get("AimedDepartureTime") or call.get("AimedArrivalTime")
                exp = call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime")
                dep_dt = parse_iso(exp) or parse_iso(aimed)
                delay_text = ""
                delay_mins: Optional[int] = None
                if aimed and exp:
                    a = parse_iso(aimed); e = parse_iso(exp)
                    if a and e:
                        dm = int(round((e - a).total_seconds() / 60.0))
                        if dm != 0:
                            delay_mins = dm
                            delay_text = f"{dm:+d}m"
                is_due = bool(dep_dt and abs((now_uk() - dep_dt).total_seconds()) < 60)

                items.append({
                    "line": line,
                    "operator": (op or "").lower()[:4],
                    "headsign": j.get("DestinationName") or "",
                    "vehicle_ref": j.get("VehicleRef") or "",
                    "dep_dt": dep_dt,
                    "delay_mins": delay_mins,
                    "delay_text": delay_text,
                    "is_due": is_due,
                    "trip_id": (j.get("FramedVehicleJourneyRef", {}) or {}).get("DatedVehicleJourneyRef") or "",
                })
    except Exception as e:
        log.warning("parse SM failed: %s", e)
        items = []

    cache_set(ck, items)
    return items

# ------------------------- GTFS betöltés -------------------------
routes = []; stops = []; trips = []; stop_times = []

routes_by_id = {}
routes_by_short = defaultdict(list)

stops_by_id = {}
stops_by_code = {}

trips_by_id = {}
stop_times_by_stop = defaultdict(list)
stop_times_by_trip = defaultdict(list)

def _read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def load_gtfs():
    rp = os.path.join(DATA_DIR, "routes.txt")
    sp = os.path.join(DATA_DIR, "stops.txt")
    tp = os.path.join(DATA_DIR, "trips.txt")
    stp = os.path.join(DATA_DIR, "stop_times.txt")

    routes[:] = _read_csv(rp)
    stops[:] = _read_csv(sp)
    trips[:] = _read_csv(tp)
    stop_times[:] = _read_csv(stp)

    routes_by_id.clear(); routes_by_short.clear()
    for r in routes:
        rid = (r.get("route_id") or "").strip()
        routes_by_id[rid] = r
        short = (r.get("route_short_name") or "").strip()
        if short:
            routes_by_short[short.lower()].append(r)

    stops_by_id.clear(); stops_by_code.clear()
    for s in stops:
        sid = (s.get("stop_id") or "").strip()
        sc = (s.get("stop_code") or "").strip()
        if sid:
            stops_by_id[sid] = s
        if sc:
            stops_by_code[sc] = s

    trips_by_id.clear()
    for t in trips:
        tid = (t.get("trip_id") or "").strip()
        if tid:
            trips_by_id[tid] = t

    stop_times_by_stop.clear(); stop_times_by_trip.clear()
    for st in stop_times:
        sid = (st.get("stop_id") or "").strip()
        tid = (st.get("trip_id") or "").strip()
        if sid:
            stop_times_by_stop[sid].append(st)
        if tid:
            stop_times_by_trip[tid].append(st)

    for tid, arr in stop_times_by_trip.items():
        arr.sort(key=lambda x: gtfs_sec(x.get("departure_time") or x.get("arrival_time") or ""))

def stop_by_any(id_or_code: str) -> Optional[Dict[str, str]]:
    return stops_by_id.get(id_or_code) or stops_by_code.get(id_or_code)

def list_routes_compact() -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in routes:
        short = (r.get("route_short_name") or "").strip()
        if not short:
            continue
        ag = (r.get("agency_id") or "").strip().lower()[:4]
        if ag and ag not in ALLOWED_OPERATORS:
            continue
        key = short.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"short": short, "key": key, "operator": ag or ""})

    def sk(x):
        s = x["short"]
        num = ""; suf = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            else:
                suf += ch
        return (int(num or 0), suf.lower())

    out.sort(key=sk)
    return out

async def build_departure_rows_for_stop(stop_obj: Dict[str, str], minutes_ahead: int = 120) -> List[Dict[str, Any]]:
    now = now_uk()
    until = now + timedelta(minutes=minutes_ahead)
    sid = (stop_obj.get("stop_id") or "").strip()
    scode = (stop_obj.get("stop_code") or stop_obj.get("stop_id") or "").strip()

    # LIVE: key = (trip_id, line_lower)
    live_raw = await fetch_live_sm(scode)
    live_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for it in live_raw or []:
        if not it.get("dep_dt"):
            continue
        key = ((it.get("trip_id") or "").strip(), (it.get("line") or "").strip().lower())
        live_by_key[key] = it

    rows: List[Dict[str, Any]] = []

    # MENETRENDI + LIVE override
    for st in stop_times_by_stop.get(sid, []):
        tid = (st.get("trip_id") or "").strip()
        trip = trips_by_id.get(tid, {}) or {}
        rid = (trip.get("route_id") or "").strip()
        route = routes_by_id.get(rid, {}) or {}

        route_short = (route.get("route_short_name") or "").strip()
        agency = (route.get("agency_id") or "").strip().lower()[:4]
        if agency and agency not in ALLOWED_OPERATORS:
            continue
        if not route_short:
            continue

        dep_sec = gtfs_sec(st.get("departure_time") or st.get("arrival_time") or "")
        dep_dt = sec_to_today(dep_sec)
        if dep_dt < (now - timedelta(minutes=1)) or dep_dt > until:
            continue

        headsign = (trip.get("trip_headsign") or "").strip()
        key = (tid, route_short.lower())
        live_hit = live_by_key.get(key)

        if live_hit:
            ldt: datetime = live_hit["dep_dt"]
            rows.append({
                "time": fmt_hhmm(ldt),
                "time_dt": ldt.isoformat(),
                "route_short": route_short,
                "route_key": route_short.lower(),
                "headsign": (live_hit.get("headsign") or headsign),
                "is_live": True,
                "is_due": bool(live_hit.get("is_due")),
                "fleet": (live_hit.get("vehicle_ref") or ""),
                "delay_mins": live_hit.get("delay_mins"),
                "delay_text": live_hit.get("delay_text") or "",
                "wait_mins": mins_from_now(ldt),
            })
        else:
            rows.append({
                "time": fmt_hhmm(dep_dt),
                "time_dt": dep_dt.isoformat(),
                "route_short": route_short,
                "route_key": route_short.lower(),
                "headsign": headsign,
                "is_live": False,
                "is_due": False,
                "fleet": "",
                "delay_mins": None,
                "delay_text": "",
                "wait_mins": mins_from_now(dep_dt),
            })

    # LIVE-only sorok (amire nincs GTFS match)
    for it in (live_raw or []):
        d: Optional[datetime] = it.get("dep_dt")
        if not d:
            continue
        if d < (now - timedelta(minutes=1)) or d > until:
            continue
        key = ((it.get("trip_id") or "").strip(), (it.get("line") or "").strip().lower())
        if any((r.get("route_key") == key[1] and r.get("is_live")) for r in rows):
            # ha már volt ugyanarra élő, ne szemeteljünk
            continue

        rows.append({
            "time": fmt_hhmm(d),
            "time_dt": d.isoformat(),
            "route_short": it.get("line") or "",
            "route_key": (it.get("line") or "").strip().lower(),
            "headsign": it.get("headsign") or "",
            "is_live": True,
            "is_due": bool(it.get("is_due")),
            "fleet": (it.get("vehicle_ref") or ""),
            "delay_mins": it.get("delay_mins"),
            "delay_text": it.get("delay_text") or "",
            "wait_mins": mins_from_now(d),
        })

    # Rendezés: idő szerint, live előre
    def sortk(r):
        try:
            dt = datetime.fromisoformat(r["time_dt"])
        except Exception:
            dt = now_uk()
        return (dt, 0 if r.get("is_live") else 1)

    rows.sort(key=sortk)
    return rows

# ------------------------- App + Templates -------------------------
app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
def _startup():
    load_gtfs()
    log.info("GTFS loaded routes=%d stops=%d", len(routes), len(stops))

@app.get("/healthz")
def healthz():
    return JSONResponse({"ok": True})

@app.get("/c")
def diag():
    return JSONResponse({
        "DATA_DIR": DATA_DIR,
        "routes_count": len(routes),
        "stops_count": len(stops),
        "httpx_available": httpx is not None,
        "live_vm_url": SIRI_VM_URL_RAW,
        "live_sm_url": SIRI_SM_URL_RAW,
        "extra_headers": list(EXTRA_HEADERS.keys()),
        "google_maps_api_key_set": bool(GOOGLE_MAPS_API_KEY),
    })

# ------------------------- HTML oldalak -------------------------
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    ctx = {
        "request": request,
        "routes": list_routes_compact(),
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "google_maps_api_key": GOOGLE_MAPS_API_KEY,
    }
    return templates.TemplateResponse("index.html", ctx)

@app.get("/search", response_class=HTMLResponse)
def search_page(request: Request, q: str = ""):
    q = (q or "").strip()
    routes_found = []
    stops_found = []

    if q:
        ql = q.lower()
        for short, arr in routes_by_short.items():
            if ql in short:
                r = arr[0]
                ag = (r.get("agency_id") or "").strip().lower()[:4]
                if ag and ag not in ALLOWED_OPERATORS:
                    continue
                routes_found.append({"short": r.get("route_short_name"), "key": short, "operator": ag})
        for s in stops:
            name = (s.get("stop_name") or "")
            if ql in name.lower():
                stops_found.append({
                    "id": s.get("stop_id"),
                    "code": s.get("stop_code") or s.get("stop_id"),
                    "name": name
                })

    ctx = {
        "request": request,
        "q": q,
        "routes": routes_found,
        "stops": stops_found,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "google_maps_api_key": GOOGLE_MAPS_API_KEY,
    }
    return templates.TemplateResponse("search.html", ctx)

@app.get("/stop", response_class=HTMLResponse)
def stop_missing():
    return RedirectResponse("/")

@app.get("/stop/{sid_or_code}", response_class=HTMLResponse)
async def stop_view(request: Request, sid_or_code: str):
    s = stop_by_any(sid_or_code)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")
    rows = await build_departure_rows_for_stop(s, minutes_ahead=180)
    ctx = {
        "request": request,
        "stop": s,
        "rows": rows,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "google_maps_api_key": GOOGLE_MAPS_API_KEY,
    }
    return templates.TemplateResponse("stop.html", ctx)

@app.get("/r", response_class=HTMLResponse)
def route_missing():
    return RedirectResponse("/")

@app.get("/r/{route_key}", response_class=HTMLResponse)
async def route_view(request: Request, route_key: str):
    key = (route_key or "").strip().lower()
    # route_short alapján
    rlist = routes_by_short.get(key, [])
    if not rlist:
        # route_id fallback
        r = routes_by_id.get(route_key)
        if r:
            key = (r.get("route_short_name") or "").strip().lower()
            rlist = [r]
    if not rlist:
        raise HTTPException(status_code=404, detail="Route not found")

    route_short = (rlist[0].get("route_short_name") or "").strip()
    live = await fetch_live_vm(route_short)

    ctx = {
        "request": request,
        "route_short": route_short,
        "route_key": route_short.lower(),
        "live_vehicles": live,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "google_maps_api_key": GOOGLE_MAPS_API_KEY,
    }
    return templates.TemplateResponse("route.html", ctx)

# ------------------------- JSON API (frontend frissítéshez) -------------------------
@app.get("/api/routes")
def api_routes():
    return JSONResponse({"routes": list_routes_compact(), "now_uk": now_uk().isoformat()})

@app.get("/api/search")
def api_search(q: str = ""):
    q = (q or "").strip().lower()
    routes_found = []
    stops_found = []
    if q:
        for short, arr in routes_by_short.items():
            if q in short:
                r = arr[0]
                ag = (r.get("agency_id") or "").strip().lower()[:4]
                if ag and ag not in ALLOWED_OPERATORS:
                    continue
                routes_found.append({"short": r.get("route_short_name"), "key": short, "operator": ag})
        for s in stops:
            name = (s.get("stop_name") or "")
            if q in name.lower():
                stops_found.append({
                    "id": s.get("stop_id"),
                    "code": s.get("stop_code") or s.get("stop_id"),
                    "name": name
                })
    return JSONResponse({"q": q, "routes": routes_found, "stops": stops_found})

@app.get("/api/route/{route_short}/vehicles")
async def api_route_vehicles(route_short: str):
    route_short = (route_short or "").strip()
    if not route_short:
        return JSONResponse({"vehicles": []})
    live = await fetch_live_vm(route_short)
    return JSONResponse({"route": route_short, "vehicles": live, "now_uk": now_uk().isoformat()})

@app.get("/api/stop/{sid_or_code}/departures")
async def api_stop_departures(sid_or_code: str):
    s = stop_by_any(sid_or_code)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")
    rows = await build_departure_rows_for_stop(s, minutes_ahead=180)
    return JSONResponse({
        "stop": {
            "stop_id": s.get("stop_id"),
            "stop_code": s.get("stop_code") or s.get("stop_id"),
            "stop_name": s.get("stop_name"),
            "lat": s.get("stop_lat"),
            "lon": s.get("stop_lon"),
        },
        "rows": rows,
        "now_uk": now_uk().isoformat()
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
