import os
import csv
import json
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict

import pytz
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

# httpx opcionális – de ha nincs, a live egyszerűen üres lesz, nem dob 500-at
try:
    import httpx
except Exception:
    httpx = None

# ----------------------------- Alapok -----------------------------
UK_TZ = pytz.timezone("Europe/London")
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
ALLOWED_OPERATORS = {"blus", "unil"}  # csak ezeket engedjük át

def now_uk():
    return datetime.now(UK_TZ)

def midnight_uk(dt=None):
    dt = dt or now_uk()
    return UK_TZ.localize(datetime(dt.year, dt.month, dt.day, 0, 0, 0))

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
    return int(round((dt - now_uk()).total_seconds() / 60.0))

# ---------------------- Live ENV autodetekció ---------------------
def _first_truthy(*vals):
    for v in vals:
        if v:
            return v
    return ""

def _guess_url(kind: str) -> str:
    """
    kind: 'vm' (VehicleMonitoring) vagy 'sm' (StopMonitoring)
    Elfogad rengeteg névvariánst – az első http(s) értéket visszaadja.
    """
    cands = []
    for k, v in os.environ.items():
        kk = k.upper()
        if not isinstance(v, str):
            continue
        vv = v.strip()
        if not vv or not vv.lower().startswith("http"):
            continue
        if kind == "vm" and ("VEHICLE" in kk or "VM" in kk or kk.endswith("_VM") or kk.endswith("_VEHICLE")):
            cands.append(vv)
        if kind == "sm" and ("STOP" in kk or "SM" in kk or kk.endswith("_SM") or kk.endswith("_STOP")):
            cands.append(vv)
    return cands[0] if cands else ""

def _build_extra_headers() -> dict:
    h = {}
    # 1) explicit páros
    if os.getenv("SIRI_KEY_HEADER") and os.getenv("SIRI_KEY_VALUE"):
        h[os.getenv("SIRI_KEY_HEADER")] = os.getenv("SIRI_KEY_VALUE")
    # 2) általános HEAD/KEY
    if os.getenv("SIRI_HEADER_NAME") and os.getenv("SIRI_HEADER_VALUE"):
        h[os.getenv("SIRI_HEADER_NAME")] = os.getenv("SIRI_HEADER_VALUE")
    # 3) rövidebb variánsok
    #    pl. SIRI_HEAD_NAME / SIRI_KEY (korábbi screenshot alapján)
    for k, v in os.environ.items():
        ku = k.upper()
        if ku.startswith("SIRI_HEAD") and v:
            # próbálunk mellé VALUE-t találni
            if os.getenv("SIRI_KEY") or os.getenv("SIRI_KEY_VALUE"):
                h[v] = os.getenv("SIRI_KEY") or os.getenv("SIRI_KEY_VALUE")
    # 4) BODS tipikus
    if os.getenv("OCP_APIM_SUBSCRIPTION_KEY"):
        h["Ocp-Apim-Subscription-Key"] = os.getenv("OCP_APIM_SUBSCRIPTION_KEY")
    return h

SIRI_VM_URL = _first_truthy(
    os.getenv("SIRI_API_VEHICLE_MONITORING"),
    os.getenv("SIRI_VM_URL"),
    _guess_url("vm"),
)

SIRI_SM_URL = _first_truthy(
    os.getenv("SIRI_STOP_MONITORING"),
    os.getenv("SIRI_SM_URL"),
    _guess_url("sm"),
)

EXTRA_HEADERS = _build_extra_headers()

# ------------------------------ App -------------------------------
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --------------------------- GTFS betöltés ------------------------
routes = []
stops = []
trips = []
stop_times = []

routes_by_id = {}
routes_by_short = defaultdict(list)
stops_by_id = {}
stops_by_code = {}
trips_by_id = {}
trips_by_route = defaultdict(list)
stop_times_by_stop = defaultdict(list)
stop_times_by_trip = defaultdict(list)

def _read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def load_gtfs():
    routes_path = os.path.join(DATA_DIR, "routes.txt")
    stops_path = os.path.join(DATA_DIR, "stops.txt")
    trips_path = os.path.join(DATA_DIR, "trips.txt")
    st_path = os.path.join(DATA_DIR, "stop_times.txt")

    routes[:] = _read_csv(routes_path)
    stops[:] = _read_csv(stops_path)
    trips[:] = _read_csv(trips_path)
    stop_times[:] = _read_csv(st_path)

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
        if sid: stops_by_id[sid] = s
        sc = (s.get("stop_code") or "").strip()
        if sc: stops_by_code[sc] = s

    trips_by_id.clear(); trips_by_route.clear()
    for t in trips:
        tid = (t.get("trip_id") or "").strip()
        rid = (t.get("route_id") or "").strip()
        if tid: trips_by_id[tid] = t
        if rid: trips_by_route[rid].append(t)

    stop_times_by_stop.clear(); stop_times_by_trip.clear()
    for st in stop_times:
        sid = (st.get("stop_id") or "").strip()
        tid = (st.get("trip_id") or "").strip()
        if sid: stop_times_by_stop[sid].append(st)
        if tid: stop_times_by_trip[tid].append(st)
    for tid, arr in stop_times_by_trip.items():
        arr.sort(key=lambda x: gtfs_sec(x.get("departure_time") or x.get("arrival_time") or ""))

# ----------------------------- Live hívások -----------------------
LIVE_CACHE = {}
LIVE_TTL = 20  # másodperc

def cache_get(k):
    v = LIVE_CACHE.get(k)
    if not v: return None
    if v["exp"] < datetime.utcnow().timestamp():
        LIVE_CACHE.pop(k, None)
        return None
    return v["val"]

def cache_set(k, val):
    LIVE_CACHE[k] = {"val": val, "exp": datetime.utcnow().timestamp() + LIVE_TTL}

async def http_get_json(url, params=None):
    if not url or httpx is None:
        return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params or {}, headers=EXTRA_HEADERS)
            r.raise_for_status()
            return r.json()
    except Exception:
        return None

def operator_ok(op: str) -> bool:
    return (op or "").strip().lower()[:4] in ALLOWED_OPERATORS

async def fetch_live_vm(route_short: str):
    ck = ("vm", (route_short or "").lower())
    c = cache_get(ck)
    if c is not None:
        return c
    out = []
    if SIRI_VM_URL:
        data = await http_get_json(SIRI_VM_URL, params={"LineRef": route_short})
        try:
            deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
            for d in deliveries:
                for a in d.get("VehicleActivity", []):
                    j = a.get("MonitoredVehicleJourney", {}) or {}
                    line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                    op = (j.get("OperatorRef") or "").strip()
                    if route_short and line and route_short.lower() != str(line).lower():
                        continue
                    if op and not operator_ok(op):
                        continue
                    loc = j.get("VehicleLocation") or {}
                    lat = loc.get("Latitude")
                    lon = loc.get("Longitude")
                    if lat is None or lon is None:
                        continue
                    out.append({
                        "lat": float(lat), "lon": float(lon),
                        "bearing": j.get("Bearing"),
                        "fleet": str(j.get("VehicleRef") or a.get("VehicleRef") or ""),
                        "line": str(line), "operator": op.lower()[:4]
                    })
        except Exception:
            out = []
    cache_set(ck, out)
    return out

async def fetch_live_sm(stop_code_or_id: str):
    ck = ("sm", stop_code_or_id)
    c = cache_get(ck)
    if c is not None:
        return c
    items = []
    if SIRI_SM_URL:
        data = await http_get_json(SIRI_SM_URL, params={"MonitoringRef": stop_code_or_id})
        try:
            deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("StopMonitoringDelivery", [])
            for d in deliveries:
                for v in d.get("MonitoredStopVisit", []):
                    j = v.get("MonitoredVehicleJourney", {}) or {}
                    line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                    op = (j.get("OperatorRef") or "").strip()
                    if op and not operator_ok(op):
                        continue
                    call = j.get("MonitoredCall") or {}
                    aimed = call.get("AimedDepartureTime") or call.get("AimedArrivalTime")
                    exp = call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime")
                    dep_dt = None
                    delay_text = ""
                    is_due = False
                    try:
                        if exp:
                            dep_dt = datetime.fromisoformat(exp.replace("Z", "+00:00")).astimezone(UK_TZ)
                        elif aimed:
                            dep_dt = datetime.fromisoformat(aimed.replace("Z", "+00:00")).astimezone(UK_TZ)
                        if aimed and exp:
                            a = datetime.fromisoformat(aimed.replace("Z", "+00:00"))
                            e = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                            mins = round((e - a).total_seconds() / 60.0)
                            if mins != 0:
                                delay_text = f"{mins:+d}m"
                        if dep_dt:
                            is_due = abs((now_uk() - dep_dt).total_seconds()) < 60
                    except Exception:
                        pass
                    items.append({
                        "line": line,
                        "operator": op.lower()[:4],
                        "headsign": j.get("DestinationName"),
                        "vehicle_ref": j.get("VehicleRef"),
                        "dep_dt": dep_dt,
                        "delay_text": delay_text,
                        "is_due": is_due,
                        "trip_id": j.get("FramedVehicleJourneyRef", {}).get("DatedVehicleJourneyRef") or "",
                    })
        except Exception:
            items = []
    cache_set(ck, items)
    return items

# ----------------------------- Segédek ----------------------------
def stop_by_any(id_or_code: str):
    return stops_by_id.get(id_or_code) or stops_by_code.get(id_or_code)

def routes_for_short(short_lower: str):
    lst = routes_by_short.get(short_lower, [])
    if not lst: return []
    res = []
    for r in lst:
        ag = (r.get("agency_id") or "").strip().lower()[:4]
        if ag and ag not in ALLOWED_OPERATORS:
            continue
        res.append(r)
    return res or lst

async def rows_for_stop(stop_obj, minutes_ahead=120):
    now = now_uk()
    until = now + timedelta(minutes=minutes_ahead)
    sid = stop_obj.get("stop_id")
    scode = (stop_obj.get("stop_code") or stop_obj.get("stop_id") or "").strip()

    live_raw = await fetch_live_sm(scode)
    live_by_trip = {}
    for it in live_raw or []:
        if not it.get("dep_dt"): 
            continue
        if it.get("operator") not in ALLOWED_OPERATORS:
            continue
        key = (it.get("trip_id") or "", it.get("line") or "", it.get("vehicle_ref") or "")
        live_by_trip[key] = it

    rows = []
    for st in stop_times_by_stop.get(sid, []):
        tid = st.get("trip_id")
        trip = trips_by_id.get(tid, {})
        rid = (trip or {}).get("route_id", "")
        route = routes_by_id.get(rid, {})
        route_short = (route.get("route_short_name") or "").strip()
        agency = (route.get("agency_id") or "").strip().lower()[:4]
        if agency and agency not in ALLOWED_OPERATORS:
            continue

        dep_sec = gtfs_sec(st.get("departure_time") or st.get("arrival_time") or "")
        dep_dt = sec_to_today(dep_sec)
        if dep_dt < (now - timedelta(minutes=1)) or dep_dt > until:
            continue

        headsign = (trip.get("trip_headsign") or "").strip()
        k = (tid or "", route_short, "")
        live_hit = live_by_trip.get(k)

        if live_hit:
            rows.append({
                "time_str": fmt_hhmm(live_hit["dep_dt"]),
                "headsign": live_hit.get("headsign") or headsign,
                "route_short": route_short or live_hit.get("line") or "",
                "wait_mins": mins_from_now(live_hit["dep_dt"]),
                "is_live": True,
                "is_due": live_hit.get("is_due", False),
                "row_class": "live due" if live_hit.get("is_due") else "live",
                "trip_id": tid,
                "fleet": live_hit.get("vehicle_ref") or "",
                "delay_text": live_hit.get("delay_text") or "",
            })
        else:
            rows.append({
                "time_str": fmt_hhmm(dep_dt),
                "headsign": headsign,
                "route_short": route_short,
                "wait_mins": mins_from_now(dep_dt),
                "is_live": False,
                "is_due": False,
                "row_class": "timetable",
                "trip_id": tid,
                "fleet": "",
                "delay_text": "",
            })

    # élők, amikhez nincs menetrendi pár
    for _, it in (live_by_trip or {}).items():
        if any(r["is_live"] and r["trip_id"] == (it.get("trip_id") or "") for r in rows):
            continue
        d = it.get("dep_dt")
        if not d:
            continue
        if d < (now - timedelta(minutes=1)) or d > until:
            continue
        rows.append({
            "time_str": fmt_hhmm(d),
            "headsign": it.get("headsign") or "",
            "route_short": it.get("line") or "",
            "wait_mins": mins_from_now(d),
            "is_live": True,
            "is_due": it.get("is_due", False),
            "row_class": "live due" if it.get("is_due") else "live",
            "trip_id": it.get("trip_id") or "",
            "fleet": it.get("vehicle_ref") or "",
            "delay_text": it.get("delay_text") or "",
        })

    rows.sort(key=lambda x: (x["time_str"], 0 if x["is_live"] else 1))
    return rows

# ------------------------------ Endpontok -------------------------
@app.on_event("startup")
def _startup():
    load_gtfs()

@app.get("/healthz")
def healthz():
    return PlainTextResponse("ok")

@app.get("/c")
def diag():
    return JSONResponse({
        "DATA_DIR": DATA_DIR,
        "routes.txt": os.path.exists(os.path.join(DATA_DIR, "routes.txt")),
        "stops.txt": os.path.exists(os.path.join(DATA_DIR, "stops.txt")),
        "trips.txt": os.path.exists(os.path.join(DATA_DIR, "trips.txt")),
        "stop_times.txt": os.path.exists(os.path.join(DATA_DIR, "stop_times.txt")),
        "routes_count": len(routes),
        "stops_count": len(stops),
        "live_enabled": bool(SIRI_VM_URL or SIRI_SM_URL),
        "requests_available": httpx is not None,
        "live_cache_ok": True,
        "live_cache_err": "",
        "vm_url": SIRI_VM_URL,
        "sm_url": SIRI_SM_URL,
        "extra_headers": list(EXTRA_HEADERS.keys()),
    })

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    # egyedi short-ok only, csak Bluestar/Unilink
    seen = set()
    items = []
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
        # A sablonod valószínűleg a 'agency' mezőt írja ki – ezért oda is a short-ot tesszük,
        # hogy a link szöveg mindig a járatszám legyen.
        items.append({
            "short": short,
            "agency": short,   # <<— ez jelenjen meg szövegként
            "operator": ag or ""
        })
    # egyszerű rendezés: előbb szám, majd betű
    def sort_key(x):
        s = x["short"]
        num = ""; suf = ""
        for ch in s:
            if ch.isdigit(): num += ch
            else: suf += ch
        return (int(num or 0), suf.lower())
    items.sort(key=sort_key)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "routes": items,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

@app.get("/r/{route_key}", response_class=HTMLResponse)
async def route_view(request: Request, route_key: str):
    # támogatjuk a régi „BLUS:...:19a” formát – az utolsó darab a short
    short = (route_key.split(":")[-1]).lower()
    rlist = routes_for_short(short)
    if not rlist:
        raise HTTPException(status_code=404, detail="Route not found")

    live = await fetch_live_vm(short.upper() if any(c.isalpha() for c in short) else short)
    live = [v for v in (live or []) if (v.get("operator") or "") in ALLOWED_OPERATORS]

    return templates.TemplateResponse("route.html", {
        "request": request,
        "route_short": rlist[0].get("route_short_name"),
        "live_vehicles": live,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

@app.get("/stop/{sid_or_code}", response_class=HTMLResponse)
async def stop_view(request: Request, sid_or_code: str):
    s = stop_by_any(sid_or_code)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")

    rows = await rows_for_stop(s)
    return templates.TemplateResponse("stop.html", {
        "request": request,
        "stop": s,
        "rows": rows,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

@app.get("/t/{trip_id}", response_class=HTMLResponse)
async def trip_view(request: Request, trip_id: str):
    trip = trips_by_id.get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    rid = trip.get("route_id", "")
    route = routes_by_id.get(rid, {})
    route_short = (route.get("route_short_name") or "").strip()

    sts = list(stop_times_by_trip.get(trip_id, []))
    rows = []
    for st in sts:
        sid = st.get("stop_id")
        dep_sec = gtfs_sec(st.get("departure_time") or st.get("arrival_time") or "")
        dep_dt = sec_to_today(dep_sec)
        s = stops_by_id.get(sid, {})
        rows.append({
            "time_str": fmt_hhmm(dep_dt),
            "headsign": s.get("stop_name") or "",
            "route_short": route_short,
            "wait_mins": mins_from_now(dep_dt),
            "is_live": False,
            "is_due": False,
            "row_class": "timetable",
            "trip_id": trip_id,
            "fleet": "",
            "delay_text": "",
            "lat": float(s.get("stop_lat") or 0) if s else None,
            "lon": float(s.get("stop_lon") or 0) if s else None,
        })

    live = await fetch_live_vm(route_short)
    delay_text_top = ""
    if live:
        for v in live:
            if v.get("operator") in ALLOWED_OPERATORS and v.get("line", "").lower() == route_short.lower():
                # ha lenne delay mező a VM-ben, itt lehetne kiolvasni; placeholder marad
                delay_text_top = delay_text_top or ""
    return templates.TemplateResponse("trip.html", {
        "request": request,
        "trip": trip,
        "route_short": route_short,
        "rows": rows,
        "live_vehicles": live or [],
        "delay_text_top": delay_text_top,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = ""):
    q = (q or "").strip()
    routes_found = []
    stops_found = []
    if q:
        for short, arr in routes_by_short.items():
            if q.lower() in short:
                r = arr[0]
                ag = (r.get("agency_id") or "").strip().lower()[:4]
                if ag and ag not in ALLOWED_OPERATORS:
                    continue
                routes_found.append({"short": r.get("route_short_name"), "agency": r.get("route_short_name")})
        for s in stops:
            name = (s.get("stop_name") or "")
            if q.lower() in name.lower():
                stops_found.append({
                    "id": s.get("stop_id"),
                    "code": s.get("stop_code"),
                    "name": name
                })
    return templates.TemplateResponse("search.html", {
        "request": request,
        "q": q,
        "routes": routes_found,
        "stops": stops_found,
        "now_uk": now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
