import os
import csv
import math
import json
import asyncio
from datetime import datetime, timedelta, date
from collections import defaultdict

import pytz
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

# ---- opcionális, de hasznos, hogy ne legyen 500 import hiba ----
try:
    import httpx
except Exception:  # Railway-n néha requirements telepítés előtt fut be
    httpx = None

# ---------------------------------------------------------------
# Beállítások / környezet
# ---------------------------------------------------------------
UK_TZ = pytz.timezone("Europe/London")
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
ALLOWED_OPERATORS = {"blus", "unil"}  # csak ezek látszódjanak a térképen

# SIRI / BODS env-k – rugalmas, ha nincs megadva, a live csendben üres lesz
SIRI_VM_URL = os.getenv("SIRI_API_VEHICLE_MONITORING", "")
SIRI_SM_URL = os.getenv("SIRI_STOP_MONITORING", "")
SIRI_KEY_HEADER = os.getenv("SIRI_KEY_HEADER", "").strip()
SIRI_KEY_VALUE = os.getenv("SIRI_KEY_VALUE", "").strip()
EXTRA_HEADERS = {}
if SIRI_KEY_HEADER and SIRI_KEY_VALUE:
    EXTRA_HEADERS[SIRI_KEY_HEADER] = SIRI_KEY_VALUE
# egyes szolgáltatók a "Ocp-Apim-Subscription-Key" fejlécet kérik – ezt is támogatjuk
if os.getenv("SIRI_HEADER_NAME") and os.getenv("SIRI_HEADER_VALUE"):
    EXTRA_HEADERS[os.getenv("SIRI_HEADER_NAME")] = os.getenv("SIRI_HEADER_VALUE")

# ---------------------------------------------------------------
# App
# ---------------------------------------------------------------
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------
# GTFS betöltés (memóriába), masszív, de védett
# ---------------------------------------------------------------
routes = []
stops = []
trips = []
stop_times = []

routes_by_short = defaultdict(list)
routes_by_id = {}
stops_by_id = {}
stops_by_code = {}
trips_by_id = {}
trips_by_route_id = defaultdict(list)
stop_times_by_stop_id = defaultdict(list)
stop_times_by_trip_id = defaultdict(list)

def _safe_read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def load_gtfs():
    global routes, stops, trips, stop_times
    (routes.clear(), stops.clear(), trips.clear(), stop_times.clear())
    routes_path = os.path.join(DATA_DIR, "routes.txt")
    stops_path = os.path.join(DATA_DIR, "stops.txt")
    trips_path = os.path.join(DATA_DIR, "trips.txt")
    stop_times_path = os.path.join(DATA_DIR, "stop_times.txt")

    for d in (routes_by_short, routes_by_id, stops_by_id, stops_by_code,
              trips_by_id, trips_by_route_id, stop_times_by_stop_id, stop_times_by_trip_id):
        d.clear() if hasattr(d, "clear") else None

    try:
        routes.extend(_safe_read_csv(routes_path))
        for r in routes:
            r_id = r.get("route_id", "").strip()
            routes_by_id[r_id] = r
            short = (r.get("route_short_name") or "").strip()
            if short:
                routes_by_short[short.lower()].append(r)
    except Exception:
        routes[:] = []

    try:
        stops.extend(_safe_read_csv(stops_path))
        for s in stops:
            sid = (s.get("stop_id") or "").strip()
            if sid:
                stops_by_id[sid] = s
            scode = (s.get("stop_code") or "").strip()
            if scode:
                stops_by_code[scode] = s
    except Exception:
        stops[:] = []

    try:
        trips.extend(_safe_read_csv(trips_path))
        for t in trips:
            tid = (t.get("trip_id") or "").strip()
            rid = (t.get("route_id") or "").strip()
            if tid:
                trips_by_id[tid] = t
            if rid:
                trips_by_route_id[rid].append(t)
    except Exception:
        trips[:] = []

    try:
        stop_times.extend(_safe_read_csv(stop_times_path))
        for st in stop_times:
            sid = (st.get("stop_id") or "").strip()
            tid = (st.get("trip_id") or "").strip()
            if sid:
                stop_times_by_stop_id[sid].append(st)
            if tid:
                stop_times_by_trip_id[tid].append(st)
        # idő szerint rendezzük tripen belül
        for tid, arr in stop_times_by_trip_id.items():
            arr.sort(key=lambda x: _gtfs_sec(x.get("departure_time") or x.get("arrival_time") or ""))
    except Exception:
        stop_times[:] = []

def _now_uk():
    return datetime.now(UK_TZ)

def _midnight_uk(dt=None):
    dt = dt or _now_uk()
    return UK_TZ.localize(datetime(dt.year, dt.month, dt.day, 0, 0, 0))

def _gtfs_sec(hhmmss: str) -> int:
    # "27:15:00" is valid in GTFS -> 27*3600+...
    try:
        parts = [int(x) for x in (hhmmss or "00:00:00").split(":")]
        if len(parts) == 2:
            parts.append(0)
        h, m, s = parts
        return h * 3600 + m * 60 + s
    except Exception:
        return 0

def _sec_to_dt_today(sec: int) -> datetime:
    base = _midnight_uk()
    days = sec // 86400
    rem = sec % 86400
    return base + timedelta(seconds=rem, days=days)

def _fmt_hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")

def _mins_from_now(dt: datetime) -> int:
    now = _now_uk()
    delta = dt - now
    return int(round(delta.total_seconds() / 60.0))

def _route_key_to_short(key: str) -> str:
    # "/r/BLUS:HAT0019A:19a" -> "19a", "/r/19a" -> "19a"
    if ":" in key:
        return key.split(":")[-1]
    return key

# ---------------------------------------------------------------
# Egyszerű TTL cache a live hívásokra
# ---------------------------------------------------------------
LIVE_CACHE = {}
LIVE_TTL = 20  # másodperc

def _cache_get(k):
    v = LIVE_CACHE.get(k)
    if not v: return None
    if v["exp"] < datetime.utcnow().timestamp():
        LIVE_CACHE.pop(k, None)
        return None
    return v["val"]

def _cache_set(k, val):
    LIVE_CACHE[k] = {"val": val, "exp": datetime.utcnow().timestamp() + LIVE_TTL}

# ---------------------------------------------------------------
# Live SIRI/BODS – óvatos parse, sose dobjon 500-at
# ---------------------------------------------------------------
async def _http_json(url: str, params=None):
    if not url or httpx is None:
        return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params or {}, headers=EXTRA_HEADERS)
            r.raise_for_status()
            return r.json()
    except Exception:
        return None

def _operator_ok(op: str) -> bool:
    return (op or "").strip().lower()[:4] in ALLOWED_OPERATORS

async def fetch_live_route(route_short: str):
    """Vissza: list[ {lat, lon, bearing, fleet, line, operator} ]"""
    ck = ("vm", route_short.lower())
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    out = []
    # Ha nincs VM URL, adjunk vissza üreset
    if SIRI_VM_URL:
        data = await _http_json(SIRI_VM_URL, params={"LineRef": route_short})
        # SIRI v2 VehicleMonitoring szerkezetek – óvatos bontás
        try:
            deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
            for d in deliveries:
                for mvj in d.get("VehicleActivity", []):
                    mon = mvj.get("MonitoredVehicleJourney", {}) or {}
                    line = (mon.get("LineRef") or mon.get("PublishedLineName") or "").strip()
                    op = (mon.get("OperatorRef") or "").strip()
                    if route_short and line and route_short.lower() != str(line).lower():
                        continue
                    if op and not _operator_ok(op):
                        continue
                    veh = (mon.get("VehicleRef") or "") or (mvj.get("VehicleRef") or "")
                    loc = mon.get("VehicleLocation") or {}
                    lat = loc.get("Latitude")
                    lon = loc.get("Longitude")
                    if lat is None or lon is None:
                        continue
                    out.append({
                        "lat": float(lat),
                        "lon": float(lon),
                        "bearing": mon.get("Bearing"),
                        "fleet": str(veh),
                        "line": str(line),
                        "operator": op.lower()[:4]
                    })
        except Exception:
            out = []

    _cache_set(ck, out)
    return out

async def fetch_live_stop(stop_code_or_id: str):
    """Vissza: list[ dict ] – SM adatok soronként, a sablonhoz szükséges mezők kiszámolva a hívó oldalon"""
    ck = ("sm", stop_code_or_id)
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    items = []
    if SIRI_SM_URL:
        data = await _http_json(SIRI_SM_URL, params={"MonitoringRef": stop_code_or_id})
        try:
            deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("StopMonitoringDelivery", [])
            for d in deliveries:
                for mvj in d.get("MonitoredStopVisit", []):
                    j = mvj.get("MonitoredVehicleJourney", {}) or {}
                    line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                    op = (j.get("OperatorRef") or "").strip()
                    if op and not _operator_ok(op):
                        continue
                    call = j.get("MonitoredCall") or {}
                    aimed = call.get("AimedDepartureTime") or call.get("AimedArrivalTime")
                    exp = call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime")
                    # idő formátumok: ISO 8601
                    delay_text = ""
                    is_due = False
                    dep_dt = None
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
                            is_due = abs((_now_uk() - dep_dt).total_seconds()) < 60
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

    _cache_set(ck, items)
    return items

# ---------------------------------------------------------------
# GTFS segédek menetrendhez
# ---------------------------------------------------------------
def find_stop_any(id_or_code: str):
    s = stops_by_id.get(id_or_code)
    if s:
        return s
    return stops_by_code.get(id_or_code)

def find_routes_for_short(short_lower: str):
    arr = routes_by_short.get(short_lower, [])
    # csak blus/unil
    if not arr:
        return []
    res = []
    for r in arr:
        ag = (r.get("agency_id") or "").strip().lower()[:4]
        if ag in ALLOWED_OPERATORS:
            res.append(r)
    return res or arr  # ha nincs agency_id, akkor is legyen valami

def make_departure_rows_for_stop(stop_obj, minutes_ahead=120):
    """Menetrendi + live merge – ha van live ugyanarra a tripre, menetrendi elrejtve."""
    now = _now_uk()
    until = now + timedelta(minutes=minutes_ahead)
    sid = stop_obj.get("stop_id")
    live_raw = []

    # live SM a stop_code szerint a gyakoribb – ha nincs code, próbáljuk id-vel
    scode = (stop_obj.get("stop_code") or stop_obj.get("stop_id") or "").strip()
    # Live adatok
    live_raw = asyncio.run(fetch_live_stop(scode)) if asyncio.get_event_loop().is_closed() else None
    if live_raw is None:
        # ha már van futó loop (FastAPI), futtassuk taskként
        live_raw = []
    live_by_trip = {}
    for it in live_raw:
        if not it.get("dep_dt"):
            continue
        # csak BLUS/UNIL
        if it.get("operator") not in ALLOWED_OPERATORS:
            continue
        key = (it.get("trip_id") or "", it.get("line") or "", it.get("vehicle_ref") or "")
        live_by_trip[key] = it

    # menetrendi jelöltek
    rows = []
    for st in stop_times_by_stop_id.get(sid, []):
        tid = st.get("trip_id")
        trip = trips_by_id.get(tid, {})
        rid = (trip or {}).get("route_id", "")
        route = routes_by_id.get(rid, {})
        route_short = (route.get("route_short_name") or "").strip()
        agency = (route.get("agency_id") or "").strip().lower()[:4]
        # csak blus/unil route-ok
        if agency and agency not in ALLOWED_OPERATORS:
            continue

        dep_sec = _gtfs_sec(st.get("departure_time") or st.get("arrival_time") or "")
        dep_dt = _sec_to_dt_today(dep_sec)
        # ablakon kívül
        if dep_dt < (now - timedelta(minutes=1)) or dep_dt > until:
            continue

        headsign = (trip.get("trip_headsign") or "").strip()
        key_candidates = [
            (tid or "", route_short, ""),  # trip id
        ]
        chosen = None
        for k in key_candidates:
            if k in live_by_trip:
                chosen = live_by_trip[k]
                break

        if chosen:
            # élő elsőbbséget élvez
            wait = _mins_from_now(chosen["dep_dt"])
            rows.append({
                "time_str": _fmt_hhmm(chosen["dep_dt"]),
                "headsign": chosen.get("headsign") or headsign or "",
                "route_short": route_short,
                "wait_mins": wait,
                "is_live": True,
                "is_due": chosen.get("is_due", False),
                "row_class": "live due" if chosen.get("is_due") else "live",
                "trip_id": tid,
                "fleet": chosen.get("vehicle_ref") or "",
                "delay_text": chosen.get("delay_text") or "",
            })
            # ne tegyük hozzá a menetrendit
            continue

        # csak menetrendi
        wait = _mins_from_now(dep_dt)
        rows.append({
            "time_str": _fmt_hhmm(dep_dt),
            "headsign": headsign,
            "route_short": route_short,
            "wait_mins": wait,
            "is_live": False,
            "is_due": False,
            "row_class": "timetable",
            "trip_id": tid,
            "fleet": "",
            "delay_text": "",
        })

    # élő, ami nem párosítható menetrendhez (pl. késő esti kóbor)
    for k, it in live_by_trip.items():
        # ha már hozzáadtuk, ugorjunk
        if any(r["is_live"] and r["trip_id"] == (it.get("trip_id") or "") for r in rows):
            continue
        if not it.get("dep_dt"):
            continue
        dep_dt = it["dep_dt"]
        if dep_dt < (now - timedelta(minutes=1)) or dep_dt > until:
            continue
        rows.append({
            "time_str": _fmt_hhmm(dep_dt),
            "headsign": it.get("headsign") or "",
            "route_short": (it.get("line") or "").strip(),
            "wait_mins": _mins_from_now(dep_dt),
            "is_live": True,
            "is_due": it.get("is_due", False),
            "row_class": "live due" if it.get("is_due") else "live",
            "trip_id": it.get("trip_id") or "",
            "fleet": it.get("vehicle_ref") or "",
            "delay_text": it.get("delay_text") or "",
        })

    # rendezés idő szerint
    rows.sort(key=lambda x: (_now_uk().date(), x["time_str"], 0 if x["is_live"] else 1))
    return rows

# ---------------------------------------------------------------
# Endpontok
# ---------------------------------------------------------------
@app.on_event("startup")
def _startup():
    load_gtfs()

@app.get("/healthz")
def healthz():
    return PlainTextResponse("ok")

@app.get("/c")
def diag():
    live_ok = True
    live_err = ""
    if httpx is None:
        live_ok = False
        live_err = "httpx-not-installed"
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
        "live_cache_ok": live_ok,
        "live_cache_err": live_err
    })

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    # Járatok listája route_short_name szerint, csak blus/unil
    seen = {}
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
        seen[key] = True
        items.append({
            "short": short,
            "agency": ag or "",
        })
    # abc + numerikus okosan
    def _sort_key(x):
        s = x["short"]
        # Numeric eleje, utána betű (pl. 19a)
        num = ""
        suf = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            else:
                suf += ch
        num = int(num or 0)
        return (suf.isalpha(), num, suf)
    items.sort(key=_sort_key)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "routes": items,
        "now_uk": _now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

@app.get("/r/{route_key}", response_class=HTMLResponse)
async def route_view(request: Request, route_key: str):
    short = _route_key_to_short(route_key).lower()
    rlist = find_routes_for_short(short)
    if not rlist:
        raise HTTPException(status_code=404, detail="Route not found")

    # élő járművek a rövid név alapján
    live = await fetch_live_route(short.upper() if any(c.isalpha() for c in short) else short)
    # csak allowed op
    live = [v for v in (live or []) if (v.get("operator") or "") in ALLOWED_OPERATORS]

    return templates.TemplateResponse("route.html", {
        "request": request,
        "route_short": rlist[0].get("route_short_name"),
        "live_vehicles": live,
        "now_uk": _now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

@app.get("/stop/{sid_or_code}", response_class=HTMLResponse)
def stop_view(request: Request, sid_or_code: str):
    s = find_stop_any(sid_or_code)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")

    rows = make_departure_rows_for_stop(s)

    title = s.get("stop_name") or sid_or_code
    return templates.TemplateResponse("stop.html", {
        "request": request,
        "stop": s,
        "rows": rows,
        "now_uk": _now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar",
        # sablonban: ha row['row_class'] == 'live' -> zöld; 'live due' -> villogó zöld; 'timetable' -> fehér
    })

@app.get("/t/{trip_id}", response_class=HTMLResponse)
async def trip_view(request: Request, trip_id: str):
    trip = trips_by_id.get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    rid = trip.get("route_id", "")
    route = routes_by_id.get(rid, {})
    route_short = (route.get("route_short_name") or "").strip()

    # megállók és idők
    sts = list(stop_times_by_trip_id.get(trip_id, []))
    rows = []
    for st in sts:
        sid = st.get("stop_id")
        dep_sec = _gtfs_sec(st.get("departure_time") or st.get("arrival_time") or "")
        dep_dt = _sec_to_dt_today(dep_sec)
        s = stops_by_id.get(sid, {})
        rows.append({
            "time_str": _fmt_hhmm(dep_dt),
            "headsign": s.get("stop_name") or "",
            "route_short": route_short,
            "wait_mins": _mins_from_now(dep_dt),
            "is_live": False,
            "is_due": False,
            "row_class": "timetable",
            "trip_id": trip_id,
            "fleet": "",
            "delay_text": "",
            "lat": float(s.get("stop_lat") or 0) if s else None,
            "lon": float(s.get("stop_lon") or 0) if s else None,
        })

    # élő adatok a teljes route-ra, majd megpróbáljuk az adott tripre szűrni
    live = await fetch_live_route(route_short)
    # próbálunk késést számolni – ha van kiválasztott élő, a legközelebbi megálló alapján
    delay_text_top = ""
    if live:
        delay_texts = [v.get("delay_text") for v in live if v.get("delay_text")]
        if delay_texts:
            delay_text_top = delay_texts[0]

    return templates.TemplateResponse("trip.html", {
        "request": request,
        "trip": trip,
        "route_short": route_short,
        "rows": rows,
        "live_vehicles": live or [],
        "delay_text_top": delay_text_top,
        "now_uk": _now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = ""):
    q = (q or "").strip()
    routes_found = []
    stops_found = []

    if q:
        # route short name egyezés (case-insensitive)
        for short, arr in routes_by_short.items():
            if q.lower() in short:
                r = arr[0]
                ag = (r.get("agency_id") or "").strip().lower()[:4]
                if ag and ag not in ALLOWED_OPERATORS:
                    continue
                routes_found.append({"short": r.get("route_short_name"), "agency": ag or ""})

        # megálló névben keresés
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
        "now_uk": _now_uk().strftime("%H:%M:%S"),
        "brand": "bluestar"
    })

# ---------------------------------------------------------------
# Lokális futtatáshoz (Railway start cmd maradhat: uvicorn main:app ...)
# ---------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
