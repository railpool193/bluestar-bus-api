import os, json, math, time, asyncio
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from dateutil import tz
import humanize

# ---------- Helpers: config ----------

def getenv(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def parse_json_env(name: str, default):
    raw = getenv(name, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default

DATA_DIR = getenv("DATA_DIR", "gtfs")

# Live (SIRI)
SIRI_SM_URL = (
    getenv("SIRI_SM_URL")
    or getenv("SIRI_STOP_URL")
    or getenv("SIRI_STOP_MONITORING_URL")
)

SIRI_VM_URL = (
    getenv("SIRI_VM_URL")
    or getenv("SIRI_VEHICLE_URL")
    or getenv("SIRI_VEHICLE_MONITORING_URL")
)

# Auth – 3-féle mód
SIRI_HEADERS = parse_json_env("SIRI_HEADERS", [])
SIRI_KEY_HEADER = getenv("SIRI_KEY_HEADER")
SIRI_KEY_PARAM = getenv("SIRI_KEY_PARAM")
SIRI_API_KEY = getenv("SIRI_API_KEY")

# Operator szűrő (térképen és élőn)
OPERATORS = [s.strip() for s in getenv("OPERATORS", "BLUS,UNIL").split(",") if s.strip()]

# ---------- GTFS betöltés (nagyon könnyített, memóriába) ----------

# Minimal parser: routes.txt, stops.txt, trips.txt, stop_times.txt
# Feltételezzük, hogy léteznek és validak (a /c oldalon jelezzük, ha nem).
def read_csv(path):
    import csv
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows

def safe_path(filename):
    return os.path.join(DATA_DIR, filename)

ROUTES = []
STOPS = []
TRIPS = []
STOP_TIMES = []

def load_gtfs():
    global ROUTES, STOPS, TRIPS, STOP_TIMES
    try:
        ROUTES = read_csv(safe_path("routes.txt"))
    except Exception:
        ROUTES = []
    try:
        STOPS = read_csv(safe_path("stops.txt"))
    except Exception:
        STOPS = []
    try:
        TRIPS = read_csv(safe_path("trips.txt"))
    except Exception:
        TRIPS = []
    try:
        STOP_TIMES = read_csv(safe_path("stop_times.txt"))
    except Exception:
        STOP_TIMES = []

load_gtfs()

# Gyors indexek
STOP_BY_ID = {s.get("stop_id"): s for s in STOPS}
TRIPS_BY_ID = {t.get("trip_id"): t for t in TRIPS}
ROUTE_BY_ID = {r.get("route_id"): r for r in ROUTES}

TRIPS_BY_ROUTE = {}
for t in TRIPS:
    TRIPS_BY_ROUTE.setdefault(t.get("route_id"), []).append(t)

STOPTIMES_BY_TRIP = {}
for st in STOP_TIMES:
    STOPTIMES_BY_TRIP.setdefault(st.get("trip_id"), []).append(st)
for k in STOPTIMES_BY_TRIP:
    STOPTIMES_BY_TRIP[k].sort(key=lambda r: (int(r.get("stop_sequence", "0"))))

# ---------- Live fetch helpers ----------

def add_auth(url: str, headers: dict) -> (str, dict):
    """auth hozzáadása headerként vagy query paramként"""
    h = dict(headers) if headers else {}
    if SIRI_API_KEY:
        if SIRI_KEY_HEADER:
            h[SIRI_KEY_HEADER] = SIRI_API_KEY
        elif SIRI_KEY_PARAM:
            # add key as query
            u = urlparse(url)
            q = parse_qs(u.query)
            q[SIRI_KEY_PARAM] = [SIRI_API_KEY]
            url = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))
    if SIRI_HEADERS:
        for pair in SIRI_HEADERS:
            try:
                h[pair["name"]] = pair["value"]
            except Exception:
                pass
    return url, h

async def fetch_json(url: str, client: httpx.AsyncClient, timeout=8.0, headers=None):
    try:
        url, hdr = add_auth(url, headers or {})
        r = await client.get(url, timeout=timeout, headers=hdr)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def is_live_enabled():
    return bool(SIRI_SM_URL or SIRI_VM_URL)

# ---------- Merge logic (duplikációmentes) ----------

def merge_departures(timetable_rows, live_rows, now_utc: datetime):
    """
    Egy adott megálló kimenete.
    - timetable_rows: [{time: datetime, headsign, route_id, trip_id}]
    - live_rows: [{time: datetime, headsign, route_id, trip_id, delay_sec, vehicle_ref, is_due}]
    Ha van élő ugyanarra a trip_id + ~2 percen belüli időre, akkor a menetrendi nem jelenik meg.
    """
    out = []
    used_trips = set()

    # élő először, zölddel
    for lr in live_rows:
        key = (lr.get("trip_id"), lr["time"].replace(second=0, microsecond=0))
        used_trips.add(key)
        lr["_kind"] = "live"
        out.append(lr)

    for tr in timetable_rows:
        key = (tr.get("trip_id"), tr["time"].replace(second=0, microsecond=0))
        # ha nincs élő ugyanarra a percre UGYANARRA a tripre, mutatjuk fehérrel
        if key not in used_trips:
            tr["_kind"] = "timetable"
            tr["delay_sec"] = None
            tr["vehicle_ref"] = None
            tr["is_due"] = False
            out.append(tr)

    out.sort(key=lambda r: r["time"])
    return out

def fmt_delay(delay_sec: int | None):
    if delay_sec is None:
        return ""
    if abs(delay_sec) < 30:
        return "±0"
    late = delay_sec > 0
    mins = int(round(abs(delay_sec) / 60))
    return f"+{mins} min" if late else f"-{mins} min"

# ---------- FastAPI + Templates ----------

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# ---------- Pages ----------

@app.get("/c", response_class=PlainTextResponse)
def config_probe():
    # diagnosztika
    exists = lambda fn: os.path.exists(safe_path(fn))
    return json.dumps({
        "DATA_DIR": DATA_DIR,
        "routes.txt": exists("routes.txt"),
        "stops.txt": exists("stops.txt"),
        "trips.txt": exists("trips.txt"),
        "stop_times.txt": exists("stop_times.txt"),
        "routes_count": len(ROUTES),
        "stops_count": len(STOPS),
        "live_enabled": is_live_enabled(),
        "requests_available": True,
        "live_cache_ok": True,
        "live_cache_err": "",
        "vm_url": SIRI_VM_URL or "",
        "sm_url": SIRI_SM_URL or "",
        "extra_headers": SIRI_HEADERS,
    })

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # egyszerű route lista
    routes = []
    for r in ROUTES:
        routes.append({
            "route_id": r.get("route_id"),
            "short_name": r.get("route_short_name") or r.get("route_id"),
            "agency": (r.get("agency_id") or "").lower(),
        })
    routes.sort(key=lambda x: (x["agency"], x["short_name"]))
    return templates.TemplateResponse("index.html", {
        "request": request,
        "now_uk": datetime.now(tz=tz.gettz("Europe/London")),
        "routes": routes,
    })

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str):
    ql = q.strip().lower()
    # route: short_name, id
    route_hits = []
    for r in ROUTES:
        rn = (r.get("route_short_name") or "").lower()
        if ql and ql in rn:
            route_hits.append({"route_id": r.get("route_id"), "short_name": r.get("route_short_name") or r.get("route_id")})
    route_hits = route_hits[:40]
    # stops: name
    stop_hits = []
    for s in STOPS:
        name = (s.get("stop_name") or "")
        if ql and ql in name.lower():
            stop_hits.append({"stop_id": s.get("stop_id"), "name": name})
        if len(stop_hits) >= 40: break

    return templates.TemplateResponse("search.html", {
        "request": request,
        "q": q,
        "route_hits": route_hits,
        "stop_hits": stop_hits,
        "now_uk": datetime.now(tz=tz.gettz("Europe/London")),
    })

@app.get("/stop/{stop_id}", response_class=HTMLResponse)
async def stop_view(request: Request, stop_id: str):
    stop = STOP_BY_ID.get(stop_id)
    if not stop:
        raise HTTPException(404, "Stop not found")

    # --- Menetrendi (következő 60 perc) ---
    now = datetime.now(timezone.utc)
    timetable_rows = []
    window_end = now + timedelta(minutes=60)

    # kigyűjtjük az összes tripet, ami áthalad a megállón
    for trip_id, rows in STOPTIMES_BY_TRIP.items():
        for st in rows:
            if st.get("stop_id") != stop_id:
                continue
            # idő 24:xx formától lehet, kezeljük:
            dep = st.get("departure_time") or st.get("arrival_time")
            if not dep: 
                continue
            h,m,s = [int(x) for x in dep.split(":")]
            # napátfordulás kezelése
            days = h // 24
            h = h % 24
            dt_local = datetime.now(tz=tz.gettz("Europe/London")).replace(hour=0, minute=0, second=0, microsecond=0) \
                + timedelta(days=days, hours=h, minutes=m, seconds=s)
            dt_utc = dt_local.astimezone(timezone.utc)
            if now <= dt_utc <= window_end:
                trip = TRIPS_BY_ID.get(trip_id) or {}
                route = ROUTE_BY_ID.get(trip.get("route_id") or "") or {}
                timetable_rows.append({
                    "time": dt_utc,
                    "headsign": trip.get("trip_headsign") or "",
                    "route_id": route.get("route_id"),
                    "trip_id": trip_id,
                })

    # --- Élő (SIRI StopMonitoring) ---
    live_rows = []
    if SIRI_SM_URL:
        url = SIRI_SM_URL.replace("{stop_id}", stop_id)
        async with httpx.AsyncClient() as client:
            data = await fetch_json(url, client)
        # a SIRI JSON elágazásai sokfélék; védetten olvasunk
        try:
            visits = (
                data.get("Siri", {})
                    .get("ServiceDelivery", {})
                    .get("StopMonitoringDelivery", [{}])[0]
                    .get("MonitoredStopVisit", [])
            )
            for v in visits:
                mvj = v.get("MonitoredVehicleJourney", {})
                op = (mvj.get("OperatorRef") or "").upper()
                if OPERATORS and op and op not in OPERATORS:
                    continue  # szűrés más szolgáltatókra
                aimed = v.get("MonitoredVehicleJourney", {}).get("MonitoredCall", {}).get("AimedDepartureTime") \
                        or v.get("MonitoredVehicleJourney", {}).get("MonitoredCall", {}).get("AimedArrivalTime")
                exp = v.get("MonitoredVehicleJourney", {}).get("MonitoredCall", {}).get("ExpectedDepartureTime") \
                      or v.get("MonitoredVehicleJourney", {}).get("MonitoredCall", {}).get("ExpectedArrivalTime")
                headsign = mvj.get("DestinationName") or mvj.get("PublishedLineName") or ""
                route_short = mvj.get("LineRef")
                trip_id = mvj.get("FramedVehicleJourneyRef", {}).get("DatedVehicleJourneyRef")
                veh_ref = mvj.get("VehicleRef")
                # idő
                t_iso = exp or aimed
                if not t_iso:
                    continue
                dep_utc = datetime.fromisoformat(t_iso.replace("Z","+00:00")).astimezone(timezone.utc)
                # késés
                delay_sec = None
                if exp and aimed:
                    exp_utc = datetime.fromisoformat(exp.replace("Z","+00:00")).astimezone(timezone.utc)
                    aim_utc = datetime.fromisoformat(aimed.replace("Z","+00:00")).astimezone(timezone.utc)
                    delay_sec = int((exp_utc - aim_utc).total_seconds())
                is_due = (dep_utc - now).total_seconds() <= 60
                # route azonosító (best-effort összekapcsolás)
                route_id = None
                for r in ROUTES:
                    if (r.get("route_short_name") or "") == str(route_short):
                        route_id = r.get("route_id")
                        break
                live_rows.append({
                    "time": dep_utc,
                    "headsign": headsign,
                    "route_id": route_id,
                    "trip_id": trip_id,
                    "vehicle_ref": veh_ref,
                    "delay_sec": delay_sec,
                    "is_due": is_due,
                })
        except Exception:
            # ha baj van, nem dőlünk el – marad a menetrend
            pass

    merged = merge_departures(timetable_rows, live_rows, now)

    return templates.TemplateResponse("stop.html", {
        "request": request,
        "now_uk": datetime.now(tz=tz.gettz("Europe/London")),
        "stop": stop,
        "rows": merged,
        "fmt_delay": fmt_delay,
    })

@app.get("/r/{route_id}", response_class=HTMLResponse)
async def route_view(request: Request, route_id: str):
    route = ROUTE_BY_ID.get(route_id)
    if not route:
        raise HTTPException(404, "Route not found")

    vehicles = []
    if SIRI_VM_URL:
        # egységes VM hívás
        url = SIRI_VM_URL
        # Ha nincs benne operator param, hagyjuk; úgyis szűrünk operátorra a feed alapján
        async with httpx.AsyncClient() as client:
            data = await fetch_json(url, client)
        try:
            activities = (
                data.get("Siri", {})
                    .get("ServiceDelivery", {})
                    .get("VehicleMonitoringDelivery", [{}])[0]
                    .get("VehicleActivity", [])
            )
            for a in activities:
                mvj = a.get("MonitoredVehicleJourney", {})
                op = (mvj.get("OperatorRef") or "").upper()
                if OPERATORS and op and op not in OPERATORS:
                    continue
                line = str(mvj.get("LineRef") or "")
                # csak a kiválasztott route short_name-jére engedjük
                if (route.get("route_short_name") or "") and line != (route.get("route_short_name") or ""):
                    continue
                lat = mvj.get("VehicleLocation", {}).get("Latitude")
                lon = mvj.get("VehicleLocation", {}).get("Longitude")
                if lat is None or lon is None:
                    continue
                vehicles.append({
                    "lat": lat, "lon": lon,
                    "vehicle_ref": mvj.get("VehicleRef"),
                    "headsign": mvj.get("DestinationName"),
                })
        except Exception:
            vehicles = []

    return templates.TemplateResponse("route.html", {
        "request": request,
        "now_uk": datetime.now(tz=tz.gettz("Europe/London")),
        "route": route,
        "vehicles": vehicles,
        "veh_count": len(vehicles),
    })

@app.get("/t/{trip_id}", response_class=HTMLResponse)
def trip_view(request: Request, trip_id: str):
    trip = TRIPS_BY_ID.get(trip_id)
    if not trip:
        raise HTTPException(404, "Trip not found")
    rows = STOPTIMES_BY_TRIP.get(trip_id, [])

    # késés becslés (ha StopMonitoring-ból találunk egyező tripet)
    delay_txt = ""
    # Best-effort: nincs trip->live összerendelés garantálva, ezért itt csak akkor mutatjuk, ha a /stop élőben volt DatedVehicleJourneyRef.
    # (Ez a rész üres maradhat, ha a feed nem adja vissza a trip_id-t.)

    return templates.TemplateResponse("trip.html", {
        "request": request,
        "now_uk": datetime.now(tz=tz.gettz("Europe/London")),
        "trip": trip,
        "rows": rows,
        "fmt_delay": fmt_delay,
        "delay_txt": delay_txt,
        "stops": STOP_BY_ID,
    })
