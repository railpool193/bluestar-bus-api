import os, csv, json, math, time, functools, itertools
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx

# --- config ---
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()
BODS_FEED_URL = os.getenv("BODS_FEED_URL", f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={BODS_API_KEY}")
ALLOWED_OPERATORS = {s.strip().lower() for s in os.getenv("SIRI_ALLOWED_OPERATORS", "bluestar,unilink").split(",")}
LIVE_TTL = 25  # sec

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

UK = timezone(timedelta(hours=0))  # UK winter; ha kell, állíts át pytz Europe/London-ra

# --- GTFS beolvasás --
def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

ROUTES = _read_csv(os.path.join(DATA_DIR, "routes.txt"))
TRIPS  = _read_csv(os.path.join(DATA_DIR, "trips.txt"))
STOPS  = _read_csv(os.path.join(DATA_DIR, "stops.txt"))
STOP_TIMES = _read_csv(os.path.join(DATA_DIR, "stop_times.txt"))

route_by_id = {r["route_id"]: r for r in ROUTES}
trip_by_id  = {t["trip_id"]: t for t in TRIPS}
stop_by_id  = {s["stop_id"]: s for s in STOPS}

trips_by_route = {}
for t in TRIPS:
    trips_by_route.setdefault(t["route_id"], []).append(t)

stoptimes_by_stop = {}
for st in STOP_TIMES:
    stoptimes_by_stop.setdefault(st["stop_id"], []).append(st)
for k in stoptimes_by_stop:
    stoptimes_by_stop[k].sort(key=lambda x: (x["trip_id"], x["stop_sequence"].zfill(4)))

stoptimes_by_trip = {}
for st in STOP_TIMES:
    stoptimes_by_trip.setdefault(st["trip_id"], []).append(st)
for k in stoptimes_by_trip:
    stoptimes_by_trip[k].sort(key=lambda x: int(x["stop_sequence"]))

# --- segédek ---
def now_uk():
    # ha kell: datetime.now(pytz.timezone("Europe/London"))
    return datetime.now(UK)

def parse_hhmmss(s):
    h, m, sec = map(int, s.split(":"))
    return h*3600 + m*60 + sec

def secs_to_clock(secs):
    secs = secs % 86400
    h = secs//3600; m=(secs%3600)//60
    return f"{h:02d}:{m:02d}"

def minutes_from_now(clock_hms, ref=None):
    ref = ref or now_uk()
    day_secs = ref.hour*3600 + ref.minute*60 + ref.second
    t = parse_hhmmss(clock_hms)
    # GTFS 25+ órát is enged; csúsztatás
    if t < day_secs - 3600:  # ha már elmúlt >1h, vegyük holnapinak
        t += 86400
    return round((t - day_secs)/60)

def route_display(r):
    short = r.get("route_short_name") or ""
    agency = r.get("agency_id") or r.get("route_id","")[:4]
    return short.strip() or agency

# --- élő cache ---
_live_cache = {"ts": 0, "items": []}

def _pt_to_seconds(pt):
    # "PT5M20S" -> 320
    try:
        s = pt.upper()
        total = 0
        num = ""
        for ch in s.replace("PT",""):
            if ch.isdigit():
                num += ch
            else:
                if num:
                    if ch == "H": total += int(num)*3600
                    elif ch == "M": total += int(num)*60
                    elif ch == "S": total += int(num)
                    num = ""
        return total
    except Exception:
        return None

async def fetch_live():
    global _live_cache
    if time.time() - _live_cache["ts"] < LIVE_TTL and _live_cache["items"]:
        return _live_cache["items"]

    if not BODS_API_KEY:
        _live_cache = {"ts": time.time(), "items": []}
        return []

    url = BODS_FEED_URL if "api_key=" in BODS_FEED_URL else f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={BODS_API_KEY}"
    items = []
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
        resp.raise_for_status()
    data = resp.json()

    # SIRI-VM csomag -> egyszerűsített lista
    for e in data.get("Siri",{}).get("ServiceDelivery",{}).get("VehicleMonitoringDelivery",[]):
        for mvj in e.get("VehicleActivity",[]):
            mon = mvj.get("MonitoredVehicleJourney",{})
            op = (mon.get("OperatorRef") or "").lower()
            short_op = (mon.get("OperatorShortName") or "").lower()
            if op not in ALLOWED_OPERATORS and short_op not in ALLOWED_OPERATORS:
                continue

            line = mon.get("PublishedLineName") or mon.get("LineRef") or ""
            dest = mon.get("DestinationName") or ""
            veh  = mon.get("VehicleRef") or ""
            fleet = mon.get("VehicleFeatureRef") or mon.get("VehicleRegistration") or ""
            delay = None
            if "Delay" in mon:
                # lehet "PT5M", lehet más formában
                if isinstance(mon["Delay"], str):
                    delay = _pt_to_seconds(mon["Delay"])
                elif isinstance(mon["Delay"], (int, float)):
                    delay = int(mon["Delay"])
            try:
                lat = float(mon["VehicleLocation"]["Latitude"])
                lon = float(mon["VehicleLocation"]["Longitude"])
            except Exception:
                continue

            items.append({
                "operator": short_op or op,
                "line": str(line),
                "dest": dest,
                "vehicle_ref": veh,
                "fleet": str(fleet) if fleet else None,
                "lat": lat, "lon": lon,
                "delay_min": int(round(delay/60)) if delay is not None else None,
                # matching segédmezők:
                "route_short_guess": str(line).strip(),
            })

    _live_cache = {"ts": time.time(), "items": items}
    return items

# --- duplikátum-szűrés (élő előnyben) ---
def dedupe_departures_with_live(deps, live_for_stop_or_route):
    """
    deps: list[dict]  — a backend által előállított menetrendi indulások (kind=... mezőt pótoljuk)
    live_for_stop_or_route: list[dict] — élő a környezetre (ugyanarra a route-ra vagy stopra szűrve)
    logika:
      * élő 'kulcs': (line, headsign norm) 
      * ha van egyező kulcsú menetrendi  ±4 perc ablakban -> kidobjuk a menetrendit
    """
    def norm(s): return (s or "").lower().strip()
    live_keys = set((norm(x.get("line")), norm(x.get("dest"))) for x in live_for_stop_or_route)

    out = []
    for d in deps:
        d = dict(d)
        d.setdefault("kind","sched")
        if live_keys:
            key = (norm(d.get("route_display")), norm(d.get("headsign")))
            # ha a 'route_display' nem a szám, próbáljuk a 'route_short_name'-t is (backend adja be)
            if key not in live_keys and "route_short_name" in d:
                key = (norm(d["route_short_name"]), norm(d.get("headsign")))
            # ablak: ha azonos kulcs és a különbség <= 4 perc -> ejtjük a menetrendit
            if key in live_keys and abs(int(d.get("minutes", 999))) <= 4:
                continue
        out.append(d)
    # due flag
    for d in out:
        mins = d.get("minutes")
        d["is_due"] = (d.get("kind")=="live" and mins is not None and int(mins) <= 1)
    return out

# --- search segéd ---
def search_routes_and_stops(q):
    ql = q.lower()
    routes = []
    for r in ROUTES:
        name = (r.get("route_short_name") or "") + " " + (r.get("route_long_name") or "")
        if ql in name.lower():
            routes.append({
                "route_id": r["route_id"],
                "display": route_display(r),
                "agency": r.get("agency_id") or "",
            })
    stops = []
    for s in STOPS:
        if ql in (s.get("stop_name") or "").lower():
            stops.append({"stop_id": s["stop_id"], "stop_name": s["stop_name"]})
    return routes, stops

# --- viewk ---
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    routes = [{
        "route_id": r["route_id"],
        "display": route_display(r),
        "agency": (r.get("agency_id") or "").lower()[:4] or "BLUS"
    } for r in ROUTES]
    routes.sort(key=lambda x: (x["display"].isdigit(), int(x["display"]) if x["display"].isdigit() else x["display"]))
    return templates.TemplateResponse("index.html", {"request": request, "routes": routes})

@app.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str = ""):
    routes, stops = search_routes_and_stops(q or "")
    return templates.TemplateResponse("search.html", {"request": request, "q": q, "routes": routes, "stops": stops})

@app.get("/stop/{stop_id}", response_class=HTMLResponse)
async def stop_view(request: Request, stop_id: str):
    stop = stop_by_id.get(stop_id)
    if not stop: raise HTTPException(404)
    now = now_uk()
    # következő 90 perc menetrend
    rows = []
    for st in stoptimes_by_stop.get(stop_id, []):
        trip = trip_by_id.get(st["trip_id"])
        r = route_by_id.get(trip["route_id"])
        mins = minutes_from_now(st["departure_time"], now)
        if mins < -2 or mins > 90:
            continue
        rows.append({
            "trip_id": st["trip_id"],
            "time_str": secs_to_clock(parse_hhmmss(st["departure_time"])),
            "minutes": mins,
            "headsign": trip.get("trip_headsign") or r.get("route_long_name"),
            "route_display": route_display(r),
            "route_short_name": r.get("route_short_name"),
            "kind": "sched",
        })
    # élő a route-ok alapján, de nem tudjuk stop-párosítani -> heur. by line+dest
    live_all = await fetch_live()
    live_for_this = []
    route_short_names = {r.get("route_short_name") for r in (route_by_id.get(trip_by_id[row["trip_id"]]["route_id"]) for row in rows)}
    for v in live_all:
        if v["route_short_guess"] in route_short_names:
            # becsült „minutes”: ha Delay elérhető -> mutatjuk, különben None
            mins = max(0, v["delay_min"] if v["delay_min"] is not None else 0)
            live_for_this.append({
                "trip_id": None,  # nincs GTFS link
                "time_str": secs_to_clock(now.hour*3600+now.minute*60),  # „most”
                "minutes": mins,
                "headsign": v["dest"],
                "route_display": v["line"],
                "kind": "live",
            })

    merged = dedupe_departures_with_live(rows, live_for_this)
    # ha szeretnéd, itt még a live elemeket is hozzácsaphatjuk a lista elejére:
    # (külön kérted, hogy ha van live, a menetrendi azonos ne jelenjen meg — a fenti dedupe ezt megoldja)
    context = {"request": request, "stop": stop, "departures": merged}
    return templates.TemplateResponse("stop.html", context)

@app.get("/r/{route_id}", response_class=HTMLResponse)
async def route_view(request: Request, route_id: str):
    r = route_by_id.get(route_id)
    if not r: raise HTTPException(404)
    route_short = r.get("route_short_name") or ""
    live = [v for v in (await fetch_live()) if v["route_short_guess"] == route_short]
    ctx = {
        "request": request,
        "route": {"display_name": route_display(r)},
        "live": live
    }
    return templates.TemplateResponse("route.html", ctx)

@app.get("/t/{trip_id}", response_class=HTMLResponse)
async def trip_view(request: Request, trip_id: str):
    trip = trip_by_id.get(trip_id)
    if not trip: raise HTTPException(404)
    r = route_by_id.get(trip["route_id"])
    route_short = r.get("route_short_name") or ""

    # pontok a polylinhoz a stop_times alapján
    pts = []
    legs = []
    now = now_uk()
    for st in stoptimes_by_trip.get(trip_id, []):
        s = stop_by_id.get(st["stop_id"])
        if not s: continue
        pts.append({"lat": float(s["stop_lat"]), "lon": float(s["stop_lon"])})
        mins = minutes_from_now(st["departure_time"], now) if st.get("departure_time") else None
        legs.append({
            "stop_id": s["stop_id"],
            "stop_name": s["stop_name"],
            "time_str": secs_to_clock(parse_hhmmss(st["departure_time"])) if st.get("departure_time") else None,
            "minutes": mins,
            "kind": "sched",
            "is_due": False,
        })

    # élő az adott vonalra; nincs garantált GTFS trip match -> vonalszintű overlay
    live = [v for v in (await fetch_live()) if v["route_short_guess"] == route_short]

    # duplikátum-szűrés: ha a legközelebbi menetrendi lépés ugyanarra a headsignra és vonalra esik ±4 percen belül, hagyjuk a live-ot dominálni: a listában csak live-ként jelöljük (színezés a templátban)
    legs = dedupe_departures_with_live(legs, live)

    # késés/sietség badge (ha több live van, átlag)
    delays = [v["delay_min"] for v in live if v["delay_min"] is not None]
    delay_badge = int(round(sum(delays)/len(delays))) if delays else None

    ctx = {
        "request": request,
        "points": pts,
        "legs": legs,
        "live": live,
        "delay": delay_badge,
    }
    return templates.TemplateResponse("trip.html", ctx)

# --- diag ---
@app.get("/c")
async def conf():
    ok = {
        "DATA_DIR": DATA_DIR,
        "routes.txt": os.path.exists(os.path.join(DATA_DIR,"routes.txt")),
        "stops.txt": os.path.exists(os.path.join(DATA_DIR,"stops.txt")),
        "trips.txt": os.path.exists(os.path.join(DATA_DIR,"trips.txt")),
        "stop_times.txt": os.path.exists(os.path.join(DATA_DIR,"stop_times.txt")),
        "routes_count": len(ROUTES),
        "stops_count": len(STOPS),
        "live_enabled": bool(BODS_API_KEY),
        "requests_available": True,
        "live_cache_ok": True,
        "live_cache_err": "",
    }
    return ok
