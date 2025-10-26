import os
import csv
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------
# Alap beállítások
# ---------------------------------------------------------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "gtfs"))
TEMPLATES_DIR = Path("templates")
STATIC_DIR = Path("static")
UK_TZ = ZoneInfo("Europe/London")

app = FastAPI(title="bluestar")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)

def render(tpl_name: str, **ctx) -> HTMLResponse:
    tpl = env.get_template(tpl_name)
    return HTMLResponse(tpl.render(**ctx))

# ---------------------------------------------------------------------
# GTFS betöltés
# ---------------------------------------------------------------------
routes: Dict[str, Dict] = {}
routes_by_short: Dict[str, List[Dict]] = {}
stops: Dict[str, Dict] = {}
trips: Dict[str, Dict] = {}
stop_times_by_stop: Dict[str, List[Dict]] = {}
stop_times_by_trip: Dict[str, List[Dict]] = {}
shapes: Dict[str, List[Tuple[float, float]]] = {}

def read_csv(path: Path) -> List[Dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def parse_hhmmss_to_seconds(v: str) -> int:
    if not v:
        return -1
    h, m, s = [int(x) for x in v.split(":")]
    return h * 3600 + m * 60 + s

def seconds_to_hhmm(sec: int) -> str:
    if sec < 0:
        return ""
    h = (sec // 3600) % 24
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

def load_gtfs():
    # routes
    for r in read_csv(DATA_DIR / "routes.txt"):
        routes[r["route_id"]] = r
        rs = r.get("route_short_name", "").strip()
        if rs:
            routes_by_short.setdefault(rs, []).append(r)

    # stops
    for s in read_csv(DATA_DIR / "stops.txt"):
        stops[s["stop_id"]] = s

    # trips
    for t in read_csv(DATA_DIR / "trips.txt"):
        trips[t["trip_id"]] = t

    # stop_times
    for st in read_csv(DATA_DIR / "stop_times.txt"):
        stop_times_by_stop.setdefault(st["stop_id"], []).append(st)
        stop_times_by_trip.setdefault(st["trip_id"], []).append(st)

    # rendezés sorrend szerint
    for sid, lst in stop_times_by_stop.items():
        lst.sort(key=lambda x: (int(x.get("stop_sequence", "0")), parse_hhmmss_to_seconds(x.get("departure_time", ""))))
    for tid, lst in stop_times_by_trip.items():
        lst.sort(key=lambda x: int(x.get("stop_sequence", "0")))

    # shapes (opcionális)
    shp_path = DATA_DIR / "shapes.txt"
    if shp_path.exists():
        pts: Dict[str, List[Tuple[int, float, float]]] = {}
        for row in read_csv(shp_path):
            sid = row["shape_id"]
            seq = int(row.get("shape_pt_sequence", "0"))
            lat = float(row["shape_pt_lat"])
            lon = float(row["shape_pt_lon"])
            pts.setdefault(sid, []).append((seq, lat, lon))
        for sid, lst in pts.items():
            lst.sort(key=lambda x: x[0])
            shapes[sid] = [(lat, lon) for _, lat, lon in lst]

load_gtfs()

# ---------------------------------------------------------------------
# BODS élő adat (egyszerű cache)
# ---------------------------------------------------------------------
_live_cache = {"ts": 0.0, "items": []}

def fetch_bods_live() -> List[Dict]:
    # 15 mp cache
    if time.time() - _live_cache["ts"] < 15:
        return _live_cache["items"]

    api_key = os.getenv("BODS_API_KEY", "").strip()
    if not api_key:
        _live_cache.update(ts=time.time(), items=[])
        return []

    try:
        import requests  # lazy import

        url = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={api_key}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception:
        _live_cache.update(ts=time.time(), items=[])
        return []

    items = []
    # A feed SIRI-VM struktúrájú – általános mezők:
    for v in data.get("vehicles", []):
        # PublishedLineName vagy LineRef – ezeket tudjuk a route_short_name-hez illeszteni
        line = (
            v.get("PublishedLineName")
            or (v.get("LineRef") or "").split(":")[-1]
            or ""
        )
        lat = v.get("Latitude") or v.get("VehicleLocation", {}).get("Latitude")
        lon = v.get("Longitude") or v.get("VehicleLocation", {}).get("Longitude")
        if lat is None or lon is None:
            continue
        items.append(
            {
                "line": str(line).strip(),
                "bearing": v.get("Bearing"),
                "timestamp": v.get("RecordedAtTime") or v.get("ValidUntilTime"),
                "lat": float(lat),
                "lon": float(lon),
                "raw": v,
            }
        )

    _live_cache.update(ts=time.time(), items=items)
    return items

def live_by_route_short() -> Dict[str, List[Dict]]:
    # csoportosítás route_short_name szerint
    grp: Dict[str, List[Dict]] = {}
    for it in fetch_bods_live():
        ln = it["line"]
        if not ln:
            continue
        grp.setdefault(ln, []).append(it)
    return grp

# ---------------------------------------------------------------------
# Segéd: rendezett route lista (számok előre)
# ---------------------------------------------------------------------
def sort_key_route_short(v: str):
    try:
        # pl. "U6C" → (False, "U", 6, "C") de egyszerűbben: szám ha lehet
        return (0, int(v))
    except Exception:
        return (1, v)

def routes_for_home() -> List[Dict]:
    items = []
    for rid, r in routes.items():
        short = (r.get("route_short_name") or "").strip()
        if not short:
            continue
        items.append(
            {
                "route_id": rid,
                "short": short,
                "agency": r.get("agency_id") or r.get("agency_name") or "GoSouthCoast",
            }
        )
    items.sort(key=lambda x: sort_key_route_short(x["short"]))
    return items

# ---------------------------------------------------------------------
# Végpontok
# ---------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return render(
        "index.html",
        request=request,
        now=datetime.now(UK_TZ),
        routes=routes_for_home(),
    )

@app.get("/routes", response_class=HTMLResponse)
def routes_alias(request: Request):
    # alias a főoldalra
    return RedirectResponse(url="/")

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = Query("", alias="q")):
    qn = q.strip().lower()
    route_hits: List[Dict] = []
    stop_hits: List[Dict] = []

    if qn:
        # routes – short_name vagy long_name találat
        for r in routes_for_home():
            rid = r["route_id"]
            src = routes[rid]
            hay = f'{src.get("route_short_name","")} {src.get("route_long_name","")}'.lower()
            if qn in hay:
                route_hits.append(
                    {"route_id": rid, "short": r["short"], "agency": r["agency"]}
                )
        # stops – névben részleges találat
        for sid, s in stops.items():
            if qn in (s.get("stop_name", "").lower()):
                stop_hits.append({"stop_id": sid, "name": s.get("stop_name")})

    route_hits = route_hits[:50]
    stop_hits = stop_hits[:50]

    return render(
        "search.html",
        request=request,
        q=q,
        routes=route_hits,
        stops=stop_hits,
        now=datetime.now(UK_TZ),
    )

@app.get("/r/{route_id}", response_class=HTMLResponse)
def route_page(request: Request, route_id: str):
    r = routes.get(route_id)
    if not r:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    short = (r.get("route_short_name") or "").strip()
    live_grp = live_by_route_short()
    lives = live_grp.get(short, [])

    return render(
        "route.html",
        request=request,
        route=r,
        short=short,
        lives=lives,
        now=datetime.now(UK_TZ),
    )

def _now_seconds_uk() -> int:
    dt = datetime.now(UK_TZ)
    return dt.hour * 3600 + dt.minute * 60 + dt.second

@app.get("/stop/{stop_id}", response_class=HTMLResponse)
def stop_page(request: Request, stop_id: str):
    s = stops.get(stop_id)
    if not s:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    # Csak a MOST UTÁNI indulások (következő 4 óra)
    now_sec = _now_seconds_uk()
    horizon = now_sec + 4 * 3600

    departures = []
    for st in stop_times_by_stop.get(stop_id, []):
        dep = parse_hhmmss_to_seconds(st.get("departure_time", ""))
        if dep < 0:
            continue
        if dep < now_sec:
            continue
        if dep > horizon:
            continue
        trip = trips.get(st["trip_id"], {})
        route = routes.get(trip.get("route_id", ""), {})
        headsign = trip.get("trip_headsign") or route.get("route_long_name") or ""
        departures.append(
            {
                "time": seconds_to_hhmm(dep),
                "trip_id": st["trip_id"],
                "headsign": headsign,
                "route_short": route.get("route_short_name", ""),
            }
        )

    # időrend
    departures.sort(key=lambda x: x["time"])

    return render(
        "stop.html",
        request=request,
        stop=s,
        departures=departures,
        now=datetime.now(UK_TZ),
    )

@app.get("/trip/{trip_id}", response_class=HTMLResponse)
@app.get("/t/{trip_id}", response_class=HTMLResponse)  # alias
def trip_page(request: Request, trip_id: str):
    t = trips.get(trip_id)
    if not t:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    st_list = stop_times_by_trip.get(trip_id, [])
    stops_on_trip = []
    for st in st_list:
        s = stops.get(st["stop_id"])
        if not s:
            continue
        stops_on_trip.append(
            {
                "stop_id": s["stop_id"],
                "name": s["stop_name"],
                "lat": float(s.get("stop_lat", "0")),
                "lon": float(s.get("stop_lon", "0")),
                "time": st.get("departure_time") or st.get("arrival_time") or "",
            }
        )

    # shape polyline (ha van)
    poly: List[Tuple[float, float]] = []
    shp_id = t.get("shape_id")
    if shp_id and shp_id in shapes:
        poly = shapes[shp_id]
    else:
        # fallback: pontok a megállókból
        poly = [(p["lat"], p["lon"]) for p in stops_on_trip]

    # élő jármű szűrés – csak route_short alapon (triphez nincs közvetlen kulcs)
    route = routes.get(t.get("route_id", ""))
    route_short = (route.get("route_short_name") or "").strip()
    lives = live_by_route_short().get(route_short, [])

    return render(
        "trip.html",
        request=request,
        trip=t,
        route=route,
        route_short=route_short,
        stops=stops_on_trip,
        poly=poly,
        lives=lives,  # lehet üres – ez rendben van
        now=datetime.now(UK_TZ),
    )

# Debug / állapot
@app.get("/cfg")
def cfg():
    return JSONResponse(
        {
            "DATA_DIR": str(DATA_DIR),
            "routes.txt": (DATA_DIR / "routes.txt").exists(),
            "stops.txt": (DATA_DIR / "stops.txt").exists(),
            "trips.txt": (DATA_DIR / "trips.txt").exists(),
            "stop_times.txt": (DATA_DIR / "stop_times.txt").exists(),
            "routes_count": len(routes),
            "stops_count": len(stops),
        }
    )

# Egyszerű JSON a live adatokhoz
@app.get("/live.json")
def live_json():
    items = fetch_bods_live()
    return JSONResponse({"count": len(items), "items": items})

# Gyári uvicorn belépési pont (Railway Procfile használja)
app_app = app
