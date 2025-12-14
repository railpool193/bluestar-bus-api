import csv
import os
import time
import math
import html
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Any, Optional

import httpx
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


# ----------------------------
# Config
# ----------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")

LIVE_FEED_URL = os.getenv(
    "LIVE_FEED_URL",
    "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key=9d2f6818e2723996467fedb958ba682aa9860a93",
)
LIVE_ENABLED = os.getenv("LIVE_ENABLED", "true").lower() == "true"

LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "20"))
NEAR_RADIUS_M = float(os.getenv("NEAR_RADIUS_M", "250"))

UK_TZ = ZoneInfo("Europe/London")

app = FastAPI()
templates = Jinja2Templates(directory="templates")


# ----------------------------
# Small helpers
# ----------------------------
def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def now_uk() -> datetime:
    # Valódi UK idő (DST-vel)
    return datetime.now(UK_TZ)


def parse_gtfs_time_to_dt_window(nowdt: datetime, hhmmss: str) -> Optional[datetime]:
    """
    Robusztus GTFS idő -> datetime:
    - kezeli a 24+ órát (25:10:00)
    - kezeli az éjfél körüli "holnap" indulásokat is (pl. most 23:50, menetrend 00:10)
    """
    parts = (hhmmss or "").strip().split(":")
    if len(parts) != 3:
        return None
    try:
        hh, mm, ss = map(int, parts)
    except Exception:
        return None

    base = datetime(nowdt.year, nowdt.month, nowdt.day, 0, 0, 0, tzinfo=UK_TZ)
    dt = base + timedelta(hours=hh, minutes=mm, seconds=ss)

    # ha most késő este van és dt "túl korai", akkor ez valószínűleg másnapi indulás
    if dt < (nowdt - timedelta(hours=6)):
        dt += timedelta(days=1)

    return dt


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def esc(s: str) -> str:
    return html.escape(s or "")


# ----------------------------
# GTFS in-memory store
# ----------------------------
routes: Dict[str, Dict[str, str]] = {}
stops: Dict[str, Dict[str, str]] = {}
trips: Dict[str, Dict[str, str]] = {}
stop_times_by_stop: Dict[str, List[Dict[str, str]]] = {}
stop_times_by_trip: Dict[str, List[Dict[str, str]]] = {}

calendar: Dict[str, Dict[str, str]] = {}
calendar_dates: Dict[str, Dict[str, str]] = {}

routes_search: List[Tuple[str, str, str]] = []  # (route_id, short, long)
stops_search: List[Tuple[str, str]] = []        # (stop_id, stop_name)

# SIRI StopPointRef -> GTFS stop_id megfeleltetés (stop_id + stop_code)
stop_ref_map: Dict[str, str] = {}


def load_gtfs() -> None:
    global routes, stops, trips, stop_times_by_stop, stop_times_by_trip
    global calendar, calendar_dates, routes_search, stops_search, stop_ref_map

    def p(name: str) -> str:
        return os.path.join(DATA_DIR, name)

    if not os.path.isdir(DATA_DIR):
        raise RuntimeError(f"DATA_DIR not found: {DATA_DIR}")

    routes_list = read_csv(p("routes.txt"))
    stops_list = read_csv(p("stops.txt"))
    trips_list = read_csv(p("trips.txt"))
    stop_times_list = read_csv(p("stop_times.txt"))

    routes = {r["route_id"]: r for r in routes_list}
    stops = {s["stop_id"]: s for s in stops_list}
    trips = {t["trip_id"]: t for t in trips_list}

    stop_times_by_stop = {}
    stop_times_by_trip = {}

    for st in stop_times_list:
        sid = st.get("stop_id", "")
        tid = st.get("trip_id", "")
        stop_times_by_stop.setdefault(sid, []).append(st)
        stop_times_by_trip.setdefault(tid, []).append(st)

    for sid, lst in stop_times_by_stop.items():
        lst.sort(key=lambda x: (x.get("arrival_time", ""), x.get("departure_time", ""), x.get("trip_id", "")))
    for tid, lst in stop_times_by_trip.items():
        lst.sort(key=lambda x: int(x.get("stop_sequence", "0") or 0))

    calendar = {}
    calendar_dates = {}
    if os.path.exists(p("calendar.txt")):
        for row in read_csv(p("calendar.txt")):
            calendar[row["service_id"]] = row
    if os.path.exists(p("calendar_dates.txt")):
        for row in read_csv(p("calendar_dates.txt")):
            sid = row["service_id"]
            dt = row["date"]
            calendar_dates.setdefault(sid, {})[dt] = row["exception_type"]

    routes_search = []
    for rid, r in routes.items():
        short = (r.get("route_short_name") or "").strip()
        longn = (r.get("route_long_name") or "").strip()
        if not short and not longn:
            short = rid
        routes_search.append((rid, short, longn))
    routes_search.sort(key=lambda x: (x[1] or x[2] or x[0]))

    stops_search = []
    for sid, s in stops.items():
        nm = (s.get("stop_name") or "").strip()
        if nm:
            stops_search.append((sid, nm))
    stops_search.sort(key=lambda x: x[1].lower())

    # StopPointRef mapping: stop_id és stop_code -> stop_id
    stop_ref_map = {}
    for sid, s in stops.items():
        stop_ref_map[sid] = sid
        code = (s.get("stop_code") or "").strip()
        if code:
            stop_ref_map[code] = sid


def service_active_today(service_id: str, d: date) -> bool:
    ymd = d.strftime("%Y%m%d")

    if service_id in calendar_dates and ymd in calendar_dates[service_id]:
        return calendar_dates[service_id][ymd] == "1"  # 1=added, 2=removed

    cal = calendar.get(service_id)
    if not cal:
        return True

    start = cal.get("start_date", "19000101")
    end = cal.get("end_date", "29991231")
    if not (start <= ymd <= end):
        return False

    weekday = d.weekday()  # Mon=0
    key = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][weekday]
    return cal.get(key, "0") == "1"


# ----------------------------
# LIVE cache + parser (SIRI-ish)
# ----------------------------
_live_cache: Dict[str, Any] = {"ts": 0.0, "vehicles": [], "stop_departures": {}}


def _xml_find_text(elem, path: str) -> str:
    parts = path.split("/")
    cur = elem
    for p in parts:
        found = None
        for ch in list(cur):
            if ch.tag.split("}")[-1] == p:
                found = ch
                break
        if found is None:
            return ""
        cur = found
    return (cur.text or "").strip()


def parse_siri(xml_bytes: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """
    SIRI VehicleMonitoring + best-effort StopMonitoring (MonitoredCall StopPointRef).
    Returns:
      vehicles: [{lat, lon, line, operator, vehicle_ref, trip_ref, aimed, expected, delay_min}]
      stop_departures: {gtfs_stop_id: [{line, destination, aimed, expected, delay_min, vehicle_ref, is_due}]}
    """
    import xml.etree.ElementTree as ET

    root = ET.fromstring(xml_bytes)
    vehicles: List[Dict[str, Any]] = []
    stop_deps: Dict[str, List[Dict[str, Any]]] = {}

    for va in root.iter():
        if va.tag.split("}")[-1] != "VehicleActivity":
            continue

        mvj = None
        for ch in list(va):
            if ch.tag.split("}")[-1] == "MonitoredVehicleJourney":
                mvj = ch
                break
        if mvj is None:
            continue

        veh_loc = None
        for ch in list(mvj):
            if ch.tag.split("}")[-1] == "VehicleLocation":
                veh_loc = ch
                break

        lat = lon = None
        if veh_loc is not None:
            try:
                lat = float(_xml_find_text(veh_loc, "Latitude"))
                lon = float(_xml_find_text(veh_loc, "Longitude"))
            except Exception:
                lat = lon = None

        line = _xml_find_text(mvj, "LineRef") or _xml_find_text(mvj, "PublishedLineName")
        operator_ref = _xml_find_text(mvj, "OperatorRef")
        vehicle_ref = _xml_find_text(mvj, "VehicleRef") or _xml_find_text(mvj, "VehicleMonitoringRef")
        trip_ref = (
            _xml_find_text(mvj, "DatedVehicleJourneyRef")
            or _xml_find_text(mvj, "FramedVehicleJourneyRef/DatedVehicleJourneyRef")
        )

        aimed = expected = ""
        mc = None
        for ch in list(mvj):
            if ch.tag.split("}")[-1] == "MonitoredCall":
                mc = ch
                break

        if mc is not None:
            aimed = _xml_find_text(mc, "AimedDepartureTime") or _xml_find_text(mc, "AimedArrivalTime")
            expected = _xml_find_text(mc, "ExpectedDepartureTime") or _xml_find_text(mc, "ExpectedArrivalTime")

            stop_point = _xml_find_text(mc, "StopPointRef").strip()
            if stop_point:
                mapped_stop = stop_ref_map.get(stop_point, stop_point)

                dep = {
                    "line": line,
                    "destination": _xml_find_text(mvj, "DestinationName") or _xml_find_text(mvj, "DestinationRef"),
                    "aimed": aimed,
                    "expected": expected,
                    "vehicle_ref": vehicle_ref,
                    "operator": operator_ref,
                    "trip_ref": trip_ref,
                    "delay_min": None,
                    "is_due": False,
                }
                try:
                    if aimed and expected:
                        a = datetime.fromisoformat(aimed.replace("Z", "+00:00"))
                        e = datetime.fromisoformat(expected.replace("Z", "+00:00"))
                        dep["delay_min"] = int(round((e - a).total_seconds() / 60.0))
                        dep["is_due"] = abs((e - now_uk()).total_seconds()) <= 60
                except Exception:
                    pass

                stop_deps.setdefault(mapped_stop, []).append(dep)

        delay_min = None
        try:
            if aimed and expected:
                a = datetime.fromisoformat(aimed.replace("Z", "+00:00"))
                e = datetime.fromisoformat(expected.replace("Z", "+00:00"))
                delay_min = int(round((e - a).total_seconds() / 60.0))
        except Exception:
            pass

        if lat is not None and lon is not None:
            vehicles.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "line": line,
                    "operator": operator_ref,
                    "vehicle_ref": vehicle_ref,
                    "trip_ref": trip_ref,
                    "aimed": aimed,
                    "expected": expected,
                    "delay_min": delay_min,
                }
            )

    for sid, lst in stop_deps.items():
        def key(x):
            return x.get("expected") or x.get("aimed") or ""
        lst.sort(key=key)

    return vehicles, stop_deps


async def get_live() -> Dict[str, Any]:
    if not LIVE_ENABLED:
        return {"vehicles": [], "stop_departures": {}, "enabled": False}

    t = time.time()
    if (t - float(_live_cache["ts"])) < LIVE_CACHE_TTL_SEC:
        return {"vehicles": _live_cache["vehicles"], "stop_departures": _live_cache["stop_departures"], "enabled": True}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(LIVE_FEED_URL, headers={"User-Agent": "bluestar-bus-api/1.0"})
            r.raise_for_status()
            content = r.content
    except Exception as e:
        return {
            "vehicles": _live_cache.get("vehicles", []),
            "stop_departures": _live_cache.get("stop_departures", {}),
            "enabled": True,
            "error": str(e),
        }

    try:
        vehicles, stop_deps = parse_siri(content)
    except Exception as e:
        return {
            "vehicles": [],
            "stop_departures": {},
            "enabled": True,
            "error": f"Live parse failed (expected SIRI XML). {e}",
        }

    _live_cache["ts"] = t
    _live_cache["vehicles"] = vehicles
    _live_cache["stop_departures"] = stop_deps
    return {"vehicles": vehicles, "stop_departures": stop_deps, "enabled": True}


# ----------------------------
# HTML style (shared for non-template pages)
# ----------------------------
CSS = """
:root{--bg:#0b1220;--card:#0f1b33;--txt:#e8eefc;--mut:#9fb0d0;--blue:#6aa9ff;--green:#24d26a;--white:#ffffff;}
*{box-sizing:border-box}
body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial;background:var(--bg);color:var(--txt)}
a{color:var(--blue);text-decoration:none}
.top{position:sticky;top:0;background:rgba(11,18,32,.92);backdrop-filter: blur(6px);padding:10px 12px;border-bottom:1px solid rgba(255,255,255,.08);z-index:10}
.brand{font-weight:800;letter-spacing:.2px}
.search{margin-top:8px;display:flex;gap:8px}
.search input{flex:1;background:#0b1730;border:1px solid rgba(255,255,255,.12);color:var(--txt);padding:10px;border-radius:10px}
.container{padding:14px;max-width:980px;margin:0 auto}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
@media (max-width:700px){.grid{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:14px}
.h1{font-size:36px;margin:10px 0 6px}
.h2{font-size:22px;margin:14px 0 8px}
.small{color:var(--mut);font-size:13px}
.badge{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;font-size:12px;border:1px solid rgba(255,255,255,.14);color:var(--mut)}
.badge.live{color:var(--green);border-color:rgba(36,210,106,.35)}
.badge.err{color:#ff8a8a;border-color:rgba(255,138,138,.35)}
.list{display:flex;flex-direction:column;gap:10px}
.row{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:12px;border-radius:14px;background:rgba(0,0,0,.12);border:1px solid rgba(255,255,255,.06)}
.left{display:flex;align-items:center;gap:12px}
.time{min-width:64px;text-align:center;background:rgba(255,255,255,.08);padding:8px 10px;border-radius:12px;font-weight:700}
.title{font-weight:800}
.sub{font-size:12px;color:var(--mut);margin-top:2px}
.right{color:var(--mut)}
.tag{font-size:12px;padding:4px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.14)}
.tag.tt{color:var(--white)}
.tag.lv{color:var(--green);border-color:rgba(36,210,106,.35)}
.tag.due{color:var(--green);border-color:rgba(36,210,106,.55);animation: blink 1s infinite}
@keyframes blink{0%,50%{opacity:1}51%,100%{opacity:.25}}
.map{height:280px;border-radius:16px;overflow:hidden;border:1px solid rgba(255,255,255,.08)}
"""

LEAFLET = """
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
"""


def page_shell(title: str, body: str, q: str = "") -> str:
    uk = now_uk().strftime("%H:%M:%S")
    return f"""<!doctype html>
<html lang="hu">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{esc(title)}</title>
<style>{CSS}</style>
{LEAFLET}
</head>
<body>
  <div class="top">
    <div class="brand">★ bluestar</div>
    <div class="search">
      <form action="/search" method="get" style="display:flex;gap:8px;width:100%">
        <input name="q" value="{esc(q)}" placeholder="Keresés: járat vagy megálló"/>
      </form>
    </div>
    <div class="small">UK: {uk}</div>
  </div>
  <div class="container">
    {body}
  </div>
</body>
</html>"""


# ----------------------------
# Startup
# ----------------------------
@app.on_event("startup")
async def _startup():
    load_gtfs()


# ----------------------------
# Health
# ----------------------------
@app.get("/health")
async def health():
    live = await get_live()
    return {
        "DATA_DIR": DATA_DIR,
        "routes.txt": os.path.exists(os.path.join(DATA_DIR, "routes.txt")),
        "stops.txt": os.path.exists(os.path.join(DATA_DIR, "stops.txt")),
        "trips.txt": os.path.exists(os.path.join(DATA_DIR, "trips.txt")),
        "stop_times.txt": os.path.exists(os.path.join(DATA_DIR, "stop_times.txt")),
        "routes_count": len(routes),
        "stops_count": len(stops),
        "live_enabled": LIVE_ENABLED,
        "live_cache_err": live.get("error", ""),
        "vm_url": LIVE_FEED_URL if LIVE_ENABLED else "",
    }


# ----------------------------
# Home (Template: templates/index.html)
# ----------------------------
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    # Kiemelt viszonylatok: első 24 a routes_search-ből (szabadon módosítható)
    featured = []
    for rid, short, longn in routes_search[:24]:
        r = routes.get(rid, {})
        featured.append(
            {
                "route_id": rid,
                "short": (short or rid).strip(),
                "long": (longn or "").strip(),
                "agency": (r.get("agency_id") or "").strip(),
            }
        )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "uk_time": now_uk().strftime("%H:%M:%S"),
            "featured_routes": featured,
        },
    )


# ----------------------------
# Search
# ----------------------------
@app.get("/search", response_class=HTMLResponse)
async def search(q: str = Query(default="")):
    qn = (q or "").strip().lower()
    matched_routes = []
    matched_stops = []

    if qn:
        for rid, short, longn in routes_search:
            s = f"{short} {longn} {rid}".lower()
            if qn in s:
                matched_routes.append((rid, short, longn))
        for sid, nm in stops_search:
            if qn in nm.lower():
                matched_stops.append((sid, nm))
    else:
        matched_routes = routes_search[:40]
        matched_stops = stops_search[:40]

    body = f'<div class="h1">Keresés: "{esc(q)}"</div>'

    body += '<div class="h2">Járatok</div>'
    if not matched_routes:
        body += '<div class="small">Nincs találat.</div>'
    else:
        body += '<div class="card">'
        for rid, short, longn in matched_routes[:80]:
            label = esc((short or rid).strip())
            body += f'<a href="/r/{esc(rid)}" style="margin-right:10px;display:inline-block;margin-bottom:8px">{label}</a>'
        body += '</div>'

    body += '<div class="h2">Megállók</div>'
    if not matched_stops:
        body += '<div class="small">Nincs találat.</div>'
    else:
        body += '<div class="list">'
        for sid, nm in matched_stops[:60]:
            body += f'''
            <a class="row" href="/stop/{esc(sid)}">
              <div class="left">
                <div>
                  <div class="title">{esc(nm)}</div>
                  <div class="sub">{esc(sid)}</div>
                </div>
              </div>
              <div class="right">Megnyitás</div>
            </a>'''
        body += '</div>'

    return page_shell("Keresés", body, q=q)


# ----------------------------
# Route page (map: filtered + clustered + refresh)
# ----------------------------
@app.get("/r/{route_id}", response_class=HTMLResponse)
async def route_page(route_id: str):
    r = routes.get(route_id)
    if not r:
        raise HTTPException(status_code=404, detail="Route not found")

    short = (r.get("route_short_name") or route_id).strip()

    # stops sample from a few trips
    route_trip_ids = [tid for tid, t in trips.items() if t.get("route_id") == route_id][:6]
    stop_ids: List[str] = []
    seen = set()
    for tid in route_trip_ids:
        for st in stop_times_by_trip.get(tid, [])[:999]:
            sid = st.get("stop_id", "")
            if sid and sid in stops and sid not in seen:
                seen.add(sid)
                stop_ids.append(sid)
            if len(stop_ids) >= 30:
                break
        if len(stop_ids) >= 30:
            break

    center_lat, center_lon = 50.91, -1.40
    if stop_ids:
        s0 = stops[stop_ids[0]]
        try:
            center_lat = float(s0.get("stop_lat", "50.91"))
            center_lon = float(s0.get("stop_lon", "-1.40"))
        except Exception:
            pass

    live = await get_live()
    badge = '<span class="badge live">Live: ON</span>' if LIVE_ENABLED else '<span class="badge">Live: OFF</span>'
    if live.get("error"):
        badge = f'<span class="badge err">Live hiba: {esc(live["error"])}</span>'

    body = f"""
    <div class="h1">Járat {esc(short)}</div>
    <div class="kv">{badge}</div>

    <div class="h2">Élő járművek (térkép)</div>
    <div id="map" class="map"></div>

    <div class="h2">Megállók (minták)</div>
    <div class="list">
    """
    for sid in stop_ids[:20]:
        s = stops[sid]
        body += f'''
          <a class="row" href="/stop/{esc(sid)}">
            <div class="left">
              <div>
                <div class="title">{esc(s.get("stop_name",""))}</div>
                <div class="sub">{esc(sid)}</div>
              </div>
            </div>
            <div class="right">Megnyitás</div>
          </a>
        '''
    body += "</div>"

    body += f"""
<script>
const map = L.map('map').setView([{center_lat},{center_lon}], 12);
L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19, attribution: '&copy; OpenStreetMap'
}}).addTo(map);

const cluster = L.markerClusterGroup();
map.addLayer(cluster);

function popupHtml(v){{
  const line = v.line || '';
  const fleet = v.vehicle_ref || '';
  const op = v.operator || '';
  const dm = (v.delay_min === null || v.delay_min === undefined) ? '' : (v.delay_min>0?('+'+v.delay_min):(''+v.delay_min));
  return `<div style="min-width:180px">
    <div style="font-weight:800">Line: ${{line}}</div>
    <div>Fleet: <b>${{fleet}}</b></div>
    <div class="small">Op: ${{op}}</div>
    <div class="small">Δ: ${{dm}} min</div>
  </div>`;
}}

async function loadVehicles(){{
  const line = "{esc(short)}";
  const res = await fetch(`/api/live/vehicles?line=${{encodeURIComponent(line)}}`);
  const data = await res.json();
  const vs = data.vehicles || [];
  cluster.clearLayers();
  vs.forEach(v => {{
    if(!v.lat || !v.lon) return;
    const m = L.marker([v.lat, v.lon]).bindPopup(popupHtml(v));
    cluster.addLayer(m);
  }});
}}

map.on('click', async (e) => {{
  const url = `/api/vehicles/near?lat=${{e.latlng.lat}}&lon=${{e.latlng.lng}}&r={NEAR_RADIUS_M}`;
  const res = await fetch(url);
  const data = await res.json();
  const list = (data.vehicles||[]).map(v => `${{v.line||''}} / fleet=${{v.vehicle_ref||''}}`).join('<br/>') || 'Nincs a közelben.';
  L.popup().setLatLng(e.latlng).setContent(`<b>Közeli buszok</b><br/>${{list}}`).openOn(map);
}});

loadVehicles();
setInterval(loadVehicles, 15000);
</script>
"""
    return page_shell(f"Járat {short}", body)


# ----------------------------
# Stop page (LIVE + menetrend, éjfél fix)
# ----------------------------
@app.get("/stop/{stop_id}", response_class=HTMLResponse)
async def stop_page(stop_id: str):
    s = stops.get(stop_id)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")

    live = await get_live()
    live_deps = (live.get("stop_departures") or {}).get(stop_id, [])

    nowdt = now_uk()
    window_end = nowdt + timedelta(hours=2)

    timetable = []
    for st in stop_times_by_stop.get(stop_id, []):
        tid = st.get("trip_id", "")
        trip = trips.get(tid) or {}
        service_id = trip.get("service_id", "")

        dep = st.get("departure_time") or st.get("arrival_time") or ""
        dep_dt = parse_gtfs_time_to_dt_window(nowdt, dep)
        if not dep_dt:
            continue

        service_day = dep_dt.date()
        if service_id and not service_active_today(service_id, service_day):
            continue

        if dep_dt < nowdt - timedelta(minutes=2):
            continue
        if dep_dt > window_end:
            continue

        route_id = trip.get("route_id", "")
        route = routes.get(route_id) or {}
        line = (route.get("route_short_name") or route_id).strip()
        dest = trip.get("trip_headsign") or ""
        timetable.append(
            {
                "line": line,
                "destination": dest,
                "dt": dep_dt,
                "display": dep_dt.strftime("%H:%M"),
            }
        )
    timetable.sort(key=lambda x: x["dt"])

    badge = '<span class="badge live">Live: ON</span>' if LIVE_ENABLED else '<span class="badge">Live: OFF</span>'
    if live.get("error"):
        badge = f'<span class="badge err">Live hiba: {esc(live["error"])}</span>'

    body = f"""
    <div class="h1">Megálló</div>
    <div class="card">
      <div class="title">{esc(s.get("stop_name",""))}</div>
      <div class="small">{esc(stop_id)}</div>
      <div class="kv" style="margin-top:10px">{badge}</div>
    </div>

    <div class="h2">Indulások</div>
    """

    # LIVE blokk (ha van)
    if live_deps:
        body += '<div class="small" style="margin-bottom:8px">Élő (Live)</div><div class="list">'
        for dep in live_deps[:30]:
            line = dep.get("line", "") or "?"
            dest = dep.get("destination", "") or ""
            fleet = dep.get("vehicle_ref", "") or ""

            t_disp = "--:--"
            mins = ""
            try:
                ex = dep.get("expected") or dep.get("aimed") or ""
                if ex:
                    exdt = datetime.fromisoformat(ex.replace("Z", "+00:00"))
                    t_disp = exdt.astimezone(UK_TZ).strftime("%H:%M")
                    m = int(round((exdt - now_uk()).total_seconds() / 60.0))
                    mins = f"{m} perc"
            except Exception:
                pass

            tag_cls = "tag lv"
            if dep.get("is_due"):
                tag_cls = "tag due"

            body += f"""
            <div class="row">
              <div class="left">
                <div class="time">{esc(t_disp)}</div>
                <div>
                  <div class="title">{esc(line)}</div>
                  <div class="sub">{esc(dest)}</div>
                  <div class="sub">Fleet: <b>{esc(fleet)}</b></div>
                </div>
              </div>
              <div class="right">
                <span class="{tag_cls}">Live</span>
                <div class="small">{esc(mins)}</div>
              </div>
            </div>
            """
        body += "</div>"
    else:
        body += '<div class="small">Nincs élő indulás adat ehhez a megállóhoz.</div>'

    # Menetrend blokk (mindig mutatjuk, nem “live-only”)
    body += '<div class="h2">Menetrend (következő 2 óra)</div>'
    if not timetable:
        body += '<div class="small">Nincs közeli indulás a menetrendben.</div>'
    else:
        body += '<div class="list">'
        for t in timetable[:30]:
            body += f"""
            <div class="row">
              <div class="left">
                <div class="time">{esc(t["display"])}</div>
                <div>
                  <div class="title">{esc(t["line"] or "?")}</div>
                  <div class="sub">{esc(t["destination"])}</div>
                </div>
              </div>
              <div class="right">
                <span class="tag tt">Menetrend</span>
              </div>
            </div>
            """
        body += "</div>"

    return page_shell(f"Stop {stop_id}", body)


# ----------------------------
# Trip page (GTFS stop list)
# ----------------------------
@app.get("/trip/{trip_id}", response_class=HTMLResponse)
async def trip_page(trip_id: str):
    t = trips.get(trip_id)
    if not t:
        raise HTTPException(status_code=404, detail="Trip not found")

    seq = stop_times_by_trip.get(trip_id, [])
    if not seq:
        raise HTTPException(status_code=404, detail="No stop_times for trip")

    live = await get_live()
    delay_badge = ""
    for v in (live.get("vehicles") or []):
        if v.get("trip_ref") and str(v["trip_ref"]).strip() == str(trip_id).strip():
            dm = v.get("delay_min")
            if dm is not None:
                if dm > 0:
                    delay_badge = f'<span class="badge live" style="margin-left:10px">Késés: +{dm} perc</span>'
                elif dm < 0:
                    delay_badge = f'<span class="badge live" style="margin-left:10px">Siet: {dm} perc</span>'
                else:
                    delay_badge = f'<span class="badge live" style="margin-left:10px">Pontos</span>'
            break

    headsign = t.get("trip_headsign", "")
    route_id = t.get("route_id", "")
    route = routes.get(route_id) or {}
    line = (route.get("route_short_name") or route_id).strip()

    body = f"""
    <div class="h1">Trip {delay_badge}</div>
    <div class="card">
      <div class="title">{esc(line)} — {esc(headsign)}</div>
      <div class="small">{esc(trip_id)}</div>
    </div>

    <div class="h2">Megállók</div>
    <div class="list">
    """
    for st in seq[:160]:
        sid = st.get("stop_id", "")
        s = stops.get(sid) or {}
        body += f"""
        <a class="row" href="/stop/{esc(sid)}">
          <div class="left">
            <div>
              <div class="title">{esc(s.get("stop_name",""))}</div>
              <div class="sub">{esc(sid)}</div>
            </div>
          </div>
          <div class="right">Megnyitás</div>
        </a>
        """
    body += "</div>"

    return page_shell(f"Trip {trip_id}", body)


# ----------------------------
# API endpoints (frontend)
# ----------------------------
@app.get("/api/live/vehicles")
async def api_live_vehicles(line: str = Query(default="")):
    live = await get_live()
    vs = live.get("vehicles", [])
    if line:
        ln = line.strip()
        vs = [v for v in vs if (v.get("line") or "").strip() == ln]
    return {"enabled": live.get("enabled", False), "error": live.get("error", ""), "vehicles": vs}


@app.get("/api/vehicles/near")
async def api_vehicles_near(lat: float, lon: float, r: float = NEAR_RADIUS_M):
    live = await get_live()
    out = []
    for v in live.get("vehicles", []):
        try:
            d = haversine_m(lat, lon, float(v["lat"]), float(v["lon"]))
        except Exception:
            continue
        if d <= r:
            vv = dict(v)
            vv["distance_m"] = int(d)
            out.append(vv)
    out.sort(key=lambda x: x.get("distance_m", 10**9))
    return {"count": len(out), "vehicles": out}
