import os, csv, io, math, json, time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict, namedtuple

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.requests import Request

# =========================
# ---- Konfiguráció --------
# =========================
UK_TZ = ZoneInfo("Europe/London")
DATA_DIR = os.getenv("DATA_DIR", "gtfs")

# SIRI / BODS endpontok
# Példa:
#   SIRI_STOP_URL="https://.../StopMonitoring?MonitoringRef={stop_id}&MaximumStopVisits=10"
#   SIRI_VM_URL  ="https://.../VehicleMonitoring?VehicleMonitoringDetailLevel=calls&LineRef={line_ref}"
SIRI_STOP_URL = os.getenv("SIRI_STOP_URL", "")
SIRI_VM_URL   = os.getenv("SIRI_VM_URL", "")
# ha kell header vagy api-key:
#   SIRI_HEADERS='Authorization:apikey xxx; X-Client:foo'
def _parse_extra_headers(raw: str):
    heads = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part: 
            continue
        if ":" in part:
            k, v = part.split(":", 1)
            heads[k.strip()] = v.strip()
    return heads
SIRI_HEADERS = _parse_extra_headers(os.getenv("SIRI_HEADERS", ""))

ALLOWED_OPERATORS = set([s.strip().lower() for s in os.getenv("ALLOWED_OPERATORS", "blus,unil,bluestar,unilink").split(",") if s.strip()])

# Rate limit / cache
LIVE_TTL_SECONDS = int(os.getenv("LIVE_TTL_SECONDS", "20"))
VM_TTL_SECONDS   = int(os.getenv("VM_TTL_SECONDS", "25"))

# mennyi indulást mutassunk
MAX_DEPS = int(os.getenv("MAX_DEPS", "20"))
LOOKAHEAD_MIN = int(os.getenv("LOOKAHEAD_MIN", "120"))  # menetrendi keresés előre

app = FastAPI(title="bluestar")

# HTTP kliens
_http = httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=8.0))

# ==================================
#            TTL cache
# ==================================
class TTLCache:
    def __init__(self, ttl_seconds=20, max_size=256):
        self.ttl = ttl_seconds
        self.max = max_size
        self._d = {}
    def get(self, key):
        v = self._d.get(key)
        if not v: return None
        value, ts = v
        if time.time() - ts > self.ttl:
            self._d.pop(key, None)
            return None
        return value
    def set(self, key, value):
        if len(self._d) >= self.max:
            # primitív trim
            self._d.pop(next(iter(self._d)))
        self._d[key] = (value, time.time())

live_cache  = TTLCache(LIVE_TTL_SECONDS, 256)
vm_cache    = TTLCache(VM_TTL_SECONDS, 256)

# ==================================
#      GTFS betöltés
# ==================================
def _read_csv(path):
    with io.open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _opt_path(name):
    p = os.path.join(DATA_DIR, name)
    return p if os.path.exists(p) else None

# adatok
stops = {}
stops_by_code = {}
routes = {}
routes_by_short = {}
trips = {}
stop_times_by_stop = defaultdict(list)
calendar = {}
calendar_dates = defaultdict(dict)  # service_id -> {yyyymmdd: include/exclude}
shapes = defaultdict(list)

def _to_sec(hms):
    # HH:MM:SS lehet 24+ órával is (GTFS)
    parts = [int(x) for x in hms.split(":")]
    h, m, s = parts if len(parts) == 3 else (parts[0], parts[1], 0)
    return h*3600 + m*60 + s

def _today_str(d: date):
    return d.strftime("%Y%m%d")

def _service_active(service_id: str, d: date):
    # calendar + calendar_dates
    c = calendar.get(service_id)
    ds = _today_str(d)
    cd = calendar_dates.get(service_id, {}).get(ds)
    if cd is not None:
        return cd == 1  # 1=include, 2=exclude
    if not c:
        # ha nincs calendar, tekintsük aktívnak
        return True
    # dátum range
    if not (c["start"] <= d <= c["end"]):
        return False
    wd = d.weekday()  # 0=Mon
    flags = ("monday","tuesday","wednesday","thursday","friday","saturday","sunday")
    return c[flags[wd]] == 1

def load_gtfs():
    # stops
    p = _opt_path("stops.txt")
    if p:
        for r in _read_csv(p):
            sid = r["stop_id"]
            rec = {
                "id": sid,
                "name": r.get("stop_name","").strip(),
                "code": (r.get("stop_code","") or r.get("atco_code","")).strip(),
                "lat": float(r.get("stop_lat","0") or 0),
                "lon": float(r.get("stop_lon","0") or 0),
            }
            stops[sid] = rec
            if rec["code"]:
                stops_by_code[rec["code"]] = rec

    # routes
    p = _opt_path("routes.txt")
    if p:
        for r in _read_csv(p):
            rid = r["route_id"]
            rsn = (r.get("route_short_name","") or "").strip()
            agency_id = (r.get("agency_id","") or "").strip().lower()
            routes[rid] = {
                "id": rid,
                "short": rsn,
                "long": (r.get("route_long_name","") or "").strip(),
                "agency": agency_id
            }
            if rsn:
                routes_by_short[rsn] = routes[rid]

    # trips
    p = _opt_path("trips.txt")
    if p:
        for r in _read_csv(p):
            tid = r["trip_id"]
            trips[tid] = {
                "id": tid,
                "route_id": r["route_id"],
                "service_id": r.get("service_id",""),
                "headsign": (r.get("trip_headsign","") or "").strip(),
                "direction_id": r.get("direction_id",""),
                "shape_id": r.get("shape_id","")
            }

    # stop_times
    p = _opt_path("stop_times.txt")
    if p:
        for r in _read_csv(p):
            sid = r["stop_id"]
            trow = {
                "trip_id": r["trip_id"],
                "dep": _to_sec(r.get("departure_time", r.get("arrival_time","00:00:00"))),
                "arr": _to_sec(r.get("arrival_time", r.get("departure_time","00:00:00"))),
                "seq": int(r.get("stop_sequence","0"))
            }
            stop_times_by_stop[sid].append(trow)
        # rendezés
        for sid in stop_times_by_stop:
            stop_times_by_stop[sid].sort(key=lambda x: (x["dep"], x["seq"]))

    # calendar
    p = _opt_path("calendar.txt")
    if p:
        for r in _read_csv(p):
            sid = r["service_id"]
            calendar[sid] = {
                "start": datetime.strptime(r["start_date"], "%Y%m%d").date(),
                "end": datetime.strptime(r["end_date"], "%Y%m%d").date(),
                "monday": int(r["monday"]), "tuesday": int(r["tuesday"]), "wednesday": int(r["wednesday"]),
                "thursday": int(r["thursday"]), "friday": int(r["friday"]), "saturday": int(r["saturday"]), "sunday": int(r["sunday"])
            }

    p = _opt_path("calendar_dates.txt")
    if p:
        for r in _read_csv(p):
            sid = r["service_id"]
            day = r["date"]
            ex = int(r["exception_type"])
            calendar_dates[sid][day] = ex

    # shapes – opcionális
    p = _opt_path("shapes.txt")
    if p:
        for r in _read_csv(p):
            sid = r["shape_id"]
            shapes[sid].append((int(r["shape_pt_sequence"]), float(r["shape_pt_lat"]), float(r["shape_pt_lon"])))
        for sid in list(shapes):
            shapes[sid].sort(key=lambda x: x[0])

load_gtfs()

# ================================
#      Sablonok (inline Jinja)
# ================================
BASE_HTML = """
<!DOCTYPE html>
<html lang="hu">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>bluestar</title>
<link rel="preconnect" href="https://unpkg.com">
<link rel="preconnect" href="https://cdn.jsdelivr.net">
<link rel="stylesheet" href="https://unpkg.com/modern-css-reset/dist/reset.min.css">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
:root{
  --bg:#0f1115; --card:#171a21; --muted:#9aa4b2; --text:#e7ebf0; --brand:#78a6ff; 
  --green:#18c48f; --white:#fff; --danger:#ff6b6b;
}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--text);font:16px/1.5 system-ui,Segoe UI,Roboto,Arial}
a{color:#9bbcff;text-decoration:none}
a:hover{text-decoration:underline}
.wrap{max-width:980px;margin:0 auto;padding:16px}
.nav{display:flex;gap:12px;align-items:center}
.brand{font-weight:700}
.pill{display:inline-block;min-width:54px;padding:6px 10px;border-radius:12px;background:#263042;color:#cfe1ff;text-align:center}
.grid{display:grid;gap:12px}
.cards{display:grid;gap:12px;grid-template-columns:repeat(auto-fill, minmax(240px, 1fr))}
.card{background:var(--card);padding:14px;border-radius:14px;box-shadow:0 2px 8px rgba(0,0,0,.25)}
.head{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.sub{color:var(--muted);font-size:14px}
.live{background:rgba(24,196,143,.15);border:1px solid rgba(24,196,143,.35)}
.badge{font-size:12px;padding:4px 8px;border-radius:999px;background:#243; color:#9ff;border:1px solid #355}
.white{background:#222;border:1px solid #333}
.right{margin-left:auto;color:#9bbcff}
.keslelt{color:var(--danger);font-weight:600}
.eleny{opacity:.75}
.flex{display:flex;align-items:center;gap:10px}
.h1{font-size:34px;font-weight:800;margin:18px 0}
.h2{font-size:22px;font-weight:700;margin:12px 0}
input[type=text]{background:#0c0f14;border:1px solid #2a3140;border-radius:10px;color:#dbe6ff;padding:8px 10px}
.small{font-size:12px;color:#97a4b3}
.row{display:flex;align-items:center;justify-content:space-between;gap:12px;background:var(--card);padding:12px;border-radius:12px;margin-bottom:10px}
.row.live{background:rgba(24,196,143,.10);border:1px solid rgba(24,196,143,.35)}
.row .left{display:flex;align-items:center;gap:12px}
.row .dest{font-weight:700}
.row .mins{color:#9bbcff}
.flash{animation:flash 1s infinite}
@keyframes flash{0%{box-shadow:0 0 0 0 rgba(24,196,143,.6)}50%{box-shadow:0 0 0 6px rgba(24,196,143,0)}100%{box-shadow:0 0 0 0 rgba(24,196,143,0)}}
.map{height:320px;border-radius:14px;overflow:hidden;border:1px solid #2b3344}
code{background:#1a1f2b;padding:2px 6px;border-radius:6px}
hr{border-color:#20283a;margin:18px 0}
</style>
</head>
<body>
<div class="wrap">
  <div class="nav">
    <div class="brand"><a href="/">★ bluestar</a></div>
    <form action="/search" method="get" class="flex" style="margin-left:12px;">
      <input type="text" name="q" placeholder="Keresés: járat vagy megálló" value="{{ q or '' }}">
    </form>
    <div class="right small">UK: {{ now }}</div>
  </div>
  {% block body %}{% endblock %}
</div>
</body>
</html>
"""

HOME_HTML = """
{% extends "base" %}{% block body %}
<div class="h1">Járatok</div>
<div class="cards">
{% for r in routes %}
  <div class="card white">
    <div class="head"><div class="h2">{{ r.short or r.long }}</div><span class="small">{{ r.agency or 'blus' }}</span></div>
    <div><a href="/r/{{ r.short or r.id }}">Megnyitás</a></div>
  </div>
{% endfor %}
</div>
{% endblock %}
"""

SEARCH_HTML = """
{% extends "base" %}{% block body %}
<div class="h1">Keresés: "{{ term }}"</div>
<div class="h2">Járatok</div>
{% if routes %}
  <div class="cards">{% for r in routes %}
    <div class="card white"><div class="h2">{{ r.short or r.long }}</div><div><a href="/r/{{ r.short or r.id }}">Megnyitás</a></div></div>
  {% endfor %}</div>
{% else %}<div class="small">Nincs találat.</div>{% endif %}

<div class="h2" style="margin-top:16px;">Megállók</div>
{% if stops %}
  <div class="grid">{% for s in stops %}
    <div class="row"><div class="left"><div class="pill">{{ s.code or s.id }}</div><div>{{ s.name }}</div></div><a href="/stop/{{ s.code or s.id }}">Megnyitás</a></div>
  {% endfor %}</div>
{% else %}<div class="small">Nincs találat.</div>{% endif %}
{% endblock %}
"""

STOP_HTML = """
{% extends "base" %}{% block body %}
<div class="h1">Megálló</div>
<div class="sub">{{ stop.name }} {% if stop.code %}[{{ stop.code }}]{% endif %}</div>
<div class="h2" style="margin-top:14px;">Indulások</div>
{% for row in deps %}
  <div class="row {% if row.is_live %}live{% endif %} {% if row.is_due %}flash{% endif %}">
    <div class="left">
      <div class="pill">{{ row.time_str }}</div>
      <div>
        <div class="dest"><a href="/t/{{ row.trip_hint }}">{{ row.dest }}</a></div>
        <div class="small">{{ row.line }} • {{ row.provider }} {% if row.fleet %}• {{ row.fleet }}{% endif %}</div>
      </div>
    </div>
    <div class="mins">
      {% if row.delta_min is not none and row.is_live %}
        <span class="{% if row.delta_min>0 %}keslelt{% else %}eleny{% endif %}">{% if row.delta_min>0 %}+{% endif %}{{ row.delta_min }} min</span>
      {% else %}
        {{ row.mins }} perc
      {% endif %}
    </div>
  </div>
{% endfor %}
{% endblock %}
"""

ROUTE_HTML = """
{% extends "base" %}{% block body %}
<div class="h1">Route <span class="pill">{{ route.short or route.id }}</span></div>
<div class="sub">Élő járművek (Bluestar/Unilink)</div>
<div id="map" class="map" style="margin-top:12px;"></div>
<div class="small" style="margin-top:8px;">Járművek: {{ vehicles|length }} db</div>
<script>
  const map = L.map('map').setView([50.9097,-1.4044], 10);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19}).addTo(map);
  const markers = [];
  {% for v in vehicles %}
    const m = L.marker([{{ v.lat }}, {{ v.lon }}]).addTo(map);
    m.bindPopup(`<b>{{ v.line }}</b> → {{ v.dest }}<br>Fleet: {{ v.fleet or '-' }}<br>{{ v.when }}`);
    markers.push(m);
  {% endfor %}
  if (markers.length){ 
    const g = L.featureGroup(markers); map.fitBounds(g.getBounds().pad(0.2));
  }
</script>
{% endblock %}
"""

TRIP_HTML = """
{% extends "base" %}{% block body %}
<div class="h1">Járat</div>
<div id="map" class="map"></div>
<script>
  const map = L.map('map').setView([50.9097,-1.4044], 11);
  L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19}).addTo(map);
  const pts = {{ points | tojson }};
  const latlngs = pts.map(p => [p[0], p[1]]);
  if (latlngs.length){
    const line = L.polyline(latlngs, {weight:5}).addTo(map);
    map.fitBounds(line.getBounds().pad(0.15));
    for (const p of pts){
      L.circleMarker([p[0], p[1]], {radius:4}).addTo(map);
    }
  }
</script>
<div class="h2" style="margin-top:12px;">Megállók</div>
{% for r in rows %}
  <div class="row {% if r.is_live %}live{% endif %} {% if r.is_due %}flash{% endif %}">
    <div class="left">
      <div class="pill">{{ r.time_str }}</div>
      <div class="dest">{{ r.name }}</div>
    </div>
    <div class="mins">
      {% if r.delta_min is not none and r.is_live %}
        <span class="{% if r.delta_min>0 %}keslelt{% else %}eleny{% endif %}">{% if r.delta_min>0 %}+{% endif %}{{ r.delta_min }} min</span>
      {% endif %}
    </div>
  </div>
{% endfor %}
{% endblock %}
"""

# egyszerű sablon motor
from jinja2 import Environment, DictLoader, select_autoescape
_env = Environment(
    loader=DictLoader({
        "base": BASE_HTML,
        "home": HOME_HTML,
        "search": SEARCH_HTML,
        "stop": STOP_HTML,
        "route": ROUTE_HTML,
        "trip": TRIP_HTML,
    }),
    autoescape=select_autoescape(['html'])
)

def render(tpl, **ctx):
    ctx.setdefault("now", datetime.now(UK_TZ).strftime("%H:%M:%S"))
    return HTMLResponse(_env.get_template(tpl).render(**ctx))

# ==================================
#        SIRI parserek
# ==================================
def _lower(s): return (s or "").strip().lower()

def _ok_live():
    return bool(SIRI_STOP_URL or SIRI_VM_URL)

async def siri_stop(stop_code: str):
    """StopMonitoring lekérés + cache"""
    if not _ok_live() or not SIRI_STOP_URL:
        return {"ok": False, "items": []}
    url = SIRI_STOP_URL.replace("{stop_id}", stop_code)
    cached = live_cache.get(url)
    if cached is not None:
        return cached
    try:
        r = await _http.get(url, headers=SIRI_HEADERS)
        r.raise_for_status()
        data = r.json()
        items = []
        # BODS SIRI-JSON struktúra (általános)
        visits = (
            data.get("Siri", {})
                .get("ServiceDelivery", {})
                .get("StopMonitoringDelivery", [{}])[0]
                .get("MonitoredStopVisit", [])
        )
        for v in visits:
            mvj = v.get("MonitoredVehicleJourney", {})
            mc  = mvj.get("MonitoredCall", {})
            aimed = mc.get("AimedDepartureTime") or mc.get("AimedArrivalTime")
            exp   = mc.get("ExpectedDepartureTime") or mc.get("ExpectedArrivalTime")
            def _parse_iso(x):
                if not x: return None
                try:
                    return datetime.fromisoformat(x.replace("Z","+00:00")).astimezone(UK_TZ)
                except: return None
            aimed_dt = _parse_iso(aimed)
            exp_dt   = _parse_iso(exp)
            line = mvj.get("LineRef") or mvj.get("PublishedLineName")
            dest = mvj.get("DestinationName") or mvj.get("DirectionName") or ""
            veh  = mvj.get("VehicleRef") or mvj.get("VehicleRefRef")
            owner= _lower(mvj.get("DataOwnerRef") or mvj.get("OperatorRef") or "")
            # csak engedélyezett operátorok
            if ALLOWED_OPERATORS and owner and (owner not in ALLOWED_OPERATORS):
                continue
            delta_min = None
            if aimed_dt and exp_dt:
                delta_min = round((exp_dt - aimed_dt).total_seconds()/60)
            when = exp_dt or aimed_dt
            if not when: 
                continue
            items.append({
                "line": str(line or ""),
                "dest": str(dest or ""),
                "when": when,
                "aimed": aimed_dt,
                "expected": exp_dt,
                "delta_min": delta_min,
                "fleet": veh,
                "provider": owner or "blus",
                "trip_hint": mvj.get("DatedVehicleJourneyRef") or ""
            })
        res = {"ok": True, "items": items}
        live_cache.set(url, res)
        return res
    except Exception as e:
        return {"ok": False, "err": str(e), "items": []}

async def siri_vm(line_ref: str):
    """VehicleMonitoring adott viszonylatra"""
    if not _ok_live() or not SIRI_VM_URL:
        return {"ok": False, "vehicles": []}
    url = SIRI_VM_URL.replace("{line_ref}", line_ref)
    cached = vm_cache.get(url)
    if cached is not None:
        return cached
    try:
        r = await _http.get(url, headers=SIRI_HEADERS)
        r.raise_for_status()
        data = r.json()
        vehicles = []
        ent = (
            data.get("Siri", {})
                .get("ServiceDelivery", {})
                .get("VehicleMonitoringDelivery", [{}])[0]
                .get("VehicleActivity", [])
        )
        for v in ent:
            mj = v.get("MonitoredVehicleJourney", {})
            owner = _lower(mj.get("DataOwnerRef") or mj.get("OperatorRef") or "")
            if ALLOWED_OPERATORS and owner and (owner not in ALLOWED_OPERATORS):
                continue
            line = mj.get("LineRef") or mj.get("PublishedLineName")
            dest = mj.get("DestinationName") or ""
            veh  = mj.get("VehicleRef")
            vp = mj.get("VehicleLocation", {})
            lat = vp.get("Latitude")
            lon = vp.get("Longitude")
            ts = v.get("RecordedAtTime")
            vehicles.append({
                "line": str(line or ""),
                "dest": str(dest or ""),
                "fleet": veh,
                "lat": float(lat or 0),
                "lon": float(lon or 0),
                "when": ts or ""
            })
        res = {"ok": True, "vehicles": vehicles}
        vm_cache.set(url, res)
        return res
    except Exception as e:
        return {"ok": False, "err": str(e), "vehicles": []}

# ==================================
#             Segédek
# ==================================
def _now_sec():
    now = datetime.now(UK_TZ)
    return now, now.hour*3600 + now.minute*60 + now.second

def _mins_until(target_dt: datetime):
    now = datetime.now(UK_TZ)
    return max(0, int(round((target_dt - now).total_seconds()/60)))

def _fmt_time(dt: datetime): return dt.strftime("%H:%M")

def _merge_departures(scheduled_rows, live_rows):
    """
    Duplikáció kiszűrése:
    - kulcs: (line, dest_norm, minute_bucket)
    - ha van LIVE egy kulcsra, menetrendit eldobjuk.
    """
    def key_s(row):
        minute_bucket = int(row["when_sec"]/60)
        return (row["line"], row["dest_norm"], minute_bucket)

    def key_l(row):
        minute_bucket = int(row["when_dt"].hour*60 + row["when_dt"].minute)
        return (row["line"], row["dest_norm"], minute_bucket)

    live_keys = set()
    for r in live_rows:
        live_keys.add(key_l(r))

    out = []
    for r in live_rows:
        out.append({
            "is_live": True,
            "line": r["line"],
            "dest": r["dest"],
            "dest_norm": r["dest_norm"],
            "time_str": _fmt_time(r["when_dt"]),
            "mins": _mins_until(r["when_dt"]),
            "delta_min": r["delta_min"],
            "fleet": r["fleet"],
            "provider": r["provider"],
            "is_due": _mins_until(r["when_dt"]) <= 1,
            "trip_hint": r.get("trip_hint","")
        })
    for r in scheduled_rows:
        k = key_s(r)
        if k in live_keys:
            continue  # van live -> menetrendit rejtjük
        out.append({
            "is_live": False,
            "line": r["line"],
            "dest": r["dest"],
            "dest_norm": r["dest_norm"],
            "time_str": r["time_str"],
            "mins": max(0, int(round((r["when_sec"] - _now_sec()[1])/60))),
            "delta_min": None,
            "fleet": None,
            "provider": r["provider"],
            "is_due": False,
            "trip_hint": r.get("trip_hint","")
        })
    # rendezés idő szerint
    out.sort(key=lambda x: (x["is_live"] is False, x["mins"]))  # live előrébb
    return out[:MAX_DEPS]

def _norm(s): return " ".join((s or "").lower().split())

# ==================================
#             Endpontok
# ==================================
@app.get("/", response_class=HTMLResponse)
async def home():
    lst = sorted(routes.values(), key=lambda r: (r["agency"], r["short"] or r["long"]))
    return render("home", routes=lst, q="")

@app.get("/c", response_class=JSONResponse)
async def cfg():
    return {
        "DATA_DIR": DATA_DIR,
        "routes.txt": bool(_opt_path("routes.txt")),
        "stops.txt": bool(_opt_path("stops.txt")),
        "trips.txt": bool(_opt_path("trips.txt")),
        "stop_times.txt": bool(_opt_path("stop_times.txt")),
        "routes_count": len(routes),
        "stops_count": len(stops),
        "live_enabled": _ok_live(),
        "requests_available": True,
        "live_cache_ok": True,
        "live_cache_err": "",
        "vm_url": SIRI_VM_URL or "",
        "sm_url": SIRI_STOP_URL or "",
        "extra_headers": list(SIRI_HEADERS.items()) if SIRI_HEADERS else []
    }

@app.get("/search", response_class=HTMLResponse)
async def search(q: str = Query(""), request: Request = None):
    term = (q or "").strip()
    r_hits = []
    s_hits = []
    if term:
        tl = term.lower()
        for r in routes.values():
            if (r["short"] and tl in r["short"].lower()) or (r["long"] and tl in r["long"].lower()):
                r_hits.append(r)
        for s in stops.values():
            if tl in (s["name"] or "").lower() or tl in (s["code"] or "").lower():
                s_hits.append(s)
        r_hits = sorted(r_hits, key=lambda x:(x["agency"], x["short"] or x["id"]))[:60]
        s_hits = sorted(s_hits, key=lambda x:x["name"])[:60]
    return render("search", routes=r_hits, stops=s_hits, term=term, q=term)

def _find_stop_any(code_or_id: str):
    # próbáld code alapján, ha nincs, id
    s = stops_by_code.get(code_or_id) or stops.get(code_or_id)
    return s

@app.get("/stop/{code}", response_class=HTMLResponse)
async def stop_view(code: str):
    st = _find_stop_any(code)
    if not st:
        return HTMLResponse("Ismeretlen megálló.", status_code=404)
    now_dt, now_sec = _now_sec()
    # SCHEDULED – keresés a következő LOOKAHEAD_MIN percre
    scheduled = []
    end_sec = now_sec + LOOKAHEAD_MIN*60
    for row in stop_times_by_stop.get(st["id"], []):
        dep = row["dep"]
        # napátcsúszás kezelése – engedjük meg, hogy 24:xx is legyen
        if dep < now_sec: 
            # ha nagyon múlt, hagyjuk
            if dep < now_sec - 300: 
                continue
        if dep > end_sec:
            break
        # route + headsign
        tr = trips.get(row["trip_id"])
        if not tr: 
            continue
        if not _service_active(tr["service_id"], now_dt.date()):
            continue
        rt = routes.get(tr["route_id"], {})
        line = rt.get("short") or rt.get("long") or rt.get("id")
        dest = tr.get("headsign") or ""
        scheduled.append({
            "line": str(line),
            "dest": dest,
            "dest_norm": _norm(dest),
            "time_str": "{:02d}:{:02d}".format((dep//3600)%24, (dep//60)%60),
            "when_sec": dep,
            "provider": rt.get("agency") or "blus",
            "trip_hint": tr["id"]
        })
    # LIVE
    live_rows = []
    live = await siri_stop(st.get("code") or st["id"])
    if live.get("ok"):
        for it in live["items"]:
            live_rows.append({
                "line": str(it["line"]),
                "dest": it["dest"],
                "dest_norm": _norm(it["dest"]),
                "when_dt": it["expected"] or it["when"],
                "delta_min": it["delta_min"],
                "fleet": it.get("fleet"),
                "provider": it.get("provider") or "blus",
                "trip_hint": it.get("trip_hint") or ""
            })
    deps = _merge_departures(scheduled, live_rows)
    return render("stop", stop=st, deps=deps, q="")

@app.get("/r/{short}", response_class=HTMLResponse)
async def route_view(short: str):
    r = routes_by_short.get(short) or routes.get(short)
    if not r:
        return HTMLResponse(json.dumps({"detail":"Not Found"}), status_code=404, media_type="application/json")
    # vehicle monitoring adott line-ra
    vehicles = []
    vm = await siri_vm(r.get("short") or r["id"])
    if vm.get("ok"):
        vehicles = vm["vehicles"]
    return render("route", route=r, vehicles=vehicles, q="")

@app.get("/t/{trip_id}", response_class=HTMLResponse)
async def trip_view(trip_id: str):
    tr = trips.get(trip_id)
    if not tr:
        # ha link csak "hint", ne dőljünk el
        return render("trip", rows=[], points=[], q="")
    # a shape-ből vagy a stopokból poliline
    pts = []
    if tr.get("shape_id") and tr["shape_id"] in shapes:
        pts = [[lat, lon] for _,lat,lon in shapes[tr["shape_id"]]]
    else:
        # állítsuk össze a stop_times alapján
        rows = []
        for sid, arr in stop_times_by_stop.items():
            for stt in arr:
                if stt["trip_id"] == trip_id:
                    s = stops.get(sid)
                    if s: pts.append([s["lat"], s["lon"]])
    # megálló sorok + (ha van) live delta
    rows=[]
    # stop sorrendhez: gyűjtsük ki a saját stop_times-okat
    seqs=[]
    for sid, arr in stop_times_by_stop.items():
        for stt in arr:
            if stt["trip_id"] == trip_id:
                seqs.append((stt["seq"], sid, stt["dep"]))
    seqs.sort()
    now_dt = datetime.now(UK_TZ)
    for seq, sid, dep in seqs:
        st = stops.get(sid)
        if not st: 
            continue
        aimed_dt = now_dt.replace(hour=(dep//3600)%24, minute=(dep//60)%60, second=0, microsecond=0)
        rows.append({
            "name": st["name"],
            "time_str": "{:02d}:{:02d}".format((dep//3600)%24, (dep//60)%60),
            "is_live": False,
            "delta_min": None,
            "is_due": False
        })
    return render("trip", rows=rows, points=pts, q="")

# --------------------------
# redirect a régi rövid utakról
@app.get("/stop", include_in_schema=False)
async def stop_root():
    return JSONResponse({"detail":"Not Found"}, status_code=404)

# ======================
#        Futás
# ======================
@app.on_event("shutdown")
async def _shutdown():
    await _http.aclose()
