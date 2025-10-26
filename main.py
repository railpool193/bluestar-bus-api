import os
import csv
import json
from datetime import datetime, timedelta
from collections import defaultdict

import pytz
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

# httpx opcionális – ha nincs telepítve, a live rész egyszerűen üres lesz
try:
    import httpx
except Exception:
    httpx = None

# ------------------ Alapok / időzóna ------------------
UK_TZ = pytz.timezone("Europe/London")
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
ALLOWED_OPERATORS = {"blus", "unil"}  # csak Bluestar / Unilink

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

# ------------------ Live ENV autodetekció ------------------
def _first_truthy(*vals):
    for v in vals:
        if v:
            return v
    return ""

def _guess_url(kind: str) -> str:
    """kind: 'vm' (VehicleMonitoring) vagy 'sm' (StopMonitoring)"""
    cands = []
    for k, v in os.environ.items():
        kk = k.upper()
        vv = str(v or "").strip()
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
    # 3) BODS tipikus
    if os.getenv("OCP_APIM_SUBSCRIPTION_KEY"):
        h["Ocp-Apim-Subscription-Key"] = os.getenv("OCP_APIM_SUBSCRIPTION_KEY")
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

def _format_vm_url(line_ref: str) -> tuple[str, dict]:
    """
    Támogat:
      - "...VehicleMonitoring?LineRef={line_ref}" sablonos
      - "...VehicleMonitoring" + params={"LineRef": line_ref}
    """
    u = SIRI_VM_URL_RAW or ""
    if "{line_ref}" in u:
        return u.replace("{line_ref}", str(line_ref)), {}
    return u, {"LineRef": line_ref} if u else ({}, {})

def _format_sm_url(stop_id: str) -> tuple[str, dict]:
    """
    Támogat:
      - "...StopMonitoring?MonitoringRef={stop_id}" sablonos
      - "...StopMonitoring" + params={"MonitoringRef": stop_id}
    """
    u = SIRI_SM_URL_RAW or ""
    if "{stop_id}" in u:
        return u.replace("{stop_id}", str(stop_id)), {}
    return u, {"MonitoringRef": stop_id, "MaximumStopVisits": "10"} if u else ({}, {})

# ------------------ App & fallback sablonok ------------------
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# beépített (fallback) sablon – akkor használjuk, ha nincs fájl
from jinja2 import Environment, BaseLoader, select_autoescape

JINJA_FALLBACK = Environment(
    loader=BaseLoader(),
    autoescape=select_autoescape(["html", "xml"]),
)

STYLE = """
<style>
:root{--bg:#0f172a;--card:#111827;--muted:#94a3b8;--txt:#e5e7eb;--live:#10b981;--due:#22c55e;--link:#60a5fa;}
*{box-sizing:border-box} body{margin:0;font:16px system-ui,Segoe UI,Roboto,Helvetica,Arial;color:var(--txt);background:var(--bg)}
.wrap{max-width:980px;margin:0 auto;padding:16px}
h1{font-size:40px;margin:16px 0 12px}
h2{font-size:20px;color:var(--muted);margin:24px 0 8px}
a{color:var(--link);text-decoration:none}
a:hover{text-decoration:underline}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:12px}
.card{background:var(--card);padding:14px 16px;border-radius:14px;box-shadow:0 1px 0 rgba(255,255,255,.05) inset}
.badge{display:inline-block;background:#1f2937;border-radius:10px;padding:6px 10px;margin-right:8px}
.row{display:flex;align-items:center;justify-content:space-between;background:var(--card);padding:14px 16px;border-radius:14px;margin:10px 0}
.row .left{display:flex;gap:12px;align-items:center}
.row .time{background:#1f2937;border-radius:12px;padding:8px 10px;min-width:56px;text-align:center;font-weight:600}
.row.live{border:1px solid var(--live)}
.row.live .time{background:rgba(16,185,129,.15)}
.row.timetable{}
.row .right{color:var(--muted)}
.row.live .right{color:var(--live)}
.row.live.due{animation:blink 1s steps(2,end) infinite;border-color:var(--due)}
@keyframes blink{to{visibility:hidden}}
.topbar{display:flex;align-items:center;gap:16px;margin:8px 0 16px}
input[type="text"]{background:#0b1220;border:1px solid #1f2937;color:var(--txt);border-radius:10px;padding:8px 10px}
.small{color:var(--muted);font-size:13px}
</style>
"""

TPL_INDEX = """
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>bluestar</title>""" + STYLE + """
</head><body><div class="wrap">
<div class="topbar"><a href="/"><strong>★ bluestar</strong></a>
<form action="/search"><input name="q" placeholder="Keresés: járat vagy megálló" value="{{ q or '' }}"></form>
<div class="small">UK: {{ now_uk }}</div></div>

<h1>Járatok</h1>
<div class="grid">
{% for r in routes %}
  <div class="card"><a href="/r/{{ r.short }}"><span class="badge">{{ r.short }}</span></a><div class="small">{{ r.operator or '' }}</div></div>
{% endfor %}
</div>
</div></body></html>
"""

TPL_SEARCH = """
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Keresés</title>""" + STYLE + """
</head><body><div class="wrap">
<div class="topbar"><a href="/"><strong>★ bluestar</strong></a>
<form action="/search"><input name="q" placeholder="Keresés: járat vagy megálló" value="{{ q or '' }}"></form>
<div class="small">UK: {{ now_uk }}</div></div>

<h1>Keresés: "{{ q }}"</h1>
<h2>Járatok</h2>
{% if not routes %}<div class="small">Nincs találat.</div>{% endif %}
<div class="grid">
{% for r in routes %}
  <div class="card"><a href="/r/{{ r.short }}"><span class="badge">{{ r.short }}</span></a></div>
{% endfor %}
</div>

<h2>Megállók</h2>
{% if not stops %}<div class="small">Nincs találat.</div>{% endif %}
<div>
{% for s in stops %}
  <div class="row">
    <div class="left"><div class="time">🔹</div><div>
      <div><strong>{{ s.name }}</strong></div>
      <div class="small">{{ s.code or s.id }}</div>
    </div></div>
    <div class="right"><a href="/stop/{{ s.code or s.id }}">Megnyitás</a></div>
  </div>
{% endfor %}
</div>
</div></body></html>
"""

TPL_STOP = """
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Megálló</title>""" + STYLE + """
</head><body><div class="wrap">
<div class="topbar"><a href="/"><strong>★ bluestar</strong></a>
<form action="/search"><input name="q" placeholder="Keresés: járat vagy megálló"></form>
<div class="small">UK: {{ now_uk }}</div></div>

<h1>Megálló</h1>
<div class="small">{{ stop.stop_name }}{% if stop.stop_code %} [{{ stop.stop_code }}]{% endif %}</div>

<h2>Indulások</h2>
{% for r in rows %}
  <div class="row {{ r.row_class }}">
    <div class="left">
      <div class="time">{{ r.time_str }}</div>
      <div>
        <div><a href="/r/{{ r.route_short }}"><strong>{{ r.headsign or r.route_short }}</strong></a></div>
        <div class="small">{{ r.route_short }}{% if r.fleet %} • {{ r.fleet }}{% endif %}</div>
      </div>
    </div>
    <div class="right">
      {% if r.delay_text %}{{ r.delay_text }} • {% endif %}
      {% if r.is_live %}{{ r.wait_mins }} perc{% else %}<span class="small">menetrendi</span>{% endif %}
    </div>
  </div>
{% endfor %}
</div></body></html>
"""

TPL_ROUTE = """
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Route {{ route_short }}</title>""" + STYLE + """
</head><body><div class="wrap">
<div class="topbar"><a href="/"><strong>★ bluestar</strong></a>
<form action="/search"><input name="q" placeholder="Keresés: járat vagy megálló"></form>
<div class="small">UK: {{ now_uk }}</div></div>

<h1>Route <span class="badge">{{ route_short }}</span></h1>
<h2>Élő járművek (Bluestar/Unilink)</h2>
{% if not live_vehicles %}<div class="small">Jelenleg nincs élő jármű.</div>{% endif %}
{% for v in live_vehicles %}
  <div class="row live">
    <div class="left">
      <div class="time">BUS</div>
      <div>
        <div><strong>{{ v.fleet or "—" }}</strong></div>
        <div class="small">lat {{ '%.5f'|format(v.lat) }}, lon {{ '%.5f'|format(v.lon) }}</div>
      </div>
    </div>
    <div class="right">{{ route_short }}</div>
  </div>
{% endfor %}
</div></body></html>
"""

def render_with_fallback(template_name: str, context: dict) -> HTMLResponse:
    # Ha van fájlsablon, azt használjuk
    try:
        return templates.TemplateResponse(template_name, context)
    except Exception:
        pass
    # Fallback beépített HTML
    tpl_src = {
        "index.html": TPL_INDEX,
        "search.html": TPL_SEARCH,
        "stop.html": TPL_STOP,
        "route.html": TPL_ROUTE,
    }.get(template_name)
    if not tpl_src:
        return HTMLResponse("<h1>Template not found</h1>", status_code=500)
    tpl = JINJA_FALLBACK.from_string(tpl_src)
    return HTMLResponse(tpl.render(**{k: v for k, v in context.items() if k != "request"}))

# ------------------ GTFS betöltés és indexek ------------------
routes = []; stops = []; trips = []; stop_times = []
routes_by_id = {}; routes_by_short = defaultdict(list)
stops_by_id = {}; stops_by_code = {}
trips_by_id = {}; trips_by_route = defaultdict(list)
stop_times_by_stop = defaultdict(list); stop_times_by_trip = defaultdict(list)

def _read_csv(path):
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def load_gtfs():
    rp = os.path.join(DATA_DIR, "routes.txt")
    sp = os.path.join(DATA_DIR, "stops.txt")
    tp = os.path.join(DATA_DIR, "trips.txt")
    stp = os.path.join(DATA_DIR, "stop_times.txt")
    routes[:] = _read_csv(rp); stops[:] = _read_csv(sp)
    trips[:] = _read_csv(tp); stop_times[:] = _read_csv(stp)

    routes_by_id.clear(); routes_by_short.clear()
    for r in routes:
        rid = (r.get("route_id") or "").strip()
        routes_by_id[rid] = r
        short = (r.get("route_short_name") or "").strip()
        if short: routes_by_short[short.lower()].append(r)

    stops_by_id.clear(); stops_by_code.clear()
    for s in stops:
        sid = (s.get("stop_id") or "").strip()
        sc = (s.get("stop_code") or "").strip()
        if sid: stops_by_id[sid] = s
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

# ------------------ Live hívások (cache + URL sablon támogatás) ------------------
LIVE_CACHE = {}
LIVE_TTL = 20  # mp

def cache_get(k):
    v = LIVE_CACHE.get(k)
    if not v: return None
    if v["exp"] < datetime.utcnow().timestamp():
        LIVE_CACHE.pop(k, None); return None
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
    if c is not None: return c
    out = []
    url, params = _format_vm_url(route_short)
    if url:
        data = await http_get_json(url, params=params)
        try:
            deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
            for d in deliveries:
                for a in d.get("VehicleActivity", []):
                    j = a.get("MonitoredVehicleJourney", {}) or {}
                    line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                    op = (j.get("OperatorRef") or "").strip()
                    if route_short and line and route_short.lower() != str(line).lower():
                        continue
                    if op and not operator_ok(op): continue
                    loc = j.get("VehicleLocation") or {}
                    lat = loc.get("Latitude"); lon = loc.get("Longitude")
                    if lat is None or lon is None: continue
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
    if c is not None: return c
    items = []
    url, params = _format_sm_url(stop_code_or_id)
    if url:
        data = await http_get_json(url, params=params)
        try:
            deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("StopMonitoringDelivery", [])
            for d in deliveries:
                for v in d.get("MonitoredStopVisit", []):
                    j = v.get("MonitoredVehicleJourney", {}) or {}
                    line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                    op = (j.get("OperatorRef") or "").strip()
                    if op and not operator_ok(op): continue
                    call = j.get("MonitoredCall") or {}
                    aimed = call.get("AimedDepartureTime") or call.get("AimedArrivalTime")
                    exp = call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime")
                    dep_dt = None; delay_text = ""; is_due = False
                    try:
                        if exp:
                            dep_dt = datetime.fromisoformat(exp.replace("Z", "+00:00")).astimezone(UK_TZ)
                        elif aimed:
                            dep_dt = datetime.fromisoformat(aimed.replace("Z", "+00:00")).astimezone(UK_TZ)
                        if aimed and exp:
                            a = datetime.fromisoformat(aimed.replace("Z", "+00:00"))
                            e = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                            mins = round((e - a).total_seconds() / 60.0)
                            if mins != 0: delay_text = f"{mins:+d}m"
                        if dep_dt: is_due = abs((now_uk() - dep_dt).total_seconds()) < 60
                    except Exception:
                        pass
                    items.append({
                        "line": line, "operator": op.lower()[:4],
                        "headsign": j.get("DestinationName"),
                        "vehicle_ref": j.get("VehicleRef"),
                        "dep_dt": dep_dt, "delay_text": delay_text, "is_due": is_due,
                        "trip_id": j.get("FramedVehicleJourneyRef", {}).get("DatedVehicleJourneyRef") or "",
                    })
        except Exception:
            items = []
    cache_set(ck, items)
    return items

# ------------------ Segédek ------------------
def stop_by_any(id_or_code: str):
    return stops_by_id.get(id_or_code) or stops_by_code.get(id_or_code)

def routes_for_short(short_lower: str):
    lst = routes_by_short.get(short_lower, [])
    if not lst: return []
    res = []
    for r in lst:
        ag = (r.get("agency_id") or "").strip().lower()[:4]
        if ag and ag not in ALLOWED_OPERATORS: continue
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

    # élő, amihez nincs menetrendi pár
    for _, it in (live_by_trip or {}).items():
        if any(r["is_live"] and r["trip_id"] == (it.get("trip_id") or "") for r in rows):
            continue
        d = it.get("dep_dt")
        if not d: continue
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

# ------------------ Endpontok ------------------
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
        "live_enabled": bool(SIRI_VM_URL_RAW or SIRI_SM_URL_RAW),
        "requests_available": httpx is not None,
        "vm_url": SIRI_VM_URL_RAW,
        "sm_url": SIRI_SM_URL_RAW,
        "extra_headers": list(EXTRA_HEADERS.keys()),
    })

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    # egyedi shortok, csak BLUS/UNIL
    seen = set(); items = []
    for r in routes:
        short = (r.get("route_short_name") or "").strip()
        if not short: continue
        ag = (r.get("agency_id") or "").strip().lower()[:4]
        if ag and ag not in ALLOWED_OPERATORS: continue
        key = short.lower()
        if key in seen: continue
        seen.add(key)
        items.append({"short": short, "operator": ag or ""})
    def sort_key(x):
        s = x["short"]; num=""; suf=""
        for ch in s:
            (num := num + ch) if ch.isdigit() else (suf := suf + ch)
        return (int(num or 0), suf.lower())
    items.sort(key=sort_key)

    ctx = {"request": request, "routes": items, "q": "", "now_uk": now_uk().strftime("%H:%M:%S")}
    return render_with_fallback("index.html", ctx)

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = ""):
    q = (q or "").strip()
    routes_found = []; stops_found = []
    if q:
        for short, arr in routes_by_short.items():
            if q.lower() in short:
                r = arr[0]
                ag = (r.get("agency_id") or "").strip().lower()[:4]
                if ag and ag not in ALLOWED_OPERATORS: continue
                routes_found.append({"short": r.get("route_short_name")})
        for s in stops:
            name = s.get("stop_name") or ""
            if q.lower() in name.lower():
                stops_found.append({"id": s.get("stop_id"), "code": s.get("stop_code"), "name": name})
    ctx = {"request": request, "q": q, "routes": routes_found, "stops": stops_found, "now_uk": now_uk().strftime("%H:%M:%S")}
    return render_with_fallback("search.html", ctx)

@app.get("/stop/{sid_or_code}", response_class=HTMLResponse)
async def stop_view(request: Request, sid_or_code: str):
    s = stop_by_any(sid_or_code)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")
    rows = await rows_for_stop(s)
    ctx = {"request": request, "stop": s, "rows": rows, "now_uk": now_uk().strftime("%H:%M:%S")}
    return render_with_fallback("stop.html", ctx)

# rövid alias: régi linkekhez pl. /s/XYZ
@app.get("/s/{sid_or_code}", response_class=HTMLResponse)
async def stop_alias(request: Request, sid_or_code: str):
    return await stop_view(request, sid_or_code)

@app.get("/r/{route_short}", response_class=HTMLResponse)
async def route_view(request: Request, route_short: str):
    short = (route_short or "").lower()
    rlist = routes_for_short(short)
    if not rlist:
        raise HTTPException(status_code=404, detail="Route not found")
    live = await fetch_live_vm(rlist[0].get("route_short_name"))
    live = [v for v in (live or []) if (v.get("operator") or "") in ALLOWED_OPERATORS]
    ctx = {"request": request, "route_short": rlist[0].get("route_short_name"),
           "live_vehicles": live, "now_uk": now_uk().strftime("%H:%M:%S")}
    return render_with_fallback("route.html", ctx)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
