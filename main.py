import os
import csv
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse

# httpx opcionális – ha nincs telepítve, a live egyszerűen üres lesz (de nem crashel)
try:
    import httpx
except Exception:
    httpx = None

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bluestar")

# ------------------------- Alapbeállítások -------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
UK_TZ = pytz.timezone("Europe/London")
ALLOWED_OPERATORS = {"blus", "unil"}  # Bluestar / Unilink

def now_uk() -> datetime:
    return datetime.now(UK_TZ)

def midnight_uk(dt: datetime | None = None) -> datetime:
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
    return int(round((dt - now_uk()).total_seconds() / 60))

def operator_ok(op: str) -> bool:
    return (op or "").strip().lower()[:4] in ALLOWED_OPERATORS

# ------------------------- Live URL autodetekció -------------------------
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

def _build_extra_headers() -> dict:
    h = {}
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

def _format_vm_url(line_ref: str):
    u = SIRI_VM_URL_RAW or ""
    if not u:
        return "", {}
    if "{line_ref}" in u:
        return u.replace("{line_ref}", str(line_ref)), {}
    return u, {"LineRef": line_ref}

def _format_sm_url(stop_id: str):
    u = SIRI_SM_URL_RAW or ""
    if not u:
        return "", {}
    if "{stop_id}" in u:
        return u.replace("{stop_id}", str(stop_id)), {}
    return u, {"MonitoringRef": stop_id, "MaximumStopVisits": "10"}

# ------------------------- Beépített sötét sablonok -------------------------
from jinja2 import Environment, BaseLoader, select_autoescape
JINJA = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html", "xml"]))

STYLE = """
<style>
:root{--bg:#0f172a;--card:#111827;--muted:#94a3b8;--txt:#e5e7eb;--live:#10b981;--due:#22c55e;--link:#60a5fa;}
*{box-sizing:border-box} body{margin:0;font:16px system-ui,Segoe UI,Roboto,Helvetica,Arial;color:var(--txt);background:var(--bg)}
.wrap{max-width:980px;margin:0 auto;padding:16px}
h1{font-size:40px;margin:16px 0 12px}
h2{font-size:20px;color:var(--muted);margin:24px 0 8px}
a{color:var(--link);text-decoration:none} a:hover{text-decoration:underline}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:12px}
.card{background:var(--card);padding:14px 16px;border-radius:14px;box-shadow:0 1px 0 rgba(255,255,255,.05) inset}
.badge{display:inline-block;background:#1f2937;border-radius:10px;padding:6px 10px;margin-right:8px}
.row{display:flex;align-items:center;justify-content:space-between;background:var(--card);padding:14px 16px;border-radius:14px;margin:10px 0}
.row .left{display:flex;gap:12px;align-items:center}
.row .time{background:#1f2937;border-radius:12px;padding:8px 10px;min-width:62px;text-align:center;font-weight:600}
.row.live{border:1px solid var(--live)}
.row.live .time{background:rgba(16,185,129,.15)}
.row .right{color:var(--muted)}
.row.live .right{color:var(--live)}
.row.live.due{animation:blink 1s steps(2,end) infinite;border-color:var(--due)}
@keyframes blink{to{visibility:hidden}}
.topbar{display:flex;align-items:center;gap:16px;margin:8px 0 16px;flex-wrap:wrap}
input[type="text"]{background:#0b1220;border:1px solid #1f2937;color:var(--txt);border-radius:10px;padding:8px 10px;min-width:240px}
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
  <div class="card">
    <a href="/r/{{ r.key }}"><span class="badge">{{ r.short }}</span></a>
    <div class="small">{{ r.operator or '' }}</div>
  </div>
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
  <div class="card"><a href="/r/{{ r.key }}"><span class="badge">{{ r.short }}</span></a></div>
{% endfor %}
</div>

<h2>Megállók</h2>
{% if not stops %}<div class="small">Nincs találat.</div>{% endif %}
<div>
{% for s in stops %}
  <div class="row">
    <div class="left">
      <div class="time">🔹</div>
      <div>
        <div><strong>{{ s.name }}</strong></div>
        <div class="small">{{ s.code or s.id }}</div>
      </div>
    </div>
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
{% if not rows %}<div class="small">Nincs közelgő indulás.</div>{% endif %}
{% for r in rows %}
  <div class="row {{ r.row_class }}">
    <div class="left">
      <div class="time">{{ r.time_str }}</div>
      <div>
        <div><a href="/r/{{ r.route_key }}"><strong>{{ r.headsign or r.route_short }}</strong></a></div>
        <div class="small">
          {{ r.route_short }}
          {% if r.fleet %} • fleet {{ r.fleet }}{% endif %}
          {% if r.is_live %} • <span style="color:var(--live)">LIVE</span>{% else %} • <span class="small">menetrendi</span>{% endif %}
        </div>
      </div>
    </div>
    <div class="right">
      {% if r.delay_text %}{{ r.delay_text }} • {% endif %}
      {% if r.is_live %}
        {% if r.wait_mins <= 0 %}due{% else %}{{ r.wait_mins }} perc{% endif %}
      {% else %}
        {{ r.wait_mins }} perc
      {% endif %}
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

def render(tpl_src: str, **ctx) -> HTMLResponse:
    tpl = JINJA.from_string(tpl_src)
    return HTMLResponse(tpl.render(**ctx))

# ------------------------- GTFS betöltés -------------------------
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

def _read_csv(path: str):
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

    routes_by_id.clear()
    routes_by_short.clear()
    for r in routes:
        rid = (r.get("route_id") or "").strip()
        routes_by_id[rid] = r
        short = (r.get("route_short_name") or "").strip()
        if short:
            routes_by_short[short.lower()].append(r)

    stops_by_id.clear()
    stops_by_code.clear()
    for s in stops:
        sid = (s.get("stop_id") or "").strip()
        sc = (s.get("stop_code") or "").strip()
        if sid:
            stops_by_id[sid] = s
        if sc:
            stops_by_code[sc] = s

    trips_by_id.clear()
    trips_by_route.clear()
    for t in trips:
        tid = (t.get("trip_id") or "").strip()
        rid = (t.get("route_id") or "").strip()
        if tid:
            trips_by_id[tid] = t
        if rid:
            trips_by_route[rid].append(t)

    stop_times_by_stop.clear()
    stop_times_by_trip.clear()
    for st in stop_times:
        sid = (st.get("stop_id") or "").strip()
        tid = (st.get("trip_id") or "").strip()
        if sid:
            stop_times_by_stop[sid].append(st)
        if tid:
            stop_times_by_trip[tid].append(st)

    for tid, arr in stop_times_by_trip.items():
        arr.sort(key=lambda x: gtfs_sec(x.get("departure_time") or x.get("arrival_time") or ""))

# ------------------------- Live hívások + cache -------------------------
LIVE_CACHE = {}
LIVE_TTL = 20  # sec

def cache_get(k):
    v = LIVE_CACHE.get(k)
    if not v:
        return None
    if v["exp"] < datetime.utcnow().timestamp():
        LIVE_CACHE.pop(k, None)
        return None
    return v["val"]

def cache_set(k, val):
    LIVE_CACHE[k] = {"val": val, "exp": datetime.utcnow().timestamp() + LIVE_TTL}

async def http_get_json(url: str, params=None):
    if not url or httpx is None:
        return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params or {}, headers=EXTRA_HEADERS)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        log.warning("live request failed: %s", e)
        return None

def _parse_iso(dt_str: str):
    try:
        if not dt_str:
            return None
        ds = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ds).astimezone(UK_TZ)
    except Exception:
        return None

async def fetch_live_vm(route_short: str):
    if not route_short:
        return []
    ck = ("vm", route_short.lower())
    c = cache_get(ck)
    if c is not None:
        return c

    out = []
    url, params = _format_vm_url(route_short)
    if url:
        data = await http_get_json(url, params=params)
        try:
            deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", []) or []
            for d in deliveries:
                for a in (d.get("VehicleActivity") or []):
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

async def fetch_live_sm(stop_code_or_id: str):
    ck = ("sm", stop_code_or_id)
    c = cache_get(ck)
    if c is not None:
        return c

    items = []
    url, params = _format_sm_url(stop_code_or_id)
    if url:
        data = await http_get_json(url, params=params)
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
                    dep_dt = _parse_iso(exp) or _parse_iso(aimed)

                    delay_text = ""
                    if aimed and exp:
                        a = _parse_iso(aimed)
                        e = _parse_iso(exp)
                        if a and e:
                            mins = round((e - a).total_seconds() / 60.0)
                            if mins != 0:
                                delay_text = f"{mins:+d}m"

                    is_due = bool(dep_dt and abs((now_uk() - dep_dt).total_seconds()) < 60)

                    items.append({
                        "line": line,
                        "operator": (op or "").lower()[:4],
                        "headsign": j.get("DestinationName") or "",
                        "vehicle_ref": j.get("VehicleRef") or "",
                        "dep_dt": dep_dt,
                        "delay_text": delay_text,
                        "is_due": is_due,
                        # ha nincs jó trip_id, akkor is működjön a dedup route+time alapján
                        "trip_id": (j.get("FramedVehicleJourneyRef", {}) or {}).get("DatedVehicleJourneyRef") or "",
                    })
        except Exception as e:
            log.warning("parse SM failed: %s", e)
            items = []

    cache_set(ck, items)
    return items

# ------------------------- Segédek -------------------------
def stop_by_any(id_or_code: str):
    return stops_by_id.get(id_or_code) or stops_by_code.get(id_or_code)

def routes_for_short(short_lower: str):
    lst = routes_by_short.get(short_lower, [])
    if not lst:
        return []
    res = []
    for r in lst:
        ag = (r.get("agency_id") or "").strip().lower()[:4]
        if ag and ag not in ALLOWED_OPERATORS:
            continue
        res.append(r)
    return res or lst

def _dedup_key_live(it):
    # prefer trip_id, de ha üres, akkor line+dep_dt perc pontossággal
    tid = (it.get("trip_id") or "").strip()
    line = (it.get("line") or "").strip().lower()
    dep = it.get("dep_dt")
    if tid:
        return ("tid", tid, line)
    if dep:
        return ("fallback", line, dep.replace(second=0, microsecond=0))
    return ("fallback", line, None)

async def rows_for_stop(stop_obj, minutes_ahead=120):
    now = now_uk()
    until = now + timedelta(minutes=minutes_ahead)
    sid = stop_obj.get("stop_id")
    scode = (stop_obj.get("stop_code") or stop_obj.get("stop_id") or "").strip()

    live_raw = await fetch_live_sm(scode)
    live_index = {}
    for it in live_raw or []:
        if not it.get("dep_dt"):
            continue
        live_index[_dedup_key_live(it)] = it

    rows = []

    # menetrendi sorok -> ha van élő, menetrendit elnyomjuk ugyanarra
    for st in stop_times_by_stop.get(sid, []):
        tid = st.get("trip_id")
        trip = trips_by_id.get(tid, {}) or {}
        rid = (trip.get("route_id") or "").strip()
        route = routes_by_id.get(rid, {}) or {}

        route_short = (route.get("route_short_name") or "").strip()
        agency = (route.get("agency_id") or "").strip().lower()[:4]
        if agency and agency not in ALLOWED_OPERATORS:
            continue

        dep_sec = gtfs_sec(st.get("departure_time") or st.get("arrival_time") or "")
        dep_dt = sec_to_today(dep_sec)
        if dep_dt < (now - timedelta(minutes=1)) or dep_dt > until:
            continue

        headsign = (trip.get("trip_headsign") or "").strip()

        # matching: először trip_id+line, ha nem jó, akkor line+dep idő perc pontossággal
        fake_live = {"trip_id": tid or "", "line": route_short, "dep_dt": dep_dt}
        lk1 = _dedup_key_live(fake_live)
        lk2 = ("fallback", (route_short or "").lower(), dep_dt.replace(second=0, microsecond=0))
        live_hit = live_index.get(lk1) or live_index.get(lk2)

        if live_hit:
            d = live_hit["dep_dt"]
            wait = mins_from_now(d)
            rows.append({
                "time_str": fmt_hhmm(d),
                "time_dt": d,
                "headsign": (live_hit.get("headsign") or headsign),
                "route_short": route_short or live_hit.get("line") or "",
                "route_key": (route_short or live_hit.get("line") or "").lower(),
                "wait_mins": wait,
                "is_live": True,
                "row_class": "live due" if live_hit.get("is_due") or wait <= 0 else "live",
                "trip_id": tid or "",
                "fleet": live_hit.get("vehicle_ref") or "",
                "delay_text": live_hit.get("delay_text") or "",
            })
        else:
            wait = mins_from_now(dep_dt)
            rows.append({
                "time_str": fmt_hhmm(dep_dt),
                "time_dt": dep_dt,
                "headsign": headsign,
                "route_short": route_short,
                "route_key": route_short.lower(),
                "wait_mins": wait,
                "is_live": False,
                "row_class": "timetable",
                "trip_id": tid or "",
                "fleet": "",
                "delay_text": "",
            })

    # olyan LIVE indulások, amikhez nincs menetrendi sor
    for it in (live_raw or []):
        d = it.get("dep_dt")
        if not d:
            continue
        if d < (now - timedelta(minutes=1)) or d > until:
            continue

        # ha már bekerült menetrendi sor live-ként, ne duplázzuk
        key = _dedup_key_live(it)
        if any((_dedup_key_live({"trip_id": r["trip_id"], "line": r["route_short"], "dep_dt": r["time_dt"]}) == key) for r in rows if r["is_live"]):
            continue

        wait = mins_from_now(d)
        rows.append({
            "time_str": fmt_hhmm(d),
            "time_dt": d,
            "headsign": it.get("headsign") or "",
            "route_short": it.get("line") or "",
            "route_key": (it.get("line") or "").lower(),
            "wait_mins": wait,
            "is_live": True,
            "row_class": "live due" if it.get("is_due") or wait <= 0 else "live",
            "trip_id": it.get("trip_id") or "",
            "fleet": it.get("vehicle_ref") or "",
            "delay_text": it.get("delay_text") or "",
        })

    # rendezés: idő szerint, ha egyező, live előre
    rows.sort(key=lambda x: (x["time_dt"], 0 if x["is_live"] else 1))
    return rows

# ------------------------- FastAPI -------------------------
app = FastAPI()

@app.on_event("startup")
def _startup():
    load_gtfs()
    log.info("GTFS loaded: routes=%d stops=%d", len(routes), len(stops))

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
def index():
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
        items.append({"short": short, "key": key, "operator": ag or ""})

    def sort_key(x):
        s = x["short"]
        num = ""
        suf = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            else:
                suf += ch
        return (int(num or 0), suf.lower())

    items.sort(key=sort_key)
    return render(TPL_INDEX, routes=items, q="", now_uk=now_uk().strftime("%H:%M:%S"))

@app.get("/search", response_class=HTMLResponse)
def search(q: str = ""):
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
                routes_found.append({"short": r.get("route_short_name"), "key": short})
        for s in stops:
            name = s.get("stop_name") or ""
            if q.lower() in name.lower():
                stops_found.append({"id": s.get("stop_id"), "code": s.get("stop_code"), "name": name})

    return render(TPL_SEARCH, q=q, routes=routes_found, stops=stops_found, now_uk=now_uk().strftime("%H:%M:%S"))

@app.get("/stop", response_class=HTMLResponse)
def stop_missing():
    # ne 404/500 legyen, ha valaki /stop-ot nyit meg
    return RedirectResponse("/")

@app.get("/stop/{sid_or_code}", response_class=HTMLResponse)
async def stop_view(sid_or_code: str):
    s = stop_by_any(sid_or_code)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")
    rows = await rows_for_stop(s)
    return render(TPL_STOP, stop=s, rows=rows, now_uk=now_uk().strftime("%H:%M:%S"))

@app.get("/s/{sid_or_code}", response_class=HTMLResponse)
async def stop_alias(sid_or_code: str):
    return await stop_view(sid_or_code)

@app.get("/r", response_class=HTMLResponse)
def route_missing():
    # ne 404/500 legyen, ha valaki /r-t nyit meg
    return RedirectResponse("/")

@app.get("/r/{route_key}", response_class=HTMLResponse)
async def route_view(route_key: str):
    key = (route_key or "").lower()
    rlist = routes_for_short(key)
    if not rlist:
        # próbáljuk route_id-val
        r = routes_by_id.get(route_key)
        if r:
            rlist = [r]
            key = (r.get("route_short_name") or "").strip().lower()
    if not rlist:
        raise HTTPException(status_code=404, detail="Route not found")

    short = (rlist[0].get("route_short_name") or "").strip()
    live = await fetch_live_vm(short)
    live = [v for v in (live or []) if (v.get("operator") or "") in ALLOWED_OPERATORS]
    return render(TPL_ROUTE, route_short=short, live_vehicles=live, now_uk=now_uk().strftime("%H:%M:%S"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
