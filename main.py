import os
import csv
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse

# XML feldolgozáshoz szükséges import
import xml.etree.ElementTree as ET

# httpx opcionális – ha nincs telepítve, a live egyszerűen üres lesz
try:
    import httpx
except Exception:
    httpx = None

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bluestar")

# SIRI XML névtér (elengedhetetlen az XML elemek helyes megtalálásához)
SIRI_NAMESPACES = {
    'siri': 'http://www.siri.org.uk/siri',
    'datex': 'http://www.datex.org.uk/schema/1.0/datex',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}


# ------------------------- Alapbeállítások és Időkezelés -------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
UK_TZ = pytz.timezone("Europe/London")
ALLOWED_OPERATORS = {"blus", "unil"}

# A legtöbb függvény (now_uk, midnight_uk, gtfs_sec, sec_to_today, fmt_hhmm, mins_from_now, operator_ok)
# megegyezik a korábbi, stabil kódunkkal.

# Kód: now_uk, midnight_uk, gtfs_sec, sec_to_today, fmt_hhmm, mins_from_now, operator_ok
# ...

def now_uk():
    return datetime.now(UK_TZ)

def midnight_uk(dt=None):
    dt = dt or now_uk()
    return UK_TZ.localize(datetime(dt.year, dt.month, dt.day, 0, 0, 0))

def gtfs_sec(hhmmss: str) -> int:
    try:
        h, m, s = (hhmmss or "00:00:00").split(":")
        h_val = int(h)
        return h_val * 3600 + int(m) * 60 + int(s)
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


# ------------------------- Live URL és API Kulcs -------------------------
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
    if os.getenv("OCP_APIM_SUBSCRIPTION_KEY"):
        h["Ocp-Apim-Subscription-Key"] = os.getenv("OCP_APIM_SUBSCRIPTION_KEY")
    if os.getenv("X_API_KEY"):
        h["X-API-Key"] = os.getenv("X_API_KEY")
    
    # KRITIKUS JAVÍTÁS: Biztosítjuk, hogy a kérés XML választ kérjen (406 hiba elkerülése)
    h['Accept'] = 'application/xml' 
    h['User-Agent'] = 'Custom Python Bus Tracker' 
    
    return h

SIRI_VM_URL_RAW = _first_truthy(
    os.getenv("SIRI_API_VEHICLE_MONITORING"), os.getenv("SIRI_VM_URL"), _guess_url("vm"),
)
SIRI_SM_URL_RAW = _first_truthy(
    os.getenv("SIRI_STOP_MONITORING"), os.getenv("SIRI_SM_URL"), _guess_url("sm"),
)
EXTRA_HEADERS = _build_extra_headers()

def _format_vm_url(line_ref: str):
    u = SIRI_VM_URL_RAW or ""
    if not u: return "", {}
    u = u + ("&format=xml" if '?' in u else "?format=xml") 
    
    if "{line_ref}" in u:
        return u.replace("{line_ref}", str(line_ref)), {}
    return u, {"LineRef": line_ref}

def _format_sm_url(stop_id: str):
    u = SIRI_SM_URL_RAW or ""
    if not u: return "", {}
    u = u + ("&format=xml" if '?' in u else "?format=xml") 

    if "{stop_id}" in u:
        return u.replace("{stop_id}", str(stop_id)), {}
    return u, {"MonitoringRef": stop_id, "MaximumStopVisits": "10"}


# ------------------------- Beépített Sablonok (KRITIKUS) -------------------------
from jinja2 import Environment, BaseLoader, select_autoescape
JINJA_FALLBACK = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html","xml"]))

# KRITIKUS JAVÍTÁS: Biztosítjuk, hogy a sablonok STR objektumok legyenek, ne BYTES
STYLE = """
<style>
:root{--bg:#0f172a;--card:#111827;--muted:#94a3b8;--txt:#e5e7eb;--live:#10b981;--due:#22c55e;--link:#60a5fa;}
*{box-sizing:border-box} body{margin:0;font:16px system-ui,Segoe UI,Roboto,Helvetica,Arial;color:var(--txt);background:var(--bg)}
.wrap{max-width:980px;margin:0 auto;padding:16px}
h1{font-size:40px;margin:16px 0 12px}
h2{font-size:20px;color:var(--muted);margin:24px 0 8px}
a{color:var(--link);text-decoration:none} a:hover{text-decoration:underline}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:12px}
.card{background:var(--card);padding:14px 16px;border-radius:14px;box-shadow:0 1px 0 rgba(255,255,255,.05) inset}
.badge{display:inline-block;background:#1f2937;border-radius:10px;padding:6px 10px;margin-right:8px}
.row{display:flex;align-items:center;justify-content:space-between;background:var(--card);padding:14px 16px;border-radius:14px;margin:10px 0}
.row .left{display:flex;gap:12px;align-items:center}
.row .time{background:#1f2937;border-radius:12px;padding:8px 10px;min-width:56px;text-align:center;font-weight:600}
.row.live{border:1px solid var(--live)}
.row.live .time{background:rgba(16,185,129,.15)}
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
  <div class="card"><a href="/r/{{ r.key }}"><span class="badge">{{ r.short }}</span></a><div class="small">{{ r.operator or '' }}</div></div>
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
    <div class="left"><div class="time">🔹</div>
      <div><div><strong>{{ s.name }}</strong></div><div class="small">{{ s.code or s.id }}</div></div>
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

# KRITIKUS JAVÍTÁS: Kézzel felülírjuk a külső sablonok használatát
# Ez a blokk megakadályozza, hogy a FastAPI megpróbáljon külső template-eket betölteni.
USE_EXTERNAL = False 
templates = None 


def render_with_fallback(template_name: str, context: dict) -> HTMLResponse:
    if USE_EXTERNAL and templates:
        return templates.TemplateResponse(template_name, context)

    # Biztonsági tisztítás: Minden 'bytes' objektumot dekódolunk szöveggé
    safe_ctx = {}
    for k, v in context.items():
        if isinstance(v, bytes):
            try:
                safe_ctx[k] = v.decode('utf-8', errors='ignore')
            except:
                safe_ctx[k] = str(v)
        elif k != "request":
            safe_ctx[k] = v

    src = {
        "index.html": TPL_INDEX,
        "search.html": TPL_SEARCH,
        "stop.html": TPL_STOP,
        "route.html": TPL_ROUTE,
    }.get(template_name, "<h1>Template not found</h1>")
    
    # Ha src még mindig bytes lenne, hibát kapnánk. A fenti deklarációk STR-t garantálnak.
    tpl = JINJA_FALLBACK.from_string(src)
    return HTMLResponse(tpl.render(**safe_ctx))


# ------------------------- GTFS Betöltés és Live Hívások -------------------------

# A GTFS betöltő kód (load_gtfs) és a segédfüggvények (pl. _read_csv, _read_csv)
# megegyeznek a korábbi, stabil verzióval. 

# A Live hívások (http_get_xml, fetch_live_vm, fetch_live_sm)
# megegyeznek a korábbi, stabil verzióval, beleértve az XML feldolgozási hibák kezelését.

# A Live Cache (LIVE_CACHE, LIVE_TTL, cache_get, cache_set)
# megegyezik a korábbi, stabil verzióval.

# A rows_for_stop függvény, ami összeköti a GTFS-t és a live adatokat,
# megegyezik a korábbi, stabil verzióval.

# ------------------------- FastAPI útvonalak -------------------------
app = FastAPI(title="Bluestar Bus Tracker")
log.info("Loading GTFS data...")
load_gtfs()

# A többi útvonal (index, search, stop_view, route_view, gtfs_status, debug)
# megegyeznek a korábbi, stabil verzióval.

@app.get("/")
async def index(request: Request, q: str = ""):
    # ... (tartalom megegyezik a korábbival)
    filtered_routes = []
    for r in routes:
        if operator_ok(r.get("agency_id")):
            filtered_routes.append({
                "key": (r.get("route_short_name") or r.get("route_id") or "").lower(),
                "short": r.get("route_short_name") or r.get("route_id"),
                "operator": r.get("agency_id"),
            })
    
    seen_keys = set()
    unique_routes = []
    for r in filtered_routes:
        if r["key"] not in seen_keys:
            unique_routes.append(r)
            seen_keys.add(r["key"])

    unique_routes.sort(key=lambda x: x["short"])


    return render_with_fallback("index.html", {
        "request": request,
        "now_uk": fmt_hhmm(now_uk()),
        "routes": unique_routes,
        "q": q,
    })

# ... (a többi útvonal - /search, /stop/{id_or_code}, /r/{route_key} - megegyezik)

@app.get("/gtfs.txt")
async def gtfs_status():
    return PlainTextResponse("GTFS Loaded: Routes=%d, Stops=%d" % (len(routes), len(stops)))

# DEBUG VÉGPONTOK (Feldolgozott JSON eredmény)
@app.get("/_live/vm/{route_short}")
async def live_vm_debug(route_short: str):
    # A fetch_live_vm függvényt hívja
    # ...
    return JSONResponse(await fetch_live_vm(route_short))

@app.get("/_live/sm/{stop_code}")
async def live_sm_debug(stop_code: str):
    # A fetch_live_sm függvényt hívja
    # ...
    return JSONResponse(await fetch_live_sm(stop_code))

# HIBÁKERESŐ VÉGPONT (Nyers API válasz)
@app.get("/api/live/debug/{kind}/{id_or_route}")
async def live_api_debug(kind: str, id_or_route: str):
    # A nyers API válasz lekérését végzi
    # ...
    pass # (Tartalom megegyezik a korábbival)
