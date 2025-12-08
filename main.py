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


# ------------------------- Alapbeállítások -------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
UK_TZ = pytz.timezone("Europe/London")
ALLOWED_OPERATORS = {"blus", "unil"}

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
        h["X-API-Key"] = os.getenv("X-API-KEY")
    
    h['Accept'] = 'application/xml'
    h['User-Agent'] = 'Mozilla/5.0 (Custom Python Script)' 
    
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
    u = u + ("&format=xml" if '?' in u else "?format=xml") 
    
    if "{line_ref}" in u:
        return u.replace("{line_ref}", str(line_ref)), {}
    return u, {"LineRef": line_ref}

def _format_sm_url(stop_id: str):
    u = SIRI_SM_URL_RAW or ""
    if not u:
        return "", {}
    u = u + ("&format=xml" if '?' in u else "?format=xml") 

    if "{stop_id}" in u:
        return u.replace("{stop_id}", str(stop_id)), {}
    return u, {"MonitoringRef": stop_id, "MaximumStopVisits": "10"}


# ------------------------- Beépített sablonok -------------------------
from jinja2 import Environment, BaseLoader, select_autoescape
JINJA_FALLBACK = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html","xml"]))

# Stílus definíciója (a teljesség kedvéért benne van)
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
# Ezzel elkerüljük az 'AttributeError: 'bytes' object has no attribute 'find'' hibát
USE_EXTERNAL = False 
templates = None 


def render_with_fallback(template_name: str, context: dict) -> HTMLResponse:
    if USE_EXTERNAL and templates:
        return templates.TemplateResponse(template_name, context)

    # Biztonsági tisztítás: Minden 'bytes' objektumot dekódolunk szöveggé
    safe_ctx = {}
    for k, v in context.items():
        if isinstance(v, bytes):
            # Ha bytes, dekódoljuk utf-8-ra, vagy stringre konvertáljuk
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
    
    tpl = JINJA_FALLBACK.from_string(src)
    return HTMLResponse(tpl.render(**safe_ctx))


# ------------------------- GTFS betöltés -------------------------
routes = []; stops = []; trips = []; stop_times = []
routes_by_id = {}; routes_by_short = defaultdict(list)
stops_by_id = {}; stops_by_code = {}
trips_by_id = {}; trips_by_route = defaultdict(list)
stop_times_by_stop = defaultdict(list); stop_times_by_trip = defaultdict(list)

def _read_csv(path):
    if not os.path.exists(path):
        return []
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
        if short:
            routes_by_short[short.lower()].append(r)

    stops_by_id.clear(); stops_by_code.clear()
    for s in stops:
        sid = (s.get("stop_id") or "").strip()
        sc = (s.get("stop_code") or "").strip()
        if sid: stops_by_id[sid] = s
        if sc:  stops_by_code[sc] = s

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


# ------------------------- Live hívások -------------------------
LIVE_CACHE = {}
LIVE_TTL = 20

def cache_get(k):
    v = LIVE_CACHE.get(k)
    if not v: return None
    if v["exp"] < datetime.utcnow().timestamp():
        LIVE_CACHE.pop(k, None); return None
    return v["val"]

def cache_set(k, val):
    LIVE_CACHE[k] = {"val": val, "exp": datetime.utcnow().timestamp() + LIVE_TTL}

async def http_get_xml(url, params=None) -> bytes:
    """Aszinkron HTTP GET kérés XML adatokhoz. Nyers bájtokat ad vissza."""
    if not url or httpx is None:
        return b""
    try:
        all_headers = EXTRA_HEADERS.copy()
        # PARAMÉTEREK TISZTÍTÁSA AZ URL-BŐL, HA AZ URL-BEN MÁR SZEREPELNEK
        if params and "LineRef" in params: del params["LineRef"] 
        if params and "MonitoringRef" in params: del params["MonitoringRef"]
        if params and "MaximumStopVisits" in params: del params["MaximumStopVisits"]

        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params or {}, headers=all_headers)
            r.raise_for_status()
            return r.content
    except httpx.HTTPStatusError as e:
        log.warning("live request failed (HTTP Error: %s) for URL: %s", e.response.status_code, url)
        return b""
    except Exception as e:
        log.warning("live request failed (General): %s", e)
        return b""

async def http_get_raw_debug(url, params=None):
    """Aszinkron HTTP GET kérés Nyers válaszra a hibakereséshez."""
    if not url or httpx is None:
        return {"status": 500, "headers": {}, "content": "Error: httpx is not installed or URL is missing."}

    all_headers = EXTRA_HEADERS.copy()
    
    # PARAMÉTEREK TISZTÍTÁSA AZ URL-BŐL, HA AZ URL-BEN MÁR SZEREPELNEK
    if params and "LineRef" in params: del params["LineRef"] 
    if params and "MonitoringRef" in params: del params["MonitoringRef"]
    if params and "MaximumStopVisits" in params: del params["MaximumStopVisits"]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params or {}, headers=all_headers)
            
            return {
                "status": r.status_code,
                "url": str(r.url),
                "headers": dict(r.headers),
                "content": r.text,
            }
    except httpx.RequestError as e:
        return {"status": 0, "content": f"Request Error: {type(e).__name__} - {e}"}
    except Exception as e:
        return {"status": 0, "content": f"General Error: {type(e).__name__} - {e}"}


def _parse_iso(dt_str: str):
    try:
        if not dt_str:
            return None
        ds = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ds).astimezone(UK_TZ)
    except Exception:
        return None

async def fetch_live_vm(route_short: str):
    """Élő Járműfigyelő (VM) adatok lekérdezése és SIRI XML feldolgozása."""
    if not route_short:
        return []
    ck = ("vm", route_short.lower())
    c = cache_get(ck)
    if c is not None:
        return c
    out = []
    url, params = _format_vm_url(route_short)

    if url:
        xml_content = await http_get_xml(url, params=params)
        
        if not xml_content:
            log.info("VM request returned empty content (potential HTTP error or empty response).")
            cache_set(ck, [])
            return []
            
        try:
            # Explicit dekódolás utf-8-ra a bytes hiba elkerülése érdekében
            xml_text = xml_content.decode('utf-8')
            root = ET.fromstring(xml_text)
            
            deliveries = root.findall('siri:ServiceDelivery/siri:VehicleMonitoringDelivery', SIRI_NAMESPACES)
            
            for d in deliveries:
                for a in d.findall('siri:VehicleActivity', SIRI_NAMESPACES):
                    j = a.find('siri:MonitoredVehicleJourney', SIRI_NAMESPACES)
                    if j is None: continue

                    # Helyes XML element.find() használata a KeyError elkerülésére
                    line = j.find('siri:LineRef', SIRI_NAMESPACES)
                    op = j.find('siri:OperatorRef', SIRI_NAMESPACES)
                    
                    line_ref = line.text.strip() if line is not None and line.text else ""
                    op_ref = op.text.strip() if op is not None and op.text else ""
                    
                    if route_short and line_ref and route_short.lower() != str(line_ref).lower():
                        continue
                    if op_ref and not operator_ok(op_ref):
                        continue
                    
                    loc = j.find('siri:VehicleLocation', SIRI_NAMESPACES)
                    if loc is None: continue
                    
                    lat_e = loc.find('siri:Latitude', SIRI_NAMESPACES)
                    lon_e = loc.find('siri:Longitude', SIRI_NAMESPACES)
                    
                    if lat_e is not None and lon_e is not None:
                        try:
                            lat = float(lat_e.text); lon = float(lon_e.text)
                        except (ValueError, TypeError):
                            log.warning("Invalid Lat/Lon in VM response.")
                            continue
                        
                        vehicle_ref_e = j.find('siri:VehicleRef', SIRI_NAMESPACES)
                        fleet_id = vehicle_ref_e.text if vehicle_ref_e is not None and vehicle_ref_e.text else ""
                        
                        out.append({
                            "lat": lat, "lon": lon,
                            "fleet": fleet_id,
                            "line": line_ref, "operator": op_ref.lower()[:4],
                        })
        except ET.ParseError as e:
            log.warning(f"parse VM failed (XML Parse Error): {e} | Content size: {len(xml_content)}")
            out = []
        except Exception as e:
            log.error(f"FATAL parse VM error: {e} (Type: {type(e).__name__}) | Content size: {len(xml_content)}")
            out = []
            
    cache_set(ck, out)
    return out


async def fetch_live_sm(stop_code_or_id: str):
    """Élő Megállófigyelő (SM) adatok lekérdezése és SIRI XML feldolgozása."""
    ck = ("sm", stop_code_or_id)
    c = cache_get(ck)
    if c is not None:
        return c
    items = []
    url, params = _format_sm_url(stop_code_or_id)
    
    if url:
        xml_content = await http_get_xml(url, params=params)
        
        if not xml_content:
            log.info("SM request returned empty content (potential HTTP error or empty response).")
            cache_set(ck, [])
            return []

        try:
            # Explicit dekódolás utf-8-ra a bytes hiba elkerülése érdekében
            xml_text = xml_content.decode('utf-8')
            root = ET.fromstring(xml_text)
            
            deliveries = root.findall('siri:ServiceDelivery/siri:StopMonitoringDelivery', SIRI_NAMESPACES)
            
            for d in deliveries:
                for v in d.findall('siri:MonitoredStopVisit', SIRI_NAMESPACES):
                    j = v.find('siri:MonitoredVehicleJourney', SIRI_NAMESPACES)
                    if j is None: continue
                    
                    # Helyes XML element.find() használata a KeyError elkerülésére
                    op = j.find('siri:OperatorRef', SIRI_NAMESPACES)
                    op_ref = op.text.strip() if op is not None and op.text else ""
                    if op_ref and not operator_ok(op_ref):
                        continue
                        
                    call = j.find('siri:MonitoredCall', SIRI_NAMESPACES)
                    if call is None: continue
                    
                    line = j.find('siri:LineRef', SIRI_NAMESPACES)
                    headsign = j.find('siri:DestinationName', SIRI_NAMESPACES)
                    v_ref = j.find('siri:VehicleRef', SIRI_NAMESPACES)
                    trip_ref_e = j.find('siri:FramedVehicleJourneyRef', SIRI_NAMESPACES)
                    
                    line_ref = line.text.strip() if line is not None and line.text else ""
                    headsign_str = headsign.text.strip() if headsign is not None and headsign.text else ""
                    v_ref_str = v_ref.text.strip() if v_ref is not None and v_ref.text else ""
                    
                    # Időpontok
                    aimed = call.find('siri:AimedDepartureTime', SIRI_NAMESPACES)
                    exp = call.find('siri:ExpectedDepartureTime', SIRI_NAMESPACES)
                    
                    aimed_str = aimed.text.strip() if aimed is not None and aimed.text else ""
                    exp_str = exp.text.strip() if exp is not None and exp.text else aimed_str
                    
                    dep_dt = _parse_iso(exp_str)
                    delay_text = ""
                    
                    # Késés kiszámítása
                    if aimed_str and exp_str:
                        a = _parse_iso(aimed_str); e = _parse_iso(exp_str)
                        if a and e:
                            mins = round((e - a).total_seconds() / 60.0)
                            if mins != 0:
                                delay_text = f"{mins:+d}m"
                                
                    is_due = bool(dep_dt and abs((now_uk() - dep_dt).total_seconds()) < 60)
                    
                    trip_id_str = ""
                    if trip_ref_e is not None:
                         dated_ref = trip_ref_e.find('siri:DatedVehicleJourneyRef', SIRI_NAMESPACES)
                         if dated_ref is not None and dated_ref.text:
                             trip_id_str = dated_ref.text.strip()


                    items.append({
                        "line": line_ref, "operator": op_ref.lower()[:4],
                        "headsign": headsign_str,
                        "vehicle_ref": v_ref_str,
                        "dep_dt": dep_dt, "delay_text": delay_text, "is_due": is_due,
                        "trip_id": trip_id_str,
                    })
                    
        except ET.ParseError as e:
            log.warning(f"parse SM/VM failed (XML Parse Error): {e} | Content size: {len(xml_content)}")
            items = []
        except Exception as e:
            log.error(f"FATAL parse SM/VM error: {e} (Type: {type(e).__name__}) | Content size: {len(xml_content)}")
            items = []
            
    cache_set(ck, items)
    return items

# ------------------------- Segédfüggvények -------------------------
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

async def rows_for_stop(stop_obj, minutes_ahead=120):
    now = now_uk()
    until = now + timedelta(minutes=minutes_ahead)
    sid = stop_obj.get("stop_id")
    scode = (stop_obj.get("stop_code") or stop_obj.get("stop_id") or "").strip()

    live_raw = await fetch_live_sm(scode)
    live_by_key = {}
    for it in live_raw or []:
        if not it.get("dep_dt"):
            continue
        key = (it.get("trip_id") or "", (it.get("line") or "").lower())
        live_by_key[key] = it

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
        key = (tid or "", route_short.lower())
        live_hit = live_by_key.get(key)

        if live_hit:
            rows.append({
                "time_str": fmt_hhmm(live_hit["dep_dt"]),
                "time_dt": live_hit["dep_dt"],
                "headsign": live_hit.get("headsign") or headsign,
                "route_short": route_short or live_hit.get("line") or "",
                "route_key": (route_short or live_hit.get("line") or "").lower(),
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
                "time_dt": dep_dt,
                "headsign": headsign,
                "route_short": route_short,
                "route_key": route_short.lower(),
                "wait_mins": mins_from_now(dep_dt),
                "is_live": False,
                "is_due": False,
                "row_class": "timetable",
                "trip_id": tid,
                "fleet": "", 
                "delay_text": "",
            })
    
    rows.sort(key=lambda x: x["time_dt"])
    return rows


# ------------------------- FastAPI útvonalak -------------------------
app = FastAPI(title="Bluestar Bus Tracker")
log.info("Loading GTFS data...")
load_gtfs()


@app.get("/")
async def index(request: Request, q: str = ""):
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

@app.get("/search")
async def search(request: Request, q: str = ""):
    q_lower = q.lower().strip()
    if not q_lower:
        return RedirectResponse("/", status_code=302)

    found_routes = []
    for r in routes_for_short(q_lower):
        r_short = (r.get("route_short_name") or "").strip()
        if r_short:
             found_routes.append({
                "key": r_short.lower(),
                "short": r_short,
                "operator": r.get("agency_id"),
            })
            
    found_stops = []
    for s in stops:
        s_name = (s.get("stop_name") or "").lower()
        s_code = (s.get("stop_code") or "").lower()
        if q_lower in s_name or q_lower == s_code:
             found_stops.append({
                "id": s.get("stop_id"),
                "code": s.get("stop_code"),
                "name": s.get("stop_name"),
            })

    seen_routes = set()
    unique_routes = []
    for r in found_routes:
        if r["key"] not in seen_routes:
            unique_routes.append(r)
            seen_routes.add(r["key"])

    return render_with_fallback("search.html", {
        "request": request,
        "now_uk": fmt_hhmm(now_uk()),
        "q": q,
        "routes": unique_routes,
        "stops": found_stops,
    })

@app.get("/stop/{id_or_code}")
async def stop_view(request: Request, id_or_code: str):
    stop = stop_by_any(id_or_code)
    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")
    
    rows = await rows_for_stop(stop)

    return render_with_fallback("stop.html", {
        "request": request,
        "now_uk": fmt_hhmm(now_uk()),
        "stop": stop,
        "rows": rows,
    })

@app.get("/r/{route_key}")
async def route_view(request: Request, route_key: str):
    q_lower = route_key.lower().strip()
    route_list = routes_for_short(q_lower)
    
    if not route_list:
        raise HTTPException(status_code=404, detail="Route not found")
        
    route_short = (route_list[0].get("route_short_name") or q_lower).upper()
    
    live_vehicles = await fetch_live_vm(route_short)
    
    return render_with_fallback("route.html", {
        "request": request,
        "now_uk": fmt_hhmm(now_uk()),
        "route_short": route_short,
        "live_vehicles": live_vehicles,
    })

@app.get("/gtfs.txt")
async def gtfs_status():
    return PlainTextResponse("GTFS Loaded: Routes=%d, Stops=%d" % (len(routes), len(stops)))

# DEBUG VÉGPONTOK (Feldolgozott JSON eredmény)
@app.get("/_live/vm/{route_short}")
async def live_vm_debug(route_short: str):
    return JSONResponse(await fetch_live_vm(route_short))

@app.get("/_live/sm/{stop_code}")
async def live_sm_debug(stop_code: str):
    return JSONResponse(await fetch_live_sm(stop_code))

# HIBÁKERESŐ VÉGPONT (Nyers API válasz)
@app.get("/api/live/debug/{kind}/{id_or_route}")
async def live_api_debug(kind: str, id_or_route: str):
    """Nyers API válasz lekérdezése hibakereséshez."""
    
    url = ""; params = {}
    
    if kind.lower() == "vm":
        # Vehicle Monitoring (Járatra)
        url, params = _format_vm_url(id_or_route.upper())
    elif kind.lower() == "sm":
        # Stop Monitoring (Megállóra)
        url, params = _format_sm_url(id_or_route)
    else:
        raise HTTPException(status_code=400, detail="Invalid kind. Use 'vm' or 'sm'.")

    if not url:
        return JSONResponse({"status": 503, "content": "API URL not configured."}, status_code=503)

    raw_response = await http_get_raw_debug(url, params=params)
    
    # Ha a státusz 200 (OK) és XML-t kaptunk, kiírjuk a tartalmat PlainText-ként
    if raw_response["status"] == 200 and 'xml' in raw_response.get("headers", {}).get("Content-Type", "").lower():
        return PlainTextResponse(raw_response["content"])
        
    # Egyébként JSON-ban adjuk vissza a debug infókat
    return JSONResponse(raw_response)
