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

# httpx opcionális importálása
try:
    import httpx
except Exception:
    httpx = None

# Logolás beállítása
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bluestar")

# SIRI XML névtér (A leggyakoribb SIRI névtér)
SIRI_NAMESPACES = {
    'siri': 'http://www.siri.org.uk/siri',
    'datex': 'http://www.datex.org.uk/schema/1.0/datex',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}


# ------------------------- Alapbeállítások és Időkezelés -------------------------

# GTFS Fájl keresése: A gyökérkönyvtárban (ha nincs megadva más)
DATA_DIR = os.getenv("DATA_DIR", ".")
UK_TZ = pytz.timezone("Europe/London") 
# Az agency.txt adatok alapján: BLUS, UNIL, SWWD (GoSouthCoast)
ALLOWED_OPERATORS = {"blus", "unil", "swwd"} 

def now_uk():
    """Jelenlegi időpont UK időzónában."""
    return datetime.now(UK_TZ)

def midnight_uk(dt=None):
    """Az adott nap éjfél (00:00:00) UK időzónában."""
    dt = dt or now_uk()
    return UK_TZ.localize(datetime(dt.year, dt.month, dt.day, 0, 0, 0))

def gtfs_sec(hhmmss: str) -> int:
    """GTFS HH:MM:SS formátum konvertálása másodpercekké éjféltől."""
    try:
        h, m, s = (hhmmss or "00:00:00").split(":")
        h_val = int(h)
        return h_val * 3600 + int(m) * 60 + int(s)
    except Exception:
        return 0

def sec_to_today(sec: int) -> datetime:
    """GTFS másodpercek konvertálása UK időzónás datetime objektummá."""
    base = midnight_uk()
    days = sec // 86400
    rem = sec % 86400
    return base + timedelta(days=days, seconds=rem)

def fmt_hhmm(dt: datetime) -> str:
    """Datetime formázása HH:MM-re."""
    # Kezeljük az éjfél utáni időket (pl. 25:15)
    hours = dt.hour + (dt.day - midnight_uk(dt).day) * 24
    return f"{hours:02d}:{dt.minute:02d}"

def mins_from_now(dt: datetime) -> int:
    """Percben kifejezett várakozási idő a jelenlegi időponthoz képest."""
    return int(round((dt - now_uk()).total_seconds() / 60))

def operator_ok(op: str) -> bool:
    """Ellenőrzi, hogy az operátor szerepel-e az engedélyezett listában."""
    return (op or "").strip().lower() in ALLOWED_OPERATORS

def _parse_iso(iso_str: str) -> datetime | None:
    """ISO 8601 string konvertálása UK időzónás datetime objektummá."""
    # A DfT SIRI dátum formátumok kezelése
    try:
        if iso_str.endswith("Z"):
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(iso_str)
        
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            # Feltételezzük, hogy a nem időzónás stringek UTC-ben vannak és UK-ra konvertáljuk.
            dt = pytz.utc.localize(dt).astimezone(UK_TZ)
        else:
            dt = dt.astimezone(UK_TZ)
            
        return dt
    except Exception:
        return None


# ------------------------- Live URL és API kulcs beállítás -------------------------
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
    # KRITIKUS JAVÍTÁS: DfT BODS API kulcs fejléceken keresztül
    if os.getenv("OCP_APIM_SUBSCRIPTION_KEY"):
        h["Ocp-Apim-Subscription-Key"] = os.getenv("OCP_APIM_SUBSCRIPTION_KEY")
    
    # Kényszerítjük az XML formátumot és a user-agent-et
    h['Accept'] = 'application/xml'
    h['User-Agent'] = 'Custom Python Bus Tracker' 
    
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
    """URL formázása járműfigyeléshez."""
    u = SIRI_VM_URL_RAW or ""
    if not u: return "", {}
    u = u + ("&format=xml" if '?' in u else "?format=xml") 
    
    # A DfT API a 'LineRef' paramétert várja a query stringben, ha az URL nem tartalmaz sablont
    if "{line_ref}" in u:
        return u.replace("{line_ref}", str(line_ref)), {}
    return u, {"LineRef": line_ref}

def _format_sm_url(stop_id: str):
    """URL formázása megállófigyeléshez."""
    u = SIRI_SM_URL_RAW or ""
    if not u: return "", {}
    u = u + ("&format=xml" if '?' in u else "?format=xml") 

    # A DfT API a 'MonitoringRef' paramétert várja a query stringben
    if "{stop_id}" in u:
        return u.replace("{stop_id}", str(stop_id)), {}
    return u, {"MonitoringRef": stop_id, "MaximumStopVisits": "10"}


# ------------------------- Beépített Sablonok (Jinja2) -------------------------
from jinja2 import Environment, BaseLoader, select_autoescape
JINJA_FALLBACK = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html","xml"]))

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

# ... (TPL_INDEX, TPL_SEARCH, TPL_STOP, TPL_ROUTE sablonok változatlanok)
# Az egyszerűség kedvéért a sablonok tartalmát kihagytam a kódismétlés elkerülése érdekében, de a tényleges main.py-ban hagyd bent az előző üzenetben küldött teljes tartalmukat!

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


def render_with_fallback(template_name: str, context: dict) -> HTMLResponse:
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
    
    tpl = JINJA_FALLBACK.from_string(src)
    return HTMLResponse(tpl.render(**safe_ctx))


# ------------------------- GTFS betöltés -------------------------
routes = []; stops = []; trips = []; stop_times = []
routes_by_id = {}; routes_by_short = defaultdict(list)
stops_by_id = {}; stops_by_code = {}
trips_by_id = {}; trips_by_route = defaultdict(list)
stop_times_by_stop = defaultdict(list); stop_times_by_trip = defaultdict(list)

def _read_csv(path):
    """GTFS fájlok olvasása, két lehetséges helyen keresve (gyökérkönyvtár, gtfs mappa)."""
    
    # 1. Próba: Keresés a DATA_DIR-ből (ami most a gyökérkönyvtár: '.')
    full_path = os.path.join(DATA_DIR, path)
    
    # 2. Próba: Vissza a 'gtfs/' mappához
    if not os.path.exists(full_path):
        full_path = os.path.join("gtfs", path)
        if not os.path.exists(full_path):
            log.warning(f"GTFS file not found: {path}")
            return []
            
    try:
        # A newline="" fontos a CSV helyes kezeléséhez különböző OS-eken
        with open(full_path, "r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        log.error(f"Error reading {full_path}: {e}")
        return []

def load_gtfs():
    """Összes GTFS fájl betöltése és indexelése."""
    rp = "routes.txt"; sp = "stops.txt"
    tp = "trips.txt"; stp = "stop_times.txt"

    # Fájlok beolvasása
    routes[:] = _read_csv(rp); stops[:] = _read_csv(sp)
    trips[:] = _read_csv(tp); stop_times[:] = _read_csv(stp)
    # Ellenőrzés, hogy a szükséges adatok beolvasódtak-e
    if not routes or not stops:
         log.error("CRITICAL: Routes or Stops data is missing or empty!")
         # Még ha üres is, engedjük futni a live API teszteléshez

    # Routes indexelés
    routes_by_id.clear(); routes_by_short.clear()
    for r in routes:
        rid = (r.get("route_id") or "").strip()
        routes_by_id[rid] = r
        short = (r.get("route_short_name") or "").strip()
        if short:
            routes_by_short[short.lower()].append(r)

    # Stops indexelés
    stops_by_id.clear(); stops_by_code.clear()
    for s in stops:
        sid = (s.get("stop_id") or "").strip()
        sc = (s.get("stop_code") or "").strip()
        if sid: stops_by_id[sid] = s
        if sc:  stops_by_code[sc] = s

    # Trips indexelés
    trips_by_id.clear(); trips_by_route.clear()
    for t in trips:
        tid = (t.get("trip_id") or "").strip()
        rid = (t.get("route_id") or "").strip()
        if tid: trips_by_id[tid] = t
        if rid: trips_by_route[rid].append(t)

    # Stop Times indexelés
    stop_times_by_stop.clear(); stop_times_by_trip.clear()
    for st in stop_times:
        sid = (st.get("stop_id") or "").strip()
        tid = (st.get("trip_id") or "").strip()
        if sid: stop_times_by_stop[sid].append(st)
        if tid: stop_times_by_trip[tid].append(st)
    for tid, arr in stop_times_by_trip.items():
        arr.sort(key=lambda x: gtfs_sec(x.get("departure_time") or x.get("arrival_time") or ""))

def stop_by_any(q: str):
    """Megálló keresése ID, vagy kód alapján."""
    q_strip = q.strip()
    return stops_by_id.get(q_strip) or stops_by_code.get(q_strip)

def routes_for_short(q: str):
    """Járatok keresése rövid név alapján."""
    return routes_by_short.get(q.lower(), [])


# ------------------------- Live hívások és XML feldolgozás -------------------------
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
        log.error("HTTPX is not installed or SIRI URL is missing.")
        return b""
    try:
        all_headers = EXTRA_HEADERS.copy()
        p = {k: v for k, v in params.items() if k in ["MonitoringRef", "MaximumStopVisits", "LineRef"]}
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=p, headers=all_headers)
            r.raise_for_status()
            return r.content
    except httpx.HTTPStatusError as e:
        # A 4xx-es hibák kulcsfontosságúak a debugban
        log.warning(f"Live request failed (HTTP Error: {e.response.status_code}) for URL: {url} | Response: {e.response.text[:100]}")
        return b""
    except Exception as e:
        log.warning(f"Live request failed (General): {e} ({type(e).__name__})")
        return b""

async def fetch_live_vm(line_ref: str):
    """Élő Járműfigyelő (VM) adatok lekérdezése és SIRI XML feldolgozása."""
    ck = ("vm", line_ref)
    c = cache_get(ck)
    if c is not None:
        return c
    items = []
    url, params = _format_vm_url(line_ref)
    
    if url:
        xml_content = await http_get_xml(url, params=params)
        if not xml_content:
            cache_set(ck, []); return []
            
        try:
            root = ET.fromstring(xml_content)
            
            # Keresés a SIRI válaszban
            deliveries = root.findall('siri:ServiceDelivery/siri:VehicleMonitoringDelivery', SIRI_NAMESPACES)
            
            for d in deliveries:
                # KRITIKUS MÓDOSÍTÁS: Pontosabb keresés a SIRI elemekre
                for v_act in d.findall('siri:VehicleActivity', SIRI_NAMESPACES): 
                    j = v_act.find('siri:MonitoredVehicleJourney', SIRI_NAMESPACES)
                    if j is None: continue 
                    
                    op = j.find('siri:OperatorRef', SIRI_NAMESPACES)
                    op_ref = op.text.strip().lower() if op is not None and op.text else ""
                    
                    if op_ref and not operator_ok(op_ref):
                        continue
                        
                    line = j.find('siri:LineRef', SIRI_NAMESPACES)
                    v_ref = j.find('siri:VehicleRef', SIRI_NAMESPACES)
                    
                    lon_e = j.find('siri:VehicleLocation/siri:Longitude', SIRI_NAMESPACES)
                    lat_e = j.find('siri:VehicleLocation/siri:Latitude', SIRI_NAMESPACES)
                    
                    lon = float(lon_e.text) if lon_e is not None and lon_e.text else 0.0
                    lat = float(lat_e.text) if lat_e is not None and lat_e.text else 0.0

                    items.append({
                        "line": line.text.strip() if line is not None and line.text else "",
                        "operator": op_ref,
                        "fleet": v_ref.text.strip() if v_ref is not None and v_ref.text else "",
                        "lon": lon, "lat": lat,
                    })
                    
        except ET.ParseError as e:
            log.warning(f"parse VM failed (XML Parse Error): {e} | Content size: {len(xml_content)}")
            items = []
        except Exception as e:
            log.error(f"FATAL parse VM error: {e} (Type: {type(e).__name__}) | Content size: {len(xml_content)}")
            items = []
            
    cache_set(ck, items)
    return items

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
            root = ET.fromstring(xml_content)
            
            deliveries = root.findall('siri:ServiceDelivery/siri:StopMonitoringDelivery', SIRI_NAMESPACES)
            
            for d in deliveries:
                for v in d.findall('siri:MonitoredStopVisit', SIRI_NAMESPACES):
                    j = v.find('siri:MonitoredVehicleJourney', SIRI_NAMESPACES)
                    if j is None: continue 
                    
                    op = j.find('siri:OperatorRef', SIRI_NAMESPACES)
                    op_ref = op.text.strip().lower() if op is not None and op.text else ""
                    if op_ref and not operator_ok(op_ref):
                        continue
                        
                    call = j.find('siri:MonitoredCall', SIRI_NAMESPACES)
                    if call is None: continue
                    
                    line = j.find('siri:LineRef', SIRI_NAMESPACES)
                    headsign = j.find('siri:DestinationName', SIRI_NAMESPACES)
                    v_ref = j.find('siri:VehicleRef', SIRI_NAMESPACES)
                    
                    line_ref = line.text.strip() if line is not None and line.text else ""
                    headsign_str = headsign.text.strip() if headsign is not None and headsign.text else ""
                    v_ref_str = v_ref.text.strip() if v_ref is not None and v_ref.text else ""
                    
                    # Aimed (menetrendi) és Expected (élő) időpontok
                    aimed = call.find('siri:AimedDepartureTime', SIRI_NAMESPACES)
                    exp = call.find('siri:ExpectedDepartureTime', SIRI_NAMESPACES)
                    
                    aimed_str = aimed.text.strip() if aimed is not None and aimed.text else ""
                    exp_str = exp.text.strip() if exp is not None and exp.text else aimed_str
                    
                    dep_dt = _parse_iso(exp_str)
                    delay_text = ""
                    
                    if aimed_str and exp_str:
                        a = _parse_iso(aimed_str); e = _parse_iso(exp_str)
                        if a and e:
                            mins = round((e - a).total_seconds() / 60.0)
                            if mins != 0:
                                delay_text = f"{mins:+d}m" # +1m, -2m formátum
                                
                    is_due = bool(dep_dt and abs((now_uk() - dep_dt).total_seconds()) < 60)
                    
                    
                    items.append({
                        "line": line_ref, "operator": op_ref,
                        "headsign": headsign_str,
                        "vehicle_ref": v_ref_str,
                        "dep_dt": dep_dt, "delay_text": delay_text, "is_due": is_due,
                        # A SIRI nem mindig ad GTFS trip_id-t, de használjuk a LineRef/Headsign/AimedTime kombinációt az illesztéshez
                        "aimed_str": aimed_str, 
                    })
                    
        except ET.ParseError as e:
            log.warning(f"parse SM failed (XML Parse Error): {e} | Content size: {len(xml_content)}")
            items = []
        except Exception as e:
            log.error(f"FATAL parse SM error: {e} (Type: {type(e).__name__}) | Content size: {len(xml_content)}")
            items = []
            
    cache_set(ck, items)
    return items

async def rows_for_stop(stop_obj, minutes_ahead=120):
    """GTFS menetrend és élő adatok egyesítése a megállóhoz. Duplikáció szűréssel."""
    now = now_uk()
    until = now + timedelta(minutes=minutes_ahead)
    sid = stop_obj.get("stop_id")
    scode = (stop_obj.get("stop_code") or stop_obj.get("stop_id") or "").strip()

    # Élő adatok lekérése
    live_raw = await fetch_live_sm(scode)
    
    # Készítünk egy kulcs-térképet az élő adatokhoz (GTFS illesztéshez)
    # Kulcs: (route_short_name, headsign, aimed_departure_time_iso)
    live_by_key = {}
    for it in live_raw or []:
        if not it.get("dep_dt"): continue
        
        # A live SM adatokban használt kulcs
        key_short = (it.get("line") or "").strip().upper()
        key_headsign = (it.get("headsign") or "").strip().upper()
        # A SIRI aimed_str-t használjuk illesztőként
        key_aimed = (it.get("aimed_str") or "").strip()
        
        live_key = (key_short, key_headsign, key_aimed)
        
        # Ha több élő adat is illeszkedik, csak az elsőt fogadjuk el.
        if live_key not in live_by_key:
            live_by_key[live_key] = it

    rows = []
    seen_gtfs_trips = set() # Új szett a duplikátumok szűrésére

    for st in stop_times_by_stop.get(sid, []):
        tid = st.get("trip_id")
        trip = trips_by_id.get(tid, {})
        rid = (trip or {}).get("route_id", "")
        route = routes_by_id.get(rid, {})
        
        route_short = (route.get("route_short_name") or "").strip()
        headsign = (trip.get("trip_headsign") or "").strip()
        agency = (route.get("agency_id") or "").strip().lower()

        if agency and agency not in ALLOWED_OPERATORS:
            continue

        dep_sec = gtfs_sec(st.get("departure_time") or st.get("arrival_time") or "")
        
        # --- KRITIKUS JAVÍTÁS: DUPLIKÁCIÓS ELLENŐRZÉS ---
        unique_key = (route_short.upper(), headsign.upper(), dep_sec)
        
        if unique_key in seen_gtfs_trips:
            # Ugyanaz az indulás már megvan (menetrendi szempontból). Kihagyjuk.
            continue
        
        seen_gtfs_trips.add(unique_key)
        # -----------------------------------
        
        dep_dt = sec_to_today(dep_sec)
        if dep_dt < (now - timedelta(minutes=1)) or dep_dt > until:
            continue
        
        # Az élő adatok illesztéséhez keressük a SIRI 'aimed' idejét
        aimed_iso_str = dep_dt.isoformat() 
        # A DfT API nem mindig tartalmaz időzónát a GTFS adatokhoz illesztett aimed_str-ben.
        # Próbáljuk meg a LineRef-et és a Headsign-t is illeszteni.
        
        # A GTFS-ből a kereső kulcs: (ROUTE_SHORT, HEADSIGN, AIMED_ISO_STRING)
        gtfs_search_key = (route_short.upper(), headsign.upper(), aimed_iso_str)
        live_hit = live_by_key.get(gtfs_search_key)
        
        # Alternatív illesztés, csak Route és Headsign alapján (kevésbé pontos, de fallback)
        if not live_hit and live_raw:
             for l in live_raw:
                line_upper = (l.get("line") or "").strip().upper()
                headsign_upper = (l.get("headsign") or "").strip().upper()
                if line_upper == route_short.upper() and headsign_upper == headsign.upper():
                    # Ha csak 1 van, elfogadjuk.
                    live_hit = l
                    break

        if live_hit:
            # Élő adat illesztve
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
            # Csak menetrendi adat
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

# ... (Az összes @app.get útvonal változatlan maradt - Index, Search, Stop, Route, Debug Végpontok)

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
        if r["key"] and r["key"] not in seen_keys:
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
async def search_view(request: Request, q: str = ""):
    q_lower = q.lower().strip()
    routes_list = routes_for_short(q_lower)
    
    stop_list = []
    for sid, stop in stops_by_id.items():
        name = (stop.get("stop_name") or "").lower()
        code = (stop.get("stop_code") or "").lower()
        if q_lower in name or q_lower in code:
            stop_list.append({
                "id": sid,
                "code": stop.get("stop_code"),
                "name": stop.get("stop_name"),
            })

    return render_with_fallback("search.html", {
        "request": request,
        "now_uk": fmt_hhmm(now_uk()),
        "routes": routes_list,
        "stops": stop_list,
        "q": q,
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
    """Debug végpont a GTFS betöltés ellenőrzésére."""
    return PlainTextResponse("GTFS Loaded: Routes=%d, Stops=%d" % (len(routes), len(stops)))

# DEBUG VÉGPONTOK (Feldolgozott JSON eredmény)
@app.get("/api/live/debug/vm/{route_short}")
async def live_vm_debug(route_short: str):
    return JSONResponse(await fetch_live_vm(route_short))

@app.get("/api/live/debug/sm/{stop_code}")
async def live_sm_debug(stop_code: str):
    return JSONResponse(await fetch_live_sm(stop_code))

# HIBÁKERESŐ VÉGPONT (Nyers API válasz)
@app.get("/api/live/debug/raw/{kind}/{id_or_route}")
async def live_api_debug(kind: str, id_or_route: str):
    if kind == "vm":
        url, params = _format_vm_url(id_or_route)
    elif kind == "sm":
        url, params = _format_sm_url(id_or_route)
    else:
        raise HTTPException(status_code=400, detail="Invalid kind. Use 'vm' or 'sm'.")
    
    if not url:
         raise HTTPException(status_code=500, detail="SIRI URL not configured.")
         
    content = await http_get_xml(url, params=params)
    
    if not content:
        raise HTTPException(status_code=500, detail="API request failed or returned empty content. Check Railway logs for 4xx errors and API key settings.")
        
    return PlainTextResponse(content.decode('utf-8', errors='ignore'))
