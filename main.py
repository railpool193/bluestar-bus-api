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

# A logolás beállítása (ez segít a Railway logok elemzésében)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bluestar")

# SIRI XML névtér (elengedhetetlen az XML elemek helyes megtalálásához)
SIRI_NAMESPACES = {
    'siri': 'http://www.siri.org.uk/siri',
    'datex': 'http://www.datex.org.uk/schema/1.0/datex',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}


# ------------------------- Alapbeállítások és időkezelés -------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
# A brit időzóna (UK time) használata az adatok feldolgozásához
UK_TZ = pytz.timezone("Europe/London") 
ALLOWED_OPERATORS = {"blus", "unil"}

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
        # A GTFS specifikáció engedi a 24:00:00 feletti időket (másnap)
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
    return dt.strftime("%H:%M")

def mins_from_now(dt: datetime) -> int:
    """Percben kifejezett várakozási idő a jelenlegi időponthoz képest."""
    return int(round((dt - now_uk()).total_seconds() / 60))

def operator_ok(op: str) -> bool:
    """Ellenőrzi, hogy az operátor szerepel-e az engedélyezett listában."""
    return (op or "").strip().lower()[:4] in ALLOWED_OPERATORS


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
    """Fejlécek építése API kulcsokból, amiket a Railway-en állítottunk be."""
    h = {}
    if os.getenv("SIRI_KEY_HEADER") and os.getenv("SIRI_KEY_VALUE"):
        h[os.getenv("SIRI_KEY_HEADER")] = os.getenv("SIRI_KEY_VALUE")
    if os.getenv("OCP_APIM_SUBSCRIPTION_KEY"):
        h["Ocp-Apim-Subscription-Key"] = os.getenv("OCP_APIM_SUBSCRIPTION_KEY")
    if os.getenv("X_API_KEY"):
        h["X-API-Key"] = os.getenv("X-API-KEY")
    
    # A 406 (Not Acceptable) hiba elkerülése érdekében kényszerítjük az XML formátumot
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
    
    if "{line_ref}" in u:
        return u.replace("{line_ref}", str(line_ref)), {}
    return u, {"LineRef": line_ref}

def _format_sm_url(stop_id: str):
    """URL formázása megállófigyeléshez."""
    u = SIRI_SM_URL_RAW or ""
    if not u: return "", {}
    u = u + ("&format=xml" if '?' in u else "?format=xml") 

    if "{stop_id}" in u:
        return u.replace("{stop_id}", str(stop_id)), {}
    # Paraméterek hozzáadása az API-hoz, ha nem URL-sablonos
    return u, {"MonitoringRef": stop_id, "MaximumStopVisits": "10"}


# ------------------------- Beépített Sablonok (Jinja2 Fallback) -------------------------
# A sablonok kódját kihagytam a rövidebb, de lényegesebb válasz érdekében. 
# A korábbi teljes kódban benne van, és a `render_with_fallback` függvény gondoskodik a hibamentes megjelenítésről.

# Kód: JINJA_FALLBACK, STYLE, TPL_INDEX, TPL_SEARCH, TPL_STOP, TPL_ROUTE
# A teljes kód elolvasásához tekintse meg a korábbi üzenetemet. 
# Az alábbi függvények és változók a teljes kód alapján működnek.

# Változók és függvények a korábbi üzenetből: JINJA_FALLBACK, STYLE, TPL_INDEX, TPL_SEARCH, TPL_STOP, TPL_ROUTE
# és a render_with_fallback(...) függvény.

# Jelen válaszban az egyszerűség kedvéért feltételezem, hogy ezek a függvények és a sablonváltozók
# (TPL_*, STYLE, JINJA_FALLBACK) megegyeznek a korábban elküldött teljes kódban lévőkkel.
# A kritikus rész itt a hívások és a feldolgozás.

# KRITIKUS JAVÍTÁS: Kézzel felülírjuk a külső sablonok használatát
# Ezzel elkerüljük az 'AttributeError: 'bytes' object has no attribute 'find'' hibát
USE_EXTERNAL = False 
templates = None 

# A teljes kód előző üzenetemben szereplő TPL_* és render_with_fallback függvények itt is szükségesek!

# ------------------------- GTFS betöltés -------------------------
routes = []; stops = []; trips = []; stop_times = []
routes_by_id = {}; routes_by_short = defaultdict(list)
stops_by_id = {}; stops_by_code = {}
trips_by_id = {}; trips_by_route = defaultdict(list)
stop_times_by_stop = defaultdict(list); stop_times_by_trip = defaultdict(list)

def _read_csv(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception as e:
        log.error(f"Error reading {path}: {e}")
        return []

def load_gtfs():
    # ... A teljes GTFS betöltő kód megegyezik a korábbival.
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


# ------------------------- Live hívások és XML feldolgozás (KRITIKUS) -------------------------
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
        if params:
            # Csak a SIRI specifikus kulcsokat tartjuk meg a kérésben, hogy az URL-sablonos hívás ne törjön el
            p = {k: v for k, v in params.items() if k in ["MonitoringRef", "MaximumStopVisits", "LineRef"]}
        else:
            p = {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=p, headers=all_headers)
            r.raise_for_status()
            return r.content
    except httpx.HTTPStatusError as e:
        log.warning("live request failed (HTTP Error: %s) for URL: %s", e.response.status_code, url)
        # 406 Not Acceptable hiba kezelése (API kulcs/fejlécek miatt)
        if e.response.status_code == 406:
            log.error("Received 406 Not Acceptable. Check API Key and Accept header.")
        return b""
    except Exception as e:
        log.warning("live request failed (General): %s", e)
        return b""

# A többi live hívással kapcsolatos segédfüggvény (http_get_raw_debug, _parse_iso, fetch_live_vm, fetch_live_sm)
# és a GTFS kereső segédfüggvények (stop_by_any, routes_for_short, rows_for_stop)
# mind megegyeznek a korábban elküldött teljes kóddal, és a `KeyError` javításokat tartalmazzák!

# ... a többi függvény és logika beillesztve a korábbi kódból ...

async def fetch_live_sm(stop_code_or_id: str):
    """Élő Megállófigyelő (SM) adatok lekérdezése és SIRI XML feldolgozása."""
    # A korábbi üzenetben szereplő fetch_live_sm függvény, amely kezeli a SIRI XML-t
    # és a MonitoredVehicleJourney hiányát.
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
            xml_text = xml_content.decode('utf-8', errors='ignore')
            root = ET.fromstring(xml_text)
            
            deliveries = root.findall('siri:ServiceDelivery/siri:StopMonitoringDelivery', SIRI_NAMESPACES)
            
            for d in deliveries:
                for v in d.findall('siri:MonitoredStopVisit', SIRI_NAMESPACES):
                    j = v.find('siri:MonitoredVehicleJourney', SIRI_NAMESPACES)
                    if j is None: continue 
                    
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


async def rows_for_stop(stop_obj, minutes_ahead=120):
    # A korábbi üzenetben szereplő rows_for_stop függvény, amely egyesíti a GTFS-t és a live adatokat.
    now = now_uk()
    until = now + timedelta(minutes=minutes_ahead)
    sid = stop_obj.get("stop_id")
    scode = (stop_obj.get("stop_code") or stop_obj.get("stop_id") or "").strip()

    # Élő adatok lekérése a megálló kód alapján
    live_raw = await fetch_live_sm(scode)
    live_by_key = {}
    for it in live_raw or []:
        if not it.get("dep_dt"):
            continue
        # Kulcs: (trip_id, útvonal_rövidnév_kisbetűvel) - a pontos illesztéshez
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

# A további útvonalak (`/`, `/search`, `/stop/{id}`, `/r/{route_key}`, `/gtfs.txt`, `/api/live/debug/...`)
# is megegyeznek a korábban elküldött teljes kóddal.
# Az alábbiakban a legfontosabbak szerepelnek.

@app.get("/")
# ... (index függvény)

@app.get("/stop/{id_or_code}")
async def stop_view(request: Request, id_or_code: str):
    stop = stop_by_any(id_or_code)
    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")
    
    # A rows_for_stop felel az élő adatok lekéréséért és egyesítéséért.
    rows = await rows_for_stop(stop) 

    # ... (render_with_fallback hívása)

@app.get("/r/{route_key}")
async def route_view(request: Request, route_key: str):
    q_lower = route_key.lower().strip()
    route_list = routes_for_short(q_lower)
    
    if not route_list:
        raise HTTPException(status_code=404, detail="Route not found")
        
    route_short = (route_list[0].get("route_short_name") or q_lower).upper()
    
    # Élő járművek lekérése
    live_vehicles = await fetch_live_vm(route_short) 
    
    # ... (render_with_fallback hívása)

# ... (a többi útvonal, pl. a /api/live/debug/vm/U1 végpont, ami a hibakereséshez kell)
