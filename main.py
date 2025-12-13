import os
import csv
import math
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse

# httpx opcionális (live-hoz)
try:
    import httpx
except Exception:
    httpx = None

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bluestar")

DATA_DIR = os.getenv("DATA_DIR", "gtfs")
UK_TZ = pytz.timezone("Europe/London")

ALLOWED_OPERATORS = {"blus", "unil"}  # Bluestar / Unilink

def now_uk():
    return datetime.now(UK_TZ)

def operator_ok(op: str) -> bool:
    return (op or "").strip().lower()[:4] in ALLOWED_OPERATORS

def gtfs_sec(hhmmss: str) -> int:
    try:
        h, m, s = (hhmmss or "00:00:00").split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except Exception:
        return 0

def midnight_uk(dt=None):
    dt = dt or now_uk()
    return UK_TZ.localize(datetime(dt.year, dt.month, dt.day, 0, 0, 0))

def sec_to_today(sec: int) -> datetime:
    base = midnight_uk()
    days = sec // 86400
    rem = sec % 86400
    return base + timedelta(days=days, seconds=rem)

def fmt_hhmm(dt: datetime) -> str:
    return dt.strftime("%H:%M")

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

# ------------------------- GTFS memóriában -------------------------
routes = []
stops = []
trips = []
stop_times = []
shapes = []

routes_by_id = {}
routes_by_short = defaultdict(list)
stops_by_id = {}
stops_by_code = {}
trips_by_id = {}
trips_by_route = defaultdict(list)

stop_times_by_stop = defaultdict(list)
stop_times_by_trip = defaultdict(list)

shape_points_by_id = defaultdict(list)

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
    shp = os.path.join(DATA_DIR, "shapes.txt")

    routes[:] = _read_csv(rp)
    stops[:] = _read_csv(sp)
    trips[:] = _read_csv(tp)
    stop_times[:] = _read_csv(stp)
    shapes[:] = _read_csv(shp)

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
        arr.sort(key=lambda x: gtfs_sec(x.get("stop_sequence") or "0") or gtfs_sec(x.get("departure_time") or ""))

    # shapes
    shape_points_by_id.clear()
    for p in shapes:
        sid = (p.get("shape_id") or "").strip()
        if not sid:
            continue
        try:
            lat = float(p.get("shape_pt_lat"))
            lon = float(p.get("shape_pt_lon"))
            seq = int(float(p.get("shape_pt_sequence") or 0))
        except Exception:
            continue
        shape_points_by_id[sid].append((seq, lat, lon))
    for sid, pts in shape_points_by_id.items():
        pts.sort(key=lambda x: x[0])

def stop_by_any(id_or_code: str):
    return stops_by_id.get(id_or_code) or stops_by_code.get(id_or_code)

# ------------------------- Live: VehicleMonitoring -------------------------
LIVE_CACHE = {}
LIVE_TTL = 8

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

async def http_get_json(url, params=None):
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

async def fetch_live_vm_all():
    """
    Visszaad minden élő járművet (Bluestar/Unilink) ha a VM endpoint nem LineRef kötelező.
    Ha LineRef kötelező, akkor route-onként fogjuk hívni a /api/route/{short}-ban.
    """
    ck = ("vm_all",)
    c = cache_get(ck)
    if c is not None:
        return c

    out = []
    url = SIRI_VM_URL_RAW or ""
    if not url:
        cache_set(ck, [])
        return []

    # ha {line_ref} van benne, akkor nem tudunk "all" módot
    if "{line_ref}" in url:
        cache_set(ck, [])
        return []

    data = await http_get_json(url, params={})
    try:
        deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", []) or []
        for d in deliveries:
            for a in (d.get("VehicleActivity") or []):
                j = a.get("MonitoredVehicleJourney", {}) or {}
                op = (j.get("OperatorRef") or "").strip()
                if op and not operator_ok(op):
                    continue
                loc = j.get("VehicleLocation") or {}
                lat = loc.get("Latitude")
                lon = loc.get("Longitude")
                if lat is None or lon is None:
                    continue
                line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                out.append({
                    "lat": float(lat),
                    "lon": float(lon),
                    "line": str(line),
                    "operator": (op or "").lower()[:4],
                    "fleet": str(j.get("VehicleRef") or ""),
                    "trip_id": (j.get("FramedVehicleJourneyRef", {}) or {}).get("DatedVehicleJourneyRef") or "",
                })
    except Exception as e:
        log.warning("parse VM failed: %s", e)
        out = []

    cache_set(ck, out)
    return out

async def fetch_live_vm_for_line(route_short: str):
    ck = ("vm_line", route_short.lower())
    c = cache_get(ck)
    if c is not None:
        return c
    out = []
    url, params = _format_vm_url(route_short)
    if not url:
        cache_set(ck, [])
        return []
    data = await http_get_json(url, params=params)
    try:
        deliveries = (data or {}).get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", []) or []
        for d in deliveries:
            for a in (d.get("VehicleActivity") or []):
                j = a.get("MonitoredVehicleJourney", {}) or {}
                line = (j.get("LineRef") or j.get("PublishedLineName") or "").strip()
                op = (j.get("OperatorRef") or "").strip()
                if op and not operator_ok(op):
                    continue
                if route_short and line and route_short.lower() != str(line).lower():
                    continue
                loc = j.get("VehicleLocation") or {}
                lat = loc.get("Latitude")
                lon = loc.get("Longitude")
                if lat is None or lon is None:
                    continue
                out.append({
                    "lat": float(lat),
                    "lon": float(lon),
                    "line": str(line),
                    "operator": (op or "").lower()[:4],
                    "fleet": str(j.get("VehicleRef") or ""),
                    "trip_id": (j.get("FramedVehicleJourneyRef", {}) or {}).get("DatedVehicleJourneyRef") or "",
                })
    except Exception as e:
        log.warning("parse VM line failed: %s", e)
        out = []
    cache_set(ck, out)
    return out

# ------------------------- FastAPI -------------------------
app = FastAPI()

@app.on_event("startup")
def _startup():
    load_gtfs()
    log.info("GTFS loaded: routes=%d stops=%d shapes=%d", len(routes), len(stops), len(shapes))

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
        "shapes.txt": os.path.exists(os.path.join(DATA_DIR, "shapes.txt")),
        "routes_count": len(routes),
        "stops_count": len(stops),
        "shapes_count": len(shapes),
        "live_enabled": bool(SIRI_VM_URL_RAW or SIRI_SM_URL_RAW),
        "httpx_available": httpx is not None,
        "vm_url": SIRI_VM_URL_RAW,
        "sm_url": SIRI_SM_URL_RAW,
        "extra_headers": list(EXTRA_HEADERS.keys()),
    })

# ------------------------- UI (Leaflet + bottom sheet) -------------------------
INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>bluestar</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    :root{--bg:#0b1220;--panel:#0f172a;--card:#111827;--muted:#94a3b8;--txt:#e5e7eb;--blue:#60a5fa;}
    html,body{height:100%;margin:0;background:var(--bg);color:var(--txt);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
    #map{height:100%;width:100%}
    .sheet{
      position:fixed;left:0;right:0;bottom:0;
      background:rgba(15,23,42,.92);
      backdrop-filter: blur(8px);
      border-top-left-radius:18px;border-top-right-radius:18px;
      box-shadow:0 -10px 30px rgba(0,0,0,.45);
      padding:10px 12px 14px;
      max-height:60vh; overflow:auto;
    }
    .top{
      display:flex;align-items:center;gap:10px;
    }
    .brand{font-weight:800}
    .search{
      flex:1;display:flex;gap:8px;align-items:center;
      background:#0b1220;border:1px solid #1f2937;border-radius:14px;
      padding:10px 12px;
    }
    .search input{flex:1;background:transparent;border:0;outline:0;color:var(--txt);font-size:16px}
    .xbtn{
      width:34px;height:34px;border-radius:10px;border:1px solid #334155;
      background:#0b1220;color:var(--txt);font-size:18px
    }
    .title{font-size:18px;font-weight:700;margin:12px 0 8px}
    .list{display:flex;flex-direction:column;gap:10px}
    .item{
      background:rgba(17,24,39,.9);
      border:1px solid rgba(255,255,255,.06);
      border-radius:14px;
      padding:12px;
      display:flex;justify-content:space-between;align-items:center;gap:10px;
    }
    .item .left{display:flex;flex-direction:column;gap:4px}
    .muted{color:var(--muted);font-size:13px}
    .pill{padding:6px 10px;border-radius:12px;background:#0b1220;border:1px solid #334155;color:var(--txt);font-weight:700}
    .pill.blue{border-color:rgba(96,165,250,.7);color:var(--blue)}
    .pill.ok{border-color:rgba(16,185,129,.7);color:#10b981}
    .pill.bad{border-color:rgba(239,68,68,.7);color:#ef4444}
    .divider{height:1px;background:rgba(255,255,255,.06);margin:10px 0}
    .hint{font-size:13px;color:var(--muted);margin-top:8px}
    .leaflet-control-attribution{display:none}
  </style>
</head>
<body>
<div id="map"></div>

<div class="sheet" id="sheet">
  <div class="top">
    <div class="brand">★ bluestar</div>
    <div class="search">
      <input id="q" placeholder="Keresés: járat vagy megálló" />
      <button class="xbtn" id="clear">×</button>
    </div>
  </div>

  <div class="title" id="modeTitle">Kiemelt forgalmi változások</div>
  <div class="muted">Demo hely. (Ezt később összekötjük riasztásokkal.)</div>

  <div class="divider"></div>

  <div class="title">Találatok</div>
  <div class="list" id="results"></div>

  <div class="hint">Tipp: írj be pl. <b>U1</b>, <b>17</b> vagy egy megálló nevet.</div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
  const map = L.map('map', { zoomControl: true }).setView([50.910, -1.404], 12);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(map);

  const vehicleLayer = L.layerGroup().addTo(map);
  let routeLine = null;

  function badgeHtml(text){
    return `<div style="
      width:40px;height:40px;border-radius:999px;
      background:#2563eb;color:white;display:flex;align-items:center;justify-content:center;
      font-weight:800;box-shadow:0 8px 22px rgba(0,0,0,.35);border:2px solid rgba(255,255,255,.18)
    ">${text}</div>`;
  }

  function vehicleIcon(label){
    return L.divIcon({ html: badgeHtml(label), className:'', iconSize:[40,40], iconAnchor:[20,20] });
  }

  async function refreshVehicles(){
    try{
      const r = await fetch('/api/map/vehicles');
      const data = await r.json();
      vehicleLayer.clearLayers();
      for(const v of data.vehicles){
        const label = (v.line || '').toUpperCase().slice(0,3) || 'BUS';
        L.marker([v.lat, v.lon], { icon: vehicleIcon(label) })
          .addTo(vehicleLayer)
          .on('click', () => {
            // ha van trip_id, később innen megnyitjuk a trip sheetet
            // most csak zoom
            map.setView([v.lat, v.lon], Math.max(map.getZoom(), 14));
          });
      }
    }catch(e){}
  }

  async function search(q){
    const box = document.getElementById('results');
    box.innerHTML = '';
    if(!q){ return; }
    const r = await fetch('/api/search?q=' + encodeURIComponent(q));
    const data = await r.json();

    if(data.routes.length){
      const h = document.createElement('div');
      h.className = 'muted';
      h.textContent = 'Járatok';
      box.appendChild(h);
      for(const it of data.routes){
        const el = document.createElement('div');
        el.className = 'item';
        const name = it.long_name && it.long_name !== 'nan' ? it.long_name : (it.operator || 'bus');
        el.innerHTML = `
          <div class="left">
            <div><span class="pill blue">${it.short}</span></div>
            <div class="muted">${name}</div>
          </div>
          <div class="pill">Megnyitás</div>
        `;
        el.onclick = async () => {
          const rr = await fetch('/api/route/' + encodeURIComponent(it.short));
          const rd = await rr.json();
          if(routeLine){ map.removeLayer(routeLine); routeLine = null; }
          if(rd.shape && rd.shape.length){
            routeLine = L.polyline(rd.shape, { weight: 5 });
            routeLine.addTo(map);
            map.fitBounds(routeLine.getBounds(), { padding:[20,20] });
          }
        };
        box.appendChild(el);
      }
    }

    if(data.stops.length){
      const h = document.createElement('div');
      h.className = 'muted';
      h.style.marginTop = '10px';
      h.textContent = 'Megállók';
      box.appendChild(h);
      for(const it of data.stops){
        const el = document.createElement('div');
        el.className = 'item';
        el.innerHTML = `
          <div class="left">
            <div><b>${it.name}</b></div>
            <div class="muted">${it.code || it.id}</div>
          </div>
          <div class="pill">Megnyitás</div>
        `;
        el.onclick = () => {
          if(it.lat && it.lon){
            map.setView([it.lat, it.lon], 16);
          }
        };
        box.appendChild(el);
      }
    }

    if(!data.routes.length && !data.stops.length){
      const el = document.createElement('div');
      el.className = 'muted';
      el.textContent = 'Nincs találat.';
      box.appendChild(el);
    }
  }

  const q = document.getElementById('q');
  let t = null;
  q.addEventListener('input', () => {
    clearTimeout(t);
    t = setTimeout(() => search(q.value.trim()), 200);
  });
  document.getElementById('clear').onclick = () => { q.value=''; document.getElementById('results').innerHTML=''; };

  refreshVehicles();
  setInterval(refreshVehicles, 5000);
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(INDEX_HTML)

# ------------------------- API -------------------------
@app.get("/api/search")
def api_search(q: str = ""):
    q = (q or "").strip().lower()
    routes_found = []
    stops_found = []

    if q:
        # routes: short name partial match
        for short, arr in routes_by_short.items():
            if q in short:
                r = arr[0]
                ag = (r.get("agency_id") or "").strip().lower()[:4]
                if ag and ag not in ALLOWED_OPERATORS:
                    continue
                routes_found.append({
                    "short": (r.get("route_short_name") or "").strip(),
                    "long_name": (r.get("route_long_name") or "").strip(),
                    "operator": ag or "",
                    "route_id": (r.get("route_id") or "").strip(),
                })

        # stops: name match
        for s in stops:
            name = (s.get("stop_name") or "")
            if q in name.lower():
                try:
                    lat = float(s.get("stop_lat")) if s.get("stop_lat") else None
                    lon = float(s.get("stop_lon")) if s.get("stop_lon") else None
                except Exception:
                    lat = lon = None
                stops_found.append({
                    "id": (s.get("stop_id") or "").strip(),
                    "code": (s.get("stop_code") or "").strip(),
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                })

    # rendezzük route-okat numerikusan is
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
    routes_found.sort(key=sort_key)

    return JSONResponse({"routes": routes_found[:50], "stops": stops_found[:50]})

@app.get("/api/map/vehicles")
async def api_map_vehicles():
    # ha tudunk "all" módot: használjuk
    vehicles = await fetch_live_vm_all()
    if vehicles:
        return JSONResponse({"vehicles": vehicles})

    # különben csak a leggyakoribb route-okra adunk (hogy legyen valami)
    # (később ezt finomítjuk: látható route-okra, vagy user kedvencek)
    sample = []
    seen = set()
    for short in list(routes_by_short.keys())[:15]:
        if short in seen:
            continue
        seen.add(short)
        vs = await fetch_live_vm_for_line(short)
        sample.extend(vs)
    return JSONResponse({"vehicles": sample})

@app.get("/api/route/{short}")
async def api_route(short: str):
    key = (short or "").strip().lower()
    rlist = routes_by_short.get(key, [])
    if not rlist:
        raise HTTPException(status_code=404, detail="Route not found")

    r = rlist[0]
    rid = (r.get("route_id") or "").strip()
    ag = (r.get("agency_id") or "").strip().lower()[:4]
    if ag and ag not in ALLOWED_OPERATORS:
        raise HTTPException(status_code=404, detail="Route not allowed")

    # shape: első olyan trip shape_id-ja, ami ehhez a route_id-hoz tartozik
    shape = []
    shape_id = ""
    for t in trips_by_route.get(rid, [])[:200]:
        sid = (t.get("shape_id") or "").strip()
        if sid and sid in shape_points_by_id:
            shape_id = sid
            pts = shape_points_by_id[sid]
            shape = [[lat, lon] for (_seq, lat, lon) in pts]
            break

    vehicles = await fetch_live_vm_for_line(r.get("route_short_name") or short)

    return JSONResponse({
        "short": (r.get("route_short_name") or "").strip(),
        "route_id": rid,
        "operator": ag or "",
        "shape_id": shape_id,
        "shape": shape,
        "vehicles": vehicles,
    })

@app.get("/api/trip/{trip_id}")
def api_trip(trip_id: str):
    t = trips_by_id.get(trip_id)
    if not t:
        raise HTTPException(status_code=404, detail="Trip not found")
    st = stop_times_by_trip.get(trip_id, [])
    out = []
    for row in st:
        sid = (row.get("stop_id") or "").strip()
        s = stops_by_id.get(sid, {})
        dep = row.get("departure_time") or row.get("arrival_time") or ""
        out.append({
            "stop_id": sid,
            "stop_name": (s.get("stop_name") or "").strip(),
            "time": dep,
            "sequence": row.get("stop_sequence") or "",
        })
    return JSONResponse({"trip_id": trip_id, "stops": out})

@app.get("/health")
def health():
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
