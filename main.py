# main.py
import os, io, json, csv, zipfile, time, math, re, asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import httpx

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse

APP_VERSION = "5.0.0"
BUILD = str(int(time.time()))

# -------------------- In-memory GTFS store --------------------
GTFS = {
    "loaded": False,
    "stops": {},            # stop_id -> {"name":..., "lat":..., "lon":...}
    "routes": {},           # route_id -> {"short":..., "long":...}
    "routes_by_short": {},  # short -> set(route_id)
    "trips": {},            # trip_id -> {"route_id":..., "headsign":...}
    "stop_times": {},       # stop_id -> list[{"trip_id":..., "dep": seconds, "seq": n}]
    "trip_stops": {},       # trip_id -> list[{"stop_id":..., "time": seconds}]
}

DATA_DIR = os.environ.get("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)
GTFS_ZIP_PATH = os.path.join(DATA_DIR, "gtfs_latest.zip")
LIVE_CFG_PATH = os.path.join(DATA_DIR, "live_config.json")

def uk_now():
    return datetime.now(timezone(timedelta(hours=0))).astimezone(timezone.utc).astimezone(timezone(timedelta(hours=0)))

def parse_hhmmss(s: str) -> Optional[int]:
    """Return seconds from 00:00 (supports >24h like 25:10:00)."""
    if not s:
        return None
    try:
        parts = [int(x) for x in s.split(":")]
        if len(parts) != 3:
            return None
        h, m, sec = parts
        return h*3600 + m*60 + sec
    except:
        return None

def load_live_cfg() -> Dict[str, Any]:
    if os.path.exists(LIVE_CFG_PATH):
        try:
            with open(LIVE_CFG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_live_cfg(obj: Dict[str, Any]) -> None:
    with open(LIVE_CFG_PATH, "w", encoding="utf-8") as f:
        json.dump(obj, f)

def parse_gtfs_zip(zip_bytes: bytes):
    """Parse minimal subset of GTFS needed for search + departures."""
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))

    def read_csv(name: str):
        with zf.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8-sig")
            reader = csv.DictReader(text)
            return list(reader)

    required = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]
    for r in required:
        if r not in zf.namelist():
            raise HTTPException(status_code=400, detail=f"GTFS file missing: {r}")

    stops = {}
    for r in read_csv("stops.txt"):
        sid = r.get("stop_id")
        if not sid: 
            continue
        stops[sid] = {
            "name": r.get("stop_name", "").strip(),
            "lat": float(r.get("stop_lat") or 0.0),
            "lon": float(r.get("stop_lon") or 0.0)
        }

    routes = {}
    routes_by_short = {}
    for r in read_csv("routes.txt"):
        rid = r.get("route_id")
        if not rid:
            continue
        short = (r.get("route_short_name") or "").strip()
        longn = (r.get("route_long_name") or "").strip()
        routes[rid] = {"short": short, "long": longn}
        if short:
            routes_by_short.setdefault(short, set()).add(rid)

    trips = {}
    for r in read_csv("trips.txt"):
        tid = r.get("trip_id")
        if not tid:
            continue
        trips[tid] = {
            "route_id": r.get("route_id"),
            "headsign": (r.get("trip_headsign") or "").strip()
        }

    stop_times = {}
    trip_stops = {}
    # Only keep first pass; we will sort later
    for r in read_csv("stop_times.txt"):
        sid = r.get("stop_id")
        tid = r.get("trip_id")
        dep = parse_hhmmss(r.get("departure_time") or r.get("arrival_time") or "")
        seq = int(r.get("stop_sequence") or 0)
        if not sid or not tid or dep is None:
            continue
        stop_times.setdefault(sid, []).append({"trip_id": tid, "dep": dep, "seq": seq})
        trip_stops.setdefault(tid, []).append({"stop_id": sid, "time": dep, "seq": seq})

    # Sort
    for sid in list(stop_times.keys()):
        stop_times[sid].sort(key=lambda x: (x["dep"], x["seq"]))
    for tid in list(trip_stops.keys()):
        trip_stops[tid].sort(key=lambda x: x["seq"])

    GTFS.update({
        "loaded": True,
        "stops": stops,
        "routes": routes,
        "routes_by_short": routes_by_short,
        "trips": trips,
        "stop_times": stop_times,
        "trip_stops": trip_stops
    })

# Try to preload GTFS if present on disk
if os.path.exists(GTFS_ZIP_PATH):
    try:
        with open(GTFS_ZIP_PATH, "rb") as f:
            parse_gtfs_zip(f.read())
    except Exception as e:
        print("GTFS preload failed:", e)

# -------------------- FastAPI app --------------------
app = FastAPI(title="Bluestar Bus — API", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------- Helpers --------------------
def to_hhmm(sec: int) -> str:
    h = sec // 3600
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

def uk_seconds_since_midnight(dt: Optional[datetime] = None) -> int:
    if dt is None:
        dt = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=0)))
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int((dt - midnight).total_seconds())

def status_obj():
    live_cfg = load_live_cfg()
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": BUILD,
        "uk_time": datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=0))).strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool(live_cfg.get("feed_url")),
        "gtfs_loaded": bool(GTFS["loaded"]),
    }

# -------------------- API: status & config --------------------
@app.get("/api/status")
async def api_status():
    return status_obj()

@app.get("/api/live/config")
async def api_live_get():
    cfg = load_live_cfg()
    return {"feed_url": cfg.get("feed_url", "")}

@app.post("/api/live/config")
async def api_live_set(body: Dict[str, Any]):
    url = (body or {}).get("feed_url", "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="feed_url is required")
    save_live_cfg({"feed_url": url})
    return {"ok": True}

# -------------------- API: GTFS upload --------------------
@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Please upload a GTFS .zip file")
    content = await file.read()
    # Save
    with open(GTFS_ZIP_PATH, "wb") as f:
        f.write(content)
    # Parse
    parse_gtfs_zip(content)
    return {"ok": True, "gtfs_loaded": True, "stops": len(GTFS["stops"]), "trips": len(GTFS["trips"])}

# -------------------- API: search --------------------
@app.get("/api/stops/search")
async def api_stops_search(q: str):
    if not GTFS["loaded"]:
        raise HTTPException(status_code=503, detail="GTFS not loaded")
    ql = q.lower().strip()
    out = []
    for sid, s in GTFS["stops"].items():
        if ql in s["name"].lower():
            out.append({"id": sid, "name": s["name"]})
            if len(out) >= 30:
                break
    return out

@app.get("/api/routes/search")
async def api_routes_search(q: str):
    if not GTFS["loaded"]:
        raise HTTPException(status_code=503, detail="GTFS not loaded")
    ql = q.lower().strip()
    seen = set()
    out = []
    # search by short name
    for short in GTFS["routes_by_short"].keys():
        if ql in short.lower() and short not in seen:
            seen.add(short)
            out.append({"route": short})
            if len(out) >= 30:
                break
    # also search long names
    if len(out) < 30:
        for rid, r in GTFS["routes"].items():
            if r["long"] and ql in r["long"].lower():
                short = r["short"] or rid
                if short not in seen:
                    seen.add(short)
                    out.append({"route": short})
                    if len(out) >= 30:
                        break
    return out

# -------------------- API: departures (from GTFS) --------------------
@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, window: int = 60):
    if not GTFS["loaded"]:
        raise HTTPException(status_code=503, detail="GTFS not loaded")
    if stop_id not in GTFS["stop_times"]:
        return {"departures": []}

    now_sec = uk_seconds_since_midnight()
    max_sec = now_sec + max(1, min(window, 360)) * 60

    rows = []
    for st in GTFS["stop_times"][stop_id]:
        dep = st["dep"]
        if now_sec <= dep <= max_sec:
            trip = GTFS["trips"].get(st["trip_id"], {})
            rid = trip.get("route_id")
            route_short = (GTFS["routes"].get(rid, {}).get("short") or "") if rid else ""
            rows.append({
                "route": route_short,
                "destination": trip.get("headsign", ""),
                "time_display": to_hhmm(dep),
                "is_due": (dep - now_sec) <= 60,
                "is_live": False,
                "delay_min": None,
                "trip_id": st["trip_id"]
            })
            if len(rows) >= 80:
                break
    return {"departures": rows}

# -------------------- API: trip detail (sequence of stops) --------------------
@app.get("/api/trips/{trip_id}")
async def api_trip(trip_id: str):
    if not GTFS["loaded"]:
        raise HTTPException(status_code=503, detail="GTFS not loaded")
    seq = GTFS["trip_stops"].get(trip_id, [])
    now_sec = uk_seconds_since_midnight()
    out = []
    for r in seq:
        s = GTFS["stops"].get(r["stop_id"], {})
        out.append({
            "stop_id": r["stop_id"],
            "stop_name": s.get("name", r["stop_id"]),
            "time_display": to_hhmm(r["time"]),
            "is_past": r["time"] < now_sec
        })
    return {"stops": out}

# -------------------- API: route -> live vehicles (best-effort) --------------------
@app.get("/api/routes/{route_short}/vehicles")
async def api_route_vehicles(route_short: str):
    cfg = load_live_cfg()
    url = (cfg.get("feed_url") or "").strip()
    if not url:
        return {"vehicles": []}
    # SIRI-VM JSON expected; best-effort filter by LineRef == route_short
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        # Try several common shapes
        vehicles = []
        # 1) BODS JSON flattened
        items = (
            data.get("Siri", {})
                .get("ServiceDelivery", {})
                .get("VehicleMonitoringDelivery", [{}])[0]
                .get("VehicleActivity", [])
        )
        for it in items:
            mvj = it.get("MonitoredVehicleJourney", {})
            line = (mvj.get("LineRef") or mvj.get("PublishedLineName") or "").strip()
            if line != route_short:
                continue
            vp = mvj.get("VehicleLocation", {})
            lat, lon = vp.get("Latitude"), vp.get("Longitude")
            if lat is None or lon is None:
                continue
            vehicles.append({
                "label": mvj.get("VehicleRef") or "",
                "lat": float(lat),
                "lon": float(lon)
            })
        return {"vehicles": vehicles}
    except Exception:
        # fail soft — never 500 a UI-t
        return {"vehicles": []}

# -------------------- Frontend (embedded) --------------------
INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Bluestar Bus — stop & route finder</title>
<link rel="preconnect" href="https://unpkg.com">
<link rel="stylesheet" href="https://unpkg.com/modern-css-reset/dist/reset.min.css">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  :root { --bg:#0b1020; --card:#121a2e; --text:#e7ebff; --muted:#9aa4be; --accent:#8d94ff; --good:#2bd576; --chip:#1b2442; --board:#0a0d10;}
  body { background:var(--bg); color:var(--text); font:16px/1.4 system-ui,-apple-system,Segoe UI,Roboto,sans-serif; }
  .container{ max-width:980px; margin:24px auto; padding:0 12px; }
  .row{ display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .title { font-size:clamp(22px,4vw,34px); font-weight:800; display:flex; align-items:center; gap:10px }
  .btn{ background:var(--accent); color:#fff; border:none; border-radius:12px; padding:10px 14px; font-weight:700; cursor:pointer; }
  .ghost{ background:#0f1424; color:var(--text); border:1px solid #273153; }
  .input, select{ background:#0f1424; color:var(--text); border:1px solid #202844; border-radius:12px; padding:10px 12px; width:100%; }
  .card{ background:var(--card); border-radius:16px; padding:16px; box-shadow:0 6px 30px rgba(0,0,0,.3); }
  .tabs{ display:flex; gap:8px; margin:16px 0; }
  .tab{ padding:10px 14px; border-radius:999px; background:#0f1424; cursor:pointer; }
  .tab.active{ background:#2a3354; }
  .map{ height:360px; border-radius:12px; overflow:hidden; }
  .list{ display:grid; gap:10px; margin-top:12px; }
  .item{ background:#0f1424; border-radius:12px; padding:12px; display:grid; grid-template-columns:auto 1fr auto; align-items:center; gap:10px; }
  .pill{ min-width:48px; height:38px; border-radius:10px; background:var(--chip); display:grid; place-items:center; font-weight:800; padding:0 10px; }
  .muted{ color:var(--muted); }
  .good{ color:var(--good); }
  .blink{ animation:blink 1s step-start infinite; } @keyframes blink { 50%{opacity:0} }
  .mono{ font-variant-numeric: tabular-nums; }
  .gear { background:#213058; border:1px solid #2c3c6a; border-radius:10px; padding:8px 10px; cursor:pointer; }
  .modal{ position:fixed; inset:0; display:grid; place-items:center; background:rgba(0,0,0,.4) }
  .box{ width:min(720px,92vw); background:#0f1424; border:1px solid #2a355d; border-radius:16px; padding:16px; }
</style>
</head>
<body>
  <div class="container">
    <div class="row" style="justify-content:space-between">
      <div class="title">🚌 Bluestar Bus — stop & route finder</div>
      <div class="row">
        <div class="muted" id="ukTime">UK: –:–:–</div>
        <button class="btn ghost" id="openSettings" style="margin-left:12px">⚙️ Settings</button>
      </div>
    </div>
    <div class="muted" style="margin:6px 0 18px">Refresh every 20 s · Build: <span id="build"></span></div>

    <div class="tabs">
      <div id="tabStop" class="tab active">Stop</div>
      <div id="tabRoute" class="tab">Route</div>
      <div id="tabFav" class="tab">Favourites</div>
    </div>

    <!-- STOP -->
    <div id="stopCard" class="card">
      <div class="row">
        <input id="stopQuery" class="input" placeholder="Stop name" />
        <input id="windowMin" type="number" class="input" value="60" style="max-width:140px" />
        <button id="stopSearchBtn" class="btn">Search</button>
      </div>
      <select id="stopSelect" class="input" style="margin-top:10px"></select>
      <div class="row" style="margin-top:6px; justify-content:space-between">
        <div class="muted"><span id="selectedStopLbl">Selected stop: –</span></div>
        <div class="muted">Refresh every 20 s</div>
      </div>
      <div class="list" id="stopList"></div>
    </div>

    <!-- ROUTE -->
    <div id="routeCard" class="card" style="display:none">
      <div class="row">
        <input id="routeQuery" class="input" placeholder="Route" />
        <button id="routeSearchBtn" class="btn">Search</button>
      </div>
      <select id="routeSelect" class="input" style="margin-top:10px"></select>
      <div id="map" class="map" style="margin-top:12px"></div>
      <div class="muted" style="margin-top:8px">If there are no live vehicles on the route, the map will be empty.</div>
    </div>

    <!-- FAVS (client-side only) -->
    <div id="favCard" class="card" style="display:none">
      <h3 style="margin-bottom:10px">★ Favourites</h3>
      <div class="muted">Stops & Routes you open will be remembered locally.</div>
    </div>
  </div>

  <!-- Settings -->
  <div id="settings" class="modal" style="display:none">
    <div class="box">
      <div class="row" style="justify-content:space-between">
        <strong>Settings</strong>
        <button id="closeSettings" class="btn">×</button>
      </div>
      <div style="margin-top:10px">
        <div><strong>Live feed (BODS SIRI-VM)</strong></div>
        <div class="row" style="margin-top:8px">
          <input id="feedUrl" class="input" placeholder="https://data.bus-data.dft.gov.uk/api/v1/datafeed/...." />
          <button id="saveFeed" class="btn">Save Live URL</button>
        </div>
        <div class="muted" style="margin-top:8px">Paste the full BODS Vehicle Monitoring URL including <code>api_key</code>.</div>
        <hr style="border:none;border-top:1px solid #2a355d;margin:14px 0">
        <div><strong>Upload GTFS ZIP</strong></div>
        <div class="row" style="margin-top:8px">
          <input id="gtfsFile" type="file" accept=".zip" class="input" />
          <button id="uploadGtfs" class="btn">Upload GTFS</button>
        </div>
        <div class="muted" style="margin-top:8px">The file will be sent to <code>/api/upload</code> as <code>multipart/form-data</code> (field name: <code>file</code>).</div>
      </div>
    </div>
  </div>

<script>
const $=s=>document.querySelector(s);
const stopQuery=$("#stopQuery"), windowMin=$("#windowMin"), stopSearchBtn=$("#stopSearchBtn"), stopSelect=$("#stopSelect"), stopList=$("#stopList");
const routeQuery=$("#routeQuery"), routeSearchBtn=$("#routeSearchBtn"), routeSelect=$("#routeSelect");
const settings=$("#settings"), openSettings=$("#openSettings"), closeSettings=$("#closeSettings"), feedUrl=$("#feedUrl"), saveFeed=$("#saveFeed");
const buildSpan=$("#build"), ukTime=$("#ukTime");

buildSpan.textContent = "%BUILD%";

async function tick(){
  try{
    const r=await fetch("/api/status"); const j=await r.json();
    ukTime.textContent = "UK: " + (j.uk_time||"–:–:–");
  }catch(e){}
}
setInterval(tick, 1000); tick();

// Settings modal
openSettings.onclick=()=>{ settings.style.display="grid"; (async()=>{ try{ const r=await fetch("/api/live/config"); const j=await r.json(); feedUrl.value=j.feed_url||""; }catch(e){} })(); };
closeSettings.onclick=()=> settings.style.display="none";
saveFeed.onclick=async()=>{ try{ await fetch("/api/live/config",{method:"POST",headers:{'Content-Type':'application/json'},body:JSON.stringify({feed_url:feedUrl.value.trim()})}); alert("Saved."); }catch(e){ alert("Save failed"); } };
$("#uploadGtfs").onclick=async()=>{
  const f=$("#gtfsFile").files[0]; if(!f){ alert("Choose a .zip"); return; }
  const fd=new FormData(); fd.append("file",f);
  try{ const r=await fetch("/api/upload",{method:"POST",body:fd}); const j=await r.json(); if(j.ok){ alert("GTFS uploaded. Stops: "+j.stops); } else { alert("Upload failed"); } }catch(e){ alert("Upload failed"); }
};

// Stops
stopSearchBtn.onclick=async()=>{
  const q=stopQuery.value.trim(); if(!q) return;
  try{
    const r=await fetch(`/api/stops/search?q=${encodeURIComponent(q)}`); const arr=await r.json();
    stopSelect.innerHTML=""; arr.forEach(x=>{ const o=document.createElement("option"); o.value=x.id; o.textContent=x.name; stopSelect.appendChild(o); });
    if(arr[0]){ stopSelect.value=arr[0].id; renderDepartures(); }
  }catch(e){ toast("Stop search failed"); }
};
stopSelect.onchange=()=>renderDepartures();

function delayBadge(d){ if(!d) return ""; const n=Number(d); if(!n) return ""; const sign=n>0?"+":""; const cls=n>0?"":"muted"; return `<span class="mono ${n>0?'good':''} ${cls}" style="margin-left:8px">${sign}${n} min</span>`; }
async function renderDepartures(){
  const id=stopSelect.value; if(!id) return;
  const w=Math.max(1, Number(windowMin.value||60));
  try{
    const r=await fetch(`/api/stops/${encodeURIComponent(id)}/next_departures?window=${w}`); const j=await r.json();
    stopList.innerHTML="";
    (j.departures||[]).forEach(row=>{
      const it=document.createElement("div"); it.className="item";
      it.innerHTML=`<div class="pill">${row.route||"–"}</div><div>${row.destination||"–"}</div><div class="mono ${row.is_due?'good blink':''}">${row.is_due?"Due":row.time_display}${delayBadge(row.delay_min)}</div>`;
      it.onclick=()=>openTrip(row.trip_id,row.route,row.destination);
      stopList.appendChild(it);
    });
  }catch(e){ toast("Departures failed: "+e); }
}
setInterval(renderDepartures, 20000);

// Trip modal
function openTrip(tripId, route, headsign){
  if(!tripId) return;
  (async()=>{
    const r=await fetch(`/api/trips/${encodeURIComponent(tripId)}`); const j=await r.json();
    const wrap=document.createElement("div"); wrap.className="modal";
    const box=document.createElement("div"); box.className="box";
    box.innerHTML=`<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
      <div><strong>Trip</strong> · ${route||""} → ${headsign||""}</div><button class="btn" id="closeTrip">×</button></div>`;
    const list=document.createElement("div"); list.className="list";
    (j.stops||[]).forEach(s=>{ const row=document.createElement("div"); row.className="item";
      const left=document.createElement("div"); left.className="mono muted"; left.textContent=s.time_display; if(s.is_past) left.style.opacity=.6;
      const name=document.createElement("div"); name.textContent=s.stop_name; if(s.is_past) name.style.opacity=.6;
      row.appendChild(document.createElement("div")); row.appendChild(name); row.appendChild(left); list.appendChild(row); });
    box.appendChild(list); wrap.appendChild(box); document.body.appendChild(wrap);
    document.getElementById("closeTrip").onclick=()=>document.body.removeChild(wrap);
  })();
}

// Route vehicles
let map, markers=[];
function ensureMap(){ if(map) return; map = L.map('map',{zoomControl:true}).setView([50.9097,-1.4044],12);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'Leaflet | © OpenStreetMap'}).addTo(map); }
routeSearchBtn.onclick=async()=>{ const q=routeQuery.value.trim(); if(!q) return;
  const r=await fetch(`/api/routes/search?q=${encodeURIComponent(q)}`); const items=await r.json(); routeSelect.innerHTML=""; items.forEach(it=>{ const o=document.createElement("option"); o.value=it.route; o.textContent=it.route; routeSelect.appendChild(o);}); if(items[0]){ routeSelect.value=items[0].route; renderVehicles(); } };
routeSelect.onchange=renderVehicles;
async function renderVehicles(){
  ensureMap(); markers.forEach(m=>m.remove()); markers=[];
  const route=routeSelect.value; if(!route) return;
  try{
    const r=await fetch(`/api/routes/${encodeURIComponent(route)}/vehicles`); const j=await r.json(); const vs=j.vehicles||[];
    vs.forEach(v=>{ const m=L.marker([v.lat,v.lon]).addTo(map).bindPopup(`${v.label||""}`); markers.push(m); });
    if(vs.length){ const g=L.featureGroup(markers); map.fitBounds(g.getBounds().pad(0.2)); }
  }catch(e){}
}

// Tabs
const tabStop=$("#tabStop"), tabRoute=$("#tabRoute"), tabFav=$("#tabFav");
const stopCard=$("#stopCard"), routeCard=$("#routeCard"), favCard=$("#favCard");
tabStop.onclick=()=>{ tabStop.classList.add("active"); tabRoute.classList.remove("active"); tabFav.classList.remove("active"); stopCard.style.display="block"; routeCard.style.display="none"; favCard.style.display="none"; };
tabRoute.onclick=()=>{ tabRoute.classList.add("active"); tabStop.classList.remove("active"); tabFav.classList.remove("active"); routeCard.style.display="block"; stopCard.style.display="none"; favCard.style.display="none"; };
tabFav.onclick=()=>{ tabFav.classList.add("active"); tabStop.classList.remove("active"); tabRoute.classList.remove("active"); favCard.style.display="block"; stopCard.style.display="none"; routeCard.style.display="none"; };

// Toast
function toast(msg){ const t=document.createElement("div"); t.textContent=msg; t.style.cssText="position:fixed;left:12px;bottom:12px;background:#6b1f1f;color:#fff;padding:10px 12px;border-radius:10px;z-index:9999"; document.body.appendChild(t); setTimeout(()=>document.body.removeChild(t), 4000); }
</script>
</body>
</html>
""".replace("%BUILD%", BUILD)

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(INDEX_HTML)

# Health for Railway
@app.get("/health")
async def health():
    return {"ok": True}

# Uvicorn entrypoint for local run
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
