import os, json, time, math, io, zipfile
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Query, Body, Response, UploadFile, File, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone

app = FastAPI(title="Bluestar Bus — API", version="5.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

STATE = {
    "live_cfg": {"feed_url": os.getenv("LIVE_FEED_URL", "").strip()},
    "gtfs_ready": False,
    "gtfs": {"stops":{}, "routes":{}, "trips":{}, "stop_times":{}, "shapes":{}, "index_stop_name":{}},
    "live": {"fetched_at":0.0, "vehicles":[]},
    "build": str(int(time.time()))
}

TZ = timezone.utc
def now_utc(): return datetime.now(tz=TZ)
def parse_hhmmss(s:str)->int:
    if not s: return 0
    hh, mm, ss = (s.split(":")+["0","0","0"])[:3]
    return int(hh)*3600+int(mm)*60+int(ss)
def normalize_route(x: Optional[str])->str:
    if x is None: return ""
    s=str(x).strip().upper()
    for sep in [":","/"]:
        if sep in s: s=s.split(sep)[-1]
    if s.startswith("HAA0") and s[4:].isdigit(): return str(int(s[4:]))
    if s.isdigit(): return str(int(s))
    return s

def status_ok():
    return {
        "ok": True, "version": app.version, "build": STATE["build"],
        "time": now_utc().strftime("%H:%M:%S"), "tz":"Europe/London",
        "live_feed_configured": bool(STATE["live_cfg"]["feed_url"]),
        "gtfs_dir":"data/gtfs", "gtfs_ready":STATE["gtfs_ready"],
        "gtfs_stops": len(STATE["gtfs"]["stops"])
    }

@app.get("/", response_class=JSONResponse)
def root(): return {"detail":"Open /index.html","docs":"/docs"}

@app.get("/index.html", response_class=PlainTextResponse)
def serve_index():
    try:
        with open("index.html","r",encoding="utf-8") as f:
            return Response(f.read(), media_type="text/html; charset=utf-8")
    except FileNotFoundError:
        return Response("<h1>index.html missing</h1>", media_type="text/html")

@app.get("/api/status")
def api_status(): return status_ok()

class LiveConfigIn(BaseModel):
    feed_url: str

@app.get("/api/live/config")
def get_live_cfg(): return STATE["live_cfg"]

@app.post("/api/live/config")
def set_live_cfg(cfg: LiveConfigIn):
    STATE["live_cfg"]["feed_url"] = cfg.feed_url.strip()
    return {"ok": True, "feed_url": STATE["live_cfg"]["feed_url"]}

# ---------- GTFS betöltő / új végpontok ----------
def ensure_dir(p:str):
    os.makedirs(p, exist_ok=True)

def extract_gtfs_zip(zip_bytes: bytes, target_dir="data/gtfs")->Dict[str,Any]:
    ensure_dir(target_dir)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        # csak a szükséges txt-ket bontjuk
        need = {"stops.txt","routes.txt","trips.txt","stop_times.txt","shapes.txt"}
        names = set(z.namelist())
        missing = [n for n in need if n not in names]
        if missing:
            # mégis kibontunk mindent, hátha shape nincs – ez nem végzetes a kereséshez
            pass
        for name in names:
            if not name.lower().endswith(".txt"): continue
            with z.open(name) as src, open(os.path.join(target_dir, os.path.basename(name)), "wb") as dst:
                dst.write(src.read())
    # jelöljük újratöltésre
    STATE["gtfs_ready"] = False
    G = load_gtfs_if_needed()
    return {"ok": STATE["gtfs_ready"], "stops": len(G["stops"])}

@app.post("/api/gtfs/upload")
async def gtfs_upload(file: UploadFile = File(...)):
    """GTFS .zip feltöltése Swaggeren keresztül."""
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Please upload a .zip GTFS file.")
    data = await file.read()
    info = extract_gtfs_zip(data)
    return info

class GtfsUrlIn(BaseModel):
    url: str

@app.post("/api/gtfs/load-url")
def gtfs_load_url(inp: GtfsUrlIn):
    """GTFS .zip letöltése URL-ről és betöltése."""
    import requests
    r = requests.get(inp.url, timeout=30)
    r.raise_for_status()
    info = extract_gtfs_zip(r.content)
    return info

def load_gtfs_if_needed()->Dict[str,Any]:
    if STATE["gtfs_ready"]: return STATE["gtfs"]
    gtfs_dir="data/gtfs"
    need = ["stops.txt","routes.txt","trips.txt","stop_times.txt","shapes.txt"]
    missing = [n for n in need if not os.path.exists(os.path.join(gtfs_dir,n))]
    if missing:
        STATE["gtfs_ready"]=False
        return STATE["gtfs"]
    import csv
    G = STATE["gtfs"] = {k:{} for k in ["stops","routes","trips","stop_times","shapes","index_stop_name"]}
    with open(os.path.join(gtfs_dir,"stops.txt"),encoding="utf-8") as f:
        for r in csv.DictReader(f):
            sid=r["stop_id"]; G["stops"][sid]={"stop_id":sid,"name":r.get("stop_name",""),
                "lat":float(r.get("stop_lat",0)),"lon":float(r.get("stop_lon",0))}
            key=G["stops"][sid]["name"].strip().lower()
            if key: G["index_stop_name"].setdefault(key,[]).append(sid)
    with open(os.path.join(gtfs_dir,"routes.txt"),encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rid=r["route_id"]; G["routes"][rid]={"route_id":rid,
                "route_short_name":r.get("route_short_name",""),
                "route_long_name":r.get("route_long_name","")}
    with open(os.path.join(gtfs_dir,"trips.txt"),encoding="utf-8") as f:
        for r in csv.DictReader(f):
            tid=r["trip_id"]
            G["trips"][tid]={"trip_id":tid,"route_id":r.get("route_id",""),
                "service_id":r.get("service_id",""),"shape_id":r.get("shape_id",""),
                "headsign":r.get("trip_headsign","") or r.get("trip_short_name","")}
    with open(os.path.join(gtfs_dir,"stop_times.txt"),encoding="utf-8") as f:
        for r in csv.DictReader(f):
            tid=r["trip_id"]
            G["stop_times"].setdefault(tid,[]).append({
                "stop_id":r["stop_id"],"arr":r.get("arrival_time",""),
                "dep":r.get("departure_time",""),"seq":int(r.get("stop_sequence","0"))
            })
    for tid,arr in G["stop_times"].items(): arr.sort(key=lambda x:x["seq"])
    if os.path.exists(os.path.join(gtfs_dir,"shapes.txt")):
        with open(os.path.join(gtfs_dir,"shapes.txt"),encoding="utf-8") as f:
            import csv
            for r in csv.DictReader(f):
                sid=r["shape_id"]
                STATE["gtfs"]["shapes"].setdefault(sid,[]).append({
                    "lat":float(r.get("shape_pt_lat",0)),"lon":float(r.get("shape_pt_lon",0)),
                    "seq":int(r.get("shape_pt_sequence","0"))
                })
        for sid,arr in STATE["gtfs"]["shapes"].items(): arr.sort(key=lambda x:x["seq"])
    STATE["gtfs_ready"]=True
    return G

@app.post("/api/reload-gtfs")
def reload_gtfs():
    STATE["gtfs_ready"]=False
    G=load_gtfs_if_needed()
    missing=[]
    if not G["stops"]: missing.append("stops.txt")
    if not G["routes"]: missing.append("routes.txt")
    if not G["trips"]: missing.append("trips.txt")
    if not G["stop_times"]: missing.append("stop_times.txt")
    if not STATE["gtfs"]["shapes"]: missing.append("shapes.txt")
    return {"ok": len(missing)==0, "missing": missing, "stops": len(G["stops"])}

# ---------- Keresések / indulások ----------
@app.get("/api/stops/search")
def stops_search(q: str = Query(..., min_length=1)):
    G=load_gtfs_if_needed()
    ql=q.strip().lower()
    res=[]
    for sid,st in G["stops"].items():
        if ql in st["name"].lower():
            res.append(st)
            if len(res)>=30: break
    return {"results":res}

@app.get("/api/routes/search")
def routes_search(q: str = Query(..., min_length=1)):
    G=load_gtfs_if_needed()
    qn=normalize_route(q)
    res=[]
    for rid,r in G["routes"].items():
        if qn and (normalize_route(r.get("route_short_name"))==qn or normalize_route(rid)==qn):
            res.append({"route_id":rid,**r})
    return {"results":res}

@app.get("/api/departures")
def departures(stop_id: str = Query(...), lookahead_min: int = 60):
    G=load_gtfs_if_needed()
    if stop_id not in G["stops"]: return {"departures":[]}
    now=now_utc(); end=now+timedelta(minutes=lookahead_min)
    out=[]
    for tid,times in G["stop_times"].items():
        for t in times:
            if t["stop_id"]!=stop_id: continue
            sec=parse_hhmmss(t.get("dep") or t.get("arr"))
            base=now.replace(hour=0,minute=0,second=0,microsecond=0)
            dep_dt=base+timedelta(seconds=sec)
            if dep_dt < now - timedelta(minutes=5): continue
            if dep_dt > end: continue
            trip=G["trips"].get(tid,{})
            route=G["routes"].get(trip.get("route_id",""),{})
            out.append({
                "trip_id":tid, "route":route.get("route_long_name",""),
                "route_short":route.get("route_short_name",""),
                "headsign":trip.get("headsign",""),
                "scheduled":dep_dt.isoformat(), "delay_min":0, "operator":"bluestar"
            })
    out.sort(key=lambda d:d["scheduled"])
    return {"departures":out}

@app.get("/api/trip")
def trip_detail(trip_id: str = Query(...)):
    G=load_gtfs_if_needed()
    trip=G["trips"].get(trip_id)
    if not trip: return {"trip_id":trip_id,"stops":[],"shape":[]}
    legs=[]
    for st in G["stop_times"].get(trip_id,[]):
        S=G["stops"].get(st["stop_id"],{})
        legs.append({"stop_id":st["stop_id"],"name":S.get("name",""),"time":st.get("dep") or st.get("arr") or ""})
    shape=[]
    if trip.get("shape_id") and trip["shape_id"] in G["shapes"]:
        for p in G["shapes"][trip["shape_id"]]:
            shape.append({"lat":p["lat"],"lon":p["lon"]})
    return {"trip_id":trip_id,"headsign":trip.get("headsign",""),"stops":legs,"shape":shape}

# ---------- Live járművek ----------
def fetch_live_raw()->List[Dict[str,Any]]:
    url=STATE["live_cfg"]["feed_url"]
    if not url: return STATE["live"]["vehicles"]
    if time.time()-STATE["live"]["fetched_at"]<5 and STATE["live"]["vehicles"]:
        return STATE["live"]["vehicles"]
    import requests
    try:
        r=requests.get(url,timeout=10); r.raise_for_status(); data=r.json()
    except Exception:
        return STATE["live"]["vehicles"]
    out=[]
    if isinstance(data,dict) and "vehicles" in data and isinstance(data["vehicles"],list):
        raw=data["vehicles"]
    elif isinstance(data,dict) and "Siri" in data:
        raw=[]
        try:
            va=data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"][0]["VehicleActivity"]
            for v in va:
                mon=v.get("MonitoredVehicleJourney",{})
                p=mon.get("VehicleLocation",{})
                raw.append({
                    "lat":p.get("Latitude"),"lon":p.get("Longitude"),
                    "route":mon.get("LineRef"),
                    "trip_id":mon.get("FramedVehicleJourneyRef",{}).get("DatedVehicleJourneyRef"),
                    "label":mon.get("VehicleRef"),
                    "timestamp":v.get("RecordedAtTime")
                })
        except Exception: raw=[]
    else:
        raw=[]
    for v in raw:
        try:
            lat=float(v.get("lat") or v.get("latitude")); lon=float(v.get("lon") or v.get("longitude"))
        except Exception: continue
        out.append({
            "lat":lat,"lon":lon,
            "label":str(v.get("label") or v.get("vehicle_id") or v.get("id") or ""),
            "route":normalize_route(v.get("route") or v.get("line") or v.get("line_ref") or ""),
            "trip_id":str(v.get("trip_id") or v.get("journey_id") or v.get("DatedVehicleJourneyRef") or ""),
            "timestamp":v.get("timestamp") or v.get("time") or ""
        })
    STATE["live"]["vehicles"]=out; STATE["live"]["fetched_at"]=time.time()
    return out

@app.get("/api/vehicles")
def vehicles(trip_id: Optional[str]=None, route: Optional[str]=None):
    V=fetch_live_raw()
    if trip_id:
        tid=str(trip_id).strip()
        V=[v for v in V if v.get("trip_id")==tid]
    elif route:
        rn=normalize_route(route)
        V=[v for v in V if normalize_route(v.get("route"))==rn]
    return {"vehicles":V}
