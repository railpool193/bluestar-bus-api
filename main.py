import io, json, csv, zipfile, time
from pathlib import Path
from typing import Dict, Any, List, Optional
from collections import defaultdict, OrderedDict

from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

import httpx, xmltodict
from zoneinfo import ZoneInfo
from datetime import datetime

app = FastAPI(title="Bluestar Bus – API", version="3.2.0")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=False), name="static")

@app.middleware("http")
async def no_cache_mw(request, call_next):
    resp = await call_next(request)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

def _read_json(path: Path, default): 
    return default if not path.exists() else json.loads(path.read_text(encoding="utf-8"))

def _write_json(path: Path, data: Any): 
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

def gtfs_ok() -> bool:
    return (DATA_DIR/"stops.json").exists() and (DATA_DIR/"schedule.json").exists()

def _find_member(zf: zipfile.ZipFile, name: str) -> Optional[str]:
    lname = name.lower()
    for m in zf.namelist():
        lm = m.lower()
        if lm == lname or lm.endswith("/"+lname):
            return m
    return None

def _hhmm_to_min(t: str) -> int:
    try:
        parts = t.split(":")
        h = int(parts[0]); m = int(parts[1]) if len(parts)>1 else 0
        return h*60 + m
    except:
        return 0

LIVE_CFG_PATH = DATA_DIR / "live_config.json"
def _get_live_cfg(): return _read_json(LIVE_CFG_PATH, {"feed_url": ""})
def _set_live_cfg(cfg): _write_json(LIVE_CFG_PATH, cfg or {"feed_url": ""})

class BODSAdapter:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

    async def is_available(self)->bool:
        return bool(_get_live_cfg().get("feed_url"))

    async def _fetch_raw(self):
        url = _get_live_cfg().get("feed_url","")
        if not url: return None
        try:
            r = await self.client.get(url, headers={"Cache-Control":"no-cache"}, params={"_":int(time.time())})
            r.raise_for_status()
            ct = r.headers.get("content-type","").lower()
            if "json" in ct or r.text.strip().startswith("{"): return r.json()
            return xmltodict.parse(r.text)
        except: return None

    @staticmethod
    def _as_list(x): return [] if x is None else (x if isinstance(x,list) else [x])

    def _parse_vehicles(self, raw)->List[Dict[str,Any]]:
        out=[]
        def pick_json(d):
            vs=[]
            try:
                vm=d.get("Siri",{}).get("ServiceDelivery",{}).get("VehicleMonitoringDelivery",[])
                if isinstance(vm,dict): vm=[vm]
                for dd in vm:
                    for va in dd.get("VehicleActivity",[]) or []:
                        mj=va.get("MonitoredVehicleJourney",{}) or {}
                        loc=mj.get("VehicleLocation",{}) or {}
                        vs.append({
                            "lat":float(loc.get("Latitude",0) or 0),
                            "lon":float(loc.get("Longitude",0) or 0),
                            "route":(mj.get("LineRef") or mj.get("PublishedLineName") or ""),
                            "bearing": mj.get("Bearing"),
                            "reg": mj.get("VehicleRef") or mj.get("VehicleRegistrationMark") or "",
                            "trip_id": mj.get("DatedVehicleJourneyRef") or mj.get("VehicleJourneyRef") or "",
                            "line_ref": mj.get("LineRef") or "",
                            "dest": mj.get("DestinationName") or "",
                        })
            except: pass
            return vs
        def pick_xml(d):
            vs=[]
            try:
                sd=d.get("Siri",{}).get("ServiceDelivery",{})
                vmd=sd.get("VehicleMonitoringDelivery",{})
                for va in self._as_list(vmd.get("VehicleActivity")):
                    mj=va.get("MonitoredVehicleJourney",{}) or {}
                    loc=mj.get("VehicleLocation",{}) or {}
                    def getk(o,k):
                        v=o.get(k); 
                        return v["#text"] if isinstance(v,dict) and "#text" in v else v
                    vs.append({
                        "lat":float(getk(loc,"Latitude") or 0),
                            "lon":float(getk(loc,"Longitude") or 0),
                            "route": getk(mj,"LineRef") or getk(mj,"PublishedLineName") or "",
                            "bearing": getk(mj,"Bearing"),
                            "reg": getk(mj,"VehicleRef") or getk(mj,"VehicleRegistrationMark") or "",
                            "trip_id": getk(mj,"DatedVehicleJourneyRef") or getk(mj,"VehicleJourneyRef") or "",
                            "line_ref": getk(mj,"LineRef") or "",
                            "dest": getk(mj,"DestinationName") or "",
                    })
            except: pass
            return vs
        if not raw: return out
        if isinstance(raw,dict) and "Siri" in raw: out=pick_json(raw)
        elif isinstance(raw,dict): out=pick_xml(raw)
        return [v for v in out if v["lat"] and v["lon"]]

    async def vehicles(self): 
        return self._parse_vehicles(await self._fetch_raw())

    async def vehicles_by_route(self, route_no:str):
        vs=await self.vehicles()
        q=str(route_no or "").strip()
        import re
        def canon(s):
            s=str(s or "").strip()
            s=re.sub(r"[^0-9a-zA-Z]","",s).lower()
            if s.isdigit(): s=str(int(s))
            return s
        cq=canon(q)
        out=[]
        for v in vs:
            cand=canon(v.get("route")) or canon(v.get("line_ref"))
            if cand==cq or (cq and cq in cand): out.append(v)
        return out

siri_live=BODSAdapter()

def _build_from_zip_bytes(zip_bytes: bytes)->None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        req=["stops.txt","trips.txt","stop_times.txt","routes.txt"]
        members={n:_find_member(zf,n) for n in req}
        missing=[n for n,m in members.items() if m is None]
        if missing: raise ValueError(f"Hiányzó GTFS fájlok: {', '.join(missing)}")

        routes={}
        with zf.open(members["routes.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                routes[row["route_id"]]={"short":(row.get("route_short_name") or "").strip(),"long":(row.get("route_long_name") or "").strip()}

        trips={}
        with zf.open(members["trips.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                r=routes.get(row["route_id"],{"short":"","long":""})
                trips[row["trip_id"]]={"route":r["short"] or r["long"],"headsign":(row.get("trip_headsign") or "").strip()}

        stops=[]
        with zf.open(members["stops.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                stops.append({"stop_id":row["stop_id"],"stop_name":(row.get("stop_name") or "").strip(),"stop_code":(row.get("stop_code") or "").strip()})
        _write_json(DATA_DIR/"stops.json",stops)

        schedule=defaultdict(list)
        trip_stops=defaultdict(list)
        with zf.open(members["stop_times.txt"]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                tid=row["trip_id"]; t=(row.get("departure_time") or "").strip()
                if not t: continue
                trip=trips.get(tid); 
                if not trip: continue
                schedule[row["stop_id"]].append({"time":t,"route":trip["route"],"destination":trip["headsign"],"trip_id":tid})
                trip_stops[tid].append({"seq":int(row.get("stop_sequence") or 0),"stop_id":row["stop_id"],"time":t})

        for v in schedule.values(): v.sort(key=lambda x:x["time"])
        for v in trip_stops.values(): v.sort(key=lambda x:x["seq"])
        _write_json(DATA_DIR/"schedule.json",schedule)
        _write_json(DATA_DIR/"trip_stops.json",trip_stops)

@app.get("/", include_in_schema=False, response_class=HTMLResponse)
async def root_html():
    return HTMLResponse((BASE_DIR/"index.html").read_text(encoding="utf-8"))

@app.get("/api/status")
async def api_status():
    return {"status":"ok","gtfs":gtfs_ok(),"live":await siri_live.is_available(),"build":str(int(time.time()))}

@app.get("/api/live/config")
async def get_live_cfg(): return _get_live_cfg()

@app.post("/api/live/config")
async def set_live_cfg(payload: Dict[str,Any]):
    url=(payload or {}).get("feed_url","").strip()
    if not url:
        _set_live_cfg({"feed_url":""}); return {"ok":True}
    if "api_key=" not in url:
        raise HTTPException(status_code=400, detail="Adj meg teljes BODS feed URL-t api_key paraméterrel.")
    _set_live_cfg({"feed_url":url}); return {"ok":True}

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Kérlek GTFS ZIP fájlt tölts fel.")
    content=await file.read()
    (DATA_DIR/"last_gtfs.zip").write_bytes(content)
    try: _build_from_zip_bytes(content)
    except Exception as e: raise HTTPException(status_code=400, detail=f"GTFS feldolgozási hiba: {e}")
    return {"status":"uploaded"}

@app.get("/api/stops/search")
async def api_stops_search(q: str = Query(..., min_length=1), limit:int=20):
    stops=_read_json(DATA_DIR/"stops.json",[])
    ql=q.strip().lower()
    res=[s for s in stops if ql in (s.get("stop_name") or "").lower() or ql==(s.get("stop_code") or "").lower()]
    res.sort(key=lambda s:(len(s.get("stop_name","")), s.get("stop_name","")))
    return res[:limit]

@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id:str, minutes:int=Query(60, ge=5, le=240), live:bool=True):
    schedule=_read_json(DATA_DIR/"schedule.json",{})
    base=schedule.get(stop_id,[])

    now_dt=datetime.now(ZoneInfo("Europe/London"))
    now_min=now_dt.hour*60+now_dt.minute

    upcoming=[]
    for d in base:
        t=(d.get("time") or "").strip()
        if not t: continue
        dep_abs=_hhmm_to_min(t)
        dep_disp=dep_abs%(24*60)
        in_min=(dep_disp-now_min)%(24*60)
        if in_min<=minutes:
            hh=dep_disp//60; mm=dep_disp%60
            time_str=f"{hh:02d}:{mm:02d}"
            upcoming.append({
                "route": d.get("route"),
                "destination": d.get("destination"),
                "time": time_str,
                "trip_id": d.get("trip_id"),
                "eta_min": int(in_min),  # UK idő szerinti ETA
                "delay_min": None,
                "vehicle_reg": None,
                "live": False
            })

    def k(it): return (str(it.get("route") or ""), str(it.get("destination") or ""), str(it.get("time") or ""))
    ded=OrderedDict()
    for it in upcoming:
        if k(it) not in ded or (not ded[k(it)].get("trip_id") and it.get("trip_id")):
            ded[k(it)]=it
    upcoming=list(ded.values())

    if live and await siri_live.is_available():
        try:
            all_live=await siri_live.vehicles()
            def norm(x):
                s=str(x or "").strip().lower()
                return str(int(s)) if s.isdigit() else s
            live_routes=defaultdict(list)
            for v in all_live:
                live_routes[norm(v.get("route") or v.get("line_ref"))].append(v)
            for it in upcoming:
                lr=live_routes.get(norm(it.get("route")))
                if lr:
                    it["live"]=True
                    it["vehicle_reg"]=lr[0].get("reg")
        except: pass

    upcoming.sort(key=lambda x:(not x["live"], x["time"]))
    return upcoming[:80]

@app.get("/api/trips/{trip_id}")
async def api_trip_details(trip_id:str):
    trip_stops=_read_json(DATA_DIR/"trip_stops.json",{})
    stops_idx={s["stop_id"]:s for s in _read_json(DATA_DIR/"stops.json",[])}
    seq=trip_stops.get(trip_id,[])
    calls=[]
    for r in seq:
        st=stops_idx.get(r["stop_id"])
        calls.append({
            "time": r.get("time"),
            "stop_id": r["stop_id"],
            "stop_name": (st or {}).get("stop_name") or r["stop_id"],
            "eta_min": None,
            "delay_min": None
        })
    return {"trip_id":trip_id, "route":None, "headsign":None, "vehicle":None, "calls":calls}

@app.get("/api/routes/search")
async def api_route_search(q:str=Query("", description="Járatszám/név"), limit:int=30):
    schedule=_read_json(DATA_DIR/"schedule.json",{})
    routes=set()
    for lst in schedule.values():
        for it in lst:
            if it.get("route"): routes.add(it["route"])
    res=sorted([r for r in routes if q.strip().lower() in str(r).lower()], key=lambda x:(len(str(x)), str(x)))
    return [{"route":r} for r in res[:limit]]

@app.get("/api/routes/{route}/vehicles")
async def api_route_vehicles(route:str):
    if not await siri_live.is_available(): return []
    try: return (await siri_live.vehicles_by_route(route))[:100]
    except: return []

@app.get("/index.html", include_in_schema=False)
async def index_file():
    return FileResponse(str(BASE_DIR/"index.html"), media_type="text/html")
