# main.py
from __future__ import annotations

import csv
import os
from collections import defaultdict
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import TemplateNotFound

from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    UK = ZoneInfo("Europe/London")
except Exception:
    UK = timezone.utc

# --- Beállítások
DATA_DIR = os.environ.get("DATA_DIR", "gtfs")
TEMPLATES_DIR = os.environ.get("TEMPLATES_DIR", "templates")
STATIC_DIR = os.environ.get("STATIC_DIR", "static")

# Csak Bluestar / Unilink élő járművek
OP_WHITELIST = {
    "bluestar", "bluestarbus", "go south coast (bluestar)",
    "unilink", "uni-link", "southampton unilink"
}

# --- Live (SIRI) – opcionális wrap
def _resolve_siri():
    try:
        import siri_live as m
    except Exception:
        return None, None
    rf = None
    for n in ("fetch_live_on_route", "get_live_on_route", "live_on_route"):
        if hasattr(m, n):
            rf = getattr(m, n)
            break
    tf = None
    for n in ("fetch_live_for_trip", "get_live_for_trip", "live_for_trip"):
        if hasattr(m, n):
            tf = getattr(m, n)
            break
    return rf, tf

_SIRI_ROUTE_FN, _SIRI_TRIP_FN = _resolve_siri()

def fetch_live_on_route(route_id: str) -> List[Dict[str, Any]]:
    if _SIRI_ROUTE_FN:
        try:
            return _SIRI_ROUTE_FN(route_id) or []
        except Exception:
            return []
    return []

def fetch_live_for_trip(trip_id: str) -> List[Dict[str, Any]]:
    if _SIRI_TRIP_FN:
        try:
            return _SIRI_TRIP_FN(trip_id) or []
        except Exception:
            return []
    return []

def is_allowed_operator(op_name: Optional[str], op_ref: Optional[str] = None) -> bool:
    cands = []
    if op_name: cands.append(op_name.lower())
    if op_ref:  cands.append(op_ref.lower())
    for c in cands:
        for w in OP_WHITELIST:
            if w in c:
                return True
    return False

# --- CSV/GTFS betöltés
@lru_cache(maxsize=1)
def _load_csv(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        import csv
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k: (v or "").strip() for k, v in r.items()})
    return rows

@lru_cache(maxsize=1)
def agencies() -> Dict[str, Dict[str, str]]:
    p = os.path.join(DATA_DIR, "agency.txt")
    if not os.path.exists(p): return {}
    return {r.get("agency_id",""): r for r in _load_csv(p)}

@lru_cache(maxsize=1)
def routes() -> List[Dict[str, str]]:
    p = os.path.join(DATA_DIR, "routes.txt")
    if not os.path.exists(p): return []
    ag = agencies()
    out = []
    for r in _load_csv(p):
        rr = dict(r)
        rr["agency_name"] = ag.get(r.get("agency_id",""), {}).get("agency_name","")
        out.append(rr)
    return out

@lru_cache(maxsize=1)
def routes_by_id() -> Dict[str, Dict[str, str]]:
    return {r.get("route_id",""): r for r in routes()}

@lru_cache(maxsize=1)
def trips() -> Dict[str, Dict[str,str]]:
    p = os.path.join(DATA_DIR, "trips.txt")
    if not os.path.exists(p): return {}
    return {r.get("trip_id",""): r for r in _load_csv(p)}

@lru_cache(maxsize=1)
def stops() -> Dict[str, Dict[str,str]]:
    p = os.path.join(DATA_DIR, "stops.txt")
    if not os.path.exists(p): return {}
    return {r.get("stop_id",""): r for r in _load_csv(p)}

@lru_cache(maxsize=1)
def stop_times_all() -> List[Dict[str,str]]:
    p = os.path.join(DATA_DIR, "stop_times.txt")
    if not os.path.exists(p): return []
    rows = _load_csv(p)
    for r in rows:
        try:
            r["stop_sequence"] = int(r.get("stop_sequence","0") or "0")
        except Exception:
            r["stop_sequence"] = 0
    return rows

@lru_cache(maxsize=1)
def stop_times_by_trip() -> Dict[str, List[Dict[str,str]]]:
    d: Dict[str, List[Dict[str,str]]] = defaultdict(list)
    for r in stop_times_all():
        d[r.get("trip_id","")].append(r)
    for k in d:
        d[k].sort(key=lambda x: x["stop_sequence"])
    return d

@lru_cache(maxsize=1)
def trip_minmax_seq() -> Dict[str, Tuple[int,int]]:
    d: Dict[str, Tuple[int,int]] = {}
    for tid, arr in stop_times_by_trip().items():
        if arr:
            mn = min(x["stop_sequence"] for x in arr)
            mx = max(x["stop_sequence"] for x in arr)
            d[tid] = (mn, mx)
    return d

@lru_cache(maxsize=1)
def stop_index() -> Dict[str, List[Dict[str,str]]]:
    idx: Dict[str, List[Dict[str,str]]] = defaultdict(list)
    stbt = stop_times_by_trip()
    tr = trips(); rt = routes_by_id(); st = stops()
    for tid, arr in stbt.items():
        tinfo = tr.get(tid, {})
        route_id = tinfo.get("route_id","")
        headsign = tinfo.get("trip_headsign","") or tinfo.get("direction_id","")
        for r in arr:
            sid = r.get("stop_id","")
            idx[sid].append({
                "trip_id": tid,
                "route_id": route_id,
                "headsign": headsign,
                "stop_id": sid,
                "stop_name": st.get(sid,{}).get("stop_name", sid),
                "arrival_time": r.get("arrival_time",""),
                "departure_time": r.get("departure_time",""),
                "stop_sequence": r["stop_sequence"],
                "route_short_name": rt.get(route_id,{}).get("route_short_name",""),
            })
    for sid in idx:
        idx[sid].sort(key=lambda x: (x["route_short_name"], x["stop_sequence"]))
    return idx

# --- Idő segédek
def now_uk_str() -> str:
    return datetime.now(tz=UK).strftime("%H:%M:%S")

def _parse_gtfs_hhmm(hhmmss: str) -> Optional[Tuple[int,int,int,int]]:
    if not hhmmss: return None
    parts = (hhmmss.split(":") + ["0","0"])[:3]
    try:
        h = int(parts[0]); m = int(parts[1]); s = int(parts[2])
    except Exception:
        return None
    day_off = h // 24
    h = h % 24
    return (day_off, h, m, s)

def to_epoch_ms_today(hhmmss: str) -> Optional[int]:
    p = _parse_gtfs_hhmm(hhmmss)
    if not p: return None
    day_off, h, m, s = p
    base = datetime.now(tz=UK).date()
    dt = datetime(base.year, base.month, base.day, h, m, s, tzinfo=UK) + timedelta(days=day_off)
    return int(dt.timestamp() * 1000)

def hhmm(hhmmss: str) -> str:
    p = _parse_gtfs_hhmm(hhmmss)
    if not p: return ""
    _, h, m, _ = p
    return f"{h:02d}:{m:02d}"

# --- Lekérdezések
def load_routes_filtered() -> List[Dict[str,str]]:
    lst = []
    for r in routes():
        ag = (r.get("agency_name","") or r.get("agency_id","")).lower()
        if "blue" in ag or "uni" in ag:
            lst.append(r)
    # duplikációk kiszűrése
    uniq = {r.get("route_id",""): r for r in lst}
    out = list(uniq.values())
    out.sort(key=lambda x: (x.get("route_short_name",""), x.get("route_id","")))
    return out

def trip_stops_with_times(trip_id: str) -> List[Dict[str, Any]]:
    stbt = stop_times_by_trip().get(trip_id, [])
    st = stops()
    out: List[Dict[str, Any]] = []
    for r in stbt:
        sid = r.get("stop_id","")
        sched = r.get("departure_time") or r.get("arrival_time") or ""
        out.append({
            "stop_id": sid,
            "stop_name": st.get(sid,{}).get("stop_name", sid),
            "sched_time": hhmm(sched),
            "sched_epoch": to_epoch_ms_today(sched),
            "eta_epoch": to_epoch_ms_today(sched),  # default: menetrend
        })
    return out

def is_terminal_stop_general(stop_id: str) -> bool:
    idx = stop_index().get(stop_id, [])
    if not idx: return False
    tmm = trip_minmax_seq()
    has_middle = False
    only_terminal = False
    for r in idx:
        tid = r["trip_id"]
        if tid not in tmm: continue
        mn, mx = tmm[tid]
        seq = r["stop_sequence"]
        if seq == mn or seq == mx:
            only_terminal = True
        else:
            has_middle = True
            break
    return only_terminal and not has_middle

def departures_arrivals_for_stop(stop_id: str):
    idx = stop_index().get(stop_id, [])
    tmm = trip_minmax_seq()
    deps: List[Dict[str, Any]] = []
    arrs: List[Dict[str, Any]] = []
    for r in idx:
        tid = r["trip_id"]
        mn, mx = tmm.get(tid, (None, None))
        seq = r["stop_sequence"]
        sched = r["departure_time"] or r["arrival_time"] or ""
        row = {
            "stop_id": stop_id,
            "stop_name": r["stop_name"],
            "route": r["route_short_name"],
            "headsign": r["headsign"],
            "sched_time": hhmm(sched),
            "eta_epoch": to_epoch_ms_today(sched),
            "live_time": None,
        }
        if mn is not None and seq == mn:
            deps.append(row)
        elif mx is not None and seq == mx:
            arrs.append(row)
        else:
            deps.append(row)
    deps.sort(key=lambda x: (x["eta_epoch"] or 0))
    arrs.sort(key=lambda x: (x["eta_epoch"] or 0))
    return deps, arrs

# --- FastAPI
app = FastAPI(title="bluestar")

# Ne omoljon össze, ha nincs mappa: check_dir=False
app.mount("/static", StaticFiles(directory=STATIC_DIR, check_dir=False), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

@app.get("/healthz")
def healthz():
    return PlainTextResponse("ok")

@app.get("/")
def index(request: Request):
    rts = load_routes_filtered()
    try:
        return templates.TemplateResponse("index.html", {"request": request, "routes": rts, "now_uk": now_uk_str()})
    except TemplateNotFound:
        # minimal fallback, ha nincs sablon
        items = "".join(f'<li><a href="/route/{r.get("route_id")}">{r.get("route_short_name") or r.get("route_id")}</a></li>' for r in rts)
        html = f"""<html><body><h1>Járatok</h1><ul>{items}</ul></body></html>"""
        return HTMLResponse(html)

@app.get("/route/{route_id}")
def route_view(request: Request, route_id: str):
    live_raw = fetch_live_on_route(route_id)
    vehicles = []
    for v in live_raw:
        opn = (v.get("operator_name") or v.get("OperatorName") or v.get("ProducerRef") or "")
        opr = (v.get("operator_ref") or v.get("OperatorRef") or "")
        if not is_allowed_operator(opn, opr):
            continue
        vehicles.append({
            "vehicle_ref": v.get("vehicle_ref") or v.get("VehicleRef") or v.get("VehicleId"),
            "fleet": v.get("fleet") or v.get("FleetNumber"),
            "destination": v.get("destination") or v.get("DestinationName") or v.get("PublishedLineName"),
            "updated": v.get("recorded_at_time") or v.get("RecordedAtTime"),
            "lat": v.get("lat") or v.get("Latitude"),
            "lon": v.get("lon") or v.get("Longitude"),
        })
    try:
        return templates.TemplateResponse("route.html", {
            "request": request, "route_id": route_id, "vehicles": vehicles, "now_uk": now_uk_str()
        })
    except TemplateNotFound:
        html = f"<h1>Route {route_id}</h1><p>Élő járművek: {len(vehicles)}</p>"
        return HTMLResponse(html)

@app.get("/trip/{trip_id}")
def trip_view(request: Request, trip_id: str):
    t_all = trips()
    if trip_id not in t_all:
        raise HTTPException(status_code=404, detail="Ismeretlen trip")
    t = t_all[trip_id]
    route_id = t.get("route_id","")
    headsign = t.get("trip_headsign","") or t.get("direction_id","")
    stops_list = trip_stops_with_times(trip_id)

    live = [v for v in fetch_live_for_trip(trip_id) if is_allowed_operator(v.get("operator_name"), v.get("operator_ref"))]
    vehicle = None
    if live:
        v = live[0]
        etas = v.get("etas_by_stop") or v.get("EtAsByStop") or {}
        norm = {}
        for sid, val in etas.items():
            try:
                norm[sid] = int(val) if isinstance(val, (int,float)) else to_epoch_ms_today(str(val))
            except Exception:
                pass
        vehicle = {
            "vehicle_ref": v.get("vehicle_ref") or v.get("VehicleRef"),
            "fleet": v.get("fleet") or v.get("FleetNumber"),
            "label": v.get("destination") or v.get("DestinationName") or "Bejelentkezett jármű",
            "updated": v.get("recorded_at_time") or v.get("RecordedAtTime"),
            "etas_by_stop": norm,
        }
        for s in stops_list:
            sid = s["stop_id"]
            if sid in norm:
                s["eta_epoch"] = norm[sid]
                try:
                    from datetime import datetime as _dt
                    s["live_time"] = _dt.fromtimestamp(norm[sid]/1000, tz=UK).strftime("%H:%M")
                except Exception:
                    s["live_time"] = None

    try:
        return templates.TemplateResponse("trip.html", {
            "request": request,
            "trip_id": trip_id,
            "route_id": route_id,
            "headsign": headsign,
            "stops": stops_list,
            "vehicle": vehicle,
            "now_uk": now_uk_str(),
        })
    except TemplateNotFound:
        rows = "".join(f"<li>{s['stop_name']} — {s['sched_time']}</li>" for s in stops_list)
        return HTMLResponse(f"<h1>Trip {trip_id}</h1><ul>{rows}</ul>")

@app.get("/stop/{stop_id}")
def stop_view(request: Request, stop_id: str):
    dep, arr = departures_arrivals_for_stop(stop_id)
    if is_terminal_stop_general(stop_id):
        arr = []
    stop_name = stops().get(stop_id, {}).get("stop_name", stop_id)
    try:
        return templates.TemplateResponse("stop.html", {
            "request": request,
            "stop_name": stop_name,
            "departures": dep,
            "arrivals": arr,
            "now_uk": now_uk_str(),
        })
    except TemplateNotFound:
        drows = "".join(f"<li>{d['route']} → {d['headsign']} {d['sched_time']}</li>" for d in dep)
        arows = "".join(f"<li>{a['route']} ← {a['headsign']} {a['sched_time']}</li>" for a in arr)
        return HTMLResponse(f"<h1>{stop_name}</h1><h3>Indulások</h3><ul>{drows}</ul><h3>Érkezések</h3><ul>{arows}</ul>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT","8000")), reload=True)
