# main.py
from __future__ import annotations

import csv
import os
from functools import lru_cache
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from datetime import datetime, date, timedelta, timezone
try:
    from zoneinfo import ZoneInfo  # py3.9+
    UK = ZoneInfo("Europe/London")
except Exception:
    UK = timezone.utc  # fallback, de jobb a ZoneInfo

# =========================
#  Beállítások / Konstansok
# =========================

DATA_DIR = os.environ.get("DATA_DIR", "gtfs")
TEMPLATES_DIR = os.environ.get("TEMPLATES_DIR", "templates")
STATIC_DIR = os.environ.get("STATIC_DIR", "static")

# Csak Bluestar + Unilink
OP_WHITELIST = {
    "bluestar", "bluestarbus", "go south coast (bluestar)",
    "unilink", "uni-link", "southampton unilink"
}

# =========================
#  Live (SIRI) wrapper(ek)
# =========================
# Rugalmas import: többféle névvel is próbálkozunk, hogy
# a meglévő siri_live.py-hez illeszkedjen. Ha nincs, üres lista.

def _resolve_siri():
    try:
        import siri_live as m
    except Exception:
        return None, None
    # route
    route_fn = None
    for name in ("fetch_live_on_route", "get_live_on_route", "live_on_route"):
        if hasattr(m, name):
            route_fn = getattr(m, name)
            break
    # trip
    trip_fn = None
    for name in ("fetch_live_for_trip", "get_live_for_trip", "live_for_trip"):
        if hasattr(m, name):
            trip_fn = getattr(m, name)
            break
    return route_fn, trip_fn

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
    cand = []
    if op_name: cand.append(op_name.lower())
    if op_ref: cand.append(op_ref.lower())
    for c in cand:
        for w in OP_WHITELIST:
            if w in c:
                return True
    return False

# =========================
#  GTFS betöltés (CSV)
# =========================

@lru_cache(maxsize=1)
def _load_csv(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({k: (v or "").strip() for k, v in r.items()})
    return rows

@lru_cache(maxsize=1)
def agencies() -> Dict[str, Dict[str, str]]:
    p = os.path.join(DATA_DIR, "agency.txt")
    if not os.path.exists(p):
        return {}
    out = {}
    for r in _load_csv(p):
        out[r.get("agency_id","")] = r
    return out

@lru_cache(maxsize=1)
def routes() -> List[Dict[str, str]]:
    p = os.path.join(DATA_DIR, "routes.txt")
    if not os.path.exists(p):
        return []
    ag = agencies()
    lst: List[Dict[str, str]] = []
    for r in _load_csv(p):
        a = ag.get(r.get("agency_id",""), {})
        r = dict(r)
        r["agency_name"] = a.get("agency_name","")
        lst.append(r)
    return lst

@lru_cache(maxsize=1)
def routes_by_id() -> Dict[str, Dict[str,str]]:
    return {r.get("route_id",""): r for r in routes()}

@lru_cache(maxsize=1)
def trips() -> Dict[str, Dict[str, str]]:
    p = os.path.join(DATA_DIR, "trips.txt")
    if not os.path.exists(p):
        return {}
    out = {}
    for r in _load_csv(p):
        out[r.get("trip_id","")] = r
    return out

@lru_cache(maxsize=1)
def stops() -> Dict[str, Dict[str, str]]:
    p = os.path.join(DATA_DIR, "stops.txt")
    if not os.path.exists(p):
        return {}
    out = {}
    for r in _load_csv(p):
        out[r.get("stop_id","")] = r
    return out

@lru_cache(maxsize=1)
def stop_times_all() -> List[Dict[str, str]]:
    p = os.path.join(DATA_DIR, "stop_times.txt")
    if not os.path.exists(p):
        return []
    rows = _load_csv(p)
    # normalizált sequence
    for r in rows:
        try:
            r["stop_sequence"] = int(r.get("stop_sequence","0") or "0")
        except Exception:
            r["stop_sequence"] = 0
    return rows

@lru_cache(maxsize=1)
def stop_times_by_trip() -> Dict[str, List[Dict[str, str]]]:
    by: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for r in stop_times_all():
        by[r.get("trip_id","")].append(r)
    for k in by:
        by[k].sort(key=lambda x: x["stop_sequence"])
    return by

@lru_cache(maxsize=1)
def trip_minmax_seq() -> Dict[str, Tuple[int,int]]:
    d: Dict[str, Tuple[int,int]] = {}
    stbt = stop_times_by_trip()
    for tid, arr in stbt.items():
        if not arr:
            continue
        mn = min(x["stop_sequence"] for x in arr)
        mx = max(x["stop_sequence"] for x in arr)
        d[tid] = (mn, mx)
    return d

@lru_cache(maxsize=1)
def stop_index() -> Dict[str, List[Dict[str, str]]]:
    idx: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    stbt = stop_times_by_trip()
    tr = trips()
    rt = routes_by_id()
    st = stops()
    for tid, arr in stbt.items():
        info_t = tr.get(tid, {})
        route_id = info_t.get("route_id","")
        headsign = info_t.get("trip_headsign","") or info_t.get("direction_id","")
        for rec in arr:
            sid = rec.get("stop_id","")
            row = {
                "trip_id": tid,
                "route_id": route_id,
                "headsign": headsign,
                "stop_id": sid,
                "stop_name": st.get(sid, {}).get("stop_name", sid),
                "arrival_time": rec.get("arrival_time",""),
                "departure_time": rec.get("departure_time",""),
                "stop_sequence": rec["stop_sequence"],
                "route_short_name": rt.get(route_id,{}).get("route_short_name",""),
            }
            idx[sid].append(row)
    # időrend: departure/arrival alapján
    for sid in idx:
        idx[sid].sort(key=lambda x: (x["route_short_name"], x["stop_sequence"]))
    return idx

# =========================
#  Dátum/idő segédek
# =========================

def now_uk_str() -> str:
    return datetime.now(tz=UK).strftime("%H:%M:%S")

def _parse_gtfs_hhmm(hhmmss: str) -> Optional[Tuple[int,int,int,int]]:
    """
    GTFS-ben az óra lehet 24+ is (pl. 25:10:00).
    Visszaad: (nap_offset, hour, minute, second)
    """
    if not hhmmss:
        return None
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
    if not p:
        return None
    day_off, h, m, s = p
    base = datetime.now(tz=UK).date()
    dt = datetime(base.year, base.month, base.day, h, m, s, tzinfo=UK) + timedelta(days=day_off)
    return int(dt.timestamp() * 1000)

def hhmm(hhmmss: str) -> str:
    p = _parse_gtfs_hhmm(hhmmss)
    if not p:
        return ""
    _, h, m, _ = p
    return f"{h:02d}:{m:02d}"

# =========================
#  Lekérdező segédek
# =========================

def load_routes_filtered() -> List[Dict[str, str]]:
    """Bluestar + Unilink ügynökségekhez tartozó route-ok."""
    lst = []
    for r in routes():
        ag = (r.get("agency_name","") or r.get("agency_id","")).lower()
        if "blue" in ag or "uni" in ag:
            lst.append(r)
    # ha agency_name nem beszédes, route_short_name alapján is beengedjük az U* járatokat
    lst = list({r.get("route_id"): r for r in lst}.values())
    lst.sort(key=lambda x: x.get("route_short_name",""))
    return lst

def trip_stops_with_times(trip_id: str) -> List[Dict[str, Any]]:
    stbt = stop_times_by_trip().get(trip_id, [])
    st = stops()
    out: List[Dict[str, Any]] = []
    for rec in stbt:
        sid = rec.get("stop_id","")
        sched = rec.get("departure_time") or rec.get("arrival_time") or ""
        out.append({
            "stop_id": sid,
            "stop_name": st.get(sid, {}).get("stop_name", sid),
            "sched_time": hhmm(sched),
            "sched_epoch": to_epoch_ms_today(sched),
        })
    return out

def is_terminal_stop_general(stop_id: str) -> bool:
    """Igaz, ha a stop MINDEN ide eső tripben az eleje vagy a vége (nincs középső)."""
    idx = stop_index().get(stop_id, [])
    if not idx:
        return False
    tmm = trip_minmax_seq()
    has_middle = False
    only_terminal = False
    for r in idx:
        tid = r["trip_id"]
        if tid not in tmm:
            continue
        mn, mx = tmm[tid]
        seq = r["stop_sequence"]
        if seq == mn or seq == mx:
            only_terminal = True
        else:
            has_middle = True
            break
    return only_terminal and not has_middle

def departures_arrivals_for_stop(stop_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    idx = stop_index().get(stop_id, [])
    tmm = trip_minmax_seq()
    deps: List[Dict[str, Any]] = []
    arrs: List[Dict[str, Any]] = []
    for r in idx:
        tid = r["trip_id"]
        mn, mx = tmm.get(tid, (None, None))
        seq = r["stop_sequence"]
        # menetrendi idő + ETA epoch (ha később live-ból pótoljuk)
        sched = r["departure_time"] or r["arrival_time"] or ""
        row = {
            "stop_id": stop_id,
            "stop_name": r["stop_name"],
            "route": r["route_short_name"],
            "headsign": r["headsign"],
            "origin": r["headsign"],  # konzisztensebb templathoz
            "sched_time": hhmm(sched),
            "eta_epoch": to_epoch_ms_today(sched),
            "live_time": None,  # ha lesz élő, itt lehet kitölteni
        }
        if mn is not None and seq == mn:
            deps.append(row)
        elif mx is not None and seq == mx:
            arrs.append(row)
        else:
            # középső megálló – alapból indulás listára tesszük
            deps.append(row)
    # laza rendezés idő szerint (ha van epoch)
    deps.sort(key=lambda x: (x["eta_epoch"] or 0))
    arrs.sort(key=lambda x: (x["eta_epoch"] or 0))
    return deps, arrs

# =========================
#  FastAPI app + sablonok
# =========================

app = FastAPI(title="bluestar")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# --------- INDEX ----------
@app.get("/")
def index(request: Request):
    rts = load_routes_filtered()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "routes": rts,
            "now_uk": now_uk_str(),
        },
    )

# --------- ROUTE ----------
@app.get("/route/{route_id}")
def route_view(request: Request, route_id: str):
    # Élő járművek a megadott járaton – CSAK Bluestar/Unilink
    live_raw = fetch_live_on_route(route_id)
    vehicles = []
    for v in live_raw:
        opn = (v.get("operator_name") or v.get("OperatorName") or v.get("ProducerRef") or "")
        opr = (v.get("operator_ref") or v.get("OperatorRef") or "")
        if not is_allowed_operator(opn, opr):
            continue
        vehicles.append({
            "vehicle_ref": v.get("vehicle_ref") or v.get("VehicleRef") or v.get("VehicleId") or v.get("VehicleRefId"),
            "fleet": v.get("fleet") or v.get("FleetNumber"),
            "destination": v.get("destination") or v.get("DestinationName") or v.get("PublishedLineName"),
            "trip_headsign": v.get("trip_headsign") or v.get("DirectionName"),
            "updated": v.get("recorded_at_time") or v.get("RecordedAtTime"),
            "lat": v.get("lat") or v.get("Latitude"),
            "lon": v.get("lon") or v.get("Longitude"),
        })
    # Térkép markerek
    pts = []
    for v in vehicles:
        try:
            lat = float(v.get("lat"))
            lon = float(v.get("lon"))
            pts.append({"lat": lat, "lon": lon, "label": f'{v.get("vehicle_ref") or v.get("fleet") or ""} · {v.get("destination") or ""}'})
        except Exception:
            pass

    return templates.TemplateResponse(
        "route.html",
        {
            "request": request,
            "route_id": route_id,
            "vehicles": vehicles,
            "map_points": pts,
            "now_uk": now_uk_str(),
        },
    )

# --------- TRIP ----------
@app.get("/trip/{trip_id}")
def trip_view(request: Request, trip_id: str):
    t_all = trips()
    if trip_id not in t_all:
        raise HTTPException(status_code=404, detail="Ismeretlen trip")
    t = t_all[trip_id]
    route_id = t.get("route_id","")
    headsign = t.get("trip_headsign","") or t.get("direction_id","")

    # Megállók menetrendi idővel
    stops_list = trip_stops_with_times(trip_id)

    # Élő adatok erre a tripre – CSAK a bejelentkezett jármű és csak Bluestar/Unilink
    live = [v for v in fetch_live_for_trip(trip_id) if is_allowed_operator(v.get("operator_name"), v.get("operator_ref"))]
    vehicle = None
    if live:
        # csak az első (bejelentkezett) jármű
        v = live[0]
        etas_by_stop = v.get("etas_by_stop") or v.get("EtAsByStop") or {}
        # normalizáljuk az ETA-kat epoch-ms-re, ha string jött
        norm_etas = {}
        for sid, val in etas_by_stop.items():
            try:
                if isinstance(val, (int, float)):
                    norm_etas[sid] = int(val)
                else:
                    # ha HH:MM(:SS) jött
                    norm_etas[sid] = to_epoch_ms_today(str(val))
            except Exception:
                pass
        vehicle = {
            "vehicle_ref": v.get("vehicle_ref") or v.get("VehicleRef"),
            "fleet": v.get("fleet") or v.get("FleetNumber"),
            "label": v.get("destination") or v.get("DestinationName") or "Bejelentkezett jármű",
            "updated": v.get("recorded_at_time") or v.get("RecordedAtTime"),
            "etas_by_stop": norm_etas,
        }
        # élő idők + visszaszámláló beégetése a stop-sorokba
        for s in stops_list:
            sid = s["stop_id"]
            if sid in norm_etas:
                s["eta_epoch"] = norm_etas[sid]
                # élő idő HH:MM
                try:
                    dt = datetime.fromtimestamp(norm_etas[sid]/1000, tz=UK)
                    s["live_time"] = dt.strftime("%H:%M")
                except Exception:
                    s["live_time"] = None
            else:
                s["live_time"] = None
    else:
        # nincs élő – csak menetrend alapján visszaszámláló
        for s in stops_list:
            s["live_time"] = None

    return templates.TemplateResponse(
        "trip.html",
        {
            "request": request,
            "trip_id": trip_id,
            "route_id": route_id,
            "headsign": headsign,
            "stops": stops_list,
            "vehicle": vehicle,
            "now_uk": now_uk_str(),
        },
    )

# --------- STOP ----------
@app.get("/stop/{stop_id}")
def stop_view(request: Request, stop_id: str):
    dep, arr = departures_arrivals_for_stop(stop_id)
    is_term = is_terminal_stop_general(stop_id)
    if is_term:
        # végállomás: csak indulások
        arr = []

    # Ha a megálló neve kell
    st = stops().get(stop_id, {})
    stop_name = st.get("stop_name", stop_id)

    return templates.TemplateResponse(
        "stop.html",
        {
            "request": request,
            "stop_name": stop_name,
            "departures": dep,
            "arrivals": arr,
            "is_terminal": is_term,
            "now_uk": now_uk_str(),
        },
    )

# ------------- Dev futtatás (helyben) -------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), reload=True)
