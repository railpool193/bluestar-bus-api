from __future__ import annotations
import json, time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException

ROOT = Path(__file__).resolve().parent
GTFS_DIR = ROOT / "gtfs"
DATA_DIR = ROOT / "data"

app = FastAPI(title="Bluestar Bus – API", version="2.2.0")

# -------- GTFS betöltés mappából --------
def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")

stops_df = _read_csv(GTFS_DIR / "stops.txt")
routes_df = _read_csv(GTFS_DIR / "routes.txt")
trips_df = _read_csv(GTFS_DIR / "trips.txt")
stop_times_df = _read_csv(GTFS_DIR / "stop_times.txt")
shapes_df = _read_csv(GTFS_DIR / "shapes.txt")

# route_short_name becsatolása a trip-ekhez, hogy ne kelljen mindig joinolni
if not routes_df.empty and not trips_df.empty:
    trips_df = trips_df.merge(
        routes_df[["route_id", "route_short_name", "route_long_name"]],
        on="route_id", how="left"
    )

def _hhmmss_to_sec(x: str) -> Optional[int]:
    try:
        h, m, s = [int(p) for p in x.split(":")]
        return h*3600 + m*60 + s
    except Exception:
        return None

def _sec_since_midnight(dt: datetime) -> int:
    return dt.hour*3600 + dt.minute*60 + dt.second

def _load_json(path: Path, default=None):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _live_available() -> bool:
    return (DATA_DIR / "live_available.flag").exists()

def _load_live_for_route(route_short: str) -> List[Dict]:
    # Elsőbbség: data/live_route_<ROUTE>.json, különben data/fleet.json szűrve
    specific = DATA_DIR / f"live_route_{route_short}.json"
    items = _load_json(specific, default=None)
    if items is None:
        fleet = _load_json(DATA_DIR / "fleet.json", default=[])
        items = [v for v in fleet if (v.get("route") or "").strip() == route_short]
    # Normalizálás minimális mezőkre
    out = []
    for v in (items or []):
        out.append({
            "route": (v.get("route") or "").strip(),
            "headsign": v.get("headsign") or "",
            "trip_id": v.get("trip_id") or "",
            "eta_minutes": v.get("eta_minutes"),
            "registration": v.get("registration") or "",
            "vehicle_type": v.get("vehicle_type") or "",
            "lat": v.get("lat"),
            "lon": v.get("lon")
        })
    return out

# ---------- API ----------
@app.get("/api/status")
def api_status():
    ok_gtfs = not (stops_df.empty or stop_times_df.empty or trips_df.empty)
    return {
        "status": "ok" if ok_gtfs else "error",
        "gtfs": ok_gtfs,
        "live": _live_available(),
        "build": str(int(time.time()))
    }

@app.get("/api/stops/search")
def stops_search(q: str):
    if stops_df.empty:
        return []
    ql = q.strip().lower()
    res = stops_df[stops_df["stop_name"].str.lower().str.contains(ql, na=False)]
    res = res[["stop_id", "stop_name"]].head(50)
    return res.to_dict(orient="records")

@app.get("/api/routes/search")
def routes_search(q: str):
    if routes_df.empty:
        return []
    ql = q.strip().lower()
    df = routes_df.copy()
    mask = (
        df["route_short_name"].str.lower().str.contains(ql, na=False) |
        df.get("route_long_name", pd.Series([], dtype=str)).str.lower().str.contains(ql, na=False)
    )
    res = df[mask].head(50)
    return [
        {"route": r.get("route_short_name") or r.get("route_id"), "long_name": r.get("route_long_name", "")}
        for _, r in res.iterrows()
    ]

@app.get("/api/stops/{stop_id}/next_departures")
def next_departures(stop_id: str, minutes: int = 60):
    """
    Következő indulások a megadott megállóból.
    - Alap: GTFS stop_times (fehér szín a frontenden)
    - Ha live aktív és van az adott route-hoz live ETA, akkor az adott sor 'source'='live' (zöld) és 'eta_minutes' kitöltve.
    - Duplázódás ellen: (trip_id, time) kulccsal deduplikál.
    """
    if stop_times_df.empty or trips_df.empty:
        return []

    now = datetime.now()
    now_s = _sec_since_midnight(now)
    horizon = now_s + minutes*60

    st = stop_times_df[stop_times_df["stop_id"] == stop_id]
    if st.empty:
        return []

    st = st.merge(trips_df[["trip_id","route_id","route_short_name","trip_headsign"]], on="trip_id", how="left")

    rows: List[Dict] = []
    for _, r in st.iterrows():
        dep = _hhmmss_to_sec(r.get("departure_time","") or r.get("arrival_time",""))
        if dep is None:
            continue
        if now_s <= dep <= horizon:
            rows.append({
                "trip_id": r["trip_id"],
                "route": r.get("route_short_name") or r.get("route_id"),
                "headsign": r.get("trip_headsign","") or "",
                "time": r.get("departure_time") or r.get("arrival_time") or "",
                "source": "gtfs",
                "eta_minutes": None,
                "registration": ""
            })

    # élő adatok ráolvasztása route szerint
    if _live_available():
        # route -> live list
        live_cache: Dict[str, List[Dict]] = {}
        for r in set(x["route"] for x in rows):
            live_cache[r] = _load_live_for_route(r)

        for item in rows:
            live_list = live_cache.get(item["route"], [])
            # próbáljunk trip_id szerint egyezni, különben headsign (laza egyezés)
            live_match = None
            for v in live_list:
                if item["trip_id"] and v.get("trip_id") == item["trip_id"]:
                    live_match = v; break
            if not live_match:
                for v in live_list:
                    if (v.get("headsign","").lower() == item["headsign"].lower()) and v.get("eta_minutes") is not None:
                        live_match = v; break
            if live_match and live_match.get("eta_minutes") is not None:
                item["source"] = "live"
                item["eta_minutes"] = int(live_match["eta_minutes"])
                item["registration"] = live_match.get("registration","")

    # rendezés: (live előre?) – maradjunk idő szerint
    rows.sort(key=lambda x: x["time"])

    # deduplikáció (trip_id, time)
    seen = set()
    out = []
    for r in rows:
        key = (r["trip_id"], r["time"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

@app.get("/api/routes/{route}/vehicles")
def route_vehicles(route: str):
    return _load_live_for_route(route)

@app.get("/api/routes/{route}/shape")
def route_shape(route: str):
    if shapes_df.empty or trips_df.empty:
        return []
    tr = trips_df[(trips_df["route_short_name"] == route) | (trips_df["route_id"] == route)]
    if tr.empty:
        return []
    shape_id = tr.iloc[0].get("shape_id","")
    if not shape_id:
        return []
    sh = shapes_df[shapes_df["shape_id"] == shape_id].copy()
    if sh.empty:
        return []
    if "shape_pt_sequence" in sh.columns:
        try:
            sh["shape_pt_sequence"] = sh["shape_pt_sequence"].astype(int)
            sh = sh.sort_values("shape_pt_sequence")
        except Exception:
            pass
    return [{"lat": float(r["shape_pt_lat"]), "lon": float(r["shape_pt_lon"])}
            for _, r in sh.iterrows()
            if r.get("shape_pt_lat") and r.get("shape_pt_lon")]

@app.get("/api/trips/{trip_id}")
def trip_details(trip_id: str):
    st = stop_times_df[stop_times_df["trip_id"] == trip_id].copy()
    if st.empty:
        return {"trip_id": trip_id, "stops": []}
    if "stop_sequence" in st.columns:
        try:
            st["stop_sequence"] = st["stop_sequence"].astype(int)
            st = st.sort_values("stop_sequence")
        except Exception:
            pass
    st = st.merge(stops_df[["stop_id","stop_name"]], on="stop_id", how="left")
    out = []
    for _, r in st.iterrows():
        out.append({
            "time": r.get("departure_time") or r.get("arrival_time") or "",
            "stop_id": r.get("stop_id",""),
            "stop_name": r.get("stop_name","")
        })
    return {"trip_id": trip_id, "stops": out}
