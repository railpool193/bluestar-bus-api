from __future__ import annotations
import os, zipfile, io
import pandas as pd
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, date, timedelta
from functools import lru_cache

GTFS_PATH = os.getenv("GTFS_PATH", "./gtfs/bluestar.zip")

def _read_gtfs_file(z: zipfile.ZipFile, name: str) -> pd.DataFrame:
    with z.open(name) as f:
        return pd.read_csv(f, dtype=str).fillna("")

@lru_cache(maxsize=1)
def load_gtfs() -> Dict[str, pd.DataFrame]:
    if os.path.isdir(GTFS_PATH):
        # kibontott .txt fájlok
        read = lambda n: pd.read_csv(os.path.join(GTFS_PATH, n), dtype=str).fillna("")
        stops = read("stops.txt")
        routes = read("routes.txt")
        trips = read("trips.txt")
        stop_times = read("stop_times.txt")
        calendars = read("calendar.txt") if os.path.exists(os.path.join(GTFS_PATH,"calendar.txt")) else pd.DataFrame()
        shapes = read("shapes.txt") if os.path.exists(os.path.join(GTFS_PATH,"shapes.txt")) else pd.DataFrame()
    else:
        with zipfile.ZipFile(GTFS_PATH, "r") as z:
            stops = _read_gtfs_file(z, "stops.txt")
            routes = _read_gtfs_file(z, "routes.txt")
            trips = _read_gtfs_file(z, "trips.txt")
            stop_times = _read_gtfs_file(z, "stop_times.txt")
            calendars = _read_gtfs_file(z, "calendar.txt") if "calendar.txt" in z.namelist() else pd.DataFrame()
            shapes = _read_gtfs_file(z, "shapes.txt") if "shapes.txt" in z.namelist() else pd.DataFrame()

    # indexek
    stop_times["stop_sequence"] = stop_times["stop_sequence"].astype(int)
    stop_times.sort_values(["trip_id","stop_sequence"], inplace=True)
    return {
        "stops": stops, "routes": routes, "trips": trips,
        "stop_times": stop_times, "calendar": calendars, "shapes": shapes
    }

def hhmmss_to_seconds(s: str) -> int:
    # GTFS akár 24:xx:xx felett is lehet
    try:
        h,m,x = s.split(":")
        return int(h)*3600 + int(m)*60 + int(x)
    except Exception:
        return -1

def service_name(row: pd.Series, routes: pd.DataFrame) -> Tuple[str,str]:
    r = routes[routes["route_id"]==row["route_id"]].head(1)
    if r.empty:
        return "", ""
    short, long = r["route_short_name"].values[0], r["route_long_name"].values[0]
    return short, long

def build_indexes():
    gtfs = load_gtfs()
    trips = gtfs["trips"]
    stops = gtfs["stops"]
    routes = gtfs["routes"]
    stop_times = gtfs["stop_times"]

    trips_idx = {t["trip_id"]: t for _,t in trips.iterrows()}
    stops_idx = {s["stop_id"]: s for _,s in stops.iterrows()}
    routes_idx = {r["route_id"]: r for _,r in routes.iterrows()}
    by_route = {}
    for rid, grp in trips.groupby("route_id"):
        by_route[rid] = grp["trip_id"].tolist()
    by_trip_stop_times = {tid: df for tid, df in stop_times.groupby("trip_id")}
    return {
        "trips_idx": trips_idx,
        "stops_idx": stops_idx,
        "routes_idx": routes_idx,
        "trips_by_route": by_route,
        "stop_times_by_trip": by_trip_stop_times
    }

def trip_stop_rows(trip_id: str) -> pd.DataFrame:
    gtfs = load_gtfs()
    sts = gtfs["stop_times"]
    return sts[sts["trip_id"]==trip_id].sort_values("stop_sequence")

def route_shape_coords(shape_id: str) -> List[Tuple[float,float]]:
    gtfs = load_gtfs()
    shapes = gtfs["shapes"]
    if shapes.empty or shape_id=="":
        return []
    df = shapes[shapes["shape_id"]==shape_id].sort_values("shape_pt_sequence")
    return [(float(r.shape_pt_lat), float(r.shape_pt_lon)) for _,r in df.iterrows()]

def upcoming_departures_at_stop(stop_id: str, now_sec: int, max_results=40) -> pd.DataFrame:
    gtfs = load_gtfs()
    st = gtfs["stop_times"]
    # csak ahol van departure_time
    df = st[(st["stop_id"]==stop_id) & (st["departure_time"]!="")]
    df["_dep"] = df["departure_time"].map(hhmmss_to_seconds)
    df = df[df["_dep"]>=now_sec-60]  # kis csúszás engedélyezve
    df = df.sort_values("_dep").head(max_results)
    # összekapcsolás route/trip-hez
    trips = gtfs["trips"][["trip_id","route_id","trip_headsign","direction_id","shape_id"]]
    routes = gtfs["routes"][["route_id","route_short_name","route_long_name","agency_id"]]
    out = df.merge(trips, on="trip_id", how="left").merge(routes, on="route_id", how="left")
    # duplikátumok eltüntetése (route_short_name, trip_id, _dep)
    out = out.drop_duplicates(subset=["route_short_name","trip_id","_dep"])
    return out
