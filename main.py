from __future__ import annotations
import os, asyncio
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, ORJSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime, timedelta, timezone
from dateutil import tz

from gtfs_utils import load_gtfs, build_indexes, trip_stop_rows, upcoming_departures_at_stop, route_shape_coords
from siri_live import vehicles_live, select_vehicle_for_trip, ALLOWED_LINE_NAMES

app = FastAPI(title="bluestar")
app.mount("/static", StaticFiles(directory="static"), name="static")

env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(['html', 'xml'])
)

def render(tpl: str, **ctx) -> HTMLResponse:
    tmpl = env.get_template(tpl)
    return HTMLResponse(tmpl.render(**ctx))

def now_uk():
    # UK idő (BST/GMT)
    return datetime.now(tz.gettz("Europe/London"))

# --------- PAGES ---------

@app.get("/", response_class=HTMLResponse)
async def home():
    gtfs = load_gtfs()
    routes = (gtfs["routes"][["route_id","route_short_name","route_long_name","agency_id"]]
              .sort_values(["route_short_name"]))
    # csak Bluestar/Unilink jellegű route-ok – ha route_short_name számos vagy U*
    def keep(row):
        rsn = (row["route_short_name"] or "").strip()
        return (rsn in ALLOWED_LINE_NAMES) or rsn.isdigit() or rsn.upper().startswith("U")
    routes = routes[routes.apply(keep, axis=1)]
    return render("index.html", routes=routes.to_dict(orient="records"), now=now_uk())

@app.get("/stop/{stop_id}", response_class=HTMLResponse)
async def stop_page(stop_id: str):
    gtfs = load_gtfs()
    stops = gtfs["stops"]
    stop = stops[stops["stop_id"]==stop_id].head(1)
    if stop.empty:
        return render("base.html", content=f"Ismeretlen megálló: {stop_id}")
    return render("stop.html", stop=stop.iloc[0].to_dict(), now=now_uk())

@app.get("/route/{route_id}", response_class=HTMLResponse)
async def route_page(route_id: str):
    gtfs = load_gtfs()
    routes = gtfs["routes"]
    r = routes[routes["route_id"]==route_id].head(1)
    if r.empty:
        return render("base.html", content=f"Ismeretlen járat: {route_id}")
    return render("route.html", route=r.iloc[0].to_dict(), now=now_uk())

@app.get("/trip/{trip_id}", response_class=HTMLResponse)
async def trip_page(trip_id: str):
    gtfs = load_gtfs()
    trips = gtfs["trips"]
    routes = gtfs["routes"]
    t = trips[trips["trip_id"]==trip_id].head(1)
    if t.empty:
        return render("base.html", content=f"Ismeretlen trip: {trip_id}")

    route = routes[routes["route_id"]==t.iloc[0]["route_id"]].head(1).iloc[0].to_dict()
    return render("trip.html",
        trip=t.iloc[0].to_dict(),
        route=route,
        now=now_uk()
    )

# --------- API ---------

@app.get("/api/stop/{stop_id}/departures")
async def api_stop_departures(stop_id: str, only_departures: bool = True):
    """
    Végállomásnál (keresés destination stopra) kérheted only_departures=True
    – ilyenkor csak a departure_time-mal rendelkező sorok jönnek.
    Régi indulások kiszűrve (>= most - 60s).
    Csak Bluestar/Unilink route-okra szűrve.
    """
    now = now_uk()
    sec = now.hour*3600 + now.minute*60 + now.second
    df = upcoming_departures_at_stop(stop_id, sec, max_results=60)

    # csak Bluestar/Unilink
    def keep(row):
        rsn = (row["route_short_name"] or "").strip()
        return (rsn in ALLOWED_LINE_NAMES) or rsn.isdigit() or rsn.upper().startswith("U")
    df = df[df.apply(keep, axis=1)]

    if only_departures:
        df = df[df["departure_time"]!=""]

    out = []
    for _,r in df.iterrows():
        out.append({
            "trip_id": r["trip_id"],
            "route_id": r["route_id"],
            "route_short_name": r["route_short_name"],
            "headsign": r["trip_headsign"],
            "dep_time": r["departure_time"],
            "stop_sequence": int(r["stop_sequence"]),
            "shape_id": r.get("shape_id","")
        })
    return ORJSONResponse(out)

@app.get("/api/trip/{trip_id}")
async def api_trip(trip_id: str):
    """
    Trip stoplista + élő jármű kiválasztása (ha van).
    Soronként: scheduled_time, live_time (ha van), status: 'LIVE'/'TT'
    """
    gtfs = load_gtfs()
    trips = gtfs["trips"]
    routes = gtfs["routes"]
    t = trips[trips["trip_id"]==trip_id].head(1)
    if t.empty:
        return ORJSONResponse({"error":"unknown_trip"}, status_code=404)

    t0 = t.iloc[0].to_dict()
    route = routes[routes["route_id"]==t0["route_id"]].head(1).iloc[0].to_dict()
    short = (route.get("route_short_name") or "").strip()
    stops_df = trip_stop_rows(trip_id)

    # Élő jármű lekérés, szűrés
    vehicles = await vehicles_live()
    vehicle = select_vehicle_for_trip(
        trip_key=trip_id,
        line_name=short,
        candidates=vehicles
    )

    # status építés
    rows = []
    uk = now_uk()
    for _, row in stops_df.iterrows():
        sched = row["arrival_time"] or row["departure_time"]
        label = "TT"
        live_time = None
        if vehicle:
            # heurisztika: ha van jármű és közel van az adott megállóhoz, jelöljük live-nak,
            # illetve ha a jármű last_update < 60s
            label = "TT"
            # (egyszerű jelölés: ha van vehicle, akkor a sorok 'LIVE' jellegűek – a frontend
            # külön zölddel fogja mutatni a live ETA-t, ha kiszámítható)
        rows.append({
            "stop_id": row["stop_id"],
            "stop_sequence": int(row["stop_sequence"]),
            "stop_name": row.get("stop_name",""),
            "scheduled": sched,
            "live": live_time,
            "label": label
        })

    return ORJSONResponse({
        "trip": t0,
        "route": route,
        "vehicle": vehicle,     # Trip nézetben a frontend CSAK ezt a járművet jeleníti meg
        "rows": rows
    })

@app.get("/api/route/{route_id}/shape")
async def api_route_shape(route_id: str):
    gtfs = load_gtfs()
    trips = gtfs["trips"]
    t = trips[trips["route_id"]==route_id].head(1)
    coords = []
    if not t.empty:
        shape_id = t.iloc[0]["shape_id"]
        coords = route_shape_coords(shape_id)
    return ORJSONResponse({"coords": coords})

@app.get("/api/vehicles")
async def api_vehicles(route_short_name: Optional[str]=None, for_trip: Optional[str]=None):
    """
    Visszaadja a szűrt élő járműveket. 
    - Ha `for_trip` meg van adva, CSAK a kiválasztott jármű jön vissza.
    - Egyébként route_short_name szerint szűr.
    """
    vehicles = await vehicles_live()
    if for_trip:
        gtfs = load_gtfs()
        trips = gtfs["trips"]
        routes = gtfs["routes"]
        t = trips[trips["trip_id"]==for_trip].head(1)
        if t.empty:
            return ORJSONResponse([])
        r = routes[routes["route_id"]==t.iloc[0]["route_id"]].head(1).iloc[0].to_dict()
        short = (r.get("route_short_name") or "").strip()
        v = select_vehicle_for_trip(for_trip, short, vehicles)
        return ORJSONResponse([v] if v else [])

    if route_short_name:
        vehicles = [v for v in vehicles if v.get("line_name")==route_short_name]
    return ORJSONResponse(vehicles)
