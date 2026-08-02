import os
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.services.gtfs_calendar import service_days
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider, GTFSStoreProxy
from app.utils.geo_utils import haversine_m
from app.utils.text_utils import (
    clean_text, destination_match, extract_codes, fleet_from_vehicle_ref,
    human_name, line_norm, norm, public_stop_code, safe_float, safe_int,
    short_destination, stop_code_from_name,
)
from app.utils.time_utils import (
    LONDON, gtfs_time_to_datetime, hhmm, minutes_until, now_london, parse_iso_dt,
)

APP_NAME = "Bluestar Unilink Menetrend"
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
GTFS_DIR = Path(os.getenv("GTFS_DIR", str(BASE_DIR / "gtfs")))
GTFS_ZIP = Path(os.getenv("GTFS_ZIP", str(BASE_DIR / "gtfs.zip")))

LIVE_FEED_URL_DEFAULT = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/"
LIVE_FEED_URL = os.getenv("LIVE_FEED_URL", LIVE_FEED_URL_DEFAULT).strip()
LIVE_API_KEY = os.getenv("LIVE_API_KEY", os.getenv("BODS_API_KEY", "")).strip()
LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "8"))
LIVE_MAX_AGE_SECONDS = int(os.getenv("LIVE_MAX_AGE_SECONDS", "360"))
LIVE_OPERATOR_FILTER = os.getenv("LIVE_OPERATOR_FILTER", "BLUS").strip().upper()
DEPARTURE_WINDOW_MIN = int(os.getenv("DEPARTURE_WINDOW_MIN", "120"))
DEPARTURE_LIMIT = int(os.getenv("DEPARTURE_LIMIT", "80"))
LIVE_MATCH_MINUTES = int(os.getenv("LIVE_MATCH_MINUTES", "38"))

app = FastAPI(title=APP_NAME)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def stop_same(stop: Dict[str, Any], stop_ref: str = "", stop_name: str = "") -> bool:
    if stop_ref and clean_text(stop_ref).upper() == clean_text(stop.get("stop_id")).upper():
        return True
    if stop_name:
        a, b = norm(stop_name), norm(stop.get("stop_name"))
        if a and b and (a in b or b in a):
            return True
    return False


gtfs_provider = GTFSStoreProvider(
    GTFSStore(zip_path=GTFS_ZIP, directory_path=GTFS_DIR)
)
gtfs = GTFSStoreProxy(gtfs_provider)


def xml_text(node: Optional[ET.Element], path: str = "") -> str:
    if node is None:
        return ""
    target = node.find(path) if path else node
    if target is not None and target.text:
        return clean_text(target.text)
    # Namespace-free fallback.
    name = path.split("/")[-1]
    if not name:
        return ""
    for e in node.iter():
        if e.tag.split("}")[-1] == name and e.text:
            return clean_text(e.text)
    return ""


def children_by_local(root: ET.Element, name: str) -> List[ET.Element]:
    return [e for e in root.iter() if e.tag.split("}")[-1] == name]


class LiveStore:
    def __init__(self):
        self.vehicles: List[Dict[str, Any]] = []
        self.ok = False
        self.error = ""
        self.last_fetch: Optional[datetime] = None
        self.raw_count = 0

    def fetch(self, force: bool = False) -> List[Dict[str, Any]]:
        n = now_london()
        if not force and self.last_fetch and (n - self.last_fetch).total_seconds() < LIVE_CACHE_TTL_SEC:
            return self.vehicles
        try:
            url = LIVE_FEED_URL
            headers = {"User-Agent": "Bluestar-Unilink-App/1.0"}
            if LIVE_API_KEY:
                sep = "&" if "?" in url else "?"
                if "api_key=" not in url.lower() and "key=" not in url.lower():
                    url = f"{url}{sep}api_key={urllib.parse.quote(LIVE_API_KEY)}"
                headers["x-api-key"] = LIVE_API_KEY
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                xml = resp.read()
            root = ET.fromstring(xml)
            vehicles: List[Dict[str, Any]] = []
            raw = 0
            for mvj in children_by_local(root, "MonitoredVehicleJourney"):
                raw += 1
                operator = xml_text(mvj, "OperatorRef").upper()
                if LIVE_OPERATOR_FILTER and operator and operator != LIVE_OPERATOR_FILTER:
                    continue
                line = xml_text(mvj, "PublishedLineName") or xml_text(mvj, "LineRef")
                if not line:
                    continue
                dest = xml_text(mvj, "DestinationName") or xml_text(mvj, "DestinationRef")
                vehicle_ref = xml_text(mvj, "VehicleRef") or xml_text(mvj, "VehicleMonitoringRef")
                dated_ref = xml_text(mvj, "FramedVehicleJourneyRef/DatedVehicleJourneyRef") or xml_text(mvj, "DatedVehicleJourneyRef")
                block_ref = xml_text(mvj, "BlockRef")
                lat = safe_float(xml_text(mvj, "VehicleLocation/Latitude"))
                lon = safe_float(xml_text(mvj, "VehicleLocation/Longitude"))
                bearing = safe_float(xml_text(mvj, "Bearing"))
                recorded = parse_iso_dt(xml_text(mvj, "RecordedAtTime") or xml_text(mvj, "ValidUntilTime"))
                call = None
                for e in mvj.iter():
                    if e.tag.split("}")[-1] == "MonitoredCall":
                        call = e
                        break
                stop_ref = xml_text(call, "StopPointRef") if call is not None else ""
                stop_name = human_name(xml_text(call, "StopPointName")) if call is not None else ""
                aimed = parse_iso_dt(xml_text(call, "AimedDepartureTime") or xml_text(call, "AimedArrivalTime")) if call is not None else None
                expected = parse_iso_dt(xml_text(call, "ExpectedDepartureTime") or xml_text(call, "ExpectedArrivalTime")) if call is not None else None
                live_time = expected or aimed
                delay = None
                if aimed and expected:
                    delay = int(round((expected - aimed).total_seconds() / 60))
                vehicle_at_stop = (xml_text(call, "VehicleAtStop").lower() == "true") if call is not None else False
                age = int((n - recorded).total_seconds()) if recorded else 0
                if recorded and age > LIVE_MAX_AGE_SECONDS:
                    continue
                vehicles.append({
                    "line": clean_text(line),
                    "lineNorm": line_norm(line),
                    "destination": short_destination(dest),
                    "destinationFull": human_name(dest),
                    "operator": operator,
                    "vehicleRef": vehicle_ref,
                    "fleet": fleet_from_vehicle_ref(vehicle_ref),
                    "datedVehicleJourneyRef": dated_ref,
                    "blockRef": block_ref,
                    "codes": list(extract_codes(vehicle_ref, fleet_from_vehicle_ref(vehicle_ref), dated_ref, block_ref)),
                    "latitude": lat,
                    "longitude": lon,
                    "bearing": bearing or 0,
                    "recordedAt": recorded.isoformat() if recorded else "",
                    "currentStopRef": stop_ref,
                    "currentStopName": stop_name,
                    "vehicleAtStop": vehicle_at_stop,
                    "aimedTime": aimed.isoformat() if aimed else "",
                    "expectedTime": expected.isoformat() if expected else "",
                    "liveTime": live_time.isoformat() if live_time else "",
                    "delayMinutes": delay,
                    "status": "At stop" if vehicle_at_stop else "Moving",
                })
            self.raw_count = raw
            self.vehicles = vehicles
            self.ok = True
            self.error = ""
            self.last_fetch = n
        except Exception as exc:
            self.ok = False
            self.error = str(exc)
            self.last_fetch = n
        return self.vehicles


live_store = LiveStore()


def service_days_for_departures() -> List[date]:
    return service_days(now_london())


def trip_headsign(trip: Dict[str, Any]) -> str:
    return human_name(trip.get("trip_headsign") or trip.get("destination") or "")


def shape_for_trip(store: GTFSStore, trip: Dict[str, Any]) -> List[Dict[str, float]]:
    sid = clean_text(trip.get("shape_id"))
    if sid and store.shapes.get(sid):
        return store.shapes[sid][:3000]
    out = []
    for r in store.stop_times_by_trip.get(trip.get("trip_id"), []):
        st = store.stops.get(r.get("stop_id"), {})
        if st.get("lat") is not None and st.get("lon") is not None:
            out.append({"lat": st["lat"], "lon": st["lon"]})
    return out


def match_live_to_departure(store: GTFSStore, dep: Dict[str, Any], sched_dt: datetime, vehicles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best = None
    best_score = -9999
    for v in vehicles:
        if line_norm(v.get("line")) != line_norm(dep.get("line")):
            continue
        if not destination_match(v.get("destinationFull") or v.get("destination"), dep.get("headsign_full") or dep.get("headsign")):
            continue
        live_dt = parse_iso_dt(v.get("liveTime"))
        score = 0
        if live_dt:
            diff = abs((live_dt - sched_dt).total_seconds()) / 60
            if diff > LIVE_MATCH_MINUTES:
                continue
            score += max(0, 100 - int(diff * 3))
        stop = store.stops.get(dep.get("stop_id"), {})
        if stop_same(stop, v.get("currentStopRef", ""), v.get("currentStopName", "")):
            score += 80
        if v.get("vehicleAtStop"):
            score += 20
        if v.get("fleet"):
            score += 5
        if score > best_score:
            best_score = score
            best = v
    return best


def enrich_departure(store: GTFSStore, dep: Dict[str, Any], service_day: date, sched_dt: datetime, vehicles: List[Dict[str, Any]]) -> Dict[str, Any]:
    live = match_live_to_departure(store, dep, sched_dt, vehicles)
    live_dt = parse_iso_dt(live.get("liveTime")) if live else None
    display_dt = live_dt or sched_dt
    mins = minutes_until(display_dt)
    if mins is not None and mins < 0:
        mins = 0 if mins >= -2 else None
    is_due = bool(mins is not None and mins <= 1)
    return {
        "tripId": dep.get("trip_id"),
        "trip_id": dep.get("trip_id"),
        "serviceDate": service_day.isoformat(),
        "line": dep.get("line", ""),
        "routeId": dep.get("route_id", ""),
        "stopId": dep.get("stop_id", ""),
        "stopName": dep.get("stop_name", ""),
        "stopSequence": dep.get("stop_sequence", 0),
        "destination": short_destination(dep.get("headsign_full") or dep.get("headsign")),
        "destinationFull": dep.get("headsign_full") or dep.get("headsign"),
        "scheduledTime": hhmm(sched_dt),
        "scheduledTimeIso": sched_dt.isoformat(),
        "displayTime": hhmm(display_dt),
        "displayTimeIso": display_dt.isoformat(),
        "minutes": mins,
        "minutesText": "Due" if is_due else (f"{mins} min" if mins is not None else ""),
        "live": bool(live),
        "isDue": is_due,
        "vehicleRef": live.get("vehicleRef") if live else "",
        "fleet": live.get("fleet") if live else "",
        "delayMinutes": live.get("delayMinutes") if live else None,
    }


def find_live_for_trip(store: GTFSStore, trip: Dict[str, Any], service_day: date, vehicles: List[Dict[str, Any]], vehicle_hint: str = "") -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    line = trip.get("line", "")
    dest = trip_headsign(trip)
    trip_id = trip.get("trip_id", "")
    hint_codes = extract_codes(vehicle_hint)
    rows = store.stop_times_by_trip.get(trip_id, [])
    first = store.trip_first_stop(trip_id)
    first_dt = gtfs_time_to_datetime(service_day, first.get("departure_time") or first.get("arrival_time") or "")
    best = None
    best_seq = None
    best_score = -9999
    for v in vehicles:
        if line_norm(v.get("line")) != line_norm(line):
            continue
        score = 0
        if destination_match(v.get("destinationFull") or v.get("destination"), dest):
            score += 30
        else:
            continue
        if hint_codes and hint_codes.intersection(set(v.get("codes", []))):
            score += 400
        if trip_id and (trip_id in clean_text(v.get("datedVehicleJourneyRef")) or norm(trip_id) in norm(v.get("datedVehicleJourneyRef"))):
            score += 300
        seq_found = None
        for r in rows:
            st = store.stops.get(r.get("stop_id"), {})
            if stop_same(st, v.get("currentStopRef", ""), v.get("currentStopName", "")):
                seq_found = safe_int(r.get("stop_sequence"), 0)
                score += 160
                break
        live_dt = parse_iso_dt(v.get("liveTime"))
        if first_dt and live_dt:
            diff = abs((live_dt - first_dt).total_seconds()) / 60
            if diff < 90:
                score += max(0, 60 - int(diff))
        if score > best_score:
            best_score = score
            best = v
            best_seq = seq_found
    return best, best_seq


def api_error(text: str, status: int = 400):
    return JSONResponse({"ok": False, "error": text}, status_code=status)


def require_gtfs():
    store = gtfs_provider.get()
    if not store.loaded:
        candidate = GTFSStore(zip_path=GTFS_ZIP, directory_path=GTFS_DIR).load()
        if candidate.loaded:
            gtfs_provider.replace(candidate)
            store = candidate
    if not store.loaded:
        return store, api_error(f"GTFS data unavailable: {store.error or 'no usable dataset'}", 503)
    return store, None


def initialize_legacy_stores():
    candidate = GTFSStore(zip_path=GTFS_ZIP, directory_path=GTFS_DIR).load()
    if candidate.loaded:
        gtfs_provider.replace(candidate)
    live_store.fetch(force=True)


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "time": now_london().isoformat()}


@app.get("/api/status")
def status():
    store, _ = require_gtfs()
    vehicles = live_store.fetch()
    refresh_service = getattr(app.state, "gtfs_refresh", None)
    refresh = refresh_service.snapshot() if refresh_service else {}
    return {
        "live": {
            "ok": live_store.ok,
            "activeCount": len(vehicles),
            "rawCount": live_store.raw_count,
            "maxAgeSeconds": LIVE_MAX_AGE_SECONDS,
            "operatorFilter": LIVE_OPERATOR_FILTER,
            "error": live_store.error or None,
            "lastFetchTime": live_store.last_fetch.isoformat() if live_store.last_fetch else None,
        },
        "gtfs": {
            "ok": store.loaded,
            "loaded": store.loaded,
            "error": store.error or None,
            "source": refresh.get("source") or store.source,
            "activeDataSource": store.source,
            "refreshEnabled": refresh.get("enabled", False),
            "refreshRunning": refresh.get("running", False),
            "lastCheckedAt": refresh.get("lastCheckedAt"),
            "lastUpdatedAt": refresh.get("lastUpdatedAt"),
            "lastSuccessfulLoadAt": refresh.get("lastSuccessfulLoadAt"),
            "sha256": refresh.get("sha256"),
            "etag": refresh.get("etag"),
            "lastModified": refresh.get("lastModified"),
            "usingCachedData": refresh.get("usingCachedData", bool(store.loaded)),
            "refreshIntervalSeconds": refresh.get("refreshIntervalSeconds"),
            "lastError": refresh.get("lastError") or (store.error or None),
            "metadataPersistenceError": refresh.get("metadataPersistenceError"),
            "counts": {
                "agency": len(store.agency),
                "stops": len(store.stops),
                "routes": len(store.routes),
                "trips": len(store.trips),
                "stop_times_trips": len(store.stop_times_by_trip),
                "stop_departures_index_stops": len(store.stop_departures_index),
                "shapes": len(store.shapes),
            },
        },
        "serverTime": now_london().isoformat(),
        "timezone": "Europe/London",
    }


@app.get("/")
def index():
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")


@app.get("/api/search")
def api_search(q: str = ""):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    query = clean_text(q)
    nq = norm(query)
    stops = []
    routes = []
    if nq:
        for sid, st in store.stops.items():
            hay = norm(st.get("stop_name", "") + " " + sid + " " + st.get("code", ""))
            if nq in hay:
                stops.append({
                    "id": sid,
                    "stop_id": sid,
                    "name": st.get("stop_name", sid),
                    "code": st.get("code", "BUS"),
                    "lat": st.get("lat"),
                    "lon": st.get("lon"),
                })
                if len(stops) >= 50:
                    break
        for rid, rt in store.routes.items():
            line = rt.get("route_short_name", "")
            hay = norm(line + " " + rt.get("route_long_name", "") + " " + rid)
            if nq in hay or nq == norm(line):
                routes.append({
                    "id": line,
                    "routeId": rid,
                    "line": line,
                    "name": line,
                    "subtitle": rt.get("route_long_name", ""),
                })
        routes.sort(key=lambda r: (0 if line_norm(r.get("line")) == line_norm(query) else 1, r.get("line")))
    return {"ok": True, "query": query, "stops": stops[:50], "routes": routes[:40]}


@app.get("/api/stops/{stop_id}/departures")
def api_stop_departures(stop_id: str, minutes: int = DEPARTURE_WINDOW_MIN):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    stop = store.stops.get(stop_id)
    if not stop:
        return api_error("Stop not found", 404)
    n = now_london()
    end = n + timedelta(minutes=max(10, min(minutes, 360)))
    vehicles = live_store.fetch()
    result = []
    for service_day in service_days_for_departures():
        active = store.active_service_ids(service_day)
        for dep in store.stop_departures_index.get(stop_id, []):
            if active and dep.get("service_id") not in active:
                continue
            if clean_text(dep.get("pickup_type")) == "1" or dep.get("is_last_stop"):
                continue
            sched_dt = gtfs_time_to_datetime(service_day, dep.get("departure_time") or dep.get("arrival_time"))
            if not sched_dt or sched_dt < n - timedelta(minutes=2) or sched_dt > end:
                continue
            result.append(enrich_departure(store, dep, service_day, sched_dt, vehicles))
    result.sort(key=lambda x: x.get("displayTimeIso") or x.get("scheduledTimeIso") or "")
    dedup = []
    seen = set()
    for x in result:
        key = (x.get("tripId"), x.get("serviceDate"), x.get("stopSequence"))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(x)
    return {"ok": True, "stop": stop, "departures": dedup[:DEPARTURE_LIMIT], "now": now_london().isoformat()}


@app.get("/api/trips/{trip_id}")
def api_trip(trip_id: str, service_date: str = "", vehicle: str = ""):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    trip = store.trips.get(trip_id)
    if not trip:
        return api_error("Trip not found", 404)
    try:
        service_day = date.fromisoformat(service_date) if service_date else now_london().date()
    except Exception:
        service_day = now_london().date()
    route = store.routes.get(trip.get("route_id"), {})
    vehicles = live_store.fetch()
    live, current_seq = find_live_for_trip(store, trip, service_day, vehicles, vehicle)
    rows = store.stop_times_by_trip.get(trip_id, [])
    n = now_london()
    delay = live.get("delayMinutes") if live else None
    out = []
    for r in rows:
        st = store.stops.get(r.get("stop_id"), {})
        sched_dt = gtfs_time_to_datetime(service_day, r.get("departure_time") or r.get("arrival_time"))
        live_dt = None
        is_current = False
        live_future = False
        seq = safe_int(r.get("stop_sequence"), 0)
        if live:
            if stop_same(st, live.get("currentStopRef", ""), live.get("currentStopName", "")):
                is_current = True
                live_dt = parse_iso_dt(live.get("liveTime")) or sched_dt
            elif current_seq and seq > current_seq and isinstance(delay, int) and sched_dt:
                live_future = True
                live_dt = sched_dt + timedelta(minutes=delay)
        display_dt = live_dt or sched_dt
        mins = minutes_until(display_dt)
        if mins is not None and mins < 0:
            mins = None
        past = bool(display_dt and display_dt < n - timedelta(seconds=30) and not is_current)
        if current_seq and seq < current_seq:
            past = True
        if is_current:
            right = "LIVE" if live.get("vehicleAtStop") else "Due"
        elif mins is not None:
            right = "Due" if mins <= 1 and (live_future or live) else f"{mins}'"
        else:
            right = ""
        out.append({
            "stopId": r.get("stop_id"),
            "name": st.get("stop_name", r.get("stop_id")),
            "sequence": seq,
            "lat": st.get("lat"),
            "lon": st.get("lon"),
            "scheduledTime": hhmm(sched_dt),
            "scheduledTimeIso": sched_dt.isoformat() if sched_dt else "",
            "displayTime": hhmm(display_dt),
            "displayTimeIso": display_dt.isoformat() if display_dt else "",
            "minutes": mins,
            "rightLabel": right,
            "live": bool(is_current or live_future),
            "current": is_current,
            "past": past,
        })
    if isinstance(delay, int):
        delay_label = f"{delay:+d}"
    elif live:
        delay_label = "LIVE"
    else:
        delay_label = "--"
    return {
        "ok": True,
        "trip": {**trip, "destination": short_destination(trip_headsign(trip)), "destinationFull": trip_headsign(trip)},
        "route": route,
        "serviceDate": service_day.isoformat(),
        "stops": out,
        "live": live,
        "delayLabel": delay_label,
        "currentSequence": current_seq,
        "shape": shape_for_trip(store, trip),
        "now": now_london().isoformat(),
    }


@app.get("/api/routes/{line}")
def api_route(line: str):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    ln = line_norm(line)
    route_ids = store.route_by_short.get(ln, [])
    if not route_ids:
        route_ids = [rid for rid, r in store.routes.items() if line_norm(r.get("route_id")) == ln]
    if not route_ids:
        return api_error("Route not found", 404)
    vehicles = [v for v in live_store.fetch() if line_norm(v.get("line")) == ln]
    active = store.active_service_ids(now_london().date()) | store.active_service_ids((now_london() - timedelta(days=1)).date())
    directions_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for trip in store.trips.values():
        if trip.get("route_id") not in route_ids:
            continue
        if active and trip.get("service_id") not in active:
            continue
        dest = short_destination(trip_headsign(trip))
        key = (trip.get("direction_id", ""), dest)
        if key in directions_map:
            continue
        stops = []
        for r in store.stop_times_by_trip.get(trip.get("trip_id"), []):
            st = store.stops.get(r.get("stop_id"), {})
            stops.append({"id": r.get("stop_id"), "name": st.get("stop_name", r.get("stop_id")), "code": st.get("code", "BUS"), "lat": st.get("lat"), "lon": st.get("lon"), "sequence": r.get("stop_sequence")})
        directions_map[key] = {"directionId": key[0], "destination": dest, "tripId": trip.get("trip_id"), "stops": stops}
        if len(directions_map) >= 6:
            break
    return {"ok": True, "line": clean_text(line), "routes": [store.routes[rid] for rid in route_ids], "vehicles": vehicles, "directions": list(directions_map.values())}


@app.get("/api/vehicles")
def api_vehicles(line: str = ""):
    vehicles = live_store.fetch()
    if line:
        vehicles = [v for v in vehicles if line_norm(v.get("line")) == line_norm(line)]
    vehicles.sort(key=lambda v: (line_norm(v.get("line")), v.get("destination", ""), v.get("fleet", "")))
    return {"ok": True, "line": clean_text(line), "vehicles": vehicles, "now": now_london().isoformat()}


@app.get("/api/map")
def api_map(line: str = ""):
    store, unavailable = require_gtfs()
    if unavailable:
        return unavailable
    ln = line_norm(line)
    vehicles = live_store.fetch()
    if ln:
        vehicles = [v for v in vehicles if line_norm(v.get("line")) == ln]
    shapes = []
    if ln:
        route_ids = store.route_by_short.get(ln, [])
        active = store.active_service_ids(now_london().date()) | store.active_service_ids((now_london() - timedelta(days=1)).date())
        seen = set()
        for trip in store.trips.values():
            sid = clean_text(trip.get("shape_id"))
            if trip.get("route_id") in route_ids and sid and sid not in seen:
                if active and trip.get("service_id") not in active:
                    continue
                pts = store.shapes.get(sid, [])
                if pts:
                    shapes.append({"shapeId": sid, "points": pts[:3000]})
                    seen.add(sid)
            if len(shapes) >= 8:
                break
        if not shapes:
            for trip in store.trips.values():
                if trip.get("route_id") in route_ids:
                    shapes.append({"shapeId": trip.get("trip_id"), "points": shape_for_trip(store, trip)})
                    break
    pts = [(v.get("latitude"), v.get("longitude")) for v in vehicles if v.get("latitude") is not None and v.get("longitude") is not None]
    if pts:
        center = {"lat": sum(p[0] for p in pts) / len(pts), "lon": sum(p[1] for p in pts) / len(pts)}
    else:
        center = {"lat": 50.9097, "lon": -1.4044}
    return {"ok": True, "line": clean_text(line), "vehicles": vehicles, "shapes": shapes, "center": center, "now": now_london().isoformat()}


@app.get("/{path:path}", response_class=HTMLResponse)
def spa_fallback(path: str):
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")
