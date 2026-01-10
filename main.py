import os
import csv
import re
import math
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


# ----------------------------
# Settings
# ----------------------------
APP_TZ = ZoneInfo(os.getenv("APP_TZ", "Europe/London"))

GTFS_DIR = os.getenv("GTFS_DIR", "gtfs")

DFT_FEED_ID = os.getenv("DFT_FEED_ID", "7721")
DFT_API_KEY = os.getenv("DFT_API_KEY", "").strip()

LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "12"))  # keep same style as your JSON
LIVE_HTTP_TIMEOUT = float(os.getenv("LIVE_HTTP_TIMEOUT", "12"))

# If you want to allow live without env var, you can hardcode here (not recommended).
DFT_URL_TEMPLATE = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/{feed_id}/?api_key={api_key}"


# ----------------------------
# Helpers
# ----------------------------
def _now_dt() -> datetime:
    return datetime.now(tz=APP_TZ)


def _parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    try:
        # handle Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=APP_TZ)
        return dt.astimezone(APP_TZ)
    except Exception:
        return None


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _parse_time_to_sec(t: str) -> Optional[int]:
    """
    GTFS times may exceed 24h (e.g. 25:10:00).
    """
    if not t:
        return None
    t = t.strip()
    m = re.match(r"^(\d{1,2}|\d{2,3}):(\d{2}):(\d{2})$", t)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    ss = int(m.group(3))
    return hh * 3600 + mm * 60 + ss


def _sec_to_hhmm(sec: Optional[int]) -> Optional[str]:
    if sec is None:
        return None
    sec = int(sec)
    hh = (sec // 3600) % 24
    mm = (sec % 3600) // 60
    return f"{hh:02d}:{mm:02d}"


def _dt_from_service_date_and_seconds(svc: date, sec: int) -> datetime:
    # sec may exceed 24h; roll over accordingly
    days = sec // 86400
    rem = sec % 86400
    base = datetime(svc.year, svc.month, svc.day, tzinfo=APP_TZ) + timedelta(days=days)
    return base + timedelta(seconds=rem)


def _parse_duration_to_seconds(dur: Optional[str]) -> Optional[int]:
    """
    Parse SIRI Delay / ISO8601 duration like:
    PT2M, PT30S, -PT1M, P0DT0H2M0S
    """
    if not dur:
        return None
    s = dur.strip()
    sign = 1
    if s.startswith("-"):
        sign = -1
        s = s[1:]
    if not s.startswith("P"):
        return None

    # Very small ISO8601 duration parser for time part
    # supports PnDTnHnMnS and PTnHnMnS
    days = hours = minutes = seconds = 0
    m = re.match(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$", s)
    if not m:
        # try P0DT0H0M0S
        m2 = re.match(r"^P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", s)
        if not m2:
            return None
        m = m2
    if m.group(1):
        days = int(m.group(1))
    if m.group(2):
        hours = int(m.group(2))
    if m.group(3):
        minutes = int(m.group(3))
    if m.group(4):
        seconds = int(m.group(4))
    total = days * 86400 + hours * 3600 + minutes * 60 + seconds
    return sign * total


def _parse_int_loose(v: Any, default: int) -> int:
    """
    Accept things like '5/' or 'max=10,' and extract the first integer.
    """
    s = str(v or "").strip()
    m = re.search(r"-?\d+", s)
    if not m:
        return default
    try:
        return int(m.group(0))
    except Exception:
        return default


# ----------------------------
# GTFS Store
# ----------------------------
@dataclass
class Stop:
    stop_id: str
    stop_name: str
    lat: Optional[float]
    lon: Optional[float]


@dataclass
class StopTime:
    trip_id: str
    stop_id: str
    arrival_sec: Optional[int]
    departure_sec: Optional[int]
    stop_sequence: int


class GTFSStore:
    def __init__(self, gtfs_dir: str):
        self.gtfs_dir = gtfs_dir

        self.stops: Dict[str, Stop] = {}
        self.routes: Dict[str, Dict[str, str]] = {}
        self.trips: Dict[str, Dict[str, Any]] = {}

        self.stop_times_by_stop: Dict[str, List[StopTime]] = {}
        self.stop_times_by_trip: Dict[str, List[StopTime]] = {}
        self.last_seq_by_trip: Dict[str, int] = {}

        # Calendar
        self.calendar: Dict[str, Dict[str, Any]] = {}
        self.calendar_dates: Dict[Tuple[str, date], int] = {}  # exception_type
        self.min_cal_start: Optional[date] = None
        self.max_cal_end: Optional[date] = None

        self.loaded_ok = False
        self.load_errors: List[str] = []

    def _path(self, filename: str) -> str:
        return os.path.join(self.gtfs_dir, filename)

    def load(self) -> None:
        try:
            self._load_routes()
            self._load_trips()
            self._load_stops()
            self._load_calendar()
            self._load_calendar_dates()
            self._load_stop_times()
            self._finalize()
            self.loaded_ok = True
        except Exception as e:
            self.loaded_ok = False
            self.load_errors.append(f"GTFS load error: {e!r}")

    def _load_csv(self, filename: str) -> List[Dict[str, str]]:
        path = self._path(filename)
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _load_routes(self) -> None:
        rows = self._load_csv("routes.txt")
        for r in rows:
            rid = r.get("route_id", "").strip()
            if not rid:
                continue
            self.routes[rid] = {
                "route_short_name": (r.get("route_short_name") or "").strip(),
                "route_long_name": (r.get("route_long_name") or "").strip(),
            }

    def _load_trips(self) -> None:
        rows = self._load_csv("trips.txt")
        for r in rows:
            tid = (r.get("trip_id") or "").strip()
            if not tid:
                continue
            self.trips[tid] = {
                "route_id": (r.get("route_id") or "").strip(),
                "service_id": (r.get("service_id") or "").strip(),
                "trip_headsign": (r.get("trip_headsign") or "").strip(),
                "direction_id": (r.get("direction_id") or "").strip(),
                "shape_id": (r.get("shape_id") or "").strip(),
            }

    def _load_stops(self) -> None:
        rows = self._load_csv("stops.txt")
        for r in rows:
            sid = (r.get("stop_id") or "").strip()
            if not sid:
                continue
            name = (r.get("stop_name") or "").strip()
            lat = None
            lon = None
            try:
                if r.get("stop_lat"):
                    lat = float(r["stop_lat"])
                if r.get("stop_lon"):
                    lon = float(r["stop_lon"])
            except Exception:
                lat = None
                lon = None
            self.stops[sid] = Stop(stop_id=sid, stop_name=name, lat=lat, lon=lon)

    def _load_calendar(self) -> None:
        path = self._path("calendar.txt")
        if not os.path.exists(path):
            return
        rows = self._load_csv("calendar.txt")
        for r in rows:
            sid = (r.get("service_id") or "").strip()
            if not sid:
                continue
            def _d(x: str) -> Optional[date]:
                x = (x or "").strip()
                if not x or len(x) != 8:
                    return None
                return date(int(x[0:4]), int(x[4:6]), int(x[6:8]))
            start = _d(r.get("start_date") or "")
            end = _d(r.get("end_date") or "")
            wk = {
                "monday": (r.get("monday") == "1"),
                "tuesday": (r.get("tuesday") == "1"),
                "wednesday": (r.get("wednesday") == "1"),
                "thursday": (r.get("thursday") == "1"),
                "friday": (r.get("friday") == "1"),
                "saturday": (r.get("saturday") == "1"),
                "sunday": (r.get("sunday") == "1"),
            }
            self.calendar[sid] = {"start": start, "end": end, "wk": wk}
            if start and (self.min_cal_start is None or start < self.min_cal_start):
                self.min_cal_start = start
            if end and (self.max_cal_end is None or end > self.max_cal_end):
                self.max_cal_end = end

    def _load_calendar_dates(self) -> None:
        path = self._path("calendar_dates.txt")
        if not os.path.exists(path):
            return
        rows = self._load_csv("calendar_dates.txt")
        for r in rows:
            sid = (r.get("service_id") or "").strip()
            d = (r.get("date") or "").strip()
            ex = (r.get("exception_type") or "").strip()
            if not sid or not d or not ex:
                continue
            if len(d) != 8:
                continue
            dt = date(int(d[0:4]), int(d[4:6]), int(d[6:8]))
            try:
                et = int(ex)
            except Exception:
                continue
            self.calendar_dates[(sid, dt)] = et

    def _load_stop_times(self) -> None:
        rows = self._load_csv("stop_times.txt")
        for r in rows:
            tid = (r.get("trip_id") or "").strip()
            sid = (r.get("stop_id") or "").strip()
            if not tid or not sid:
                continue
            arr = _parse_time_to_sec(r.get("arrival_time") or "")
            dep = _parse_time_to_sec(r.get("departure_time") or "")
            try:
                seq = int((r.get("stop_sequence") or "0").strip())
            except Exception:
                seq = 0
            st = StopTime(trip_id=tid, stop_id=sid, arrival_sec=arr, departure_sec=dep, stop_sequence=seq)
            self.stop_times_by_stop.setdefault(sid, []).append(st)
            self.stop_times_by_trip.setdefault(tid, []).append(st)

    def _finalize(self) -> None:
        # sort
        for sid, lst in self.stop_times_by_stop.items():
            lst.sort(key=lambda x: (x.departure_sec if x.departure_sec is not None else 10**9, x.stop_sequence))
        for tid, lst in self.stop_times_by_trip.items():
            lst.sort(key=lambda x: x.stop_sequence)
            if lst:
                self.last_seq_by_trip[tid] = lst[-1].stop_sequence

    def gtfs_stats(self) -> Dict[str, Any]:
        return {
            "ok": self.loaded_ok,
            "stops": len(self.stops),
            "trips": len(self.trips),
            "routes": len(self.routes),
            "calendar_services": len(self.calendar),
            "calendar_dates": len(self.calendar_dates),
            "errors": self.load_errors[-3:],
        }

    def is_service_active(self, service_id: str, svc_date: date, ignore_calendar: bool = False) -> bool:
        if ignore_calendar:
            return True

        # Calendar_dates overrides
        ex = self.calendar_dates.get((service_id, svc_date))
        if ex == 1:
            return True
        if ex == 2:
            return False

        cal = self.calendar.get(service_id)
        if not cal:
            # if no calendar.txt, still allow if calendar ignored; else false
            return False

        start = cal.get("start")
        end = cal.get("end")
        if start and svc_date < start:
            return False
        if end and svc_date > end:
            return False

        wd = svc_date.weekday()  # 0 Monday
        wk = cal.get("wk") or {}
        keys = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        return bool(wk.get(keys[wd], False))

    def search_stops(self, q: str, limit: int = 30) -> List[Dict[str, Any]]:
        qn = _norm(q)
        if not qn:
            return []
        hits = []
        for s in self.stops.values():
            name_n = _norm(s.stop_name)
            id_n = _norm(s.stop_id)
            score = 0
            if qn in name_n:
                score += 100
                # prefer startswith
                if name_n.startswith(qn):
                    score += 30
            if qn in id_n:
                score += 60
            if score > 0:
                hits.append((score, s))
        hits.sort(key=lambda x: (-x[0], x[1].stop_name))
        out = []
        for _, s in hits[:limit]:
            out.append({
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "lat": s.lat,
                "lon": s.lon,
            })
        return out

    def nearby_stops(self, lat: float, lon: float, radius_m: int = 700, limit: int = 25) -> List[Dict[str, Any]]:
        # rough: haversine
        def hav_m(lat1, lon1, lat2, lon2):
            R = 6371000.0
            phi1 = math.radians(lat1)
            phi2 = math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dl = math.radians(lon2 - lon1)
            a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
            c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
            return R*c

        res = []
        for s in self.stops.values():
            if s.lat is None or s.lon is None:
                continue
            d = hav_m(lat, lon, s.lat, s.lon)
            if d <= radius_m:
                res.append((d, s))
        res.sort(key=lambda x: x[0])
        out = []
        for d, s in res[:limit]:
            out.append({
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "lat": s.lat,
                "lon": s.lon,
                "distance_m": int(d),
            })
        return out

    def get_departures(
        self,
        stop_id: str,
        now_dt: datetime,
        window_min: int = 60
    ) -> Dict[str, Any]:
        stop = self.stops.get(stop_id)
        if not stop:
            return {"stop_id": stop_id, "stop_name": None, "departures": [], "calendar_ignored": False}

        window_min = max(10, min(window_min, 240))
        window_end = now_dt + timedelta(minutes=window_min)

        # if GTFS calendar expired, allow ignore mode
        today = now_dt.date()
        ignore_calendar = False
        if self.max_cal_end and today > self.max_cal_end:
            ignore_calendar = True
        if self.min_cal_start and today < self.min_cal_start:
            ignore_calendar = True

        rows: List[Dict[str, Any]] = []

        st_list = self.stop_times_by_stop.get(stop_id, [])
        for st in st_list:
            trip = self.trips.get(st.trip_id)
            if not trip:
                continue

            # BUGFIX #1: drop trips that END at this stop (last stop_sequence)
            last_seq = self.last_seq_by_trip.get(st.trip_id)
            if last_seq is not None and st.stop_sequence >= last_seq:
                continue

            sec = st.departure_sec if st.departure_sec is not None else st.arrival_sec
            if sec is None:
                # still possible to show "unknown time" but departures list should be time-based
                continue

            # consider service_date = today and tomorrow (window can cross midnight)
            candidates: List[Tuple[date, datetime]] = []
            for svc in [today, today + timedelta(days=1)]:
                dt_sched = _dt_from_service_date_and_seconds(svc, sec)
                if now_dt - timedelta(minutes=5) <= dt_sched <= window_end:
                    candidates.append((svc, dt_sched))

            if not candidates:
                continue

            route = self.routes.get(trip.get("route_id") or "", {})
            line = (route.get("route_short_name") or "").strip() or (trip.get("route_id") or "")
            headsign = (trip.get("trip_headsign") or "").strip()

            for svc_date, dt_sched in candidates:
                service_id = (trip.get("service_id") or "").strip()
                if service_id and not self.is_service_active(service_id, svc_date, ignore_calendar=ignore_calendar):
                    continue

                mins_to = int(round((dt_sched - now_dt).total_seconds() / 60.0))
                sched_time = dt_sched.strftime("%H:%M")

                rows.append({
                    "trip_id": st.trip_id,
                    "service_date": svc_date.isoformat(),
                    "sched_dt": dt_sched.isoformat(),
                    "sched_time": sched_time,
                    "mins_to": mins_to,
                    "line": str(line),
                    "headsign": headsign,
                    "stop_id": stop_id,
                    "stop_name": stop.stop_name,
                    "status": "timetable",
                })

        rows.sort(key=lambda r: r["sched_dt"])
        return {
            "stop_id": stop_id,
            "stop_name": stop.stop_name,
            "now": now_dt.isoformat(),
            "window_min": window_min,
            "calendar_ignored": ignore_calendar,
            "departures": rows,
        }

    def get_trip_stops(self, trip_id: str) -> List[Dict[str, Any]]:
        st_list = self.stop_times_by_trip.get(trip_id, [])
        out = []
        for st in st_list:
            stop = self.stops.get(st.stop_id)
            sec = st.departure_sec if st.departure_sec is not None else st.arrival_sec
            out.append({
                "stop_id": st.stop_id,
                "stop_name": stop.stop_name if stop else st.stop_id,
                "stop_sequence": st.stop_sequence,
                "time_sec": sec,  # may be None
            })
        out.sort(key=lambda x: x["stop_sequence"])
        return out


GTFS = GTFSStore(GTFS_DIR)
GTFS.load()


# ----------------------------
# SIRI (VehicleMonitoring) live
# ----------------------------
_LIVE_CACHE: Dict[str, Any] = {"ts": 0.0, "vehicles": []}


def _siri_url() -> Optional[str]:
    if not DFT_API_KEY:
        return None
    return DFT_URL_TEMPLATE.format(feed_id=DFT_FEED_ID, api_key=DFT_API_KEY)


def _find_child(parent: ET.Element, tag_end: str) -> Optional[ET.Element]:
    for ch in list(parent):
        if ch.tag.endswith(tag_end):
            return ch
    return None


def _findall_children(parent: ET.Element, tag_end: str) -> List[ET.Element]:
    out = []
    for ch in list(parent):
        if ch.tag.endswith(tag_end):
            out.append(ch)
    return out


def _get_text(node: Optional[ET.Element]) -> Optional[str]:
    if node is None or node.text is None:
        return None
    s = node.text.strip()
    return s if s else None


def _get_text_child(parent: Optional[ET.Element], tag_end: str) -> Optional[str]:
    if parent is None:
        return None
    ch = _find_child(parent, tag_end)
    return _get_text(ch)


def _parse_call(call_el: ET.Element) -> Dict[str, Any]:
    # StopPointRef, AimedArrivalTime, ExpectedArrivalTime, AimedDepartureTime, ExpectedDepartureTime
    sp = _get_text_child(call_el, "StopPointRef")
    aimed_arr = _parse_iso_dt(_get_text_child(call_el, "AimedArrivalTime"))
    exp_arr = _parse_iso_dt(_get_text_child(call_el, "ExpectedArrivalTime"))
    aimed_dep = _parse_iso_dt(_get_text_child(call_el, "AimedDepartureTime"))
    exp_dep = _parse_iso_dt(_get_text_child(call_el, "ExpectedDepartureTime"))

    # prefer departure for “departures list”
    aimed = aimed_dep or aimed_arr
    expected = exp_dep or exp_arr

    return {
        "stop_ref": sp,
        "aimed": aimed,
        "expected": expected,
        "aimed_iso": aimed.isoformat() if aimed else None,
        "expected_iso": expected.isoformat() if expected else None,
    }


def fetch_siri_journeys() -> List[Dict[str, Any]]:
    # cache
    now = time.time()
    if _LIVE_CACHE["vehicles"] and (now - _LIVE_CACHE["ts"] < LIVE_CACHE_TTL_SEC):
        return _LIVE_CACHE["vehicles"]

    url = _siri_url()
    if not url:
        _LIVE_CACHE["vehicles"] = []
        _LIVE_CACHE["ts"] = now
        return []

    try:
        with httpx.Client(timeout=LIVE_HTTP_TIMEOUT) as client:
            r = client.get(url, headers={"Accept": "*/*"})
            r.raise_for_status()
            content = r.content
    except Exception:
        _LIVE_CACHE["vehicles"] = []
        _LIVE_CACHE["ts"] = now
        return []

    vehicles: List[Dict[str, Any]] = []

    try:
        root = ET.fromstring(content)
    except Exception:
        _LIVE_CACHE["vehicles"] = []
        _LIVE_CACHE["ts"] = now
        return []

    # Find all VehicleActivity nodes regardless of namespace
    for va in root.iter():
        if not va.tag.endswith("VehicleActivity"):
            continue

        recorded_at = _parse_iso_dt(_get_text_child(va, "RecordedAtTime"))

        mvj = None
        for ch in va.iter():
            if ch.tag.endswith("MonitoredVehicleJourney"):
                mvj = ch
                break
        if mvj is None:
            continue

        vehicle_id = _get_text_child(mvj, "VehicleRef") or _get_text_child(mvj, "VehicleId") or _get_text_child(mvj, "Vehicle")

        line = _get_text_child(mvj, "PublishedLineName") or _get_text_child(mvj, "LineRef") or ""
        destination = _get_text_child(mvj, "DestinationName") or _get_text_child(mvj, "DestinationRef") or ""

        delay_raw = _get_text_child(mvj, "Delay")
        delay_sec = _parse_duration_to_seconds(delay_raw)

        dated_vjr = _get_text_child(mvj, "DatedVehicleJourneyRef")
        vjr = _get_text_child(mvj, "VehicleJourneyRef")

        # VehicleLocation
        lat = lon = None
        vloc = None
        for ch in mvj.iter():
            if ch.tag.endswith("VehicleLocation"):
                vloc = ch
                break
        if vloc is not None:
            try:
                lat_s = _get_text_child(vloc, "Latitude")
                lon_s = _get_text_child(vloc, "Longitude")
                if lat_s is not None:
                    lat = float(lat_s)
                if lon_s is not None:
                    lon = float(lon_s)
            except Exception:
                lat = lon = None

        # MonitoredCall + OnwardCalls
        monitored_call = None
        onward_calls: List[Dict[str, Any]] = []

        mc_el = _find_child(mvj, "MonitoredCall")
        if mc_el is not None:
            monitored_call = _parse_call(mc_el)

        oc_el = _find_child(mvj, "OnwardCalls")
        if oc_el is not None:
            for call_el in oc_el.iter():
                if call_el.tag.endswith("OnwardCall"):
                    onward_calls.append(_parse_call(call_el))

        vehicles.append({
            "vehicle_id": vehicle_id or "",
            "line": str(line),
            "destination": str(destination),
            "lat": lat,
            "lon": lon,
            "recorded_at": recorded_at.isoformat() if recorded_at else None,
            "delay_sec": delay_sec,
            "dated_vehicle_journey_ref": dated_vjr,
            "vehicle_journey_ref": vjr,
            "monitored_call": monitored_call,
            "onward_calls": onward_calls,
        })

    _LIVE_CACHE["vehicles"] = vehicles
    _LIVE_CACHE["ts"] = now
    return vehicles


def _live_calls_index_for_stop(stop_id: str) -> List[Dict[str, Any]]:
    """
    Build a list of live call predictions for a given stop_id using:
    - MonitoredCall (if its stop matches)
    - OnwardCalls (if any call matches)
    """
    vehicles = fetch_siri_journeys()
    out: List[Dict[str, Any]] = []
    for v in vehicles:
        calls = []
        mc = v.get("monitored_call")
        if mc and mc.get("stop_ref"):
            calls.append(mc)
        for oc in v.get("onward_calls") or []:
            if oc.get("stop_ref"):
                calls.append(oc)

        for c in calls:
            if (c.get("stop_ref") or "").strip() != stop_id:
                continue
            out.append({
                "vehicle_id": v.get("vehicle_id") or "",
                "line": v.get("line") or "",
                "destination": v.get("destination") or "",
                "aimed_dt": _parse_iso_dt(c.get("aimed_iso")),
                "expected_dt": _parse_iso_dt(c.get("expected_iso")),
                "aimed_iso": c.get("aimed_iso"),
                "expected_iso": c.get("expected_iso"),
                "vehicle_journey_ref": v.get("vehicle_journey_ref"),
                "dated_vehicle_journey_ref": v.get("dated_vehicle_journey_ref"),
            })
    # sort by expected/aimed
    def key_fn(x):
        dt = x.get("expected_dt") or x.get("aimed_dt")
        return dt.isoformat() if dt else "9999"
    out.sort(key=key_fn)
    return out


def _match_live_to_departure(dep: Dict[str, Any], live_calls: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Heuristic match by:
    - same line
    - closest aimed/expected time to scheduled dt
    """
    sched_dt = _parse_iso_dt(dep.get("sched_dt"))
    if not sched_dt:
        return None

    dep_line = _norm(str(dep.get("line") or ""))
    best = None
    best_score = -1

    for lc in live_calls:
        lc_line = _norm(str(lc.get("line") or ""))
        if dep_line and lc_line and dep_line != lc_line:
            continue

        dt = lc.get("expected_dt") or lc.get("aimed_dt")
        if not dt:
            continue
        diff_min = abs((dt - sched_dt).total_seconds()) / 60.0

        # score (rough)
        if diff_min <= 2:
            score = 100
        elif diff_min <= 5:
            score = 85
        elif diff_min <= 10:
            score = 70
        elif diff_min <= 20:
            score = 55
        elif diff_min <= 35:
            score = 40
        else:
            score = 0

        if score > best_score:
            best_score = score
            best = dict(lc)
            best["score"] = score
            best["diff_min"] = diff_min

    return best if best_score >= 40 else None


def _status_from_live(now_dt: datetime, sched_dt: datetime, expected_dt: Optional[datetime]) -> Tuple[str, Optional[int], Optional[int]]:
    """
    Returns (status, mins_to, delta_min)
    """
    if not expected_dt:
        mins_to = int(round((sched_dt - now_dt).total_seconds() / 60.0))
        return ("timetable", mins_to, None)

    mins_to = int(round((expected_dt - now_dt).total_seconds() / 60.0))
    delta_min = int(round((expected_dt - sched_dt).total_seconds() / 60.0))

    # Due = within +-1 minute
    if -1 <= mins_to <= 1:
        return ("due", mins_to, delta_min)

    if delta_min >= 1:
        return ("late", mins_to, delta_min)
    if delta_min <= -1:
        return ("early", mins_to, delta_min)
    return ("live", mins_to, delta_min)


def _choose_vehicle_for_trip(
    stop_id: str,
    line: str,
    sched_dt_iso: str
) -> Optional[Dict[str, Any]]:
    """
    Given the clicked departure (stop_id + line + sched_dt),
    pick the best matching live vehicle using call predictions at this stop.
    """
    sched_dt = _parse_iso_dt(sched_dt_iso)
    if not sched_dt:
        return None

    live_calls = _live_calls_index_for_stop(stop_id)
    fake_dep = {"line": line, "sched_dt": sched_dt.isoformat()}
    match = _match_live_to_departure(fake_dep, live_calls)
    if not match:
        return None

    # Find full vehicle object by journey refs (best effort)
    vehicles = fetch_siri_journeys()
    for v in vehicles:
        if match.get("vehicle_journey_ref") and v.get("vehicle_journey_ref") == match.get("vehicle_journey_ref"):
            return {"vehicle": v, "match": match}
        if match.get("dated_vehicle_journey_ref") and v.get("dated_vehicle_journey_ref") == match.get("dated_vehicle_journey_ref"):
            return {"vehicle": v, "match": match}

    # fallback: match by vehicle_id
    for v in vehicles:
        if (v.get("vehicle_id") or "") == (match.get("vehicle_id") or ""):
            return {"vehicle": v, "match": match}

    return {"vehicle": None, "match": match}


# ----------------------------
# FastAPI
# ----------------------------
app = FastAPI(title="Bluestar Bus API")

# static (optional)
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
def root():
    # serve root index.html if exists
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return HTMLResponse("<h1>Missing index.html</h1>")


@app.get("/api/gtfs/stats")
def api_gtfs_stats():
    return GTFS.gtfs_stats()


@app.get("/api/stops/search")
def api_stops_search(q: str = Query("", min_length=0), limit: int = 30):
    limit = max(1, min(limit, 100))
    return {"q": q, "results": GTFS.search_stops(q, limit=limit)}


@app.get("/api/stops/nearby")
def api_stops_nearby(lat: float, lon: float, radius_m: int = 700, limit: int = 25):
    radius_m = max(100, min(radius_m, 5000))
    limit = max(1, min(limit, 100))
    return {"results": GTFS.nearby_stops(lat, lon, radius_m=radius_m, limit=limit)}


@app.get("/api/stop/departures")
def api_stop_departures(stop_id: str, window_min: int = 60):
    now_dt = _now_dt()
    base = GTFS.get_departures(stop_id, now_dt=now_dt, window_min=window_min)

    # live matching for this stop
    live_calls = _live_calls_index_for_stop(stop_id)

    deps = []
    for d in base["departures"]:
        sched_dt = _parse_iso_dt(d.get("sched_dt"))
        match = _match_live_to_departure(d, live_calls)
        expected_dt = match.get("expected_dt") if match else None

        status, mins_to, delta_min = _status_from_live(now_dt, sched_dt, expected_dt) if sched_dt else ("timetable", d.get("mins_to"), None)

        # timetable due (optional) if no live: show Due if very close
        if status == "timetable" and mins_to is not None and -1 <= mins_to <= 1:
            # keep your "Due" feature (timetable-based)
            status = "due_timetable"

        deps.append({
            **d,
            "live_expected_dt": expected_dt.isoformat() if expected_dt else None,
            "live_expected_time": expected_dt.strftime("%H:%M") if expected_dt else None,
            "delta_min": delta_min,
            "mins_to": mins_to,
            "status": status,
            "fleet": match.get("vehicle_id") if match else None,
            "live_score": match.get("score") if match else None,
        })

    base["departures"] = deps
    base["live_matches_for_stop"] = len(live_calls)
    base["cached_ttl_sec"] = LIVE_CACHE_TTL_SEC
    return base


@app.get("/api/vehicles")
def api_vehicles(line: str = "", max_results: str = "250"):
    mr = _parse_int_loose(max_results, 250)
    mr = max(1, min(mr, 500))

    qline = _norm(line)
    journeys = fetch_siri_journeys()
    if qline:
        journeys = [v for v in journeys if qline in _norm(str(v.get("line") or ""))]
    journeys = journeys[:mr]

    # shrink payload a bit for map use
    vehicles = []
    for v in journeys:
        vehicles.append({
            "vehicle_id": v.get("vehicle_id") or "",
            "line": v.get("line") or "",
            "destination": v.get("destination") or "",
            "lat": v.get("lat"),
            "lon": v.get("lon"),
            "recorded_at": v.get("recorded_at"),
            "delay_sec": v.get("delay_sec"),  # may be null
            "dated_vehicle_journey_ref": v.get("dated_vehicle_journey_ref"),
            "vehicle_journey_ref": v.get("vehicle_journey_ref"),
        })
    return {"count": len(vehicles), "vehicles": vehicles, "cached_ttl_sec": LIVE_CACHE_TTL_SEC}


@app.get("/api/trip")
def api_trip(
    trip_id: str,
    service_date: str,
    from_stop_id: str = "",
    line: str = "",
    sched_dt: str = "",
):
    """
    Trip view: show ALL stops (even if some have no GTFS time).
    If from_stop_id + line + sched_dt provided, we'll try to attach a live vehicle and onward ETAs.
    """
    try:
        svc = date.fromisoformat(service_date)
    except Exception:
        svc = _now_dt().date()

    trip_meta = GTFS.trips.get(trip_id) or {}
    route = GTFS.routes.get(trip_meta.get("route_id") or "", {})
    route_short = (route.get("route_short_name") or "").strip()
    headsign = (trip_meta.get("trip_headsign") or "").strip()

    now_dt = _now_dt()

    # get stop list (ALL)
    stops = GTFS.get_trip_stops(trip_id)

    # choose vehicle
    chosen = None
    if from_stop_id and line and sched_dt:
        chosen = _choose_vehicle_for_trip(from_stop_id, line, sched_dt)

    fleet = None
    live_mode = None
    overall_delta_min = None
    onward_map: Dict[str, datetime] = {}

    if chosen and chosen.get("match"):
        m = chosen["match"]
        fleet = m.get("vehicle_id") or None
        live_mode = f"heuristic ({m.get('score')})"
        # delta at origin stop if expected exists
        origin_expected = m.get("expected_dt")
        origin_sched = _parse_iso_dt(sched_dt)
        if origin_expected and origin_sched:
            overall_delta_min = int(round((origin_expected - origin_sched).total_seconds() / 60.0))

        v = chosen.get("vehicle")
        if v:
            # build expected times map from monitored_call + onward_calls
            all_calls = []
            mc = v.get("monitored_call")
            if mc and mc.get("stop_ref"):
                all_calls.append(mc)
            for oc in v.get("onward_calls") or []:
                if oc.get("stop_ref"):
                    all_calls.append(oc)

            for c in all_calls:
                sp = (c.get("stop_ref") or "").strip()
                exp = _parse_iso_dt(c.get("expected_iso"))
                if sp and exp:
                    onward_map[sp] = exp

    # build rows with sched + live
    out_rows = []
    for st in stops:
        sec = st.get("time_sec")
        sched_dt_row = _dt_from_service_date_and_seconds(svc, sec) if sec is not None else None
        exp_dt = onward_map.get(st["stop_id"])

        # status + mins + delta
        if sched_dt_row:
            status, mins_to, delta_min = _status_from_live(now_dt, sched_dt_row, exp_dt)
        else:
            # no schedule time
            status = "no_time"
            mins_to = int(round((exp_dt - now_dt).total_seconds() / 60.0)) if exp_dt else None
            delta_min = None

        out_rows.append({
            "stop_sequence": st["stop_sequence"],
            "stop_id": st["stop_id"],
            "stop_name": st["stop_name"],
            "sched_time": sched_dt_row.strftime("%H:%M") if sched_dt_row else None,
            "expected_time": exp_dt.strftime("%H:%M") if exp_dt else None,
            "mins_to": mins_to,
            "delta_min": delta_min,
            "status": status,
        })

    return {
        "trip_id": trip_id,
        "service_date": svc.isoformat(),
        "route_short": route_short,
        "headsign": headsign,
        "fleet": fleet,
        "live_mode": live_mode,
        "overall_delta_min": overall_delta_min,
        "now": now_dt.isoformat(),
        "stops": out_rows,
        "cached_ttl_sec": LIVE_CACHE_TTL_SEC,
    }
