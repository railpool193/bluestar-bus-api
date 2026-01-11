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
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles


# ----------------------------
# Settings
# ----------------------------
APP_TZ = ZoneInfo(os.getenv("APP_TZ", "Europe/London"))

GTFS_DIR = os.getenv("GTFS_DIR", "gtfs")

DFT_FEED_ID = os.getenv("DFT_FEED_ID", "7721")
DFT_API_KEY = os.getenv("DFT_API_KEY", "").strip()

LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "12"))
LIVE_HTTP_TIMEOUT = float(os.getenv("LIVE_HTTP_TIMEOUT", "12"))

PAST_GRACE_MIN = int(os.getenv("PAST_GRACE_MIN", "2"))  # within 2 minutes in past -> show as Due (not -2)
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


def _dt_from_service_date_and_seconds(svc: date, sec: int) -> datetime:
    days = sec // 86400
    rem = sec % 86400
    base = datetime(svc.year, svc.month, svc.day, tzinfo=APP_TZ) + timedelta(days=days)
    return base + timedelta(seconds=rem)


def _parse_duration_to_seconds(dur: Optional[str]) -> Optional[int]:
    if not dur:
        return None
    s = dur.strip()
    sign = 1
    if s.startswith("-"):
        sign = -1
        s = s[1:]
    if not s.startswith("P"):
        return None
    m = re.match(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$", s)
    if not m:
        m2 = re.match(r"^P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", s)
        if not m2:
            return None
        m = m2
    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)
    return sign * (days * 86400 + hours * 3600 + minutes * 60 + seconds)


def _parse_int_loose(v: Any, default: int) -> int:
    s = str(v or "").strip()
    m = re.search(r"-?\d+", s)
    if not m:
        return default
    try:
        return int(m.group(0))
    except Exception:
        return default


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


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

        self.calendar: Dict[str, Dict[str, Any]] = {}
        self.calendar_dates: Dict[Tuple[str, date], int] = {}
        self.min_cal_start: Optional[date] = None
        self.max_cal_end: Optional[date] = None

        self.loaded_ok = False
        self.load_errors: List[str] = []

    def _path(self, filename: str) -> str:
        return os.path.join(self.gtfs_dir, filename)

    def _load_csv(self, filename: str) -> List[Dict[str, str]]:
        path = self._path(filename)
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

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

    def _load_routes(self) -> None:
        rows = self._load_csv("routes.txt")
        for r in rows:
            rid = (r.get("route_id") or "").strip()
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

        ex = self.calendar_dates.get((service_id, svc_date))
        if ex == 1:
            return True
        if ex == 2:
            return False

        cal = self.calendar.get(service_id)
        if not cal:
            return False

        start = cal.get("start")
        end = cal.get("end")
        if start and svc_date < start:
            return False
        if end and svc_date > end:
            return False

        wd = svc_date.weekday()
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
                if name_n.startswith(qn):
                    score += 30
            if qn in id_n:
                score += 60
            if score > 0:
                hits.append((score, s))
        hits.sort(key=lambda x: (-x[0], x[1].stop_name))
        out = []
        for _, s in hits[:limit]:
            out.append({"stop_id": s.stop_id, "stop_name": s.stop_name, "lat": s.lat, "lon": s.lon})
        return out

    def nearby_stops(self, lat: float, lon: float, radius_m: int = 700, limit: int = 25) -> List[Dict[str, Any]]:
        res = []
        for s in self.stops.values():
            if s.lat is None or s.lon is None:
                continue
            d = _haversine_m(lat, lon, s.lat, s.lon)
            if d <= radius_m:
                res.append((d, s))
        res.sort(key=lambda x: x[0])
        out = []
        for d, s in res[:limit]:
            out.append(
                {"stop_id": s.stop_id, "stop_name": s.stop_name, "lat": s.lat, "lon": s.lon, "distance_m": int(d)}
            )
        return out

    def get_departures(self, stop_id: str, now_dt: datetime, window_min: int = 60) -> Dict[str, Any]:
        stop = self.stops.get(stop_id)
        if not stop:
            return {"stop_id": stop_id, "stop_name": None, "departures": [], "calendar_ignored": False}

        window_min = max(10, min(window_min, 240))
        window_end = now_dt + timedelta(minutes=window_min)

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

            # do not show "trip ends here" rows
            last_seq = self.last_seq_by_trip.get(st.trip_id)
            if last_seq is not None and st.stop_sequence >= last_seq:
                continue

            sec = st.departure_sec if st.departure_sec is not None else st.arrival_sec
            if sec is None:
                continue

            for svc in [today, today + timedelta(days=1)]:
                dt_sched = _dt_from_service_date_and_seconds(svc, sec)

                # FIX: do not show past items, only allow small grace window
                if dt_sched < now_dt - timedelta(minutes=PAST_GRACE_MIN):
                    continue
                if dt_sched > window_end:
                    continue

                service_id = (trip.get("service_id") or "").strip()
                if service_id and not self.is_service_active(service_id, svc, ignore_calendar=ignore_calendar):
                    continue

                route = self.routes.get(trip.get("route_id") or "", {})
                line = (route.get("route_short_name") or "").strip() or (trip.get("route_id") or "")
                headsign = (trip.get("trip_headsign") or "").strip()

                mins_to = int(round((dt_sched - now_dt).total_seconds() / 60.0))
                rows.append(
                    {
                        "trip_id": st.trip_id,
                        "service_date": svc.isoformat(),
                        "sched_dt": dt_sched.isoformat(),
                        "sched_time": dt_sched.strftime("%H:%M"),
                        "mins_to": mins_to,
                        "line": str(line),
                        "headsign": headsign,
                        "stop_id": stop_id,
                        "stop_name": stop.stop_name,
                        "status": "timetable",
                    }
                )

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
            out.append(
                {
                    "stop_id": st.stop_id,
                    "stop_name": stop.stop_name if stop else st.stop_id,
                    "stop_sequence": st.stop_sequence,
                    "time_sec": sec,
                    "lat": stop.lat if stop else None,
                    "lon": stop.lon if stop else None,
                }
            )
        out.sort(key=lambda x: x["stop_sequence"])
        return out


GTFS = GTFSStore(GTFS_DIR)
GTFS.load()


# ----------------------------
# SIRI VM live (DFT)
# ----------------------------
_LIVE_CACHE: Dict[str, Any] = {"ts": 0.0, "vehicles": []}


def _siri_url() -> Optional[str]:
    if not DFT_API_KEY:
        return None
    return DFT_URL_TEMPLATE.format(feed_id=DFT_FEED_ID, api_key=DFT_API_KEY)


def _get_text(node: Optional[ET.Element]) -> Optional[str]:
    if node is None or node.text is None:
        return None
    s = node.text.strip()
    return s if s else None


def _find_child(parent: ET.Element, tag_end: str) -> Optional[ET.Element]:
    for ch in list(parent):
        if ch.tag.endswith(tag_end):
            return ch
    return None


def _get_text_child(parent: Optional[ET.Element], tag_end: str) -> Optional[str]:
    if parent is None:
        return None
    return _get_text(_find_child(parent, tag_end))


def _safe_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_call(call_el: ET.Element) -> Dict[str, Any]:
    sp = _get_text_child(call_el, "StopPointRef")
    aimed_arr = _parse_iso_dt(_get_text_child(call_el, "AimedArrivalTime"))
    exp_arr = _parse_iso_dt(_get_text_child(call_el, "ExpectedArrivalTime"))
    aimed_dep = _parse_iso_dt(_get_text_child(call_el, "AimedDepartureTime"))
    exp_dep = _parse_iso_dt(_get_text_child(call_el, "ExpectedDepartureTime"))
    aimed = aimed_dep or aimed_arr
    expected = exp_dep or exp_arr
    return {
        "stop_ref": sp,
        "aimed_iso": aimed.isoformat() if aimed else None,
        "expected_iso": expected.isoformat() if expected else None,
    }


def fetch_siri_journeys() -> List[Dict[str, Any]]:
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

    try:
        root = ET.fromstring(content)
    except Exception:
        _LIVE_CACHE["vehicles"] = []
        _LIVE_CACHE["ts"] = now
        return []

    vehicles: List[Dict[str, Any]] = []

    for va in root.iter():
        if not va.tag.endswith("VehicleActivity"):
            continue

        recorded_at = _parse_iso_dt(_get_text_child(va, "RecordedAtTime"))
        valid_until = _parse_iso_dt(_get_text_child(va, "ValidUntilTime"))

        mvj = None
        for ch in va.iter():
            if ch.tag.endswith("MonitoredVehicleJourney"):
                mvj = ch
                break
        if mvj is None:
            continue

        line_ref = _get_text_child(mvj, "LineRef") or ""
        line_pub = _get_text_child(mvj, "PublishedLineName") or line_ref or ""
        direction = _get_text_child(mvj, "DirectionRef")
        operator_ref = _get_text_child(mvj, "OperatorRef")

        origin_ref = _get_text_child(mvj, "OriginRef")
        origin_name = _get_text_child(mvj, "OriginName")
        dest_ref = _get_text_child(mvj, "DestinationRef")
        dest_name = _get_text_child(mvj, "DestinationName")

        # These exist in your screenshots:
        origin_aimed_dep = _parse_iso_dt(_get_text_child(mvj, "OriginAimedDepartureTime"))
        dest_aimed_arr = _parse_iso_dt(_get_text_child(mvj, "DestinationAimedArrivalTime"))

        data_frame_ref = _get_text_child(mvj, "DataFrameRef")
        data_frame_date = None
        if data_frame_ref:
            try:
                data_frame_date = date.fromisoformat(data_frame_ref.strip())
            except Exception:
                data_frame_date = None

        dated_vjr = _get_text_child(mvj, "DatedVehicleJourneyRef")
        vjr = _get_text_child(mvj, "VehicleJourneyRef")
        delay_raw = _get_text_child(mvj, "Delay")
        delay_sec = _parse_duration_to_seconds(delay_raw)

        # Location
        lat = lon = None
        vloc = None
        for ch in mvj.iter():
            if ch.tag.endswith("VehicleLocation"):
                vloc = ch
                break
        if vloc is not None:
            lat = _safe_float(_get_text_child(vloc, "Latitude"))
            lon = _safe_float(_get_text_child(vloc, "Longitude"))

        bearing = _get_text_child(mvj, "Bearing")
        block_ref = _get_text_child(mvj, "BlockRef")
        vehicle_ref = _get_text_child(mvj, "VehicleRef") or _get_text_child(mvj, "VehicleId") or ""

        # Extensions (JourneyCode etc.)
        ticket_machine_service_code = None
        journey_code = None
        vehicle_unique_id = None

        ext = _find_child(mvj, "Extensions")
        if ext is not None:
            vj = _find_child(ext, "VehicleJourney")
            if vj is not None:
                operational = _find_child(vj, "Operational")
                if operational is not None:
                    tm = _find_child(operational, "TicketMachine")
                    if tm is not None:
                        ticket_machine_service_code = _get_text_child(tm, "TicketMachineServiceCode")
                        journey_code = _get_text_child(tm, "JourneyCode")
                vehicle_unique_id = _get_text_child(vj, "VehicleUniqueId")

        # Calls (often missing in your feed)
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

        vehicles.append(
            {
                "vehicle_id": vehicle_ref,
                "vehicle_unique_id": vehicle_unique_id,
                "line": str(line_pub),
                "line_ref": str(line_ref),
                "direction": direction,
                "operator_ref": operator_ref,
                "origin_ref": origin_ref,
                "origin_name": origin_name,
                "destination_ref": dest_ref,
                "destination": dest_name or "",
                "origin_aimed_departure_time": origin_aimed_dep.isoformat() if origin_aimed_dep else None,
                "destination_aimed_arrival_time": dest_aimed_arr.isoformat() if dest_aimed_arr else None,
                "data_frame_ref": data_frame_ref,
                "data_frame_date": data_frame_date.isoformat() if data_frame_date else None,
                "dated_vehicle_journey_ref": dated_vjr,
                "vehicle_journey_ref": vjr,
                "delay_sec": delay_sec,
                "lat": lat,
                "lon": lon,
                "bearing": bearing,
                "block_ref": block_ref,
                "ticket_machine_service_code": ticket_machine_service_code,
                "journey_code": journey_code,
                "recorded_at": recorded_at.isoformat() if recorded_at else None,
                "valid_until": valid_until.isoformat() if valid_until else None,
                "monitored_call": monitored_call,
                "onward_calls": onward_calls,
            }
        )

    _LIVE_CACHE["vehicles"] = vehicles
    _LIVE_CACHE["ts"] = now
    return vehicles


def _line_matches(query: str, line_value: str) -> bool:
    """
    FIX: "1" should match ONLY line 1 (not 16, 18).
    If query has multiple tokens separated by comma/space -> OR match.
    """
    q = (query or "").strip()
    if not q:
        return True

    tokens = [t for t in re.split(r"[,\s]+", q) if t.strip()]
    lv = (line_value or "").strip()

    # normalize versions
    lv_norm = _norm(lv)

    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        # numeric exact
        if re.fullmatch(r"\d+", tok):
            # compare digits only (so "01" equals "1")
            tok_i = int(tok)
            lv_digits = re.sub(r"\D+", "", lv)  # "18" from "18"
            if lv_digits and int(lv_digits) == tok_i:
                return True
        else:
            # alphanumeric: exact normalized match
            if _norm(tok) == lv_norm:
                return True
    return False


# ----------------------------
# FastAPI
# ----------------------------
app = FastAPI(title="Bluestar Bus API")

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
def root():
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

    deps_out = []
    for d in base["departures"]:
        mins_to = d.get("mins_to")
        status = d.get("status") or "timetable"

        # FIX: never show -2 min etc. -> Due within grace window
        if mins_to is not None and mins_to < 0:
            if mins_to >= -PAST_GRACE_MIN:
                mins_to = 0
                status = "due_timetable"
            else:
                continue

        deps_out.append({**d, "mins_to": mins_to, "status": status, "fleet": None, "delta_min": None})

    base["departures"] = deps_out
    base["cached_ttl_sec"] = LIVE_CACHE_TTL_SEC
    base["past_grace_min"] = PAST_GRACE_MIN
    return base


@app.get("/api/vehicles")
def api_vehicles(line: str = "", max_results: str = "250"):
    mr = _parse_int_loose(max_results, 250)
    mr = max(1, min(mr, 500))

    journeys = fetch_siri_journeys()
    if line.strip():
        journeys = [v for v in journeys if _line_matches(line, v.get("line") or "")]

    journeys = journeys[:mr]

    vehicles = []
    for v in journeys:
        vehicles.append(
            {
                "vehicle_id": v.get("vehicle_id") or "",
                "line": v.get("line") or "",
                "destination": v.get("destination") or "",
                "lat": v.get("lat"),
                "lon": v.get("lon"),
                "recorded_at": v.get("recorded_at"),
                "delay_sec": v.get("delay_sec"),
                "dated_vehicle_journey_ref": v.get("dated_vehicle_journey_ref"),
                "vehicle_journey_ref": v.get("vehicle_journey_ref"),
                "origin_aimed_departure_time": v.get("origin_aimed_departure_time"),
                "destination_aimed_arrival_time": v.get("destination_aimed_arrival_time"),
            }
        )
    return {"count": len(vehicles), "vehicles": vehicles, "cached_ttl_sec": LIVE_CACHE_TTL_SEC}


def _pick_vehicle_for_trip_gps(trip_id: str, service_date: date, line: str) -> Optional[Dict[str, Any]]:
    """
    Your DfT feed often has ONLY VehicleLocation (no Expected calls).
    Fallback matching:
      - match line exactly
      - match DataFrameRef date if present
      - match OriginAimedDepartureTime close to the trip start time (from GTFS)
    """
    stops = GTFS.get_trip_stops(trip_id)
    if not stops:
        return None

    # trip start from first timed stop
    first = None
    for s in stops:
        if s.get("time_sec") is not None:
            first = s
            break
    if not first:
        return None

    trip_start_dt = _dt_from_service_date_and_seconds(service_date, int(first["time_sec"]))

    candidates = []
    for v in fetch_siri_journeys():
        if not _line_matches(line, v.get("line") or ""):
            continue

        # filter by date if available
        df = v.get("data_frame_date")
        if df:
            try:
                if date.fromisoformat(df) != service_date:
                    continue
            except Exception:
                pass

        oadt = _parse_iso_dt(v.get("origin_aimed_departure_time"))
        if not oadt:
            continue

        diff_min = abs((oadt - trip_start_dt).total_seconds()) / 60.0
        if diff_min <= 25:
            candidates.append((diff_min, v))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


@app.get("/api/trip")
def api_trip(trip_id: str, service_date: str, line: str = ""):
    try:
        svc = date.fromisoformat(service_date)
    except Exception:
        svc = _now_dt().date()

    trip_meta = GTFS.trips.get(trip_id) or {}
    route = GTFS.routes.get(trip_meta.get("route_id") or "", {})
    route_short = (route.get("route_short_name") or "").strip()
    headsign = (trip_meta.get("trip_headsign") or "").strip()

    now_dt = _now_dt()
    stops = GTFS.get_trip_stops(trip_id)

    # vehicle fallback (GPS-only mode)
    chosen_vehicle = _pick_vehicle_for_trip_gps(trip_id, svc, line or route_short or "")

    fleet = chosen_vehicle.get("vehicle_id") if chosen_vehicle else None
    vehicle_lat = chosen_vehicle.get("lat") if chosen_vehicle else None
    vehicle_lon = chosen_vehicle.get("lon") if chosen_vehicle else None
    recorded_at = chosen_vehicle.get("recorded_at") if chosen_vehicle else None

    live_mode = "gps_only" if chosen_vehicle else None

    # Guess current stop by nearest distance
    current_guess_idx = None
    current_guess_dist_m = None
    if chosen_vehicle and vehicle_lat is not None and vehicle_lon is not None:
        best = None
        for i, st in enumerate(stops):
            if st.get("lat") is None or st.get("lon") is None:
                continue
            d = _haversine_m(vehicle_lat, vehicle_lon, float(st["lat"]), float(st["lon"]))
            if best is None or d < best[0]:
                best = (d, i)
        if best is not None:
            current_guess_dist_m, current_guess_idx = best

    # Estimate delay only if bus is very near a stop (<= 350m) AND that stop has a schedule time
    delay_est_min = None
    if current_guess_idx is not None and current_guess_dist_m is not None and current_guess_dist_m <= 350:
        st = stops[current_guess_idx]
        if st.get("time_sec") is not None:
            sched_dt_here = _dt_from_service_date_and_seconds(svc, int(st["time_sec"]))
            delay_est_min = int(round((now_dt - sched_dt_here).total_seconds() / 60.0))

    next_guess_idx = None
    if current_guess_idx is not None and current_guess_idx + 1 < len(stops):
        next_guess_idx = current_guess_idx + 1

    out_rows = []
    for i, st in enumerate(stops):
        sec = st.get("time_sec")
        sched_dt_row = _dt_from_service_date_and_seconds(svc, int(sec)) if sec is not None else None

        estimated_dt = None
        if sched_dt_row and delay_est_min is not None:
            estimated_dt = sched_dt_row + timedelta(minutes=delay_est_min)

        out_rows.append(
            {
                "stop_sequence": st["stop_sequence"],
                "stop_id": st["stop_id"],
                "stop_name": st["stop_name"],
                "lat": st.get("lat"),
                "lon": st.get("lon"),
                "sched_time": sched_dt_row.strftime("%H:%M") if sched_dt_row else None,
                "live_time": estimated_dt.strftime("%H:%M") if estimated_dt else None,
                "mins_to": int(round((estimated_dt - now_dt).total_seconds() / 60.0)) if estimated_dt else None,
                "delta_min": delay_est_min,
                "status": "gps_est" if estimated_dt else "no_live",
                "is_current_guess": (i == current_guess_idx),
                "is_next_guess": (i == next_guess_idx),
            }
        )

    return {
        "trip_id": trip_id,
        "service_date": svc.isoformat(),
        "route_short": route_short,
        "headsign": headsign,
        "line": line or route_short,
        "fleet": fleet,
        "live_mode": live_mode,
        "recorded_at": recorded_at,
        "vehicle_lat": vehicle_lat,
        "vehicle_lon": vehicle_lon,
        "delay_est_min": delay_est_min,
        "current_guess_dist_m": int(current_guess_dist_m) if current_guess_dist_m is not None else None,
        "now": now_dt.isoformat(),
        "stops": out_rows,
        "cached_ttl_sec": LIVE_CACHE_TTL_SEC,
    }
