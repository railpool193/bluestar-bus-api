from __future__ import annotations

import csv
import io
import zipfile
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from app.services.gtfs_calendar import active_service_ids
from app.utils.text_utils import clean_text, human_name, line_norm, public_stop_code, safe_float, safe_int, short_destination


class GTFSStore:
    def __init__(self, *, zip_path: Optional[Path] = None, directory_path: Optional[Path] = None):
        self.zip_path = Path(zip_path) if zip_path is not None else None
        self.directory_path = Path(directory_path) if directory_path is not None else None
        self._reset()

    def _reset(self) -> None:
        self.loaded = False
        self.error = ""
        self.source = ""
        self.agency: Dict[str, Dict[str, Any]] = {}
        self.stops: Dict[str, Dict[str, Any]] = {}
        self.routes: Dict[str, Dict[str, Any]] = {}
        self.trips: Dict[str, Dict[str, Any]] = {}
        self.calendar: Dict[str, Dict[str, Any]] = {}
        self.calendar_dates: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.stop_departures_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.shapes: Dict[str, List[Dict[str, float]]] = defaultdict(list)
        self.route_by_short: Dict[str, List[str]] = defaultdict(list)

    @staticmethod
    def _read_zip(archive: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
        target = next((entry for entry in archive.namelist() if entry.lower() == name.lower() or entry.lower().endswith("/" + name.lower())), None)
        if not target:
            return []
        return list(csv.DictReader(io.StringIO(archive.read(target).decode("utf-8-sig", errors="replace"))))

    @staticmethod
    def _read_directory(folder: Path, name: str) -> List[Dict[str, str]]:
        path = folder / name
        if not path.exists():
            return []
        return list(csv.DictReader(io.StringIO(path.read_text(encoding="utf-8-sig", errors="replace"))))

    def load(self, *, zip_path: Optional[Path] = None, directory_path: Optional[Path] = None):
        selected_zip = Path(zip_path) if zip_path is not None else self.zip_path
        selected_directory = Path(directory_path) if directory_path is not None else self.directory_path
        if selected_zip is not None and selected_zip.is_file():
            return self.load_from_path(selected_zip)
        if selected_directory is not None and selected_directory.is_dir():
            return self.load_from_directory(selected_directory)
        self._reset()
        self.error = "GTFS source not found"
        return self

    def load_from_path(self, path: Path):
        self._reset()
        candidate = Path(path)
        try:
            if not candidate.is_file():
                raise FileNotFoundError(f"GTFS ZIP not found: {candidate}")
            self.source = f"zip:{candidate.name}"
            with zipfile.ZipFile(candidate) as archive:
                self._load_tables(lambda name: self._read_zip(archive, name))
            self._finish_load()
        except Exception as exc:
            self.loaded = False
            self.error = str(exc)
        return self

    def load_from_directory(self, path: Path):
        self._reset()
        candidate = Path(path)
        try:
            if not candidate.is_dir():
                raise FileNotFoundError(f"GTFS directory not found: {candidate}")
            self.source = f"dir:{candidate.name}"
            self._load_tables(lambda name: self._read_directory(candidate, name))
            self._finish_load()
        except Exception as exc:
            self.loaded = False
            self.error = str(exc)
        return self

    def _finish_load(self) -> None:
        self.loaded = bool(self.agency and self.stops and self.routes and self.trips and self.stop_times_by_trip)
        if not self.loaded:
            raise RuntimeError("GTFS loaded but required tables are empty")

    def _load_tables(self, reader: Callable[[str], List[Dict[str, str]]]) -> None:
        for row in reader("agency.txt"):
            agency_id = clean_text(row.get("agency_id")) or "agency"
            self.agency[agency_id] = dict(row)
        for row in reader("stops.txt"):
            stop_id = clean_text(row.get("stop_id"))
            if not stop_id:
                continue
            name = human_name(row.get("stop_name")) or stop_id
            self.stops[stop_id] = {**dict(row), "stop_id": stop_id, "stop_name": name, "name": name, "code": public_stop_code({**dict(row), "stop_name": name}), "lat": safe_float(row.get("stop_lat")), "lon": safe_float(row.get("stop_lon"))}
        for row in reader("routes.txt"):
            route_id = clean_text(row.get("route_id"))
            if not route_id:
                continue
            short_name = clean_text(row.get("route_short_name")) or clean_text(row.get("route_long_name")) or route_id
            self.routes[route_id] = {**dict(row), "route_id": route_id, "route_short_name": short_name, "line": short_name}
            self.route_by_short[line_norm(short_name)].append(route_id)
        for row in reader("trips.txt"):
            trip_id = clean_text(row.get("trip_id"))
            if not trip_id:
                continue
            route_id = clean_text(row.get("route_id"))
            line = self.routes.get(route_id, {}).get("route_short_name", "")
            self.trips[trip_id] = {**dict(row), "trip_id": trip_id, "route_id": route_id, "line": line, "destination": short_destination(row.get("trip_headsign"))}
        for row in reader("calendar.txt"):
            service_id = clean_text(row.get("service_id"))
            if service_id:
                self.calendar[service_id] = dict(row)
        for row in reader("calendar_dates.txt"):
            service_id, day = clean_text(row.get("service_id")), clean_text(row.get("date"))
            if service_id and day:
                self.calendar_dates[service_id][day] = safe_int(row.get("exception_type"), 0)
        for row in reader("stop_times.txt"):
            trip_id, stop_id = clean_text(row.get("trip_id")), clean_text(row.get("stop_id"))
            if trip_id and stop_id:
                self.stop_times_by_trip[trip_id].append({**dict(row), "trip_id": trip_id, "stop_id": stop_id, "stop_sequence": safe_int(row.get("stop_sequence"), 0)})
        for trip_id, rows in self.stop_times_by_trip.items():
            rows.sort(key=lambda item: safe_int(item.get("stop_sequence"), 0))
            trip = self.trips.get(trip_id, {})
            if not trip:
                continue
            for index, row in enumerate(rows):
                stop_id = row.get("stop_id")
                stop = self.stops.get(stop_id, {})
                self.stop_departures_index[stop_id].append({**row, "line": trip.get("line", ""), "route_id": trip.get("route_id", ""), "service_id": trip.get("service_id", ""), "direction_id": trip.get("direction_id", ""), "headsign": short_destination(row.get("stop_headsign") or trip.get("trip_headsign") or trip.get("destination")), "headsign_full": human_name(row.get("stop_headsign") or trip.get("trip_headsign") or trip.get("destination")), "stop_name": stop.get("stop_name", stop_id), "stop_code": stop.get("code", "BUS"), "is_last_stop": index == len(rows) - 1})
        for row in reader("shapes.txt"):
            shape_id, latitude, longitude = clean_text(row.get("shape_id")), safe_float(row.get("shape_pt_lat")), safe_float(row.get("shape_pt_lon"))
            if shape_id and latitude is not None and longitude is not None:
                self.shapes[shape_id].append({"lat": latitude, "lon": longitude, "seq": safe_int(row.get("shape_pt_sequence"), 0)})
        for points in self.shapes.values():
            points.sort(key=lambda point: point.get("seq", 0))

    def active_service_ids(self, service_day: date) -> Set[str]:
        return active_service_ids(self.calendar, self.calendar_dates, service_day)

    def trip_first_stop(self, trip_id: str) -> Dict[str, Any]:
        rows = self.stop_times_by_trip.get(trip_id, [])
        return rows[0] if rows else {}

    def trip_last_stop(self, trip_id: str) -> Dict[str, Any]:
        rows = self.stop_times_by_trip.get(trip_id, [])
        return rows[-1] if rows else {}
