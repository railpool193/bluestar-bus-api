from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

from app.services.fleet_registry import adapt_bustimes_vehicle, normalize_operator


class BustimesVehicleError(RuntimeError):
    pass


@dataclass(frozen=True)
class BustimesVehicleClientConfig:
    base_url: str = "https://bustimes.org/api/vehicles/"
    timeout_seconds: int = 20
    max_bytes: int = 4 * 1024 * 1024
    max_pages: int = 5
    max_records: int = 1000
    attempts: int = 2
    user_agent: str = "Bluestar-Bus-API-Vehicle-Metadata/1.0"


class BustimesVehicleClient:
    def __init__(self, config: BustimesVehicleClientConfig, *, opener: Callable[..., Any] = urllib.request.urlopen, sleep: Callable[[float], None] = time.sleep):
        self.config, self.opener, self.sleep = config, opener, sleep
        self._validate_url(config.base_url)

    @staticmethod
    def _validate_url(url: str) -> None:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "bustimes.org" or parsed.port not in (None, 443) or parsed.username or parsed.password or not parsed.path.startswith("/api/vehicles/"):
            raise BustimesVehicleError("Untrusted Bustimes pagination URL")

    def _open(self, url: str):
        request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": self.config.user_agent})
        for attempt in range(self.config.attempts):
            try:
                return self.opener(request, timeout=self.config.timeout_seconds)
            except (urllib.error.URLError, TimeoutError, OSError):
                if attempt + 1 >= self.config.attempts:
                    raise
                self.sleep(1)

    def _page(self, url: str) -> dict[str, Any]:
        self._validate_url(url)
        response = self._open(url)
        with response:
            status = getattr(response, "status", getattr(response, "code", 200))
            if not 200 <= int(status) < 300:
                raise BustimesVehicleError(f"Unexpected Bustimes HTTP status: {status}")
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                raise BustimesVehicleError("Bustimes response is not JSON")
            declared = response.headers.get("Content-Length")
            if declared and int(declared) > self.config.max_bytes:
                raise BustimesVehicleError("Bustimes response exceeds size limit")
            payload = response.read(self.config.max_bytes + 1)
        if len(payload) > self.config.max_bytes:
            raise BustimesVehicleError("Bustimes response exceeds size limit")
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BustimesVehicleError("Invalid Bustimes JSON") from exc
        if not isinstance(value, dict) or not isinstance(value.get("results"), list):
            raise BustimesVehicleError("Invalid Bustimes response schema")
        return value

    def fetch(self, operator_id: str, *, fleet_code: str = "", registration: str = "", fetched_at: str) -> tuple[dict[str, Any], ...]:
        operator = normalize_operator(operator_id)
        if not operator:
            raise BustimesVehicleError("Operator ID is required")
        query = {"operator": operator, "limit": min(500, self.config.max_records)}
        if fleet_code:
            query["fleet_code"] = str(fleet_code).strip()
        if registration:
            query["reg"] = str(registration).strip()
        url = self.config.base_url + "?" + urllib.parse.urlencode(query)
        seen: set[str] = set()
        records: list[dict[str, Any]] = []
        for _page_number in range(self.config.max_pages):
            if url in seen:
                raise BustimesVehicleError("Bustimes pagination loop detected")
            seen.add(url)
            page = self._page(url)
            for raw in page["results"]:
                if not isinstance(raw, dict):
                    raise BustimesVehicleError("Invalid Bustimes vehicle record")
                adapted = adapt_bustimes_vehicle(raw, fetched_at)
                if adapted and normalize_operator(adapted.get("operatorId")) == operator:
                    records.append(adapted)
                if len(records) > self.config.max_records:
                    raise BustimesVehicleError("Bustimes record limit exceeded")
            next_url = page.get("next")
            if not next_url:
                return tuple(records)
            if not isinstance(next_url, str):
                raise BustimesVehicleError("Invalid Bustimes pagination URL")
            self._validate_url(next_url)
            url = next_url
        raise BustimesVehicleError("Bustimes page limit exceeded")

    def lookup_fleet(self, operator_id: str, fleet_code: str, *, fetched_at: str) -> tuple[dict[str, Any], ...]:
        return self.fetch(operator_id, fleet_code=fleet_code, fetched_at=fetched_at)

    def lookup_registration(self, operator_id: str, registration: str, *, fetched_at: str) -> tuple[dict[str, Any], ...]:
        return self.fetch(operator_id, registration=registration, fetched_at=fetched_at)
