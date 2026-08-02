from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.services.bustimes_vehicle_client import BustimesVehicleClient
from app.services.fleet_registry import FleetRegistryProvider, FleetSnapshot


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class FleetRefreshService:
    def __init__(self, *, client: BustimesVehicleClient, provider: FleetRegistryProvider, operator_id: str, cache_path: Path, metadata_path: Path, interval_seconds: int = 86400, enabled: bool = True):
        self.client, self.provider, self.operator_id = client, provider, operator_id
        self.cache_path, self.metadata_path = Path(cache_path), Path(metadata_path)
        self.interval_seconds, self.enabled = interval_seconds, enabled
        self._lock, self._stop, self._thread = threading.Lock(), threading.Event(), None
        self.running = False
        self._metadata = {"source": "bustimes.org", "lastCheckedAt": None, "lastUpdatedAt": None, "lastError": None, "sha256": None, "usingCachedData": False}
        try:
            saved_metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))
            if isinstance(saved_metadata, dict):
                self._metadata.update(saved_metadata)
        except (OSError, ValueError, TypeError):
            pass
        self.load_cache()

    @staticmethod
    def _canonical(records):
        stable_records = [{key: value for key, value in record.items() if key != "fetchedAt"} for record in records]
        return json.dumps({"records": stable_records}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=path.parent, suffix=".tmp") as stream:
                temporary = Path(stream.name); stream.write(payload); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, path); temporary = None
        finally:
            if temporary is not None: temporary.unlink(missing_ok=True)

    def load_cache(self) -> bool:
        try:
            value = json.loads(self.cache_path.read_text(encoding="utf-8"))
            records = value.get("records") if isinstance(value, dict) else None
            if not isinstance(records, list) or not all(isinstance(item, dict) for item in records): return False
            fetched_at = value.get("fetchedAt")
            self.provider.replace(FleetSnapshot(tuple(dict(item) for item in records), fetched_at, True))
            self._metadata.update({"usingCachedData": True, "lastUpdatedAt": fetched_at, "sha256": hashlib.sha256(self._canonical(records)).hexdigest()})
            return True
        except (OSError, ValueError, TypeError):
            return False

    def snapshot(self) -> dict:
        current = self.provider.get()
        ambiguous = 0
        keys = {}
        for record in current.records:
            key = (str(record.get("operatorId", "")).upper(), str(record.get("fleetCode", "")).strip().casefold())
            if key in keys and not record.get("withdrawn") and not keys[key].get("withdrawn"): ambiguous += 1
            keys[key] = record
        return {**self._metadata, "ok": bool(current.records) and not self._metadata.get("lastError"), "loaded": bool(current.records), "count": len(current.records), "refreshRunning": self.running, "refreshIntervalSeconds": self.interval_seconds, "ambiguousCount": ambiguous}

    def _persist_metadata(self) -> None:
        try:
            self._atomic_write(self.metadata_path, (json.dumps(self._metadata, indent=2, sort_keys=True) + "\n").encode("utf-8"))
        except OSError:
            pass

    def refresh(self) -> bool:
        if not self.enabled or not self._lock.acquire(blocking=False): return False
        self.running = True; checked = utc_now(); self._metadata["lastCheckedAt"] = checked
        try:
            records = self.client.fetch(self.operator_id, fetched_at=checked)
            if not records:
                raise ValueError("Bustimes operator response contains no vehicles")
            payload_hash = hashlib.sha256(self._canonical(records)).hexdigest()
            if payload_hash == self._metadata.get("sha256"):
                self._metadata.update({"lastError": None, "usingCachedData": True}); self._persist_metadata(); return False
            cache_payload = json.dumps({"schemaVersion": 1, "source": "bustimes.org", "operatorId": self.operator_id, "fetchedAt": checked, "records": list(records)}, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
            self._atomic_write(self.cache_path, cache_payload)
            self.provider.replace(FleetSnapshot(tuple(dict(item) for item in records), checked, False))
            self._metadata.update({"lastUpdatedAt": checked, "lastError": None, "sha256": payload_hash, "usingCachedData": False})
            self._persist_metadata()
            return True
        except Exception as exc:
            self._metadata.update({"lastError": str(exc), "usingCachedData": bool(self.provider.get().records)}); self._persist_metadata(); return False
        finally:
            self.running = False; self._lock.release()

    def _run(self):
        while not self._stop.is_set(): self.refresh(); self._stop.wait(self.interval_seconds)

    def start(self):
        if not self.enabled or (self._thread and self._thread.is_alive()): return
        self._stop.clear(); self._thread = threading.Thread(target=self._run, name="fleet-metadata-refresh", daemon=True); self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive(): self._thread.join(timeout=2)
