from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional


REQUIRED_GTFS_FILES = {
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
}


@dataclass
class RefreshStatus:
    enabled: bool = False
    running: bool = False
    last_attempt: Optional[str] = None
    last_success: Optional[str] = None
    last_error: Optional[str] = None
    source_url_configured: bool = False
    changed: bool = False
    sha256: Optional[str] = None


class GTFSRefreshService:
    def __init__(
        self,
        *,
        source_url: str,
        target_path: Path,
        interval_seconds: int,
        timeout_seconds: int,
        max_download_bytes: int,
        enabled: bool,
        on_changed: Optional[Callable[[], None]] = None,
    ) -> None:
        self.source_url = source_url
        self.target_path = target_path
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self.max_download_bytes = max_download_bytes
        self.enabled = enabled and bool(source_url)
        self.on_changed = on_changed
        self.status = RefreshStatus(
            enabled=self.enabled,
            source_url_configured=bool(source_url),
        )
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def validate(path: Path) -> None:
        if not zipfile.is_zipfile(path):
            raise ValueError("Downloaded GTFS source is not a ZIP file")
        with zipfile.ZipFile(path) as archive:
            names = {Path(name).name.lower() for name in archive.namelist()}
            missing = sorted(REQUIRED_GTFS_FILES - names)
            if missing:
                raise ValueError(f"GTFS ZIP is missing required files: {', '.join(missing)}")

    def snapshot(self) -> dict:
        return asdict(self.status)

    def refresh(self) -> bool:
        if not self.enabled or not self._lock.acquire(blocking=False):
            return False
        self.status.running = True
        self.status.last_attempt = datetime.now(timezone.utc).isoformat()
        temp_path: Optional[Path] = None
        try:
            self.target_path.parent.mkdir(parents=True, exist_ok=True)
            request = urllib.request.Request(
                self.source_url,
                headers={"User-Agent": "Bluestar-GTFS-Refresh/1.0"},
            )
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > self.max_download_bytes:
                    raise ValueError("GTFS download exceeds configured size limit")
                with tempfile.NamedTemporaryFile(
                    mode="wb", delete=False, dir=self.target_path.parent, suffix=".gtfs.tmp"
                ) as stream:
                    temp_path = Path(stream.name)
                    downloaded = 0
                    while chunk := response.read(1024 * 1024):
                        downloaded += len(chunk)
                        if downloaded > self.max_download_bytes:
                            raise ValueError("GTFS download exceeds configured size limit")
                        stream.write(chunk)
                    stream.flush()
                    os.fsync(stream.fileno())
            self.validate(temp_path)
            new_hash = self._sha256(temp_path)
            old_hash = self._sha256(self.target_path) if self.target_path.exists() else None
            changed = new_hash != old_hash
            if changed:
                os.replace(temp_path, self.target_path)
                temp_path = None
                if self.on_changed:
                    self.on_changed()
            self.status.changed = changed
            self.status.sha256 = new_hash
            self.status.last_success = datetime.now(timezone.utc).isoformat()
            self.status.last_error = None
            return changed
        except Exception as exc:
            self.status.changed = False
            self.status.last_error = str(exc)
            return False
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
            self.status.running = False
            self._lock.release()

    def _run(self) -> None:
        while not self._stop.is_set():
            self.refresh()
            self._stop.wait(self.interval_seconds)

    def start(self) -> None:
        if not self.enabled or (self._thread and self._thread.is_alive()):
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="gtfs-refresh", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
