from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Optional


logger = logging.getLogger(__name__)

REQUIRED_GTFS_FILES = {
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
}
CALENDAR_FILES = {"calendar.txt", "calendar_dates.txt"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def masked_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if not parsed.query:
        return url
    sensitive = {"api_key", "apikey", "key", "token", "secret", "password", "signature", "sig"}
    masked_query = urllib.parse.urlencode(
        [
            (key, "***" if key.lower() in sensitive else value)
            for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        ]
    )
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, masked_query, parsed.fragment)
    )


class GTFSRefreshService:
    def __init__(
        self,
        *,
        source_url: str,
        target_path: Path,
        metadata_path: Path,
        interval_seconds: int,
        timeout_seconds: int,
        max_download_bytes: int,
        max_uncompressed_bytes: int,
        max_attempts: int,
        enabled: bool,
        build_candidate: Callable[[Path], Any],
        activate_candidate: Callable[[Any], None],
        opener: Callable[..., Any] = urllib.request.urlopen,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.source_url = source_url
        self.target_path = Path(target_path)
        self.metadata_path = Path(metadata_path)
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self.max_download_bytes = max_download_bytes
        self.max_uncompressed_bytes = max_uncompressed_bytes
        self.max_attempts = min(3, max(1, max_attempts))
        self.enabled = enabled and bool(source_url)
        self.build_candidate = build_candidate
        self.activate_candidate = activate_candidate
        self.opener = opener
        self.sleep = sleep
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._metadata = self._load_metadata()
        self._metadata.update(
            {
                "source": masked_url(source_url),
                "activeZipPath": str(self.target_path),
                "refreshIntervalSeconds": interval_seconds,
            }
        )
        self.running = False

    def _default_metadata(self) -> dict:
        return {
            "source": masked_url(self.source_url),
            "lastCheckedAt": None,
            "lastUpdatedAt": None,
            "lastSuccessfulLoadAt": None,
            "sha256": None,
            "etag": None,
            "lastModified": None,
            "usingCachedData": self.target_path.exists(),
            "activeZipPath": str(self.target_path),
            "downloadedBytes": 0,
            "refreshIntervalSeconds": self.interval_seconds,
            "lastError": None,
        }

    def _load_metadata(self) -> dict:
        metadata = self._default_metadata()
        try:
            if self.metadata_path.is_file():
                loaded = json.loads(self.metadata_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    metadata.update(loaded)
        except Exception as exc:
            logger.warning("Ignoring unreadable GTFS metadata: %s", exc)
        return metadata

    def _write_metadata(self) -> None:
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                delete=False,
                dir=self.metadata_path.parent,
                suffix=".json.tmp",
            ) as stream:
                temp_path = Path(stream.name)
                json.dump(self._metadata, stream, ensure_ascii=False, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path, self.metadata_path)
            temp_path = None
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def validate(self, path: Path) -> None:
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError("GTFS download is empty")
        if not zipfile.is_zipfile(path):
            raise ValueError("Downloaded GTFS source is not a ZIP file")
        with zipfile.ZipFile(path) as archive:
            total_uncompressed = 0
            names = set()
            for info in archive.infolist():
                normalized = info.filename.replace("\\", "/")
                zip_path = PurePosixPath(normalized)
                if zip_path.is_absolute() or ".." in zip_path.parts:
                    raise ValueError(f"Unsafe path in GTFS ZIP: {info.filename}")
                total_uncompressed += info.file_size
                if total_uncompressed > self.max_uncompressed_bytes:
                    raise ValueError("GTFS uncompressed content exceeds configured size limit")
                names.add(zip_path.name.lower())
            bad_file = archive.testzip()
            if bad_file:
                raise ValueError(f"GTFS ZIP CRC check failed: {bad_file}")
            missing = sorted(REQUIRED_GTFS_FILES - names)
            if missing:
                raise ValueError(f"GTFS ZIP is missing required files: {', '.join(missing)}")
            if not names.intersection(CALENDAR_FILES):
                raise ValueError("GTFS ZIP must contain calendar.txt or calendar_dates.txt")

    def snapshot(self) -> dict:
        return {
            **self._metadata,
            "enabled": self.enabled,
            "running": self.running,
            "sourceUrlConfigured": bool(self.source_url),
        }

    def _request_headers(self) -> dict:
        headers = {"User-Agent": "Bluestar-GTFS-Refresh/2.0"}
        if self._metadata.get("etag"):
            headers["If-None-Match"] = self._metadata["etag"]
        if self._metadata.get("lastModified"):
            headers["If-Modified-Since"] = self._metadata["lastModified"]
        return headers

    def _open_with_retry(self, request: urllib.request.Request):
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return self.opener(request, timeout=self.timeout_seconds)
            except urllib.error.HTTPError as exc:
                if exc.code == 304:
                    return exc
                if exc.code < 500 or attempt == self.max_attempts:
                    raise
                last_error = exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt == self.max_attempts:
                    raise
                last_error = exc
            self.sleep(min(attempt, 2))
        raise RuntimeError(str(last_error) if last_error else "GTFS download failed")

    def refresh(self) -> bool:
        if not self.enabled or not self._lock.acquire(blocking=False):
            return False
        self.running = True
        self._metadata["lastCheckedAt"] = utc_now()
        temp_path: Optional[Path] = None
        try:
            self.target_path.parent.mkdir(parents=True, exist_ok=True)
            request = urllib.request.Request(self.source_url, headers=self._request_headers())
            response = self._open_with_retry(request)
            status = getattr(response, "status", getattr(response, "code", 200))
            if status == 304:
                self._metadata.update({"usingCachedData": True, "lastError": None})
                self._write_metadata()
                return False
            if not 200 <= int(status) < 300:
                raise ValueError(f"Unexpected GTFS HTTP status: {status}")
            with response:
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
                etag = response.headers.get("ETag")
                last_modified = response.headers.get("Last-Modified")
            self._metadata["downloadedBytes"] = downloaded
            self.validate(temp_path)
            new_hash = self._sha256(temp_path)
            old_hash = self._sha256(self.target_path) if self.target_path.exists() else None
            if new_hash == old_hash:
                self._metadata.update(
                    {
                        "sha256": new_hash,
                        "etag": etag or self._metadata.get("etag"),
                        "lastModified": last_modified or self._metadata.get("lastModified"),
                        "usingCachedData": True,
                        "lastError": None,
                    }
                )
                self._write_metadata()
                return False
            candidate = self.build_candidate(temp_path)
            if candidate is None or not getattr(candidate, "loaded", False):
                error = getattr(candidate, "error", None) if candidate is not None else None
                raise ValueError(error or "Candidate GTFSStore could not be loaded")
            backup_path: Optional[Path] = None
            if self.target_path.exists():
                with tempfile.NamedTemporaryFile(
                    mode="wb", delete=False, dir=self.target_path.parent, suffix=".rollback.tmp"
                ) as backup:
                    backup_path = Path(backup.name)
                shutil.copy2(self.target_path, backup_path)
            try:
                os.replace(temp_path, self.target_path)
                temp_path = None
                self.activate_candidate(candidate)
            except Exception:
                if backup_path is not None:
                    os.replace(backup_path, self.target_path)
                    backup_path = None
                else:
                    self.target_path.unlink(missing_ok=True)
                raise
            finally:
                if backup_path is not None:
                    backup_path.unlink(missing_ok=True)
            successful_at = utc_now()
            self._metadata.update(
                {
                    "lastUpdatedAt": successful_at,
                    "lastSuccessfulLoadAt": successful_at,
                    "sha256": new_hash,
                    "etag": etag,
                    "lastModified": last_modified,
                    "usingCachedData": False,
                    "lastError": None,
                }
            )
            self._write_metadata()
            return True
        except Exception as exc:
            self._metadata.update({"usingCachedData": self.target_path.exists(), "lastError": str(exc)})
            logger.exception("GTFS refresh failed: %s", exc)
            try:
                self._write_metadata()
            except Exception as metadata_exc:
                logger.warning("Could not persist GTFS refresh error: %s", metadata_exc)
            return False
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
            self.running = False
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
