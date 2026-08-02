from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    gtfs_download_url: str
    gtfs_zip_path: Path
    gtfs_auto_refresh: bool
    gtfs_refresh_interval_seconds: int
    gtfs_download_timeout_seconds: int
    gtfs_max_download_bytes: int

    @classmethod
    def from_env(cls) -> "Settings":
        url = os.getenv("GTFS_DOWNLOAD_URL", os.getenv("GTFS_URL", "")).strip()
        return cls(
            gtfs_download_url=url,
            gtfs_zip_path=Path(os.getenv("GTFS_ZIP", str(PROJECT_ROOT / "gtfs.zip"))),
            gtfs_auto_refresh=_env_bool("GTFS_AUTO_REFRESH", bool(url)),
            gtfs_refresh_interval_seconds=max(
                300, int(os.getenv("GTFS_REFRESH_INTERVAL_SECONDS", "86400"))
            ),
            gtfs_download_timeout_seconds=max(
                5, int(os.getenv("GTFS_DOWNLOAD_TIMEOUT_SECONDS", "60"))
            ),
            gtfs_max_download_bytes=max(
                1_048_576, int(os.getenv("GTFS_MAX_DOWNLOAD_BYTES", "262144000"))
            ),
        )


settings = Settings.from_env()
