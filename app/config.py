from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GTFS_DOWNLOAD_URL = "https://www.bluestarbus.co.uk/open-data/network/current?format=gtfs"


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
    gtfs_max_uncompressed_bytes: int
    gtfs_metadata_path: Path
    gtfs_refresh_attempts: int

    @classmethod
    def from_env(cls) -> "Settings":
        url = (
            os.getenv("GTFS_DOWNLOAD_URL")
            or os.getenv("GTFS_URL")
            or DEFAULT_GTFS_DOWNLOAD_URL
        ).strip()
        interval = os.getenv("GTFS_REFRESH_SECONDS") or os.getenv(
            "GTFS_REFRESH_INTERVAL_SECONDS", "21600"
        )
        return cls(
            gtfs_download_url=url,
            gtfs_zip_path=Path(os.getenv("GTFS_ZIP", str(PROJECT_ROOT / "gtfs.zip"))),
            gtfs_auto_refresh=_env_bool("GTFS_AUTO_REFRESH", bool(url)),
            gtfs_refresh_interval_seconds=max(
                300, int(interval)
            ),
            gtfs_download_timeout_seconds=max(
                5, int(os.getenv("GTFS_DOWNLOAD_TIMEOUT_SECONDS", "60"))
            ),
            gtfs_max_download_bytes=max(
                1_048_576, int(os.getenv("GTFS_MAX_DOWNLOAD_BYTES", "262144000"))
            ),
            gtfs_max_uncompressed_bytes=max(
                1_048_576, int(os.getenv("GTFS_MAX_UNCOMPRESSED_BYTES", "1073741824"))
            ),
            gtfs_metadata_path=Path(
                os.getenv("GTFS_METADATA_PATH", str(PROJECT_ROOT / "data" / "gtfs" / "metadata.json"))
            ),
            gtfs_refresh_attempts=min(
                3, max(1, int(os.getenv("GTFS_REFRESH_ATTEMPTS", "3")))
            ),
        )


settings = Settings.from_env()
