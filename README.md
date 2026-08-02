# Bluestar Bus API

FastAPI application for Bluestar and Unilink timetable, departure and live vehicle data.

## Run

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Railway starts the same application through the `Procfile`.

## Public endpoints

- `GET /`
- `GET /health`
- `GET /api/status`
- `GET /api/search?q=...`
- `GET /api/stops/{stop_id}/departures`
- `GET /api/routes/{line}`
- `GET /api/trips/{trip_id}`
- `GET /api/vehicles`
- `GET /api/map`
- `GET /api/gtfs/refresh/status`

## Configuration

The existing application accepts `GTFS_DIR`, `GTFS_ZIP`, `LIVE_FEED_URL`,
`LIVE_API_KEY` (or `BODS_API_KEY`), `LIVE_CACHE_TTL_SEC`,
`LIVE_MAX_AGE_SECONDS`, `LIVE_OPERATOR_FILTER`, `DEPARTURE_WINDOW_MIN`,
`DEPARTURE_LIMIT`, and `LIVE_MATCH_MINUTES`.

Automatic GTFS refresh is enabled by default and uses:

`https://www.bluestarbus.co.uk/open-data/network/current?format=gtfs`

`GTFS_DOWNLOAD_URL` overrides this URL; `GTFS_URL` is the lower-priority
compatibility name. No Railway variable is required for the public default.
Sensitive query parameter values are masked in status output.

- `GTFS_AUTO_REFRESH`: defaults to enabled; `false`, `0`, `no`, or `off` disables it.
- `GTFS_REFRESH_SECONDS`: primary refresh interval; minimum 300, default 21600 (6 hours).
- `GTFS_REFRESH_INTERVAL_SECONDS`: compatibility interval name.
- `GTFS_DOWNLOAD_TIMEOUT_SECONDS`: request timeout; minimum 5, default 60.
- `GTFS_MAX_DOWNLOAD_BYTES`: size limit; minimum 1 MiB, default 250 MiB.
- `GTFS_MAX_UNCOMPRESSED_BYTES`: ZIP expansion limit; default 1 GiB.
- `GTFS_REFRESH_ATTEMPTS`: bounded network attempts; 1-3, default 3.
- `GTFS_METADATA_PATH`: defaults to `data/gtfs/metadata.json`.

The downloader sends `If-None-Match` and `If-Modified-Since` when cached ETag
or Last-Modified values exist. A 304 response updates `lastCheckedAt` without
rewriting the ZIP or store. SHA-256 prevents activation when servers omit cache
headers but return unchanged content.

Downloads are validated for HTTP success, non-empty content, ZIP/CRC integrity,
safe member paths, compressed and uncompressed limits, required GTFS tables and
calendar data. A complete candidate store is built from the temporary ZIP before
the active ZIP and in-memory store are replaced.

Refresh metadata is atomically written to `data/gtfs/metadata.json`. Corrupt
metadata is ignored at startup. `/api/status` and `/health` remain available
without GTFS; GTFS-dependent data endpoints return HTTP 503.

The refresh worker is process-local. Railway should run one web worker unless
duplicate per-process feed checks are intentionally acceptable.

## Migration status

`app.main:app` is the deployment entry point. During the compatibility phase it
exports the proven FastAPI instance from the root `main.py`, preserving the
existing endpoint contracts while new services move into `app/`. No legacy GTFS,
SIRI or frontend file has been deleted. See `docs/current-system-assessment.md`.

## Tests

```bash
python -m compileall app tests
python -m unittest discover -s tests -v
```
