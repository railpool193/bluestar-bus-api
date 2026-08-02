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

Automatic GTFS refresh is enabled when `GTFS_DOWNLOAD_URL` (or the compatibility
name `GTFS_URL`) is set. It downloads to a temporary file, validates the ZIP,
and atomically replaces `GTFS_ZIP` only after validation succeeds.

- `GTFS_AUTO_REFRESH`: enable or disable refresh explicitly.
- `GTFS_REFRESH_INTERVAL_SECONDS`: refresh interval; minimum 300, default 86400.
- `GTFS_DOWNLOAD_TIMEOUT_SECONDS`: request timeout; minimum 5, default 60.
- `GTFS_MAX_DOWNLOAD_BYTES`: size limit; minimum 1 MiB, default 250 MiB.

The configured source URL itself is never returned by the status endpoint.

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
