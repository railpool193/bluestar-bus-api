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

The existing application accepts `GTFS_DIR`, `GTFS_ZIP`, `GTFS_RUNTIME_ZIP`, `LIVE_FEED_URL`,
`LIVE_API_KEY` (or `BODS_API_KEY`), `LIVE_CACHE_TTL_SEC`,
`LIVE_MAX_AGE_SECONDS`, `LIVE_OPERATOR_FILTER`, `DEPARTURE_WINDOW_MIN`,
`DEPARTURE_LIMIT`, and `LIVE_MATCH_MINUTES`.

The SIRI live layer supports these additional settings:

- `LIVE_REQUEST_TIMEOUT_SECONDS`: HTTP timeout, default 20 seconds.
- `LIVE_MAX_RESPONSE_BYTES`: maximum XML response size, default 8 MiB.
- `LIVE_USER_AGENT`: request user agent.
- `LIVE_REFRESH_ATTEMPTS`: bounded attempts, 1-3, default 3.

`LIVE_API_KEY` remains primary and `BODS_API_KEY` remains its compatibility
fallback. Keys are sent through `x-api-key` and, when the URL does not already
contain `api_key` or `key`, an `api_key` query parameter. Diagnostic URLs mask
sensitive query values.

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
- `GTFS_ZIP`: tracked or deployment-provided read-only seed, default `gtfs.zip`.
- `GTFS_RUNTIME_ZIP`: writable runtime cache, default `data/runtime/gtfs/current.zip`.
- `GTFS_METADATA_PATH`: defaults to `data/runtime/gtfs/metadata.json`.

The tracked `gtfs.zip` is never a refresh target. Startup prefers a valid runtime
cache, falls back to the seed ZIP, and finally tries `GTFS_DIR`. Downloads are
validated and atomically replace only the ignored runtime cache under
`data/runtime/`; a failed download leaves the active snapshot and previous cache
untouched. This keeps the repository working tree clean during server operation.

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

The active GTFS implementation now lives in `app/services/gtfs_loader.py`, with
calendar evaluation in `app/services/gtfs_calendar.py`. A thread-safe provider
atomically swaps fully built stores. Each GTFS-dependent request obtains one
store snapshot at its start, so an in-flight request can safely finish against
the previous immutable-by-convention instance while a refresh activates the new
one. Metadata persistence failures are reported separately and do not roll back
an already successful data activation.

The refresh worker is process-local. Railway should run one web worker unless
duplicate per-process feed checks are intentionally acceptable.

## Vehicle metadata

Optional vehicle information is enriched from the JSON API at
`https://bustimes.org/api/vehicles/`. Vehicle information: bustimes.org. The
dataset includes community contributions, is not guaranteed complete, and is
not an official Bluestar fleet register. A fleet code can identify ticketing
equipment and can move between physical vehicles.

Verified server-side filters are `operator=BLUS`, `fleet_code=...`, and
`reg=...`. The API uses `count`, `next`, `previous`, and `results` with `limit`
and `offset`; operator slug query names were observed to be ignored and are not
used. The integration caps pages, records, bytes, timeout, and retries, and only
follows HTTPS pagination URLs on `bustimes.org` below `/api/vehicles/`.

Metadata is adapted into an internal model and cached atomically in
`data/fleet/vehicles.json`; refresh defaults to 86400 seconds. A failed refresh
keeps the prior snapshot. Requests never fetch Bustimes directly. Bustimes data
is descriptive metadata only and never influences route, destination, trip,
stop or delay matching. Free-text notes, external CSS and images are not used.

- `BUSTIMES_VEHICLES_API_URL`: default `https://bustimes.org/api/vehicles/`.
- `FLEET_METADATA_REFRESH_SECONDS`: minimum 3600, default 86400.
- `FLEET_METADATA_AUTO_REFRESH`: defaults to enabled.
- `FLEET_METADATA_TIMEOUT_SECONDS`: minimum 5, default 20.
- `FLEET_METADATA_MAX_BYTES`: default 4 MiB.
- `FLEET_METADATA_OPERATOR_ID`: default verified Bluestar ID `BLUS`.
- `FLEET_METADATA_CACHE_PATH` and `FLEET_METADATA_STATUS_PATH`: cache paths.

SIRI parsing is isolated in `app/services/siri_parser.py`; network download is
isolated in `app/services/siri_client.py`. `LiveRefreshService` starts a daemon
worker without blocking application startup, immediately attempts one fetch and
then refreshes at `LIVE_CACHE_TTL_SEC`. A thread-safe provider atomically swaps
complete snapshots. On download or parse failure the last successful vehicles
remain available and the snapshot becomes stale with an error diagnostic.
`/health` and `/api/status` never initiate a SIRI download.

## Migration status

`app.main:app` is the deployment entry point and creates the production app with
`app.factory.create_app()`. `app.runtime.ApplicationRuntime` owns one GTFS
provider, one live provider, and their refresh services per application. Merely
creating or importing an app does not start workers; lifespan loads a local GTFS
candidate, then starts GTFS and live refresh, and shuts them down live-first.

Pure time, text and geographic helpers plus the GTFS and SIRI services live in
`app/`. `/api/vehicles` is implemented by
`app/api/vehicles.py` and `/api/map` by `app/api/map.py`; `/health` and
`/api/status`, `/api/search`, `/api/stops/{stop_id}/departures`, and
`/api/trips/{trip_id}` and `/api/routes/{line}` are implemented by their
matching `app/api/` modules.
Their APIRouters receive explicit runtime dependencies. Search reads one existing
GTFS snapshot. Stop departures and trips each read one GTFS snapshot, one live
snapshot and one current-time value per request; stale or unavailable SIRI data
falls back to scheduled output. Trip presentation reuses the shared live-matching
and map-shape services, including stop-coordinate shape fallback. Route requests
also read one GTFS/live/time snapshot, combine current and previous service days,
deduplicate directions by direction and destination, and retain at most six.
No migrated request loads or downloads data. `app/api/frontend.py` owns `/` and
the final SPA catch-all; `/static` is mounted once when its directory exists.
The root `main.py` is only a compatibility re-export shim and creates no app,
provider, service, route, or mount. No legacy GTFS, SIRI, frontend, or data file
has been deleted. See `docs/current-system-assessment.md`.

## Tests

```bash
python -m compileall app tests
python -m unittest discover -s tests -v
```
