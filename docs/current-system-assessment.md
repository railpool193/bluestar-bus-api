# Current system assessment and migration plan

## Runtime and deployment

The live implementation is the root `main.py`. It creates a FastAPI application,
mounts `static/` at `/static`, serves `templates/index.html` for `/` and all
non-API fallback paths, and loads GTFS plus live SIRI data during startup.

The previous `Procfile` used `gunicorn main:app`; `render.yaml` used
`uvicorn main:app`. The new deployment entry point is consistently
`uvicorn app.main:app`. `railway.toml` contains older `GTFS_PATH` and
`BODS_FEED_URL` names that are not consumed by the current root application;
Railway secrets and external variables were not changed.

## Active API surface

- `/health`: process health and London-local server time.
- `/api/status`: live-feed status, GTFS status and in-memory record counts.
- `/api/search`: substring search across stop names/IDs/codes and routes.
- `/api/stops/{stop_id}/departures`: scheduled departures merged with live data.
- `/api/trips/{trip_id}`: stop sequence, schedule, live position and shape.
- `/api/routes/{line}`: route records, representative directions and vehicles.
- `/api/vehicles`: current SIRI vehicles, optionally filtered by line.
- `/api/map`: vehicle positions and route shapes.
- `/`: the single-page frontend.
- `/{path:path}`: frontend fallback for non-API navigation.

The compatibility entry point keeps every route and response shape unchanged.

## GTFS flow

`GTFSStore` is implemented only in `app/services/gtfs_loader.py`. It accepts ZIP
and directory paths explicitly, builds all agency, stop, route, trip, stop-time
and shape indexes, and only then marks itself loaded. Calendar selection is in
`app/services/gtfs_calendar.py`; it applies both `calendar.txt` and additions or
removals from `calendar_dates.txt`. The shared time utility supports trips after
24:00.

The repository also contains `data/`, but the active application does not read
it by default. Its files differ from `gtfs/`; for example, its calendar starts
in 2025 while the checked `gtfs/` calendar starts in 2026. `gtfs.py`,
`gtfs_core.py`, and `gtfs_utils.py` are parallel implementations and are not
imported by the active application.

`GTFSRefreshService` uses Bluestar's public current-network GTFS URL by default
and checks every 21600 seconds (6 hours). `GTFS_DOWNLOAD_URL` and the compatible
`GTFS_URL` override the source; `GTFS_REFRESH_SECONDS` and the compatible
`GTFS_REFRESH_INTERVAL_SECONDS` override the interval.

Each refresh downloads to a temporary file with bounded retry, timeout and size
limits. It verifies HTTP success, non-empty content, ZIP structure, CRC, safe
member paths, total uncompressed size, required tables and at least one calendar
table. It then builds a complete `GTFSStore` through `load_from_path`. Only a
successfully loaded candidate may replace the active ZIP, after which
`GTFSStoreProvider.replace()` atomically activates it. Locking lasts only for
`get()` or `replace()`. Each GTFS-dependent endpoint keeps one local store
snapshot for the whole request, so replacement does not disturb an in-flight
request using the old complete object.

ETag and Last-Modified values are persisted and sent as `If-None-Match` and
`If-Modified-Since`. HTTP 304 only updates `lastCheckedAt`; it does not rewrite
or reload data. SHA-256 provides unchanged-content detection when cache headers
are absent. Metadata is atomically written to `data/gtfs/metadata.json`; corrupt
metadata is logged and ignored rather than blocking startup. A write failure
after successful activation appears as `metadataPersistenceError`; the new ZIP
and store stay active, while `lastError` remains reserved for data or activation
failures.

The FastAPI lifespan owns one daemon refresh thread per process and stops it on
shutdown. Multiple Railway workers would each perform their own checks, so one
worker is the recommended default. `/health` performs no GTFS read or download.
`/api/status` remains diagnostic when GTFS is missing, while GTFS-dependent
search, stop, trip, route and map endpoints return an explanatory HTTP 503.

## SIRI and live matching flow

`LiveStore` in `main.py` synchronously downloads XML from `LIVE_FEED_URL`, adding
`LIVE_API_KEY`/`BODS_API_KEY` as query parameter and header. It parses SIRI
`MonitoredVehicleJourney` records, filters by operator and maximum age, and
caches results for a short TTL. Departures are matched on normalized line,
destination, time proximity and current stop. Trip matching additionally uses
vehicle/journey identifiers and stop sequence.

The separate `siri_live.py` is not imported. It depends on undeclared `aiohttp`
and `python-dateutil`, confirming that it is a parallel legacy implementation.

## Frontend flow

The served frontend is `templates/index.html`, a single inline HTML/CSS/JS app.
It calls `/api/status`, `/api/search`, stop departures, trips, routes, vehicles
and map endpoints. It uses Leaflet and OpenStreetMap tiles, keeps favourites and
language in local storage, and refreshes status/map data every ten seconds.

The root `index.html`, `static/index.html`, `static/app.js`, other templates,
and `temples/index.html` are not referenced by the active backend. They are kept
until visual and contract regression tests support safe removal or consolidation.

## Functions that must be preserved

- Existing endpoint paths and frontend-consumed JSON fields.
- Calendar and calendar-exception service selection, including 24+ hour times.
- Stop, route and trip search/navigation.
- Scheduled departure calculation and deduplication.
- SIRI caching, freshness/operator filtering, matching and delay display.
- Trip progress, vehicle lists, route shapes and map payloads.
- Hungarian/English frontend state and favourites.
- Railway `$PORT` startup and environment-based secrets.

## Third-stage migration boundary

The root `main.py` imports and re-exports the shared helper API and imported
`GTFSStore`; it no longer contains a second GTFSStore implementation. It retains
the FastAPI instance, route declarations, SIRI XML parsing and fetching,
live/scheduled matching, response assembly and frontend serving. A compatibility
`gtfs` proxy resolves historic read access through the active provider, while
application endpoints use explicit request-level snapshots.

Contract tests cover `/health`, `/api/status`, `/api/search`, stop departures,
trips, routes, vehicles and map responses, including missing-GTFS 503 responses
and invalid identifier 404 responses. They use a local GTFS fixture and mocked
live data; no real network service is involved. No endpoint contract changed in
this stage.

The next stage should extract `LiveStore`, SIRI XML parsing and live matching
into `app/services/siri_*` modules with a provider/cache boundary and mocked XML
fixtures. Endpoint groups should move into `app/api/` routers only after those
services are stable.

## Incremental migration plan

1. Keep `app.main:app` as the stable deployment boundary and retain legacy code.
2. Add tests around current endpoint contracts and representative GTFS fixtures.
3. **Complete:** move pure time, text and geo helpers into `app/utils/`.
4. **Complete:** move `GTFSStore` and calendar logic into `app/services/` and
   activate complete candidates through a store provider.
5. Move SIRI fetching and live matching into isolated services with mocked tests.
6. Move endpoint groups into `app/api/` routers one at a time, retaining aliases.
7. Split the active inline frontend into `static/css` and `static/js/views` only
   after browser-level regression coverage exists.
8. Compare and archive duplicate data/code only after runtime references and
   deployment behavior are verified. Do not delete legacy files before then.
