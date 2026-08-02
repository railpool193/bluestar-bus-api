# Frontend implementation — phase 1

## Implemented screens

- Responsive home/search and favourites view.
- Stop departure board with route colours, live/timetable state, delay state, vehicle metadata, manual refresh, favourites and automatic refresh.
- Trip detail hero and stop timeline with current, next, future and passed stops, live vehicle metadata and preserved scroll position during refresh.
- Hash-based navigation with valid deep links, browser history and Hungarian/English UI text.
- Shared loading, empty, retry and degraded service states.

## Architecture

The HTML file is now a small application shell. Styles are split into application, component, view and map layers. JavaScript is split into API, state, router, i18n, formatter, component and per-view modules. Each mounted view owns and disposes its timers and abort controllers; service status has one application-level polling loop.

API-derived text is written through DOM text nodes instead of HTML injection. Route colours are validated before use and their label contrast is computed consistently.

## Timing behaviour

- A timetable-only departure is never labelled `Due`.
- An unmatched timetable departure older than 90 seconds is removed from the board.
- A delayed departure remains visible when it has a matching live vehicle, even when its scheduled time has passed.
- Live departures expose current-stop and at-stop metadata to the frontend.

## Verification

- Python contract and regression suite: 147 tests passed.
- JavaScript syntax checks passed for every module.
- Standalone formatter, translation and router module tests passed.
- Local HTTP root and modular assets were served successfully without starting external refresh jobs.

The Codex in-app browser could not initialize because its browser kernel asset path was unavailable (`os error 3`). Consequently no reliable visual viewport screenshots were produced in this environment.

## Next phase

The route, vehicles and map modules currently provide functional modular foundations. The next frontend phase should complete their reference-level layout, vehicle marker interaction, route geometry treatment and map/list coordination, followed by visual regression checks at mobile and desktop widths.
