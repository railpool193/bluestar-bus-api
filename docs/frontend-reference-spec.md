# Frontend reference specification

## Scope and evidence

This is a parity target derived from the supplied reference recording, the requested screen inventory, and the currently served `templates/index.html`. It copies no name, logo, source code, or protected artwork. Bluestar/Unilink remains the product identity. The reference recording could not be frame-captured in the current environment because no video decoder was available, so details not stated by the request are deliberately not invented.

## Shared visual and state language

- Dark navy/charcoal surfaces, blue focus/action colour, high-contrast text, and GTFS `route_color` where present; otherwise use the Bluestar blue fallback.
- Shared top header, route badge, dense list row, status label, loading, empty, and error treatments. Live is green, timetable is neutral, failure is red, late is warning/red, and early is distinct from late.
- Never imply live data when SIRI matching is absent. Unknown vehicle models remain blank.
- Mobile is the primary dense single-column layout. Desktop constrains readable content width and gives the map more space without changing information hierarchy.

## Screen audit

### 1. Home / search

- Visuals: shared header and service status, prominent search field, favourites, then popular routes when a trustworthy popularity source exists.
- Fields: query, stop name/code, route number/colour, favourite state.
- Interactions: incremental search, clear, open stop/route, add/remove favourite.
- API: `GET /api/search`; favourites are local browser state. Missing: popular-route ranking.
- Extension: grouped result rendering and an explicit, non-invented popularity source or configured list.

### 2. Search results

- Visuals: separate stop and route groups, route-colour badges, compact secondary text, empty/error states.
- Fields: stop id/name/code/coordinates; route id/number/long name/colour.
- Interactions: select result, preserve query on back/forward.
- API: `GET /api/search`. Route colour is now exposed as `routeColor`.
- Missing/extension: no relevance score or usage frequency; add only if backed by analytics/configuration.

### 3. Favourites

- Visuals: saved stops and routes in the same shared row language; clear empty explanation.
- Fields: type, id, display name, code/line and route colour when known.
- Interactions: open, add/remove, persist locally.
- API: none required; current `localStorage` is sufficient for one browser.
- Missing/extension: account sync needs authentication and a server-side favourites API.

### 4. Vehicle list

- Visuals: line badge, destination, fleet/vehicle reference, movement state and current/next stop.
- Fields: `line`, `destinationFull`, `fleet`, `vehicleRef`, optional model, stop, `vehicleAtStop`, bearing and position.
- Interactions: line filter, refresh, map, and later vehicle-to-trip navigation.
- API: `GET /api/vehicles?line=`.
- Missing/extension: model requires fleet registry; reliable next-stop wording needs progress semantics; trip navigation needs a stable trip id.

### 5. Stop departures

- Visuals: stop name/code and favourite; coloured route badge; destination; scheduled/expected time; minutes; live/timetable and delay state.
- Fields: current departure contract plus `routeColor`; fleet/model only when known.
- Interactions: open trip, favourite stop, refresh through navigation/reload.
- API: `GET /api/stops/{stop_id}/departures`.
- Missing/extension: model enrichment and an explicit early/late presentation policy. Live allocation is one vehicle to at most one departure per response.

### 6. Trip details

- Visuals: line/destination hero, optional fleet/model, delay, full ordered stop list and map action.
- Fields: trip/service date, route, shape, live vehicle, scheduled/expected stop times, current/past/future state.
- Interactions: map, vehicles, back/deep-link.
- API: `GET /api/trips/{trip_id}` and existing shape in the trip response.
- Missing/extension: vehicle model registry; per-stop predictions are only reliable where supplied or safely calculated.

### 7. Map

- Visuals: dark-compatible basemap, high-contrast route-number marker, bearing arrow, route shape, selected vehicle card and mobile-safe controls.
- Fields: line, destination, fleet/ref, position, bearing, current/next stop, shape.
- Interactions: line filter, clear, refresh, select/follow vehicle, later marker-to-trip navigation.
- API: `GET /api/map?line=` already supplies vehicles, shapes and centre.
- Missing/extension: follow mode and marker-to-trip require a reliable trip reference; current OSM/Leaflet can remain. A production dark tile provider must have suitable licensing and availability.

### 7a. Route details

- Coloured route badge, route name, directions, ordered stops, live vehicle
  count and map action. Source: `/api/routes/{line}` and GTFS route colour.
- Favourite/open-stop/vehicles/map interactions. Service alerts and frequency
  summaries remain unavailable without a trustworthy source.

### 7b. Selected vehicle card

- Show route/destination and SIRI position/status first; optionally show fleet,
  registration, type, fuel, livery name, garage and features from cached
  Bustimes metadata. Never inject livery CSS, notes, HTML or external images.
- Missing, ambiguous or withdrawn-only metadata omits model/registration and
  cannot alter the marker or trip action.

### 8. Navigation / back / close / deep-link

- The URL hash is canonical: `#view?key=value`; stop/trip/route require their identifiers. Map and vehicles permit an optional line.
- Browser back/forward re-parses the URL. Direct links render from the hash. Invalid or incomplete state is canonicalised to `#home`.
- Each transition replaces the complete parameter object, tears down map/timers, and updates `document.title`; view-specific state therefore cannot leak.

### 9. Loading / error / empty

- Loading is neutral and must not look live. Empty distinguishes “no departures/vehicles/results” from failure. Error is high contrast and keeps a route back home.
- Existing API errors remain authoritative; no synthetic vehicles, predictions, models, or availability are rendered.
- Next extension: accessible retry controls and `aria-live` announcements.

- **GTFS error:** explain timetable unavailability; never synthesize departures.
- **SIRI/live error:** keep timetable screens usable and mark rows scheduled.
- **Fleet metadata unavailable:** keep all live fields, omit model/registration,
  and never display an invented “Unknown bus”.

### 10. Responsive behaviour

- At <=430 px use compact rows, controls and typography with touch targets retained. Respect safe-area insets.
- At wider sizes retain the same navigation and ordering, cap list reading width where useful, and allow a taller/wider map/card layout.
- Next extension: test 320, 390, 430, 768, 1024 and 1440 px plus keyboard-only and reduced-motion modes.

Mobile retains a touch-safe single column. Desktop adds reading width and
map/card space without changing information order or navigation semantics.

## Manual browser regression checklist

1. Open `/#home`, then direct-link valid stop, trip, route, vehicles and map hashes. Confirm view and title.
2. Open `/#stop` and `/#unknown?x=1`; confirm canonical home fallback.
3. Navigate stop -> trip -> map, then back/forward twice; confirm the hash, title and visible view always agree.
4. Move between map, vehicles, stop and home; confirm map layers/timers and identifiers do not remain in another view.
5. With fixture/offline API states, verify loading, empty, timetable-only and error treatments without real network access.
6. At mobile and desktop widths inspect overflow, touch targets, map controls and route-badge contrast.
7. Confirm no unknown model is displayed and the same fleet appears live on no more than one row of a stop response.

Automated tests must mock data and must not call BODS, GTFS hosts, or map tile services.
