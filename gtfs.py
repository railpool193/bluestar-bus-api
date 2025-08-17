import sqlite3
import csv
from datetime import datetime, timedelta, timezone

# ---- importálás ----

def import_from_zip_to_sqlite(zf, db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executescript("""
    PRAGMA journal_mode=WAL;
    DROP TABLE IF EXISTS stops;
    DROP TABLE IF EXISTS routes;
    DROP TABLE IF EXISTS trips;
    DROP TABLE IF EXISTS stop_times;

    CREATE TABLE stops (
        stop_id TEXT PRIMARY KEY,
        stop_code TEXT,
        stop_name TEXT,
        stop_lat REAL,
        stop_lon REAL
    );

    CREATE TABLE routes (
        route_id TEXT PRIMARY KEY,
        route_short_name TEXT,
        route_long_name TEXT
    );

    CREATE TABLE trips (
        trip_id TEXT PRIMARY KEY,
        route_id TEXT,
        service_id TEXT,
        trip_headsign TEXT
    );

    CREATE TABLE stop_times (
        trip_id TEXT,
        arrival_time TEXT,
        departure_time TEXT,
        stop_id TEXT,
        stop_sequence INTEGER
    );

    CREATE INDEX idx_stops_name ON stops(stop_name);
    CREATE INDEX idx_stop_times_stop ON stop_times(stop_id);
    CREATE INDEX idx_stop_times_trip ON stop_times(trip_id);
    """)

    def load_csv(name, table, cols):
        if name not in zf.namelist():
            return
        with zf.open(name) as f:
            reader = csv.DictReader((line.decode("utf-8-sig") for line in f))
            rows = []
            for r in reader:
                rows.append(tuple(r.get(c) for c in cols))
                if len(rows) >= 5000:
                    cur.executemany(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})", rows)
                    rows.clear()
            if rows:
                cur.executemany(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})", rows)

    load_csv("stops.txt", "stops", ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"])
    load_csv("routes.txt", "routes", ["route_id", "route_short_name", "route_long_name"])
    load_csv("trips.txt", "trips", ["trip_id", "route_id", "service_id", "trip_headsign"])
    load_csv("stop_times.txt", "stop_times", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])

    conn.commit()
    conn.close()


# ---- keresés ----

def search_stops(db_path: str, q: str, limit: int = 12):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    like = f"%{q.strip()}%"
    cur.execute(
        """
        SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE LOWER(stop_name) LIKE LOWER(?)
        ORDER BY stop_name
        LIMIT ?
        """,
        (like, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# ---- menetrendi indulások ----

def _time_to_seconds(t: str) -> int:
    # lehet 24:xx:xx feletti is
    if not t:
        return None
    parts = t.split(":")
    if len(parts) < 3:
        return None
    h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    return h * 3600 + m * 60 + s

def get_scheduled_departures(db_path: str, stop_id: str, minutes: int):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # összejoinoljuk a szükséges mezőket
    cur.execute("""
    SELECT st.departure_time, r.route_short_name, r.route_long_name, t.trip_headsign
    FROM stop_times st
    JOIN trips t ON t.trip_id = st.trip_id
    JOIN routes r ON r.route_id = t.route_id
    WHERE st.stop_id = ?
    """, (stop_id,))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return []

    now = datetime.now(timezone.utc)
    now_sec = now.hour*3600 + now.minute*60 + now.second
    horizon = now_sec + minutes*60

    out = []

    for r in rows:
        sec = _time_to_seconds(r["departure_time"])
        if sec is None:
            continue

        # kezeljük az átnyúlást (02:xx:xx másnap)
        if horizon < 24*3600:
            if now_sec <= sec <= horizon:
                out.append((sec, r))
        else:
            # két intervallum: [now_sec..86400) ∪ [0..horizon-86400]
            over = horizon - 24*3600
            if sec >= now_sec or sec <= over:
                out.append((sec, r))

    # idő szerint rendezve
    out.sort(key=lambda x: x[0])

    results = []
    for sec, r in out[:40]:
        # az ISO időpontot a mai dátum + sec alapján állítjuk elő (UTC zónában)
        base_day = now.date()
        # ha a sec kisebb, mint "most", és a horizont miatt másnapra esik:
        day_offset = 0
        if sec < now_sec and (now_sec + minutes*60) >= 24*3600:
            day_offset = 1
        hh = sec // 3600
        mm = (sec % 3600) // 60
        ss = sec % 60
        dt = datetime(
            base_day.year, base_day.month, base_day.day,
            hh, mm, ss, tzinfo=timezone.utc
        ) + timedelta(days=day_offset)

        results.append({
            "route": r["route_short_name"] or r["route_long_name"] or "?",
            "destination": r["trip_headsign"] or "?",
            "time_iso": dt.isoformat()
        })

    return results
