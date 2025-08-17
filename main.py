# main.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import sqlite3
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import zoneinfo

# ---- Beállítások ----
TZ = zoneinfo.ZoneInfo("Europe/London")
GTFS_DB_PATH = os.getenv("GTFS_DB", "gtfs.db")
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()

# SIRI modul opcionális – csak ha van kulcs
siri_configured = bool(BODS_API_KEY)
if siri_configured:
    try:
        import siri_live  # saját modulod
    except Exception:  # ha mégsem elérhető, kezeljük úgy, mintha nem lenne SIRI
        siri_configured = False


# ---- FastAPI ----
app = FastAPI(title="Bluestar Bus – API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- Segédfüggvények ----
def get_db() -> sqlite3.Connection:
    if not os.path.exists(GTFS_DB_PATH):
        raise HTTPException(status_code=500, detail="GTFS database not found")
    conn = sqlite3.connect(GTFS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def seconds_since_midnight(dt: datetime) -> int:
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int((dt - midnight).total_seconds())


# ---- Sémák ----
class Departure(BaseModel):
    route: str
    destination: str
    time_iso: str
    is_live: bool


# ---- Endpontok ----
@app.get("/api/status")
def api_status():
    gtfs_loaded = os.path.exists(GTFS_DB_PATH)
    return {"status": "ok", "gtfs_loaded": gtfs_loaded, "siri_configured": siri_configured}


@app.get("/api/stops/search")
def search_stops(q: str = Query(min_length=2, description="stop name / id / code")):
    term = f"%{q.lower()}%"
    conn = get_db()
    cur = conn.cursor()
    # Keresés stop_name, stop_id és stop_code mezőkben is
    cur.execute(
        """
        SELECT stop_id, stop_name, coalesce(stop_code,'') AS stop_code
        FROM stops
        WHERE lower(stop_name) LIKE :t
           OR lower(stop_id) LIKE :t
           OR lower(coalesce(stop_code,'')) LIKE :t
        ORDER BY stop_name
        LIMIT 50
        """,
        {"t": term},
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"query": q, "results": rows}


@app.get("/api/stops/{stop_id}/next_departures", response_model=Dict[str, Any])
def next_departures(stop_id: str, minutes: int = Query(60, ge=5, le=720)):
    """
    Visszaadja a következő indulásokat az adott megállóból.
    Ha SIRI be van állítva, először megpróbál élő adatot kérni.
    Ha nincs vagy üres, GTFS menetrendre esik vissza.
    """
    results: List[Departure] = []

    # 1) Élő (SIRI), ha elérhető
    if siri_configured:
        try:
            live = siri_live.get_next_departures(stop_id=stop_id, minutes=minutes)  # -> List[dict]
            for d in live:
                # elvárt kulcsok: route, destination, time_iso
                results.append(
                    Departure(
                        route=str(d.get("route", "")),
                        destination=str(d.get("destination", "")),
                        time_iso=str(d.get("time_iso", "")),
                        is_live=True,
                    )
                )
        except Exception:
            # Ha SIRI hiba, simán visszaesünk GTFS-re
            pass

    # 2) Ha nincs élő adat, vagy kevés, egészítsük ki GTFS-sel (menetrend)
    if len(results) == 0:
        results.extend(_gtfs_next_departures(stop_id, minutes))

    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "results": [r.dict() for r in results],
    }


# ---- GTFS fallback logika ----
def _gtfs_next_departures(stop_id: str, minutes: int) -> List[Departure]:
    """
    Egyszerű (gyors) GTFS menetrend lekérdezés az adott napra és időablakra.
    Feltételez:
      - stops.stop_id = stop_times.stop_id
      - trips.trip_id = stop_times.trip_id
      - routes.route_id = trips.route_id
      - trips.trip_headsign a cél
    Nem kezeli részleteiben a calendar/calendar_dates kivételek teljes mátrixát,
    de napi használatra jó baseline (UK/Europe/London időzóna).
    """
    now = datetime.now(TZ)
    now_s = seconds_since_midnight(now)
    until_s = now_s + minutes * 60

    conn = get_db()
    cur = conn.cursor()

    # Ha a feed használ stop_code-ot az azonosításhoz, engedjünk egy rövid map-et
    # (ha az átadott stop_id nincs a stops.stop_id-ban, de stop_code-ban van).
    cur.execute("SELECT COUNT(1) AS c FROM stops WHERE stop_id = ?", (stop_id,))
    row = cur.fetchone()
    if row["c"] == 0:
        cur.execute(
            "SELECT stop_id FROM stops WHERE stop_code = ? LIMIT 1",
            (stop_id,),
        )
        map_row = cur.fetchone()
        if map_row:
            stop_id = map_row["stop_id"]

    # Következő indulások az adott napon és időablakban
    cur.execute(
        """
        SELECT
          st.departure_time as dep_s,                 -- másodperc éjfél óta (SQLite GTFS előkészítésnél érdemes int-re konvertálni)
          r.route_short_name as route_short_name,
          r.route_id as route_id,
          t.trip_headsign as headsign
        FROM stop_times st
        JOIN trips t ON t.trip_id = st.trip_id
        JOIN routes r ON r.route_id = t.route_id
        WHERE st.stop_id = :stop
          AND st.departure_time >= :now_s
          AND st.departure_time <= :until_s
        ORDER BY st.departure_time
        LIMIT 50
        """,
        {"stop": stop_id, "now_s": now_s, "until_s": until_s},
    )

    out: List[Departure] = []
    for r in cur.fetchall():
        dep_dt = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=int(r["dep_s"]))
        out.append(
            Departure(
                route=r["route_short_name"] or r["route_id"],
                destination=r["headsign"] or "",
                time_iso=dep_dt.isoformat(),
                is_live=False,
            )
        )

    conn.close()
    return out
