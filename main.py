# main.py
import os
import json
from datetime import datetime, date, timedelta, time as dtime
from typing import List, Dict, Optional

import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# -----------------------------
# Opció: élő adatok (SIRI)
# -----------------------------
SIRI_AVAILABLE = False
try:
    import siri_live  # a te siri_live.py modulod
    SIRI_AVAILABLE = True
except Exception:
    # ha nincs vagy hibázik, akkor timetable-only módban megyünk
    SIRI_AVAILABLE = False


app = FastAPI(title="Bluestar Bus API", version="1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# GTFS betöltés
# -----------------------------
DATA_DIR = os.getenv("GTFS_DIR", "data")

def _load_csv(path: str) -> pd.DataFrame:
    p = os.path.join(DATA_DIR, path)
    if not os.path.exists(p):
        raise FileNotFoundError(f"Hiányzó GTFS fájl: {p}")
    return pd.read_csv(p)

stops_df         = _load_csv("stops.txt")
stop_times_df    = _load_csv("stop_times.txt")
trips_df         = _load_csv("trips.txt")
routes_df        = _load_csv("routes.txt")
calendar_df      = _load_csv("calendar.txt")
calendar_dates_df= _load_csv("calendar_dates.txt")

# Normalize oszlopok (ha hiányoznának)
for col in ["stop_code", "stop_name"]:
    if col not in stops_df.columns:
        stops_df[col] = None

if "trip_headsign" not in trips_df.columns:
    trips_df["trip_headsign"] = ""

# gyors indexek
STOP_ID_SET: set = set(stops_df["stop_id"].astype(str))

# ATCO/NaPTAN (stop_code) -> GTFS stop_id
ATCO_TO_GTFS: Dict[str, str] = {}
if "stop_code" in stops_df.columns:
    for _, row in stops_df.iterrows():
        sc = str(row.get("stop_code") or "").strip()
        if sc:
            ATCO_TO_GTFS[sc] = str(row["stop_id"])

# keresőhöz: stop_name + stop_id
SEARCH_ITEMS: List[Dict[str, str]] = []
for _, r in stops_df.iterrows():
    SEARCH_ITEMS.append({
        "stop_id": str(r["stop_id"]),
        "stop_code": str(r.get("stop_code") or ""),
        "name": str(r.get("stop_name") or ""),
    })


# -----------------------------
# Segédfüggvények
# -----------------------------
def resolve_stop_id(input_id: str) -> str:
    """
    ATCO/NaPTAN kód -> GTFS stop_id feloldás.
    Ha már GTFS stop_id, változatlan.
    Ha nincs találat, visszaadjuk az eredetit (így látszik, hogy nincs rá timetable).
    """
    if not input_id:
        return input_id
    sid = str(input_id).strip()
    if sid in STOP_ID_SET:
        return sid
    # ATCO/NaPTAN feloldás
    if sid in ATCO_TO_GTFS:
        return ATCO_TO_GTFS[sid]
    u = sid.upper()
    if u in ATCO_TO_GTFS:
        return ATCO_TO_GTFS[u]
    return sid


def service_active_on(d: date, service_row) -> bool:
    """
    GTFS calendar + calendar_dates alapján aktív-e a service adott napon.
    """
    # calendar.txt ellenőrzés
    weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][d.weekday()]
    if not service_row.get(weekday, 0):
        return False

    start_date = str(service_row.get("start_date"))
    end_date   = str(service_row.get("end_date"))
    # YYYYMMDD -> date
    def parse_ymd(s: str) -> date:
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    try:
        sd = parse_ymd(start_date)
        ed = parse_ymd(end_date)
        if d < sd or d > ed:
            return False
    except Exception:
        # ha valami hibás a dátumban, próbáljunk naponként override-dal (lent)
        pass

    # calendar_dates override-ok
    #  - exception_type = 2 -> kivétel (nem jár)
    #  - exception_type = 1 -> extra nap (jár)
    sid = service_row["service_id"]
    day = int(d.strftime("%Y%m%d"))
    sub = calendar_dates_df[calendar_dates_df["service_id"] == sid]
    if not sub.empty:
        exact = sub[sub["date"] == day]
        if not exact.empty:
            et = int(exact.iloc[0]["exception_type"])
            if et == 2:
                return False
            if et == 1:
                return True
    return True


def build_active_service_set(d: date) -> set:
    """
    Mely service_id-k aktívak ma?
    """
    active = set()
    for _, srv in calendar_df.iterrows():
        if service_active_on(d, srv):
            active.add(srv["service_id"])
    # calendar_dates-ből extra 1-es (hozzáadás)
    adds = calendar_dates_df[calendar_dates_df["exception_type"] == 1]
    day = int(d.strftime("%Y%m%d"))
    adds = adds[adds["date"] == day]
    for _, r in adds.iterrows():
        active.add(r["service_id"])
    # calendar_dates kivételek (2-es) levonása – ezt már a service_active_on intézi,
    # itt nem muszáj még egyszer.
    return active


def parse_gtfs_time_to_datetime(gtfs_hms: str, base_day: date) -> Optional[datetime]:
    """
    'HH:MM:SS' (akár 24+ órás) GTFS idő -> konkrét datetime (base_day-hoz).
    """
    if not gtfs_hms or pd.isna(gtfs_hms):
        return None
    try:
        parts = gtfs_hms.split(":")
        h = int(parts[0]); m = int(parts[1]); s = int(parts[2]) if len(parts) > 2 else 0
        carry_days = h // 24
        h = h % 24
        dt = datetime.combine(base_day, dtime(0, 0, 0)) + timedelta(days=carry_days, hours=h, minutes=m, seconds=s)
        return dt
    except Exception:
        return None


def get_live_times_for_stop(stop_id: str, minutes: int) -> set:
    """
    Élő (SIRI) időpontok halmaza. Formátum: { 'HH:MM' } (lokális idő).
    Ha nincs SIRI, üres halmaz.
    """
    if not SIRI_AVAILABLE:
        return set()
    try:
        # feltételezzük, hogy a siri_live-ben van egy ilyen segédfüggvény:
        # get_departures(stop_id: str, minutes: int) -> List[Dict[str, Any]]
        # és a dict-ben van pl. "time" vagy "departure_time" "HH:MM" formátumban.
        live = siri_live.get_departures(stop_id=stop_id, minutes=minutes)  # igazítsd a saját implementációdhoz
        times = set()
        for it in live or []:
            t = str(it.get("time") or it.get("departure_time") or "")
            if t:
                # formázzuk HH:MM-re (ha HH:MM:SS jön)
                times.add(t[0:5])
        return times
    except Exception:
        return set()


def get_next_departures(stop_id: str, minutes: int = 60) -> List[Dict]:
    """
    Következő indulások GTFS alapján, minutes időablakban.
    Élő adatok bejelölése (is_live) – ha elérhető.
    """
    now = datetime.now()
    end_time = now + timedelta(minutes=minutes)

    # csak ma (és túlfutó éjfél felett) nézünk
    active_services = build_active_service_set(now.date())

    # join: stop_times + trips + routes
    st = stop_times_df[stop_times_df["stop_id"].astype(str) == str(stop_id)]
    if st.empty:
        return []
    merged = st.merge(trips_df, on="trip_id", how="left").merge(routes_df, on="route_id", how="left")

    rows = []
    for _, r in merged.iterrows():
        sid = r.get("service_id")
        if sid not in active_services:
            continue

        dep_str = str(r.get("departure_time") or "")
        dep_dt = parse_gtfs_time_to_datetime(dep_str, now.date())
        if dep_dt is None:
            continue

        if dep_dt < now or dep_dt > end_time:
            continue

        route_short = str(r.get("route_short_name") or r.get("route_id") or "")
        headsign = str(r.get("trip_headsign") or r.get("route_long_name") or "")

        rows.append({
            "route": route_short,
            "destination": headsign,
            "departure_time": dep_dt.strftime("%H:%M"),
        })

    # idő szerint rendezés
    rows.sort(key=lambda x: x["departure_time"])

    # élő időpontok bejelölése (ha van)
    live_times = get_live_times_for_stop(stop_id, minutes)
    for it in rows:
        it["is_live"] = (it["departure_time"] in live_times)

    return rows


# -----------------------------
# Végpontok
# -----------------------------
@app.get("/", response_class=JSONResponse)
def root():
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "health": "/health",
            "ck_next_60": "/vincents-walk/ck?minutes=60",
            "cm_next_60": "/vincents-walk/cm?minutes=60",
            "generic_example": "/next_departures/1980SN12619E?minutes=60",
        },
    }


@app.get("/health", response_class=JSONResponse)
def health():
    return {"status": "ok", "gtfs_loaded": True, "siri_available": SIRI_AVAILABLE}


@app.get("/search_stops", response_class=JSONResponse)
def search_stops(q: str = Query(..., min_length=2), limit: int = 12):
    """
    Név szerinti kereső (case-insensitive). Visszaadja a
    - display_name (pl. 'Southampton, Vincents Walk [CH]'),
    - stop_id (GTFS),
    - stop_code (ATCO/NaPTAN) mezőket.
    """
    tq = q.strip().lower()
    out = []
    for item in SEARCH_ITEMS:
        name = item["name"].lower()
        if tq in name:
            out.append({
                "display_name": item["name"],
                "stop_id": item["stop_id"],
                "stop_code": item["stop_code"],
            })
            if len(out) >= limit:
                break
    return {"query": q, "results": out}


@app.get("/next_departures/{stop_id}", response_class=JSONResponse)
def next_departures(stop_id: str, minutes: int = Query(60, ge=1, le=360)):
    """
    Következő indulások. Elfogad:
    - GTFS stop_id (1980SN…),
    - ATCO/NaPTAN (1900HAA…): automatikusan feloldjuk GTFS-re.
    """
    canonical = resolve_stop_id(stop_id)
    deps = get_next_departures(canonical, minutes=minutes)
    return {"stop_id": canonical, "minutes": minutes, "departures": deps}


# --- kényelmi példa endpointok (ahogy korábban használtad) ---
@app.get("/vincents-walk/ck", response_class=JSONResponse)
def vw_ck(minutes: int = Query(60, ge=1, le=360)):
    return next_departures("1980SN12618B", minutes)  # Vincents Walk [CH]


@app.get("/vincents-walk/cm", response_class=JSONResponse)
def vw_cm(minutes: int = Query(60, ge=1, le=360)):
    return next_departures("1980HAA13371", minutes)  # ATCO -> feloldás után GTFS


# Debug: feloldás teszt
@app.get("/api/debug/resolve", response_class=JSONResponse)
def debug_resolve(input: str):
    resolved = resolve_stop_id(input)
    return {"input": input, "resolved_to": resolved, "is_gtfs": resolved in STOP_ID_SET}


# -----------------------------
# Egyszerű UI (ha van index.html a repo gyökerében)
# -----------------------------
@app.get("/index.html", response_class=HTMLResponse)
def serve_index():
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse('<pre>{"message":"Bluestar Bus API - nincs index.html"}</pre>')
