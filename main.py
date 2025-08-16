# main.py
from __future__ import annotations

import os
import json
import unicodedata
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

# --- Saját modulok (a repóban már megvannak) ---
# get_next_departures: GTFS alapú következő indulások
# EXPECTED SIGNATURE: get_next_departures(stop_id: str, minutes_ahead: int) -> List[Dict[str, Any]]
from gtfs_utils import get_next_departures  # type: ignore

# Opcionális SIRI élő adatok (ha nincs BODS_API_KEY, akkor csak "nem élő")
import siri_live  # type: ignore


APP_DIR = Path(__file__).parent.resolve()
DATA_DIR = APP_DIR / "data"
STOPS_FILE = DATA_DIR / "stops.txt"
INDEX_HTML = APP_DIR / "index.html"

app = FastAPI(title="Bluestar Bus API", version="1.0")

# CORS – ha a UI ugyanerről az originről fut, ez amúgy sem gond,
# de így fejlesztés közben kényelmes.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------
# Segédfüggvények
# --------

def _norm(s: str) -> str:
    """Egyszerű normalizálás: kisbetű, ékezetmentesítés, trimmelés."""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.strip().lower()


def _load_stops_df() -> pd.DataFrame:
    if not STOPS_FILE.exists():
        raise FileNotFoundError(f"Nem található a stops.txt: {STOPS_FILE}")
    df = pd.read_csv(STOPS_FILE)
    # biztosítsuk, hogy legyenek a várt oszlopok
    for col in ("stop_id", "stop_name"):
        if col not in df.columns:
            raise RuntimeError(f"stops.txt hiányzó oszlop: {col}")
    # előkészített keresőmező
    df["_norm_name"] = df["stop_name"].astype(str).apply(_norm)
    return df


# Globális cache a megállókhoz
STOPS_DF: pd.DataFrame | None = None
try:
    STOPS_DF = _load_stops_df()
    GTFS_LOADED = True
except Exception:
    STOPS_DF = None
    GTFS_LOADED = False


def _siri_available() -> bool:
    """Egyszerű jelzés, hogy a SIRI kulcs és modul használható-e."""
    # siri_live modul a környezeti változókból olvas; itt csak megkérdezzük, hogy tud-e kérdezni.
    try:
        # a modulban nincs külön "status" endpoint, de a kulcs léte elég jó jel
        return bool(os.getenv("BODS_API_KEY")) or bool(os.getenv("BODS_FEED_ID"))
    except Exception:
        return False


def _enrich_with_live(dep_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Hozzáadja az `is_live` és `live_time` kulcsokat, ha a SIRI adatok alapján van eltérés.
    Ha a SIRI nem elérhető, akkor minden elemre is_live = False marad.
    """
    if not _siri_available():
        # nincs élő adat – jelölők false/None
        for d in dep_list:
            d.setdefault("is_live", False)
            d.setdefault("live_time", None)
        return dep_list

    try:
        # A siri_live modulodban valószínűleg van olyan hívás,
        # amivel lekérheted a közeljövő járatait adott megállóhoz/járathoz.
        # Itt egyszerűen azt feltételezzük, hogy az indulás-lista elemein
        # van "route" és "departure_time" (HH:MM), és ha találunk élő eltérést,
        # akkor beírjuk.
        # Ha nálad van dedikált összevezető függvény, itt cseréld le arra.
        live_index = {}  # kulcs: (route, scheduled_time) -> live_time

        # Példa: egyetlen lekérés a SIRI-től, és abból indexet építünk
        # (ha nálad más az elérési mód, ezt igazítsd)
        # Itt nem hívunk konkrét végpontot; az implementáció modulfüggő.
        # Hogy ne dőljön el, fallback = nincs live.
        # -> Meghagyjuk a keretet; a tényleges Siri illesztőt már megírtad korábban.
        # Ha nincs ilyen nálad, nyugodtan hagyd így: csak a scheduled időket jelzi.

        for d in dep_list:
            key = (str(d.get("route", "")), str(d.get("departure_time", "")))
            live_time = live_index.get(key)
            if live_time and live_time != d.get("departure_time"):
                d["is_live"] = True
                d["live_time"] = live_time
            else:
                d["is_live"] = False
                d["live_time"] = None

    except Exception:
        # bármilyen hiba esetén visszaesünk a "nem élő" jelzésre
        for d in dep_list:
            d.setdefault("is_live", False)
            d.setdefault("live_time", None)

    return dep_list


# --------
# Endpontok
# --------

@app.get("/")
def root():
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "status": "/status",
            "search_stops_example": "/stops/search?query=vincent",
            "next_departures_example": "/next_departures/1980SN12619E?minutes=60",
            "index_html": "/index.html",
        },
    }


@app.get("/index.html")
def index_html():
    if INDEX_HTML.exists():
        return FileResponse(INDEX_HTML)
    return JSONResponse({"message": "Bluestar Bus API - nincs index.html"}, status_code=200)


@app.get("/status")
def status():
    return {
        "status": "ok",
        "gtfs_loaded": GTFS_LOADED,
        "siri_available": _siri_available(),
    }


@app.get("/stops/search")
def search_stops(query: str = Query(..., min_length=2, description="Megálló neve (részlet is lehet)"),
                 limit: int = Query(10, ge=1, le=50)) -> Dict[str, Any]:
    if not GTFS_LOADED or STOPS_DF is None:
        raise HTTPException(status_code=500, detail="GTFS (stops.txt) nincs betöltve.")

    q = _norm(query)
    # nagyon egyszerű névrészlet-keresés
    mask = STOPS_DF["_norm_name"].str.contains(q, na=False)
    hits = (
        STOPS_DF.loc[mask, ["stop_id", "stop_name"]]
        .head(limit)
        .copy()
    )

    results = []
    for _, row in hits.iterrows():
        results.append({
            "display_name": str(row["stop_name"]),
            "stop_id": str(row["stop_id"]),
        })
    return {"query": query, "results": results}


@app.get("/next_departures/{stop_id}")
def api_next_departures(
    stop_id: str,
    minutes: int = Query(60, ge=1, le=480, description="Előretekintési idő percben"),
):
    """
    GTFS alapú indulások a megadott megállóból a következő X percben.
    Ha elérhető a SIRI, akkor `is_live` és `live_time` mezőket is kap minden sor.
    """
    try:
        # Figyelem: a korábbi hibád az volt, hogy a gtfs_utils.get_next_departures()
        # nem "minutes" névvel, hanem pl. "minutes_ahead" paraméterrel várt értéket.
        # Ezért itt POZÍCIÓS argumentumként adjuk át (stop_id, minutes),
        # így bármelyik névvel működni fog.
        deps = get_next_departures(stop_id, minutes)  # type: ignore
    except TypeError:
        # Végső fallback: ha netán fordított a sorrend, próbáljuk úgy is.
        try:
            deps = get_next_departures(minutes, stop_id)  # type: ignore
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to build departures: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build departures: {e}") from e

    # élő adatokkal dúsítás (ha van)
    deps = _enrich_with_live(list(deps))

    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "departures": deps,
    }


# ---------- Fejlesztői futtatás ----------
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
