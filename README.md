
# Bluestar Bus API (FastAPI)

FastAPI backend that serves upcoming departures from Bluestar GTFS data.

## Endpoints
- `GET /health` → simple status
- `GET /next_departures/{stop_id}?minutes=60`

Vincent's Walk stops:
- CK: `1980SN12619E`
- CM: `1980HAA13371`

## Configure
Update `GTFS_URL` in `gtfs_utils.py` if Bluestar publishes a newer period.
You can also set it via environment variable `GTFS_URL`.

## Run locally
```
pip install -r requirements.txt
uvicorn main:app --reload
```
Open: http://127.0.0.1:8000/next_departures/1980SN12619E
