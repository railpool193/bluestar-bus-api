import pandas as pd
import os
import random
import datetime
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

gtfs_data = {}

def load_gtfs_data():
    global gtfs_data
    gtfs_path = os.path.join(os.path.dirname(__file__), 'gtfs')

    if not os.path.exists(gtfs_path):
        print("HIBA: A 'gtfs/' könyvtár nem található.")
        return

    files_to_load = [
        'stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt',
        'shapes.txt', 'calendar.txt'
    ]

    for filename in files_to_load:
        filepath = os.path.join(gtfs_path, filename)
        if os.path.exists(filepath):
            try:
                gtfs_data[filename.split('.')[0]] = pd.read_csv(filepath)
                print(f"GTFS: {filename} betöltve.")
            except Exception as e:
                print(f"HIBA a {filename} betöltésekor: {e}")
        else:
            print(f"FIGYELEM: A {filename} fájl hiányzik.")

    if 'stops' in gtfs_data:
        print(f"GTFS: {len(gtfs_data['stops'])} megálló betöltve.")
    if 'routes' in gtfs_data:
        print(f"GTFS: {len(gtfs_data['routes'])} útvonal betöltve.")
    if 'stop_times' in gtfs_data:
        print(f"GTFS: {len(gtfs_data['stop_times'])} stop_times sor betöltve.")


def gtfs_time_to_seconds(t: str) -> int:
    """
    GTFS idő (HH:MM:SS) -> másodperc a nap elejétől.
    Tudja kezelni a 24+ órát is (pl 25:10:00).
    """
    if pd.isna(t) or not str(t).strip():
        return -1
    try:
        parts = str(t).strip().split(":")
        h = int(parts[0]); m = int(parts[1]); s = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    except Exception:
        return -1


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/search', methods=['GET'])
def search_data():
    query = request.args.get('q', '').lower().strip()

    if not query or len(query) < 2:
        return jsonify({"stops": [], "routes": []})

    results = {"stops": [], "routes": []}

    if 'stops' in gtfs_data:
        stops_df = gtfs_data['stops']
        stop_matches = stops_df[
            stops_df['stop_name'].astype(str).str.lower().str.contains(query, na=False)
        ].head(10)

        results['stops'] = stop_matches[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].fillna("").to_dict('records')

    if 'routes' in gtfs_data:
        routes_df = gtfs_data['routes']
        route_matches = routes_df[
            (routes_df['route_short_name'].astype(str).str.lower().str.contains(query, na=False)) |
            (routes_df['route_long_name'].astype(str).str.lower().str.contains(query, na=False))
        ].head(10)

        results['routes'] = route_matches.apply(lambda row: {
            'route_id': str(row.get('route_id', '')),
            'route_short_name': str(row.get('route_short_name', '')),
            'route_long_name': str(row.get('route_long_name', '')),
            'route_color': str(row.get('route_color', '009933')) or "009933",
            'route_text_color': str(row.get('route_text_color', 'FFFFFF')) or "FFFFFF",
            'route_type_desc': 'Busz'
        }, axis=1).to_list()

    return jsonify(results)


@app.route('/api/route_shapes/<route_id>', methods=['GET'])
def get_route_shapes(route_id):
    if 'trips' not in gtfs_data or 'shapes' not in gtfs_data:
        return jsonify([]), 404

    trips_df = gtfs_data['trips']
    shapes_df = gtfs_data['shapes']

    try:
        shape_id = trips_df[trips_df['route_id'].astype(str) == str(route_id)]['shape_id'].iloc[0]
    except Exception:
        return jsonify([])

    route_shape = shapes_df[shapes_df['shape_id'].astype(str) == str(shape_id)].sort_values('shape_pt_sequence')

    coords = route_shape[['shape_pt_lat', 'shape_pt_lon']].dropna().values.tolist()
    return jsonify(coords)


@app.route('/api/departures/<stop_id>', methods=['GET'])
def get_departures(stop_id):
    # ha hiányzik bármelyik GTFS tábla, adjunk mockot
    if 'stop_times' not in gtfs_data or 'trips' not in gtfs_data or 'routes' not in gtfs_data or 'stops' not in gtfs_data:
        return jsonify({
            "stop_id": stop_id,
            "stop_name": f"Stop {stop_id}",
            "departures": [
                {"line": "1", "destination": "Southampton C", "time": "12:55", "delay": 2,
                 "vehicle_id": "V456", "trip_id": "MOCK_TRIP_1",
                 "route_color": "0057B8", "route_text_color": "FFFFFF", "is_realtime": True},
            ]
        })

    stop_times_df = gtfs_data['stop_times']
    trips_df = gtfs_data['trips']
    routes_df = gtfs_data['routes']
    stops_df = gtfs_data['stops']

    # stop név
    stop_row = stops_df[stops_df['stop_id'].astype(str) == str(stop_id)]
    stop_name = str(stop_row['stop_name'].iloc[0]) if not stop_row.empty else f"Stop {stop_id}"

    current_stop_times = stop_times_df[stop_times_df['stop_id'].astype(str) == str(stop_id)].copy()
    if current_stop_times.empty:
        return jsonify({"stop_id": stop_id, "stop_name": stop_name, "departures": []})

    combined_df = current_stop_times.merge(trips_df, on='trip_id', how='left')
    combined_df = combined_df.merge(routes_df, on='route_id', how='left')

    # időszűrés: mostani idő másodpercben
    now = datetime.datetime.now()
    now_sec = now.hour * 3600 + now.minute * 60 + now.second

    # departure_time -> sec
    combined_df['dep_sec'] = combined_df['departure_time'].apply(gtfs_time_to_seconds)
    combined_df = combined_df[combined_df['dep_sec'] >= 0]

    # csak a most utániak + rendezés
    combined_df_filtered = combined_df[combined_df['dep_sec'] >= now_sec].sort_values('dep_sec')

    # ha nincs utána, akkor a legkorábbi 15
    if combined_df_filtered.empty:
        combined_df_filtered = combined_df.sort_values('dep_sec').head(15)

    departures_list = []
    rows = combined_df_filtered.head(15).fillna("")

    for idx, row in rows.iterrows():
        simulated_delay = (idx % 5) - 2  # -2..+2
        is_realtime = (idx % 2 == 0)

        dep_time = str(row.get('departure_time', '')) or str(row.get('arrival_time', ''))
        dep_hhmm = dep_time[:5] if dep_time else "--:--"

        departures_list.append({
            "line": str(row.get('route_short_name', '')).strip(),
            "destination": str(row.get('trip_headsign', '')).strip(),
            "time": dep_hhmm,
            "delay": simulated_delay if is_realtime else None,
            "vehicle_id": f"BS{idx+1}",
            "trip_id": str(row.get('trip_id', '')).strip(),  # <-- EZ KELL a trip nézethez
            "route_id": str(row.get('route_id', '')).strip(),
            "route_color": str(row.get('route_color', '0057B8')) or "0057B8",
            "route_text_color": str(row.get('route_text_color', 'FFFFFF')) or "FFFFFF",
            "is_realtime": is_realtime
        })

    return jsonify({"stop_id": stop_id, "stop_name": stop_name, "departures": departures_list})


@app.route('/api/trip_route/<trip_id>', methods=['GET'])
def get_trip_route(trip_id):
    if 'stop_times' not in gtfs_data or 'stops' not in gtfs_data:
        return jsonify({"trip_id": trip_id, "route": []}), 404

    stop_times_df = gtfs_data['stop_times']
    stops_df = gtfs_data['stops']

    trip_stops = stop_times_df[stop_times_df['trip_id'].astype(str) == str(trip_id)].copy()
    if trip_stops.empty:
        return jsonify({"trip_id": trip_id, "total_delay": 0, "route": []})

    trip_stops = trip_stops.sort_values('stop_sequence')
    combined_df = trip_stops.merge(stops_df[['stop_id', 'stop_name']], on='stop_id', how='left').fillna("")

    route_list = []
    for i, row in combined_df.iterrows():
        stop_seq = int(row['stop_sequence']) if str(row['stop_sequence']).isdigit() else 0

        status = 'future'
        if stop_seq < 3:
            status = 'past'
        elif stop_seq == 3:
            status = 'current'

        simulated_delay = (i % 5) - 2

        dep_time = str(row.get('departure_time', '')) or str(row.get('arrival_time', ''))
        dep_hhmm = dep_time[:5] if dep_time else "--:--"

        route_list.append({
            "time": dep_hhmm,
            "name": str(row.get('stop_name', '')).strip(),
            "delay": simulated_delay,
            "status": status
        })

    total_delay = route_list[-1]['delay'] if route_list else 0
    return jsonify({"trip_id": trip_id, "total_delay": total_delay, "route": route_list})


@app.route('/api/live_data', methods=['GET'])
def get_live_data():
    base_lat = 50.93
    base_lon = -1.39

    active_routes = ["1", "2", "U1", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
                     "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
                     "28", "35", "61"]

    mock_live_data = []
    for i, route_name in enumerate(active_routes):
        lat = base_lat + (random.random() - 0.5) * 0.05
        lon = base_lon + (random.random() - 0.5) * 0.08

        mock_live_data.append({
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "route_short_name": route_name,
            "dest": f"Destination for {route_name}",
            "id": f"V{1800 + i}",
            "route_color": "0057B8",
            "route_text_color": "FFFFFF"
        })

    if 'routes' in gtfs_data:
        routes_df = gtfs_data['routes'].set_index('route_short_name')

        for bus in mock_live_data:
            line = str(bus['route_short_name'])
            if line in routes_df.index:
                info = routes_df.loc[line]
                bus['route_color'] = str(info.get('route_color', '0057B8')) or "0057B8"
                bus['route_text_color'] = str(info.get('route_text_color', 'FFFFFF')) or "FFFFFF"

    return jsonify(mock_live_data)


@app.route('/api/service_updates', methods=['GET'])
def get_service_updates():
    mock_updates = [
        {"route_short_name": "1", "update_text": "Winchester felé késés.", "update_type": "delay"},
        {"route_short_name": "2", "update_text": "Forgalomkorlátozás miatt elkerülő útvonal.", "update_type": "diversion"},
        {"route_short_name": "N2", "update_text": "Éjszakai járatok menetrendje megváltozott.", "update_type": "info"},
        {"route_short_name": "3", "update_text": "Eastleigh felé 1 aktív szolgáltatási frissítés van.", "update_type": "delay"},
        {"route_short_name": "4", "update_text": "Romsey felé 2 aktív szolgáltatási frissítés van.", "update_type": "delay"},
        {"route_short_name": "35", "update_text": "Romsey felé útvonal változás.", "update_type": "info"},
    ]

    if 'routes' in gtfs_data:
        routes_df = gtfs_data['routes'].set_index('route_short_name')
        for u in mock_updates:
            line = str(u['route_short_name'])
            if line in routes_df.index:
                info = routes_df.loc[line]
                u['route_color'] = str(info.get('route_color', '0057B8')) or "0057B8"
                u['route_text_color'] = str(info.get('route_text_color', 'FFFFFF')) or "FFFFFF"
            else:
                u['route_color'] = "FFC107"
                u['route_text_color'] = "333333"

    return jsonify(mock_updates)


with app.app_context():
    load_gtfs_data()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
