import pandas as pd
import json
import os
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# Globális GTFS adatok tárolására szolgáló szótár
gtfs_data = {}

def load_gtfs_data():
    """
    Betölti az összes szükséges GTFS fájlt a 'gtfs/' könyvtárból.
    """
    global gtfs_data
    gtfs_path = os.path.join(os.path.dirname(__file__), 'gtfs')
    
    if not os.path.exists(gtfs_path):
        print("HIBA: A 'gtfs/' könyvtár nem található.")
        return

    # Fájlok betöltése
    files_to_load = [
        'stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt', 
        'shapes.txt', 'calendar.txt'
    ]
    
    for filename in files_to_load:
        filepath = os.path.join(gtfs_path, filename)
        if os.path.exists(filepath):
            try:
                # pandas DataFrame betöltése
                gtfs_data[filename.split('.')[0]] = pd.read_csv(filepath)
                print(f"GTFS: {filename} betöltve.")
            except Exception as e:
                print(f"HIBA a {filename} betöltésekor: {e}")
        else:
            print(f"FIGYELEM: A {filename} fájl hiányzik.")
            
    # Alapvető ellenőrzés
    if 'stops' in gtfs_data:
        print(f"GTFS: {len(gtfs_data['stops'])} megálló betöltve.")
    if 'routes' in gtfs_data:
        print(f"GTFS: {len(gtfs_data['routes'])} útvonal betöltve.")


@app.route('/')
def index():
    """ A főoldal (index.html) megjelenítése. """
    return render_template('index.html')

# --- API VÉGPONTOK A FRONT-END SZÁMÁRA ---

@app.route('/api/search', methods=['GET'])
def search_data():
    """
    Keres a megállókban és útvonalakban a lekérdezés alapján.
    """
    query = request.args.get('q', '').lower()
    
    if not query or len(query) < 2:
        return jsonify({"stops": [], "routes": []})

    results = {"stops": [], "routes": []}

    # 1. Megállók keresése
    if 'stops' in gtfs_data:
        stops_df = gtfs_data['stops']
        
        # Szűrés a megálló nevére
        stop_matches = stops_df[
            stops_df['stop_name'].str.lower().str.contains(query, na=False)
        ].head(10)

        # JSON formátum előkészítése
        results['stops'] = stop_matches.rename(columns={
            'stop_lat': 'stop_lat',
            'stop_lon': 'stop_lon',
            'stop_id': 'stop_id',
            'stop_name': 'stop_name'
        }).to_dict('records')

    # 2. Útvonalak keresése
    if 'routes' in gtfs_data:
        routes_df = gtfs_data['routes']
        
        # Szűrés az útvonal nevére (short vagy long)
        route_matches = routes_df[
            (routes_df['route_short_name'].str.lower().str.contains(query, na=False)) |
            (routes_df['route_long_name'].str.lower().str.contains(query, na=False))
        ].head(10)
        
        # JSON formátum előkészítése (biztonságos konverzió stringekké a színekhez)
        results['routes'] = route_matches.apply(lambda row: {
            'route_id': row['route_id'],
            'route_short_name': str(row['route_short_name']),
            'route_long_name': str(row['route_long_name']),
            'route_color': str(row.get('route_color', '009933')), # Alapértelmezett szín
            'route_text_color': str(row.get('route_text_color', 'FFFFFF')),
            'route_type_desc': 'Busz' # Egyszerűsített típus
        }, axis=1).to_list()


    return jsonify(results)


@app.route('/api/route_shapes/<route_id>', methods=['GET'])
def get_route_shapes(route_id):
    """
    Visszaadja az útvonalhoz tartozó koordinátákat a térképre.
    """
    if 'trips' not in gtfs_data or 'shapes' not in gtfs_data:
        return jsonify([]), 404

    trips_df = gtfs_data['trips']
    shapes_df = gtfs_data['shapes']

    # 1. Megkeressük a shape_id-t az útvonal_id alapján
    # Csak az első shape_id-t használjuk az egyszerűség kedvéért
    try:
        shape_id = trips_df[trips_df['route_id'] == route_id]['shape_id'].iloc[0]
    except IndexError:
        return jsonify([])

    # 2. Megkeressük az összes koordinátát ehhez a shape_id-hez
    route_shape = shapes_df[shapes_df['shape_id'] == shape_id].sort_values('shape_pt_sequence')
    
    # [latitude, longitude] formátumra konvertálás
    coords = route_shape[['shape_pt_lat', 'shape_pt_lon']].values.tolist()

    return jsonify(coords)


@app.route('/api/departures/<stop_id>', methods=['GET'])
def get_departures(stop_id):
    """
    Visszaadja a valós idejű indulásokat egy adott megállóhoz (SIRI/Mock adat).
    """
    # GTFS adatok betöltése
    if 'stop_times' not in gtfs_data or 'trips' not in gtfs_data or 'routes' not in gtfs_data:
        # Ha a GTFS hiányzik, visszaadunk egy mock adatot, ami a JS-sel kompatibilis
        return jsonify({
            "stop_id": stop_id,
            "departures": [
                {"line": "1", "destination": "Southampton C", "time": "12:55", "delay": 2, "vehicle_id": "V456"},
                {"line": "2", "destination": "Romsey", "time": "13:10", "delay": -1, "vehicle_id": "V123"},
                {"line": "U1", "destination": "University", "time": "13:20", "delay": 0, "vehicle_id": "V789"}
            ]
        })

    # VALÓS GTFS INDULÁSOK (a SIRI adatokkal való kombináláshoz szükség lenne egy komplexebb modulra,
    # most csak a GTFS indulásokat szimuláljuk)
    
    # 1. Megállóhoz tartozó stop_times
    stop_times_df = gtfs_data['stop_times']
    trips_df = gtfs_data['trips']
    routes_df = gtfs_data['routes']

    current_stop_times = stop_times_df[stop_times_df['stop_id'].astype(str) == stop_id]
    
    if current_stop_times.empty:
        return jsonify({"stop_id": stop_id, "departures": []})
    
    # Összekapcsolás a trips és routes adatokkal
    combined_df = current_stop_times.merge(trips_df, on='trip_id', how='left')
    combined_df = combined_df.merge(routes_df, on='route_id', how='left')

    # Egyszerű időszűrő (pl. a következő 2 órára)
    # A GTFS-ben az idő HH:MM:SS formátumú.
    
    # Jelenlegi óra/perc megkeresése (itt szimuláljuk)
    now_hour = 12
    now_minute = 40
    
    # Csak azokat a járatokat tartjuk meg, amelyek a közeljövőben vannak (nagyon egyszerűsítve)
    
    departures_list = []
    
    for index, row in combined_df.head(10).iterrows(): # Csak az első 10 járat az egyszerűség kedvéért
        # Itt kellene egy igazi időkonverzió és dátumkezelés (calendar.txt)
        
        # Szimulálunk egy késleltetést és valós idejű állapotot
        simulated_delay = (index % 5) - 2 # -2, -1, 0, 1, 2
        is_realtime = index % 2 == 0 
        
        departures_list.append({
            "line": str(row['route_short_name']),
            "destination": str(row['trip_headsign']),
            "time": str(row['arrival_time'])[:5], # Csak HH:MM
            "delay": simulated_delay if is_realtime else None,
            "vehicle_id": f"BS{index+1}" if is_realtime else None, # Mock vehicle ID
            "route_color": str(row.get('route_color', '0057B8')),
            "route_text_color": str(row.get('route_text_color', 'FFFFFF')),
            "is_realtime": is_realtime # Segíti a front-endet
        })
        
    return jsonify({"stop_id": stop_id, "departures": departures_list})


@app.route('/api/live_data', methods=['GET'])
def get_live_data():
    """
    Visszaadja a valós idejű buszpozíciókat (SIRI/Mock adat).
    """
    # Mivel nincs aktív SIRI adatforrás, használjuk az Ön által küldött minta adatot (1000055843.png alapján)
    # Ezt a mock adatot szimuláljuk, hogy a térképen látszódjon a busz
    
    # Mock data a Bluestar területről (Southampton)
    mock_live_data = [
        {"lat": 50.9323, "lon": -1.3969, "line_ref": "1", "dest": "Winchester", "id": "1803"},
        {"lat": 50.9090, "lon": -1.3852, "line_ref": "2", "dest": "Romsey", "id": "1605"},
        {"lat": 50.9410, "lon": -1.3890, "line_ref": "U1", "dest": "University", "id": "2759"},
        {"lat": 50.8970, "lon": -1.4150, "line_ref": "3", "dest": "City Centre", "id": "1234"},
        {"lat": 50.9120, "lon": -1.3650, "line_ref": "18", "dest": "Irving Road", "id": "1805"},
        # További random buszok Southampton körül
    ]

    # Keresés a route_id-re (line_ref) és színek hozzárendelése
    if 'routes' in gtfs_data:
        routes_df = gtfs_data['routes'].set_index('route_short_name')
        
        for bus in mock_live_data:
            line_ref = str(bus['line_ref'])
            if line_ref in routes_df.index:
                route_info = routes_df.loc[line_ref]
                bus['route_color'] = str(route_info.get('route_color', '0057B8'))
                bus['route_text_color'] = str(route_info.get('route_text_color', 'FFFFFF'))
            else:
                bus['route_color'] = '0057B8' # Alapértelmezett Bluestar kék
                bus['route_text_color'] = 'FFFFFF'
            
            # A JS a line_ref helyett route_short_name-et vár
            bus['route_short_name'] = bus.pop('line_ref')

    return jsonify(mock_live_data)


# Alkalmazás indítása előtt a GTFS adatok betöltése
with app.app_context():
    load_gtfs_data()

if __name__ == '__main__':
    # Helyi fejlesztéshez
    app.run(debug=True, port=5000)
    
