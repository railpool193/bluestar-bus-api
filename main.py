import pandas as pd
import json
import os
import random
import datetime # Új import
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
            (routes_df['route_short_name'].astype(str).str.lower().str.contains(query, na=False)) |
            (routes_df['route_long_name'].astype(str).str.lower().str.contains(query, na=False))
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
        shape_id = trips_df[trips_df['route_id'].astype(str) == route_id]['shape_id'].iloc[0]
    except IndexError:
        return jsonify([])

    # 2. Megkeressük az összes koordinátát ehhez a shape_id-hez
    route_shape = shapes_df[shapes_df['shape_id'].astype(str) == shape_id].sort_values('shape_pt_sequence')
    
    # [latitude, longitude] formátumra konvertálás
    coords = route_shape[['shape_pt_lat', 'shape_pt_lon']].values.tolist()

    return jsonify(coords)


@app.route('/api/departures/<stop_id>', methods=['GET'])
def get_departures(stop_id):
    """
    Visszaadja a valós idejű indulásokat egy adott megállóhoz (GTFS szimuláció).
    """
    # GTFS adatok betöltése
    if 'stop_times' not in gtfs_data or 'trips' not in gtfs_data or 'routes' not in gtfs_data:
        # Ha a GTFS hiányzik, visszaadunk egy mock adatot
        return jsonify({
            "stop_id": stop_id,
            "departures": [
                {"line": "1", "destination": "Southampton C", "time": "12:55", "delay": 2, "vehicle_id": "V456", "route_color": "0057B8", "route_text_color": "FFFFFF"},
            ]
        })

    # VALÓS GTFS INDULÁSOK (szimuláció)
    
    stop_times_df = gtfs_data['stop_times']
    trips_df = gtfs_data['trips']
    routes_df = gtfs_data['routes']

    current_stop_times = stop_times_df[stop_times_df['stop_id'].astype(str) == stop_id].copy()
    
    if current_stop_times.empty:
        return jsonify({"stop_id": stop_id, "departures": []})
    
    # Összekapcsolás a trips és routes adatokkal
    combined_df = current_stop_times.merge(trips_df, on='trip_id', how='left')
    combined_df = combined_df.merge(routes_df, on='route_id', how='left')
    
    # Egyszerű időszűrő: csak a mostani idő utáni indulások (HH:MM:SS stringként összehasonlítva)
    current_time_str = datetime.datetime.now().strftime("%H:%M:%S")

    # Csak azokat a járatokat tartjuk meg, amelyek a jelenlegi idő után indulnak, majd rendezzük
    combined_df_filtered = combined_df[combined_df['departure_time'] >= current_time_str].sort_values('departure_time')
    
    # Ha nincs találat a mai idő után, vegyük a legkorábbi 15 járatot
    if combined_df_filtered.empty:
        combined_df_filtered = combined_df.sort_values('departure_time').head(15)
    
    
    departures_list = []
    
    for index, row in combined_df_filtered.head(15).iterrows(): # Csak a következő 15 járat
        # Szimulálunk egy késleltetést és valós idejű állapotot
        simulated_delay = (index % 5) - 2 # -2, -1, 0, 1, 2
        is_realtime = index % 2 == 0 
        
        # Használjuk a departure_time-ot az indulási időhöz
        time_to_use = str(row.get('departure_time', row['arrival_time'])) 
        
        departures_list.append({
            "line": str(row['route_short_name']),
            "destination": str(row['trip_headsign']),
            "time": time_to_use[:5], # Csak HH:MM
            "delay": simulated_delay if is_realtime else None,
            "vehicle_id": f"BS{index+1}", # Mock vehicle ID
            "route_color": str(row.get('route_color', '0057B8')),
            "route_text_color": str(row.get('route_text_color', 'FFFFFF')),
            "is_realtime": is_realtime 
        })
        
    return jsonify({"stop_id": stop_id, "departures": departures_list})

@app.route('/api/trip_route/<trip_id>', methods=['GET'])
def get_trip_route(trip_id):
    """
    Visszaadja a trip_id-hez tartozó összes megállót (útvonal nézet).
    """
    if 'stop_times' not in gtfs_data or 'stops' not in gtfs_data:
        return jsonify({"trip_id": trip_id, "route": []}), 404

    stop_times_df = gtfs_data['stop_times']
    stops_df = gtfs_data['stops']
    
    # 1. Megkeressük a triphez tartozó stop_times bejegyzéseket
    trip_stops = stop_times_df[stop_times_df['trip_id'].astype(str) == trip_id].copy()
    
    if trip_stops.empty:
        return jsonify({"trip_id": trip_id, "route": []})

    # 2. Rendezzük a sorrend szerint
    trip_stops = trip_stops.sort_values('stop_sequence')

    # 3. Összekapcsoljuk a megállónevekkel
    combined_df = trip_stops.merge(stops_df[['stop_id', 'stop_name']], on='stop_id', how='left')
    
    # 4. JSON formátum előkészítése
    route_list = []
    
    # Jelenlegi idő szimulálása (az aktuális megálló jelzéséhez)
    current_time_str = datetime.datetime.now().strftime("%H:%M:%S")
    current_stop_sequence = 1
    
    # Próbáljuk megtalálni azt a megállót, ahol éppen tart a jármű
    # (Bonyolult lenne pontosan meghatározni, szimuláljuk az első 3 megállót)
    for index, row in combined_df.iterrows():
        stop_seq = row['stop_sequence']
        
        status = 'future'
        if stop_seq < 3: # Szimuláljuk, hogy az első két megálló "past"
            status = 'past'
        elif stop_seq == 3: # A harmadik megálló "current"
            status = 'current'
            
        # Szimulált késleltetés hozzáadása
        simulated_delay = (index % 5) - 2 # -2, -1, 0, 1, 2
        
        route_list.append({
            "time": str(row['departure_time'])[:5],
            "name": str(row['stop_name']),
            "delay": simulated_delay,
            "status": status
        })

    # Szimulált teljes késleltetés (pl. az utolsó megálló késleltetése)
    total_delay = route_list[-1]['delay'] if route_list else 0

    return jsonify({
        "trip_id": trip_id,
        "total_delay": total_delay,
        "route": route_list
    })

@app.route('/api/live_data', methods=['GET'])
def get_live_data():
    """
    Visszaadja a valós idejű buszpozíciókat (Bővített mock adat).
    """
    # Mock data generálása, több busszal, random pozíciókkal Southampton körül
    base_lat = 50.93
    base_lon = -1.39
    
    # A videóban látott és a Bluestar vonalak listája
    active_routes = ["1", "2", "U1", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "28", "35", "61"]
    
    mock_live_data = []
    
    for i, route_name in enumerate(active_routes):
        # Generáljunk random koordinátákat egy szűkebb körben, a valóságot szimulálva
        lat = base_lat + (random.random() - 0.5) * 0.05
        lon = base_lon + (random.random() - 0.5) * 0.08
        
        mock_live_data.append({
            "lat": round(lat, 4), 
            "lon": round(lon, 4), 
            "line_ref": route_name, 
            "dest": f"Destination for {route_name}", 
            "id": f"V{1800 + i}"
        })

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

@app.route('/api/service_updates', methods=['GET'])
def get_service_updates():
    """
    Visszaadja a szimulált szolgáltatási frissítéseket a kért oldalról (Webkaparás szimulálása).
    """
    # Mivel a valós webkaparás (scraping) a Bluestar oldaláról (https://www.bluestarbus.co.uk/service-updates) 
    # bonyolult és külön könyvtárakat igényelne, most szimulált adatokat küldünk:
    
    mock_updates = [
        {"route_short_name": "1", "update_text": "Winchester felé késés.", "update_type": "delay"},
        {"route_short_name": "2", "update_text": "Forgalomkorlátozás miatt elkerülő útvonal.", "update_type": "diversion"},
        {"route_short_name": "N2", "update_text": "Éjszakai járatok menetrendje megváltozott.", "update_type": "info"},
        {"route_short_name": "3", "update_text": "Eastleigh felé 1 aktív szolgáltatási frissítés van.", "update_type": "delay"},
        {"route_short_name": "4", "update_text": "Romsey felé 2 aktív szolgáltatási frissítés van.", "update_type": "delay"},
        {"route_short_name": "35", "update_text": "Romsey felé útvonal változás.", "update_type": "info"},
    ]

    # Színek hozzárendelése a GTFS routes-ból
    if 'routes' in gtfs_data:
        routes_df = gtfs_data['routes'].set_index('route_short_name')
        for update in mock_updates:
            line_ref = str(update['route_short_name'])
            if line_ref in routes_df.index:
                route_info = routes_df.loc[line_ref]
                update['route_color'] = str(route_info.get('route_color', '0057B8'))
                update['route_text_color'] = str(route_info.get('route_text_color', 'FFFFFF'))
            else:
                update['route_color'] = 'FFC107' # Alapértelmezett sárga a változásokhoz
                update['route_text_color'] = '333333'
    
    return jsonify(mock_updates)


# Alkalmazás indítása előtt a GTFS adatok betöltése
with app.app_context():
    load_gtfs_data()

if __name__ == '__main__':
    # Helyi fejlesztéshez
    app.run(debug=True, port=5000)
