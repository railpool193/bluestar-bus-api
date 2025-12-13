import os
import requests
import xml.etree.ElementTree as ET
from flask import Flask, render_template, jsonify, request
import time
import pandas as pd
import random

# --- FLASK ALKALMAZÁS BEÁLLÍTÁSA ---
app = Flask(__name__)

# --- XML NÉVTÉR DEFINÍCIÓJA (SIRI LIVE DATA) ---
NAMESPACES = {
    'siri': 'http://www.siri.org.uk/siri'
}

# --- GLOBÁLIS ADATTÁROLÓK A GTFS-HEZ ---
# A betöltött pandas DataFrame-ek (adatbázisként szolgálnak)
GTFS_STOPS = None
GTFS_ROUTES = None
GTFS_SHAPES = None

# --- GTFS ADATOK BETÖLTÉSE ---

def load_gtfs_data():
    """
    Betölti a GTFS fájlokat (stops.txt, routes.txt, shapes.txt) a 'gtfs/' mappából.
    """
    global GTFS_STOPS, GTFS_ROUTES, GTFS_SHAPES
    gtfs_dir = 'gtfs'
    
    try:
        # 1. stops.txt (Megállóhelyek pozíciója és neve)
        stops_path = os.path.join(gtfs_dir, 'stops.txt')
        if os.path.exists(stops_path):
            GTFS_STOPS = pd.read_csv(stops_path)
            GTFS_STOPS['stop_name_lower'] = GTFS_STOPS['stop_name'].str.lower()
            print(f"✅ GTFS: {len(GTFS_STOPS)} megálló betöltve.")

        # 2. routes.txt (Vonalak nevei és azonosítói)
        routes_path = os.path.join(gtfs_dir, 'routes.txt')
        if os.path.exists(routes_path):
            GTFS_ROUTES = pd.read_csv(routes_path)
            GTFS_ROUTES['route_name_lower'] = GTFS_ROUTES['route_short_name'].astype(str).str.lower()
            # Szín hiányában default kék
            if 'route_color' not in GTFS_ROUTES.columns:
                 GTFS_ROUTES['route_color'] = '#00468c' 
            print(f"✅ GTFS: {len(GTFS_ROUTES)} útvonal betöltve.")

        # 3. shapes.txt (Útvonal koordináták)
        shapes_path = os.path.join(gtfs_dir, 'shapes.txt')
        if os.path.exists(shapes_path):
            GTFS_SHAPES = pd.read_csv(shapes_path)
            print(f"✅ GTFS: {len(GTFS_SHAPES)} útvonal-forma betöltve.")

    except Exception as e:
        print(f"🛑 Hiba a GTFS adatok betöltésekor. Szimulált adatok használata: {e}")
        # Tartalék szimulált adatok
        GTFS_STOPS = pd.DataFrame({'stop_id': ['STOP001', 'STOP002'], 'stop_name': ['Szimulált megálló', 'Kereszt utca'], 'stop_name_lower': ['szimulált megálló', 'kereszt utca'], 'stop_lat': [50.9097, 50.92], 'stop_lon': [-1.4043, -1.41]})
        GTFS_ROUTES = pd.DataFrame({'route_id': ['1', '2', 'U1'], 'route_short_name': ['1', '2', 'U1'], 'route_long_name': ['Szimulált járat 1', 'Szimulált járat 2', 'Szimulált járat U1'], 'route_color': ['#D9534F', '#5CB85C', '#337AB7']})
        GTFS_SHAPES = None # Nincs szimulált shape

# Alkalmazás indulásakor hívjuk meg a GTFS betöltést
load_gtfs_data()


# --- KONFIGURÁCIÓS FÜGGVÉNY ÉS ÉLŐ ADATOK LEKÉRÉSE (VÁLTOZATLAN) ---
def get_config():
    """Lekéri a kritikus környezeti változókat."""
    config = {
        'realtime_feed_url': os.environ.get('DFTBUS_REALTIME_FEED_URL'),
        'operator_ref': os.environ.get('DFTBUS_OPERATOR_REF', 'BLUS')
    }
    return config

def fetch_realtime_data():
    """
    Lekéri és feldolgozza a DfT élő SIRI XML-t.
    """
    config = get_config()
    REALTIME_FEED_URL = config['realtime_feed_url']
    OPERATOR_REF = config['operator_ref']
    
    if not REALTIME_FEED_URL:
        # Ha nincs URL, adjunk vissza szimulált élő adatot az app teszteléséhez
        return [
            {'id': 'V001', 'line_ref': '1', 'dest': 'City Centre', 'lat': 50.9100, 'lon': -1.4040, 'timestamp': time.time()},
            {'id': 'V002', 'line_ref': '2', 'dest': 'Romsey', 'lat': 50.9200, 'lon': -1.4100, 'timestamp': time.time()},
        ]

    try:
        response = requests.get(REALTIME_FEED_URL, timeout=15)
        response.raise_for_status() 
        root = ET.fromstring(response.content)
        live_data = []
        
        siri_path = './siri:ServiceDelivery/siri:VehicleMonitoringDelivery/siri:VehicleActivity'
        
        for activity in root.findall(siri_path, NAMESPACES):
            journey_ref = activity.find('siri:MonitoredVehicleJourney', NAMESPACES)
            
            if journey_ref is not None:
                operator_el = journey_ref.find('siri:OperatorRef', NAMESPACES)
                if operator_el is not None and operator_el.text == OPERATOR_REF:
                    
                    loc_el = journey_ref.find('siri:VehicleLocation', NAMESPACES)
                    lat_el = loc_el.find('siri:Latitude', NAMESPACES) if loc_el is not None else None
                    lon_el = loc_el.find('siri:Longitude', NAMESPACES) if loc_el is not None else None
                    
                    if lat_el is not None and lon_el is not None:
                        line_ref = journey_ref.find('siri:LineRef', NAMESPACES).text if journey_ref.find('siri:LineRef', NAMESPACES) is not None else 'N/A'
                        vehicle_ref = journey_ref.find('siri:VehicleRef', NAMESPACES).text if journey_ref.find('siri:VehicleRef', NAMESPACES) is not None else 'N/A'
                        destination_name = journey_ref.find('siri:DestinationName', NAMESPACES).text if journey_ref.find('siri:DestinationName', NAMESPACES) is not None else 'Ismeretlen'
                        
                        live_data.append({
                            'id': vehicle_ref, 
                            'line_ref': line_ref,
                            'dest': destination_name,
                            'lat': float(lat_el.text),
                            'lon': float(lon_el.text),
                            'timestamp': time.time()
                        })
                        
        return live_data

    except Exception as e:
        print(f"Hiba az élő adatfeldolgozásban: {e}")
        return []
        
# --- FLASK ÚTVONALAK GTFS ADATOKKAL ---

@app.route('/')
def index():
    """Főoldal megjelenítése."""
    
    # Valós útvonaladatok küldése a sablonnak (Kiemelt forgalmi változásokhoz)
    routes_list = GTFS_ROUTES[['route_id', 'route_short_name', 'route_long_name', 'route_color']].to_dict('records')
    
    return render_template('index.html', routes=routes_list)

@app.route('/api/live_data')
def api_live_data():
    """API végpont az élő buszpozíciókhoz a térképre."""
    live_positions = fetch_realtime_data()
    return jsonify(live_positions)

@app.route('/api/search', methods=['GET'])
def api_search():
    """
    API végpont a megállók és vonalak kereséséhez a GTFS adatok alapján.
    """
    query = request.args.get('q', '').lower()
    results = []

    # 1. Keresés az útvonalak között (route_short_name vagy route_long_name)
    if not GTFS_ROUTES.empty:
        route_matches = GTFS_ROUTES[
            (GTFS_ROUTES['route_name_lower'].str.contains(query, na=False)) |
            (GTFS_ROUTES['route_long_name'].str.lower().str.contains(query, na=False))
        ]
        for _, row in route_matches.iterrows():
            results.append({
                "type": "route", 
                "data": {
                    "id": str(row['route_id']), 
                    "short_name": str(row['route_short_name']), 
                    "long_name": row['route_long_name'], 
                    "color": row['route_color'] 
                }
            })

    # 2. Keresés a megállók között (stop_name)
    if not GTFS_STOPS.empty:
        stop_matches = GTFS_STOPS[
            GTFS_STOPS['stop_name_lower'].str.contains(query, na=False)
        ]
        for _, row in stop_matches.iterrows():
            results.append({
                "type": "stop", 
                "data": {
                    "id": str(row['stop_id']), 
                    "name": row['stop_name'],
                    "lat": row['stop_lat'],
                    "lon": row['stop_lon']
                }
            })
    
    return jsonify(results[:20]) # Maximum 20 találat

@app.route('/api/departures/<stop_id>', methods=['GET'])
def api_departures(stop_id):
    """
    API végpont a valós idejű indulásokhoz.
    
    Valós GTFS integráció esetén itt kellene:
    1. Lekérdezni a menetrendi időket (stop_times.txt) a stop_id alapján.
    2. Lekérdezni az aktuális járműpozíciókat (fetch_realtime_data).
    3. Összevetni a kettőt a pontos késés (delay) kiszámításához.
    
    Jelenleg a GTFS csak a megálló nevének lekérdezéséhez használatos.
    """
    
    # Valós megálló nevének lekérése a GTFS-ből a felületen történő megjelenítéshez
    stop_name = GTFS_STOPS[GTFS_STOPS['stop_id'] == stop_id]['stop_name'].iloc[0] if not GTFS_STOPS[GTFS_STOPS['stop_id'] == stop_id].empty else "Ismeretlen megálló"
    
    # Szimulált valós idejű indulások (a GTFS keresési eredményeket használja)
    departures = [
        {"line": "1", "destination": "Winchester", "time": f"{random.randint(1, 10)}'", "delay": random.randint(-1, 3), "vehicle_id": "V456"},
        {"line": "2", "destination": "Romsey", "time": f"{random.randint(5, 15)}'", "delay": random.randint(-2, 2), "vehicle_id": "V123"},
        {"line": "U1", "destination": "University", "time": f"{random.randint(10, 25)}'", "delay": random.randint(0, 5), "vehicle_id": "V789"},
    ]
    
    return jsonify({"stop_name": stop_name, "departures": departures})

@app.route('/api/route_shapes/<route_id>', methods=['GET'])
def api_route_shapes(route_id):
    """
    API végpont az útvonal kirajzolásához szükséges koordinátákhoz (shapes.txt).
    
    Jelenleg csak az első 'shape_id' koordinátáit adjuk vissza, ami egy adott route_id-hoz tartozik.
    """
    
    if GTFS_SHAPES is None or GTFS_SHAPES.empty:
        # Szimulált koordináták, ha a shapes.txt hiányzik
        simulated_coordinates = [
            [50.9080, -1.4040],
            [50.9130, -1.3980],
            [50.9150, -1.4000]
        ]
        return jsonify({"route_id": route_id, "coordinates": simulated_coordinates})

    try:
        # Keresd meg a route_id-hez tartozó 'shape_id'-t a routes.txt-ben
        # Mivel a routes.txt nem tartalmazza közvetlenül a shape_id-t, 
        # a trips.txt kellene, de most leegyszerűsítjük: kivesszük az első shape_id-t a shapes.txt-ből
        
        # Ez a logika bonyolult, mivel a routes és shapes táblákhoz a trips.txt is kell.
        # Most csak kiválasztunk egy shape_id-t, ami a route_id-hoz tartozik.
        
        # Mivel a GTFS-e teljes, a trips.txt-ből kellene shape_id-t keresni.
        # Ideiglenesen, ha a GTFS teljes, akkor feltételezzük, hogy az útvonal száma shape_id-ként szerepel:
        target_shape = GTFS_SHAPES[GTFS_SHAPES['shape_id'] == route_id]
        
        if target_shape.empty:
            # Ha nincs konkrét shape_id egyezés, szimulálunk
            raise ValueError("No shape found for this route.")
        
        # A koordináták listája [[lat, lon], [lat, lon], ...] formátumban
        coordinates = target_shape[['shape_pt_lat', 'shape_pt_lon']].values.tolist()
        
        return jsonify({"route_id": route_id, "coordinates": coordinates})

    except Exception as e:
        print(f"Hiba az útvonal formájának lekérdezésekor: {e}")
        # Hiba esetén szimulált koordináták
        simulated_coordinates = [
            [50.9080, -1.4040],
            [50.9130, -1.3980],
            [50.9150, -1.4000]
        ]
        return jsonify({"route_id": route_id, "coordinates": simulated_coordinates})
