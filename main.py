import os
import requests
import xml.etree.ElementTree as ET
from flask import Flask, render_template, jsonify, request
import time
import json
import random

# --- FLASK ALKALMAZÁS BEÁLLÍTÁSA ---
app = Flask(__name__)

# --- XML NÉVTÉR DEFINÍCIÓJA ---
NAMESPACES = {
    'siri': 'http://www.siri.org.uk/siri'
}

# --- STATIKUS/GTFS ADATOK TÁROLÁSA ---
# A GTFS fájlok hiánya miatt szimuláljuk a statikus adatokat, amíg be nem tölti a valós adatokat
# A valós alkalmazásban itt futna le a GTFS beolvasás!
SIMULATED_DATA = {
    "routes": [
        {"id": "1", "short_name": "1", "long_name": "Southampton - Winchester", "color": "#D9534F"},
        {"id": "2", "short_name": "2", "long_name": "Romsey (via Ampfield)", "color": "#5CB85C"},
        {"id": "3", "short_name": "3", "long_name": "Fareham - Portsmouth", "color": "#F0AD4E"},
        {"id": "U1", "short_name": "U1", "long_name": "University Campus", "color": "#337AB7"},
    ],
    "stops": [
        {"id": "STOP001", "name": "Southampton City Centre"},
        {"id": "STOP002", "name": "Romsey (via Ampfield)"},
        {"id": "STOP003", "name": "Kiemelt megálló A"},
        {"id": "STOP004", "name": "Kereszt utca"},
    ]
}

# --- KONFIGURÁCIÓS FÜGGVÉNY ---

def get_config():
    """Lekéri a kritikus környezeti változókat."""
    config = {
        'api_key': os.environ.get('OCP_APIM_SUBSCRIPTION_KEY'),
        'realtime_feed_url': os.environ.get('DFTBUS_REALTIME_FEED_URL'),
        'operator_ref': os.environ.get('DFTBUS_OPERATOR_REF', 'BLUS')
    }
    return config

# --- ÉLŐ ADATOK LEKÉRÉSE ÉS FELDOLGOZÁSA ---

def fetch_realtime_data():
    """Lekéri és feldolgozza a DfT élő SIRI XML-t."""
    config = get_config()
    REALTIME_FEED_URL = config['realtime_feed_url']
    OPERATOR_REF = config['operator_ref']

    if not REALTIME_FEED_URL:
        print("Hiba: DFTBUS_REALTIME_FEED_URL környezeti változó hiányzik.")
        return []

    try:
        response = requests.get(REALTIME_FEED_URL, timeout=15)
        response.raise_for_status() 
        root = ET.fromstring(response.content)
        live_data = []
        
        # JAVÍTOTT ÚTVONAL: A './' prefix a névtérfeloldáshoz
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

# --- FLASK ÚTVONALAK ---

@app.route('/')
def index():
    """A főoldal, ami a dinamikus menüt és a térképet tartalmazza."""
    
    # Kiszolgáljuk a menetrendi adatokat (szimulált adatokkal)
    routes = SIMULATED_DATA["routes"]
    return render_template('index.html', routes=routes)

@app.route('/api/live_data')
def api_live_data():
    """API végpont az élő buszpozíciókhoz a térképre."""
    live_positions = fetch_realtime_data()
    return jsonify(live_positions)

@app.route('/api/search', methods=['GET'])
def api_search():
    """API végpont a megállók és vonalak kereséséhez."""
    query = request.args.get('q', '').lower()
    results = []

    # Szimulált útvonal keresés
    route_matches = [
        r for r in SIMULATED_DATA["routes"] 
        if query in r["short_name"].lower() or query in r["long_name"].lower()
    ]
    # Szimulált megálló keresés
    stop_matches = [
        s for s in SIMULATED_DATA["stops"] 
        if query in s["name"].lower()
    ]

    # Csak az első 10 találat visszaadása
    results.extend([{"type": "route", "data": r} for r in route_matches])
    results.extend([{"type": "stop", "data": s} for s in stop_matches])
    
    return jsonify(results[:10])

@app.route('/api/departures/<stop_id>', methods=['GET'])
def api_departures(stop_id):
    """API végpont a valós idejű indulásokhoz egy adott megállónál."""
    
    # Jelenleg csak szimulált valós idejű indulások (amíg a valós GTFS/RT nincs bekötve)
    departures = [
        {"line": "1", "destination": "Winchester", "time": f"{random.randint(1, 30)}'", "delay": 2, "vehicle_id": "V456"},
        {"line": "2", "destination": "Romsey", "time": f"{random.randint(3, 35)}'", "delay": -1, "vehicle_id": "V123"},
        {"line": "U1", "destination": "University", "time": f"{random.randint(10, 40)}'", "delay": 5, "vehicle_id": "V789"},
    ]
    
    return jsonify({"stop_id": stop_id, "departures": departures})
