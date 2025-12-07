# main.py

import os
from flask import Flask, render_template, jsonify
import requests
from google.transit import gtfs_realtime_pb2
from requests.exceptions import RequestException

# --- KONFIGURÁCIÓ ---

# A kód a kulcsot közvetlenül tartalmazza. 
# Ha a Railway Variables fülén adtad meg, akkor használd az os.environ.get('API_KEY')-t!
API_KEY = "9d2f6818e2723996467fedb958ba682aa9860a93" 

# Bluestar/Unilink Live Data Feed URL
GTFS_RT_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={API_KEY}"

# A Procfile ezt a nevet használja (main:app)
app = Flask(__name__, template_folder='templates')

# --- ADAT FELDOLGOZÁS ---

@app.route('/api/live_buses', methods=['GET'])
def get_live_buses():
    """
    Lekérdezi és feldolgozza az élő GTFS-Realtime (Vehicle Positions) adatokat, 
    és naplózza a hibákat a debugoláshoz.
    """
    
    try:
        # 1. API Hívás
        response = requests.get(GTFS_RT_URL, timeout=15)
        
        # DEBUG: Írjuk ki a státuszkódot a Railway Logokba!
        print(f"DEBUG: Külső API státuszkód: {response.status_code}")
        
        # Ha a státuszkód 400 (Bad Request) vagy 500 (Server Error) feletti, hiba
        response.raise_for_status() 

        # 2. GTFS-Realtime Feed feldolgozása (Protocol Buffers)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        buses = []
        for entity in feed.entity:
            if entity.HasField('vehicle'):
                vehicle = entity.vehicle
                
                if vehicle.HasField('position') and vehicle.trip.HasField('route_id'):
                    
                    lat = vehicle.position.latitude
                    lon = vehicle.position.longitude
                    route_id = vehicle.trip.route_id 
                    vehicle_label = vehicle.vehicle.label if vehicle.vehicle.HasField('label') else entity.id

                    buses.append({
                        'id': entity.id,
                        'lat': lat,
                        'lon': lon,
                        'route': route_id,
                        'label': vehicle_label,
                    })
        
        # 3. JSON válasz küldése a frontendnek
        return jsonify(buses)

    except RequestException as e:
        # PONTOSAN NAPLÓZZUK A KÜLSŐ API HIBÁJÁT (pl. 403 Forbidden)
        print(f"KRITIKUS HIBA: Requests Exception (valószínűleg 403): {e}")
        # A 403-as hiba esetén is egy értelmes JSON-t küldünk vissza
        return jsonify({"error": f"Sikertelen adatlekérdezés (HTTP Hiba vagy API Kulcs hiba): {e}"}), 503
    
    except Exception as e:
        # PONTOSAN NAPLÓZZUK A PYTHON/PARSING HIBAÜZENETÉT
        print(f"KRITIKUS HIBA: Általános feldolgozási hiba: {e}")
        return jsonify({"error": "Belső szerver hiba a feldolgozás során"}), 500

# --- WEBOLDAL VÉGPONT ---

@app.route('/')
def index():
    """
    Betölti a fő térképoldalt a templates/index.html fájlból.
    """
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
