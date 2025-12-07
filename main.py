# main.py (JAVÍTOTT VÁLTOZAT - FIGYELD A get_live_buses FÜGGVÉNYT!)

import os
from flask import Flask, render_template, jsonify
import requests
from google.transit import gtfs_realtime_pb2
from requests.exceptions import RequestException

# --- KONFIGURÁCIÓ ---

# A kulcsot célszerű a Railway Variables fülén is beállítani (API_KEY)
API_KEY = os.environ.get('API_KEY', "9d2f6818e2723996467fedb958ba682aa9860a93") 

# Bluestar/Unilink Live Data Feed URL
GTFS_RT_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={API_KEY}"

app = Flask(__name__, template_folder='templates')

# --- ADAT FELDOLGOZÁS ---

@app.route('/api/live_buses', methods=['GET'])
def get_live_buses():
    """
    Lekérdezi, feldolgozza és hibakezeli az élő GTFS-Realtime adatokat.
    """
    
    try:
        # 1. API Hívás
        response = requests.get(GTFS_RT_URL, timeout=15)
        
        # DEBUG: Státuszkód naplózása (ezt nézd a Railway logokban!)
        print(f"DEBUG: Külső API státuszkód: {response.status_code}")
        
        response.raise_for_status() # Hibát dob, ha 4xx/5xx

        # 2. GTFS-Realtime Feed feldolgozása
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        buses = []
        for entity in feed.entity:
            # Csak azokat dolgozzuk fel, amelyek járműpozíciót tartalmaznak
            if not entity.HasField('vehicle'):
                continue
            
            vehicle = entity.vehicle
            
            # --- JAVÍTÁS: ROBUSZTUS ELLENŐRZÉS ---
            # Kizárjuk azokat, amelyeknek nincs pozíciója (lat/lon) vagy vonalszáma (route_id)
            if not vehicle.HasField('position') or not vehicle.position.HasField('latitude'):
                continue
                
            # Ezt a részt kell robusztussá tenni:
            if not vehicle.HasField('trip') or not vehicle.trip.HasField('route_id'):
                route_id = 'Ismeretlen'
            else:
                route_id = vehicle.trip.route_id
            
            # --- FELDOLGOZÁS ---
            lat = vehicle.position.latitude
            lon = vehicle.position.longitude
            vehicle_label = vehicle.vehicle.label if vehicle.vehicle.HasField('label') else entity.id

            buses.append({
                'id': entity.id,
                'lat': lat,
                'lon': lon,
                'route': route_id,
                'label': vehicle_label,
            })
        
        # 3. JSON válasz
        return jsonify(buses)

    except RequestException as e:
        # HTTP hibák (pl. 403 Forbidden az API kulcs miatt)
        print(f"KRITIKUS HIBA: Requests Exception: {e}")
        return jsonify({"error": f"Sikertelen adatlekérdezés (HTTP Hiba vagy API Kulcs hiba): {e}"}), 503
    
    except Exception as e:
        # Általános hiba a feldolgozás során (pl. Protobuf Parsing hiba)
        print(f"KRITIKUS HIBA: Általános feldolgozási hiba: {e}")
        # Hiba visszaküldése a pontos Python hibaüzenettel
        return jsonify({"error": f"Belső szerver hiba a feldolgozás során: {e}"}), 500

# --- WEBOLDAL VÉGPONT ---

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
