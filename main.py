# app.py

import os
from flask import Flask, render_template, jsonify
import requests
from google.transit import gtfs_realtime_pb2

# --- KONFIGURÁCIÓ ---
# Használd a saját API kulcsodat!
# A Railway-en érdemes környezeti változóban tárolni (os.environ.get('API_KEY'))
API_KEY = "9d2f6818e2723996467fedb958ba682aa9860a93" 

# Bluestar/Unilink Live Data Feed URL
GTFS_RT_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={API_KEY}"

app = Flask(__name__)

# --- ADAT FELDOLGOZÁS ---

@app.route('/api/live_buses', methods=['GET'])
def get_live_buses():
    """
    Lekérdezi és feldolgozza az élő GTFS-Realtime (Vehicle Positions) adatokat.
    """
    try:
        # 1. API Hívás
        response = requests.get(GTFS_RT_URL)
        response.raise_for_status() # Hibát dob, ha a státuszkód 4xx vagy 5xx

        # 2. GTFS-Realtime Feed feldolgozása (Protocol Buffers)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        buses = []
        for entity in feed.entity:
            # Csak azokat az entity-ket dolgozzuk fel, amelyek járműpozíciót tartalmaznak
            if entity.HasField('vehicle'):
                vehicle = entity.vehicle
                
                # A pozíció adatok meglétének ellenőrzése
                if vehicle.HasField('position') and vehicle.trip.HasField('route_id'):
                    
                    # 3. Adatok kinyerése
                    lat = vehicle.position.latitude
                    lon = vehicle.position.longitude
                    
                    # A vonal azonosítója (pl. U1A, 3, 11)
                    route_id = vehicle.trip.route_id 
                    
                    # A jármű egyedi azonosítója (pl. rendszám)
                    vehicle_label = vehicle.vehicle.label if vehicle.vehicle.HasField('label') else entity.id

                    buses.append({
                        'id': entity.id,
                        'lat': lat,
                        'lon': lon,
                        'route': route_id,
                        'label': vehicle_label,
                    })

        # 4. JSON válasz küldése a frontendnek
        return jsonify(buses)

    except requests.exceptions.RequestException as e:
        print(f"Hiba a GTFS-RT API hívásban: {e}")
        return jsonify({"error": "Sikertelen adatlekérdezés a külső API-tól"}), 500
    except Exception as e:
        print(f"Általános hiba az adatfeldolgozás során: {e}")
        return jsonify({"error": "Belső szerver hiba"}), 500

# --- WEBOLDAL VÉGPONT ---

@app.route('/')
def index():
    """
    Betölti a fő térképoldalt a templates/index.html fájlból.
    """
    return render_template('index.html')

if __name__ == '__main__':
    # A Railway-en a 'PORT' környezeti változót kell használni
    port = int(os.environ.get('PORT', 5000))
    # A hostot '0.0.0.0'-ra kell állítani a távoli eléréshez (Railway, Docker)
    app.run(host='0.0.0.0', port=port, debug=True)

