# main.py

import os
from flask import Flask, render_template, jsonify
import requests
# Szükséges a GTFS-Realtime protokoll puffer dekódolásához
from google.transit import gtfs_realtime_pb2 

# --- KONFIGURÁCIÓ ---
# Ha a Railway Variables fülén adtad meg, akkor a kódból vedd ki a kulcsot,
# és használd helyette az os.environ.get-et!

# A te API kulcsod: 9d2f6818e2723996467fedb958ba682aa9860a93
# Javasolt:
API_KEY = os.environ.get('API_KEY', '9d2f6818e2723996467fedb958ba682aa9860a93') 

# Bluestar/Unilink Live Data Feed URL
# FONTOS: Az API kulcsot az URL-hez kell fűzni!
GTFS_RT_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={API_KEY}"

# A Procfile ezt a nevet használja (main:app)
app = Flask(__name__, template_folder='templates')

# --- ADAT FELDOLGOZÁS ---

@app.route('/api/live_buses', methods=['GET'])
def get_live_buses():
    """
    Lekérdezi és feldolgozza az élő GTFS-Realtime (Vehicle Positions) adatokat.
    """
    try:
        # 1. API Hívás
        response = requests.get(GTFS_RT_URL, timeout=10) # 10 másodperces időkorlát
        response.raise_for_status() 

        # 2. GTFS-Realtime Feed feldolgozása (Protocol Buffers)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        buses = []
        for entity in feed.entity:
            if entity.HasField('vehicle'):
                vehicle = entity.vehicle
                
                if vehicle.HasField('position') and vehicle.trip.HasField('route_id'):
                    
                    # 3. Adatok kinyerése
                    lat = vehicle.position.latitude
                    lon = vehicle.position.longitude
                    
                    # Vonal azonosítója (pl. U1A, 3, 11)
                    route_id = vehicle.trip.route_id 
                    
                    # Jármű azonosítója (pl. rendszám)
                    vehicle_label = vehicle.vehicle.label if vehicle.vehicle.HasField('label') else entity.id

                    buses.append({
                        'id': entity.id,
                        'lat': lat,
                        'lon': lon,
                        'route': route_id,
                        'label': vehicle_label,
                    })

        return jsonify(buses)

    except requests.exceptions.RequestException as e:
        # Hibakezelés az API hívásnál (pl. időtúllépés, 403, 404)
        print(f"Hiba a GTFS-RT API hívásban: {e}")
        return jsonify({"error": "Sikertelen adatlekérdezés a külső API-tól"}), 503
    except Exception as e:
        print(f"Általános hiba az adatfeldolgozás során: {e}")
        return jsonify({"error": "Belső szerver hiba"}), 500

# --- WEBOLDAL VÉGPONT ---

@app.route('/')
def index():
    """
    Betölti a fő térképoldalt a templates/index.html fájlból.
    """
    # Flask automatikusan megkeresi a templates mappában
    return render_template('index.html')

# Csak helyi fejlesztéshez, a Railway a Gunicorn-t használja!
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # A hostot '0.0.0.0'-ra kell állítani Railway-en
    app.run(host='0.0.0.0', port=port, debug=True)
