# main.py - VÉGLEGES JAVÍTOTT KÓD

import os
from flask import Flask, render_template, jsonify
import requests
from google.transit import gtfs_realtime_pb2
from requests.exceptions import RequestException

# --- KONFIGURÁCIÓ ---

# Az API kulcsod
API_KEY = "9d2f6818e2723996467fedb958ba682aa9860a93" 

# Bluestar/Unilink Live Data Feed URL
GTFS_RT_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={API_KEY}"

# A Procfile által használt Flask alkalmazás neve
app = Flask(__name__, template_folder='templates')

# --- ADAT FELDOLGOZÁS ---

@app.route('/api/live_buses', methods=['GET'])
def get_live_buses():
    """
    Lekérdezi, feldolgozza és hibakezeli az élő GTFS-Realtime adatokat.
    Kényszeríti a Protobuf formátumot a header használatával.
    """
    
    # Kényszerítjük a DFT szervert, hogy BINÁRIS GTFS-RT Protobuf-ot küldjön XML (SIRI) helyett.
    headers = {
        # Két Accept-et adunk meg, hogy minél nagyobb eséllyel sikerüljön
        'Accept': 'application/x-protobuf, application/octet-stream', 
        # User-Agent a 406-os hiba elkerülésére
        'User-Agent': 'Custom Python Script' 
    }
    
    try:
        # 1. API Hívás (Kényszerítő Headerrel)
        response = requests.get(GTFS_RT_URL, headers=headers, timeout=15, stream=True)
        
        # DEBUG: Státuszkód naplózása a Railway Logokba
        print(f"DEBUG: Külső API státuszkód: {response.status_code}")
        
        response.raise_for_status() # Hibát dob, ha 4xx/5xx

        # 2. GTFS-Realtime Feed feldolgozása
        feed = gtfs_realtime_pb2.FeedMessage()
        
        # A response.content helyett raw/chunk-okat használunk, ha a stream=True be van állítva
        feed.ParseFromString(response.raw.read()) 

        buses = []
        for entity in feed.entity:
            # Csak a járműpozíciókat dolgozzuk fel
            if not entity.HasField('vehicle'):
                continue
            
            vehicle = entity.vehicle
            
            # Ellenőrizzük, hogy van-e pozíció (lat/lon)
            if not vehicle.HasField('position') or not vehicle.position.HasField('latitude'):
                continue
                
            # Robusztus ellenőrzés a trip/route_id-re
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
        
        return jsonify(buses)

    except RequestException as e:
        # HTTP hibák (406, 403, 401, Timeout)
        # Fontos, hogy itt a pontos HTTP hibaüzenetet küldjük vissza!
        print(f"KRITIKUS HIBA: Requests Exception (HTTP hiba): {e}")
        return jsonify({"error": f"Sikertelen adatlekérdezés (HTTP Hiba vagy API Kulcs hiba): {e}"}), 503
    
    except Exception as e:
        # Általános feldolgozási hiba (Protobuf parsing)
        print(f"KRITIKUS HIBA: Általános feldolgozási hiba: {e}")
        # Ha itt az "Error parsing message" hiba jön vissza, az azt jelenti, hogy még mindig XML-t kapunk.
        return jsonify({"error": f"Belső szerver hiba a feldolgozás során: {e}"}), 500

# --- WEBOLDAL VÉGPONT ---

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
