# main.py - VÉGLEGES KÓD (Hibakezeléssel)

import os
from flask import Flask, render_template, jsonify
import requests
from google.transit import gtfs_realtime_pb2
from requests.exceptions import RequestException
import traceback

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
        # Két Accept-et adunk meg a bináris formátumhoz
        'Accept': 'application/x-protobuf, application/octet-stream', 
        # User-Agent hozzáadása a 406-os hiba elkerülésére
        'User-Agent': 'Custom Python Bus Tracker Script' 
    }
    
    try:
        # 1. API Hívás
        response = requests.get(GTFS_RT_URL, headers=headers, timeout=15)
        
        # DEBUG: Státuszkód naplózása a Railway Logokba
        print(f"DEBUG: Külső API státuszkód: {response.status_code}")
        
        # Ha a státuszkód 400-as vagy 500-as, azonnal HTTP hibaüzenetet küldünk vissza.
        response.raise_for_status() 

        # 2. GTFS-Realtime Feed feldolgozása
        feed = gtfs_realtime_pb2.FeedMessage()
        
        # Próbáljuk dekódolni a bináris adatot
        feed.ParseFromString(response.content) 

        buses = []
        for entity in feed.entity:
            # ... (A robusztus adatfeldolgozás kódja) ...
            if not entity.HasField('vehicle'):
                continue
            
            vehicle = entity.vehicle
            
            if not vehicle.HasField('position') or not vehicle.position.HasField('latitude'):
                continue
                
            if not vehicle.HasField('trip') or not vehicle.trip.HasField('route_id'):
                route_id = 'Ismeretlen'
            else:
                route_id = vehicle.trip.route_id
            
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
        # HTTP hiba: Itt kapjuk el a 406 Not Acceptable hibát!
        print(f"KRITIKUS HIBA: Requests Exception (HTTP Hiba): {e}")
        return jsonify({"error": f"Sikertelen adatlekérdezés (HTTP Hiba vagy API Kulcs hiba): {e}"}), 503
    
    except Exception as e:
        # Általános feldolgozási hiba (Protobuf parsing)
        # Itt fut be az "Error parsing message" hiba, ha XML-t kapunk
        print(f"KRITIKUS HIBA: Általános feldolgozási hiba: {e}")
        # Nyomkövetés kiírása a logokba (nagyon hasznos)
        traceback.print_exc() 
        
        # Ha a dekódolás hiba fut be, a kapott adat első 100 karakterét visszaküldjük a debugoláshoz
        try:
            sample_content = response.text[:100] if response.content else "Nincs tartalom."
        except:
            sample_content = "Tartalom nem olvasható."
            
        return jsonify({
            "error": f"Belső szerver hiba a feldolgozás során: {e}",
            "debug_info": f"A kapott adatok eleje: {sample_content}"
        }), 500

# --- WEBOLDAL VÉGPONT ---

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
