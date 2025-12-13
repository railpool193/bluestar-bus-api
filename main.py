import os
import requests
import xml.etree.ElementTree as ET
from flask import Flask, render_template, jsonify
import json
import time

# --- FLASK ALKALMAZÁS BEÁLLÍTÁSA ---
# A Railway gunicorn parancsa ezt a változót fogja használni az app indításához: main:app
app = Flask(__name__)

# --- XML NÉVTÉR DEFINÍCIÓJA ---
NAMESPACES = {
    'siri': 'http://www.siri.org.uk/siri'
}

# --- KONFIGURÁCIÓS FÜGGVÉNY ---

def get_config():
    """Lekéri a kritikus környezeti változókat a Railway-ről."""
    config = {
        'api_key': os.environ.get('OCP_APIM_SUBSCRIPTION_KEY'),
        'realtime_feed_url': os.environ.get('DFTBUS_REALTIME_FEED_URL'),
        'operator_ref': os.environ.get('DFTBUS_OPERATOR_REF', 'BLUS')
    }
    return config

# --- ÉLŐ ADATOK LEKÉRÉSE ÉS FELDOLGOZÁSA ---

def fetch_realtime_data():
    """
    Lekéri a nyers élő adatokat a DfT datafeed URL-jéről,
    és kiszedi a buszok GPS pozícióit az XML-ből.
    """
    config = get_config()
    REALTIME_FEED_URL = config['realtime_feed_url']
    OPERATOR_REF = config['operator_ref']

    if not REALTIME_FEED_URL:
        # Ha nincs beállítva az URL, nem próbáljuk meg lekérni
        print("Hiba: DFTBUS_REALTIME_FEED_URL környezeti változó hiányzik.")
        return []

    try:
        # GET LEKÉRDEZÉS az ömlesztett adatokhoz
        response = requests.get(REALTIME_FEED_URL, timeout=15)
        response.raise_for_status() # Hibát dob, ha a státusz 4xx vagy 5xx
        
        # XML PARSZOLÁS
        root = ET.fromstring(response.content)
        
        live_data = []

        # JAVÍTÁS: A './' prefix a névtérfeloldási hiba elkerülése érdekében
        siri_path = './siri:ServiceDelivery/siri:VehicleMonitoringDelivery/siri:VehicleActivity'
        
        for activity in root.findall(siri_path, NAMESPACES):
            
            journey_ref = activity.find('siri:MonitoredVehicleJourney', NAMESPACES)
            
            if journey_ref is not None:
                # Szűrés a saját operátorunkra
                operator_el = journey_ref.find('siri:OperatorRef', NAMESPACES)
                if operator_el is not None and operator_el.text == OPERATOR_REF:
                    
                    # GPS Pozíciók kinyerése
                    loc_el = journey_ref.find('siri:VehicleLocation', NAMESPACES)
                    lat_el = loc_el.find('siri:Latitude', NAMESPACES) if loc_el is not None else None
                    lon_el = loc_el.find('siri:Longitude', NAMESPACES) if loc_el is not None else None
                    
                    if lat_el is not None and lon_el is not None:
                        line_ref = journey_ref.find('siri:LineRef', NAMESPACES).text if journey_ref.find('siri:LineRef', NAMESPACES) is not None else 'N/A'
                        vehicle_ref = journey_ref.find('siri:VehicleRef', NAMESPACES).text if journey_ref.find('siri:VehicleRef', NAMESPACES) is not None else 'N/A'
                        
                        live_data.append({
                            'id': vehicle_ref, 
                            'line_ref': line_ref,
                            'operator_ref': OPERATOR_REF,
                            'lat': float(lat_el.text),
                            'lon': float(lon_el.text),
                            'timestamp': time.time()
                        })
                        
        print(f"Sikeresen feldolgozva {len(live_data)} darab élő járatadat.")
        return live_data

    except requests.exceptions.RequestException as e:
        print(f"Hiba a DfT feed lekérdezésekor: {e}")
        return []
    except ET.ParseError as e:
        print(f"Hiba az XML feldolgozásakor: {e}")
        return []
    except Exception as e:
        print(f"Váratlan hiba történt az élő adatfeldolgozásban: {e}")
        return []

# --- FLASK ÚTVONALAK ---

@app.route('/')
def index():
    """Főoldal megjelenítése."""
    
    # Helyettesítő statikus adatok
    static_departures = [
        {'line': '1', 'destination': 'Southampton City Centre', 'time': '14:30', 'badge_class': 'line-1'},
        {'line': '2', 'destination': 'Romsey (via Ampfield)', 'time': '14:45', 'badge_class': 'line-2'},
    ]
    
    return render_template('index.html', departures=static_departures)

@app.route('/api/live_data')
def api_live_data():
    """API végpont az élő buszpozíciókhoz."""
    live_positions = fetch_realtime_data()
    return jsonify(live_positions)

# --- A Konfliktust Okozó Rész TÖRÖLVE! ---
# Ezt a részt (if __name__ == '__main__': app.run(...) ) a gunicorn/Railway veszi át.
