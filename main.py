import os
import requests
import xml.etree.ElementTree as ET
from flask import Flask, render_template, jsonify
import json
import time

# --- FLASK ALKALMAZÁS BEÁLLÍTÁSA ---
app = Flask(__name__)

# --- XML NÉVTÉR DEFINÍCIÓJA (KRITIKUS A SIRI PARSZOLÁSHOZ) ---
# A DfT SIRI XML formátumához szükséges
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

# --- ÉLŐ ADATOK LEKÉRÉSE ÉS FELDOLGOZÁSA (A JAVÍTOTT RÉSZ) ---

def fetch_realtime_data():
    """
    Lekéri a nyers élő adatokat a DfT datafeed URL-jéről,
    és kiszedi a buszok GPS pozícióit az XML-ből.
    """
    config = get_config()
    REALTIME_FEED_URL = config['realtime_feed_url']
    OPERATOR_REF = config['operator_ref']

    if not REALTIME_FEED_URL:
        print("Hiba: DFTBUS_REALTIME_FEED_URL környezeti változó hiányzik.")
        return []

    try:
        # 1. EGYSZERŰ GET LEKÉRDEZÉS az ömlesztett adatokhoz
        response = requests.get(REALTIME_FEED_URL, timeout=15)
        response.raise_for_status() # Hibát dob, ha a státusz 4xx vagy 5xx
        
        # 2. XML PARSZOLÁS
        # Az eltérő DfT XML formátumok kezelésére a 'strip_string' paramétert is használhatjuk,
        # de a fromstring a leggyakoribb.
        root = ET.fromstring(response.content)
        
        live_data = []

        # 3. KIBÁNYÁSZÁS AZ ÖMLESZTETT XML-BŐL
        # JAVÍTÁS: A './' prefix hozzáadva az XPath-hez a névtérfeloldási hiba elkerülése érdekében!
        siri_path = './siri:ServiceDelivery/siri:VehicleMonitoringDelivery/siri:VehicleActivity'
        
        # Iterálás a VehicleActivity elemeken (minden busz)
        for activity in root.findall(siri_path, NAMESPACES):
            
            journey_ref = activity.find('siri:MonitoredVehicleJourney', NAMESPACES)
            
            if journey_ref is not None:
                # Szűrünk a saját operátorunkra (BLUS)
                operator_el = journey_ref.find('siri:OperatorRef', NAMESPACES)
                if operator_el is not None and operator_el.text == OPERATOR_REF:
                    
                    # GPS Pozíciók kinyerése
                    loc_el = journey_ref.find('siri:VehicleLocation', NAMESPACES)
                    lat_el = loc_el.find('siri:Latitude', NAMESPACES) if loc_el is not None else None
                    lon_el = loc_el.find('siri:Longitude', NAMESPACES) if loc_el is not None else None
                    
                    if lat_el is not None and lon_el is not None:
                        # Kinyerjük a kulcsadatokat
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
                        
        print(f"Sikeresen feldolgozva {len(live_data)} darab élő járatadat. Készen áll a térkép frissítésére.")
        return live_data

    except requests.exceptions.RequestException as e:
        print(f"Hiba a DfT feed lekérdezésekor: {e}")
        return []
    except ET.ParseError as e:
        print(f"Hiba az XML feldolgozásakor (ellenőrizze, hogy az URL valóban XML-t küld-e): {e}")
        return []
    except Exception as e:
        print(f"Váratlan hiba történt az élő adatfeldolgozásban: {e}")
        return []

# --- FLASK ÚTVONALAK ---

@app.route('/')
def index():
    """Főoldal megjelenítése (ahol a statikus adatok és a térkép van)"""
    
    # Helyettesítő statikus adatok (Ezeket a GTFS-ből kellene töltenie)
    static_departures = [
        {'line': '1', 'destination': 'Southampton City Centre', 'time': '14:30', 'badge_class': 'line-1'},
        {'line': '2', 'destination': 'Romsey (via Ampfield)', 'time': '14:45', 'badge_class': 'line-2'},
        {'line': 'U1', 'destination': 'University Campus', 'time': '15:00', 'badge_class': 'line-1'},
    ]
    
    # A statikus megállókat, útvonalakat (amelyeket a GTFS-ből töltött be) 
    # is át kell adnia az index.html sablonnak!
    
    return render_template('index.html', departures=static_departures)

@app.route('/api/live_data')
def api_live_data():
    """API végpont az élő buszpozíciókhoz"""
    live_positions = fetch_realtime_data()
    return jsonify(live_positions)

# --- INDÍTÁS ---
if __name__ == '__main__':
    # A host=0.0.0.0 beállítás szükséges a Railway-en való futtatáshoz
    app.run(host='0.0.0.0', port=os.environ.get('PORT', 5000), debug=True)
