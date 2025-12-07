# main.py - VÉGLEGES MEGOLDÁSI KÍSÉRLET: SIRI XML (URL Explicit formátum kényszerítéssel)

import os
from flask import Flask, render_template, jsonify
import requests
import xml.etree.ElementTree as ET
from requests.exceptions import RequestException
import traceback

# --- KONFIGURÁCIÓ ---

# API kulcs beolvasása a Railway környezeti változójából.
API_KEY = os.environ.get("BODS_API_KEY", "9d2f6818e2723996467fedb958ba682aa9860a93") 

# A VÉGLEGES URL: explicit módon hozzáadjuk a format=xml-t
# Ez a legbiztosabb módja annak, hogy XML-t kérjünk, ha a szerver támogatja.
GTFS_RT_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={API_KEY}&format=xml" 

# SIRI XML névtér
NAMESPACES = {
    'siri': 'http://www.siri.org.uk/siri',
    'datex': 'http://www.datex.org.uk/schema/1.0/datex',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

app = Flask(__name__, template_folder='templates')

# --- ADAT FELDOLGOZÁS ---

@app.route('/api/live_buses', methods=['GET'])
def get_live_buses():
    """
    Lekérdezi és feldolgozza a SIRI XML formátumú buszadatokat.
    """
    
    headers = {
        'Accept': 'application/xml',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'
    }
    
    try:
        # 1. API Hívás
        response = requests.get(GTFS_RT_URL, headers=headers, timeout=15)
        
        print(f"DEBUG: Külső API státuszkód: {response.status_code}")
        response.raise_for_status() 

        # 2. XML feldolgozás
        root = ET.fromstring(response.content)
        buses = []
        deliveries = root.findall('siri:ServiceDelivery/siri:VehicleMonitoringDelivery', NAMESPACES)
        
        for delivery in deliveries:
            journeys = delivery.findall('siri:VehicleActivity/siri:MonitoredVehicleJourney', NAMESPACES)
            
            for journey in journeys:
                route_element = journey.find('siri:LineRef', NAMESPACES)
                route_id = route_element.text if route_element is not None else 'Ismeretlen'
                location_element = journey.find('siri:VehicleLocation', NAMESPACES)
                
                if location_element is not None:
                    lat_element = location_element.find('siri:Latitude', NAMESPACES)
                    lon_element = location_element.find('siri:Longitude', NAMESPACES)
                    
                    if lat_element is not None and lon_element is not None:
                        try:
                            lat = float(lat_element.text)
                            lon = float(lon_element.text)
                            vehicle_ref_element = journey.find('siri:VehicleRef', NAMESPACES)
                            vehicle_id = vehicle_ref_element.text if vehicle_ref_element is not None else 'N/A'

                            buses.append({
                                'id': vehicle_id,
                                'lat': lat,
                                'lon': lon,
                                'route': route_id,
                                'label': route_id,
                            })
                        except (ValueError, TypeError):
                            continue 
        
        return jsonify(buses)

    except RequestException as e:
        print(f"KRITIKUS HIBA: Requests Exception (HTTP Hiba): {e}")
        return jsonify({"error": f"Sikertelen adatlekérdezés (HTTP Hiba): {e}"}), 503
    
    except ET.ParseError as e:
        print(f"KRITIKUS HIBA: XML Parse hiba: {e}")
        return jsonify({"error": f"XML dekódolási hiba: {e}"}), 500
        
    except Exception as e:
        print(f"KRITIKUS HIBA: Általános szerverhiba: {e}")
        traceback.print_exc() 
        return jsonify({"error": f"Belső szerver hiba: {e}"}), 500

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
