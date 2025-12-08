# ... (Minden import, alapbeállítás, sablon és GTFS betöltés változatlan)...

# ... (Közvetlenül a fetch_live_vm() elé) ...

# ------------------------- Live hívások (Javítva: Hiba észlelése) -------------------------
LIVE_CACHE = {}
LIVE_TTL = 20  # másodperc

def cache_get(k):
    v = LIVE_CACHE.get(k)
    if not v: return None
    if v["exp"] < datetime.utcnow().timestamp():
        LIVE_CACHE.pop(k, None); return None
    return v["val"]

def cache_set(k, val):
    LIVE_CACHE[k] = {"val": val, "exp": datetime.utcnow().timestamp() + LIVE_TTL}

async def http_get_xml(url, params=None) -> bytes:
    """Aszinkron HTTP GET kérés XML adatokhoz."""
    if not url or httpx is None:
        return b""
    try:
        all_headers = EXTRA_HEADERS.copy()
        # Tisztítjuk a paramétereket, mert a DFT API a query stringben várja
        if params and "LineRef" in params: del params["LineRef"] 
        if params and "MonitoringRef" in params: del params["MonitoringRef"]
        if params and "MaximumStopVisits" in params: del params["MaximumStopVisits"]

        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, params=params or {}, headers=all_headers)
            r.raise_for_status() # HTTP hibákat itt elkapjuk (pl. 406)
            return r.content # Visszaadjuk a nyers XML tartalmat (bytes)
    except httpx.HTTPStatusError as e:
        log.warning("live request failed (HTTP Error: %s) for URL: %s", e.response.status_code, url)
        return b""
    except Exception as e:
        log.warning("live request failed (General): %s", e)
        return b""


def _parse_iso(dt_str: str):
    try:
        if not dt_str:
            return None
        ds = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(ds).astimezone(UK_TZ)
    except Exception:
        return None

async def fetch_live_vm(route_short: str):
    """Élő Járműfigyelő (VM) adatok lekérdezése és SIRI XML feldolgozása."""
    if not route_short:
        return []
    ck = ("vm", route_short.lower())
    c = cache_get(ck)
    if c is not None:
        return c
    out = []
    url, params = _format_vm_url(route_short)

    if url:
        xml_content = await http_get_xml(url, params=params)
        
        # JAVÍTÁS: Ellenőrizzük, hogy a tartalom üres-e, és ha igen, azonnal térjünk vissza.
        if not xml_content:
            log.info("VM request returned empty content (potential HTTP error).")
            cache_set(ck, [])
            return []
            
        try:
            root = ET.fromstring(xml_content)
            
            # SIRI XML feldolgozása a Vehicle Monitoring Delivery-ben
            deliveries = root.findall('siri:ServiceDelivery/siri:VehicleMonitoringDelivery', SIRI_NAMESPACES)
            
            for d in deliveries:
                # Vehicle Activity keresése
                for a in d.findall('siri:VehicleActivity', SIRI_NAMESPACES):
                    j = a.find('siri:MonitoredVehicleJourney', SIRI_NAMESPACES)
                    if j is None: continue

                    line = j.find('siri:LineRef', SIRI_NAMESPACES)
                    op = j.find('siri:OperatorRef', SIRI_NAMESPACES)
                    
                    line_ref = line.text.strip() if line is not None else ""
                    op_ref = op.text.strip() if op is not None else ""
                    
                    if route_short and line_ref and route_short.lower() != str(line_ref).lower():
                        continue
                    if op_ref and not operator_ok(op_ref):
                        continue
                    
                    loc = j.find('siri:VehicleLocation', SIRI_NAMESPACES)
                    if loc is None: continue
                    
                    lat_e = loc.find('siri:Latitude', SIRI_NAMESPACES)
                    lon_e = loc.find('siri:Longitude', SIRI_NAMESPACES)
                    
                    if lat_e is not None and lon_e is not None:
                        try:
                            lat = float(lat_e.text); lon = float(lon_e.text)
                        except (ValueError, TypeError):
                            log.warning("Invalid Lat/Lon in VM response.")
                            continue # Kihagyjuk ezt az elemet
                        
                        vehicle_ref_e = j.find('siri:VehicleRef', SIRI_NAMESPACES)
                        fleet_id = vehicle_ref_e.text if vehicle_ref_e is not None else ""
                        
                        out.append({
                            "lat": lat, "lon": lon,
                            "fleet": fleet_id,
                            "line": line_ref, "operator": op_ref.lower()[:4],
                        })
        except ET.ParseError as e:
            log.warning("parse VM failed (XML Parse Error): %s", e)
            out = []
        except Exception as e:
            log.warning("parse VM failed (General Error): %s", e)
            out = []
            
    cache_set(ck, out)
    return out


async def fetch_live_sm(stop_code_or_id: str):
    """Élő Megállófigyelő (SM) adatok lekérdezése és SIRI XML feldolgozása."""
    ck = ("sm", stop_code_or_id)
    c = cache_get(ck)
    if c is not None:
        return c
    items = []
    url, params = _format_sm_url(stop_code_or_id)
    
    if url:
        xml_content = await http_get_xml(url, params=params)
        
        # JAVÍTÁS: Ellenőrizzük, hogy a tartalom üres-e, és ha igen, azonnal térjünk vissza.
        if not xml_content:
            log.info("SM request returned empty content (potential HTTP error).")
            cache_set(ck, [])
            return []

        try:
            root = ET.fromstring(xml_content)
            
            deliveries = root.findall('siri:ServiceDelivery/siri:StopMonitoringDelivery', SIRI_NAMESPACES)
            
            for d in deliveries:
                for v in d.findall('siri:MonitoredStopVisit', SIRI_NAMESPACES):
                    j = v.find('siri:MonitoredVehicleJourney', SIRI_NAMESPACES)
                    if j is None: continue
                    
                    op = j.find('siri:OperatorRef', SIRI_NAMESPACES)
                    op_ref = op.text.strip() if op is not None else ""
                    if op_ref and not operator_ok(op_ref):
                        continue
                        
                    call = j.find('siri:MonitoredCall', SIRI_NAMESPACES)
                    if call is None: continue
                    
                    line = j.find('siri:LineRef', SIRI_NAMESPACES)
                    headsign = j.find('siri:DestinationName', SIRI_NAMESPACES)
                    v_ref = j.find('siri:VehicleRef', SIRI_NAMESPACES)
                    trip_ref_e = j.find('siri:FramedVehicleJourneyRef', SIRI_NAMESPACES)
                    
                    line_ref = line.text.strip() if line is not None else ""
                    headsign_str = headsign.text.strip() if headsign is not None else ""
                    v_ref_str = v_ref.text.strip() if v_ref is not None else ""
                    
                    # Időpontok
                    aimed = call.find('siri:AimedDepartureTime', SIRI_NAMESPACES)
                    exp = call.find('siri:ExpectedDepartureTime', SIRI_NAMESPACES)
                    
                    aimed_str = aimed.text.strip() if aimed is not None else ""
                    exp_str = exp.text.strip() if exp is not None else aimed_str # Ha nincs exp, akkor aimed
                    
                    dep_dt = _parse_iso(exp_str)
                    delay_text = ""
                    
                    # Késés kiszámítása
                    if aimed_str and exp_str:
                        a = _parse_iso(aimed_str); e = _parse_iso(exp_str)
                        if a and e:
                            mins = round((e - a).total_seconds() / 60.0)
                            if mins != 0:
                                delay_text = f"{mins:+d}m"
                                
                    is_due = bool(dep_dt and abs((now_uk() - dep_dt).total_seconds()) < 60)
                    
                    # Megpróbáljuk beolvasni a DatedVehicleJourneyRef-et
                    trip_id_str = ""
                    if trip_ref_e is not None:
                         dated_ref = trip_ref_e.find('siri:DatedVehicleJourneyRef', SIRI_NAMESPACES)
                         if dated_ref is not None:
                             trip_id_str = dated_ref.text.strip()


                    items.append({
                        "line": line_ref, "operator": op_ref.lower()[:4],
                        "headsign": headsign_str,
                        "vehicle_ref": v_ref_str,
                        "dep_dt": dep_dt, "delay_text": delay_text, "is_due": is_due,
                        "trip_id": trip_id_str,
                    })
                    
        except ET.ParseError as e:
            log.warning("parse SM failed (XML Parse Error): %s", e)
            items = []
        except Exception as e:
            log.warning("parse SM failed (General Error): %s", e)
            items = []
            
    cache_set(ck, items)
    return items

# ... (A többi függvény és FastAPI útvonal változatlan)...
