import time
import jwt
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
load_dotenv()

USER_EMAIL = os.getenv("USER_EMAIL", "b.cristobal@opendeusto.es")
PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", "Apikey/privateKey.pem")

# URLs
EUSKALMET_BASE_URL = "https://api.euskadi.eus/euskalmet"
TRAFFIC_BASE_URL = "https://api.euskadi.eus/traffic/v1.0/incidences"
REGION_ID = "basque_country"

# Escanear más páginas para encontrar más pueblos distintos
PAGES_TO_SCAN = 200

def generate_euskadi_jwt(email, key_path):
    try:
        with open(key_path, 'r') as f:
            private_key = f.read()
    except FileNotFoundError:
        private_key = os.getenv("PRIVATE_KEY_EUSKALMET")
        if not private_key:
            print(f"❌ Error: No se encuentra la clave privada.")
            return None

    current_time = int(time.time())
    payload = {
        "aud": "met01.apikey",
        "iss": "PythonScript_Mapper",
        "exp": current_time + 3600,
        "iat": current_time,
        "version": "1.0.0",
        "email": email
    }
    headers = {"alg": "RS256", "typ": "JWT"}
    return jwt.encode(payload, private_key, algorithm="RS256", headers=headers)

def get_data(url, token=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"❌ Error conectando a {url}: {e}")
        return None

def get_traffic_cities_sample():
    """Descarga incidencias y extrae una lista única de 'cityTown'."""
    print(f"\n🚗 Analizando las últimas {PAGES_TO_SCAN} páginas de Tráfico...")
    traffic_cities = set()
    
    for page in range(1, PAGES_TO_SCAN + 1):
        url = f"{TRAFFIC_BASE_URL}?_page={page}"
        data = get_data(url)
        
        if data and 'incidences' in data:
            incidences = data['incidences']
            if not incidences: break # Fin de datos
            
            for inc in incidences:
                city = inc.get('cityTown')
                if city:
                    # Guardamos el nombre tal cual viene de Tráfico
                    traffic_cities.add(city)
            print(f"   -> Página {page}: {len(incidences)} incidencias procesadas.")
        else:
            break
            
    print(f"✅ Se han encontrado {len(traffic_cities)} municipios únicos con incidencias recientes.")
    return sorted(list(traffic_cities))

def main():
    # 1. AUTENTICACIÓN
    print("🔑 Generando token Euskalmet...")
    token = generate_euskadi_jwt(USER_EMAIL, PRIVATE_KEY_PATH)
    if not token: return

    # 2. CARGAR DICCIONARIO EUSKALMET (Referencia)
    euskalmet_map = {} # Diccionario: {nombre_minuscula: datos_reales}
    euskalmet_list_for_csv = [] # Lista para guardar el CSV final

    print(f"\n🌍 Descargando catálogo de ubicaciones de Euskalmet ({REGION_ID})...")
    zones_url = f"{EUSKALMET_BASE_URL}/geo/regions/{REGION_ID}/zones"
    zones_data = get_data(zones_url, token)

    if zones_data:
        for zone in zones_data:
            zone_id = zone.get('regionZoneId')
            if not zone_id: continue
            
            locs_url = f"{EUSKALMET_BASE_URL}/geo/regions/{REGION_ID}/zones/{zone_id}/locations"
            locs_data = get_data(locs_url, token)
            
            if locs_data:
                for loc in locs_data:
                    loc_id = loc.get('regionZoneLocationId')
                    loc_name = loc.get('regionZoneLocationName', loc_id)
                    
                    # Guardamos para el CSV
                    entry = {
                        'municipio_trafico': loc_name, # Este es el nombre "oficial" de Euskalmet
                        'region_id': REGION_ID,
                        'region_zone_id': zone_id,
                        'location_id': loc_id
                    }
                    euskalmet_list_for_csv.append(entry)
                    
                    # Guardamos en el diccionario de búsqueda (clave normalizada)
                    norm_name = str(loc_name).strip().lower()
                    euskalmet_map[norm_name] = entry

    print(f"✅ Euskalmet tiene {len(euskalmet_list_for_csv)} ubicaciones disponibles.")

    # 3. CARGAR MUESTRA DE TRÁFICO
    traffic_cities = get_traffic_cities_sample()

    # 4. VALIDACIÓN INVERSA (Tráfico -> Euskalmet)
    print("\n🔎 --- RESULTADO DE LA VALIDACIÓN (Tráfico vs Euskalmet) ---")
    print("Objetivo: Encontrar ciudades de tráfico que NO tienen coincidencia exacta en Euskalmet.\n")
    
    missing_count = 0
    matched_count = 0
    
    for city in traffic_cities:
        city_norm = str(city).strip().lower()
        
        if city_norm in euskalmet_map:
            matched_count += 1
            # print(f"✅ OK: {city}") # Descomentar si quieres ver los aciertos
        else:
            missing_count += 1
            # ESTE ES EL OUTPUT IMPORTANTE PARA TI:
            print(f"❌ SIN PAREJA: '{city}' (Aparece en Tráfico pero NO coincide con Euskalmet)")

    # 5. GUARDAR CSV BASE
    # Guardamos el catálogo de Euskalmet, que es lo que usarás para descargar el tiempo.
    # TÚ tendrás que añadir manualmente al CSV las correcciones basándote en los logs de arriba.
    if euskalmet_list_for_csv:
        df = pd.DataFrame(euskalmet_list_for_csv)
        cols = ['municipio_trafico', 'region_id', 'region_zone_id', 'location_id']
        df = df[cols]
        
        output_file = "mapping_municipios_base.csv"
        df.to_csv(output_file, index=False)
        print(f"\n💾 Catálogo base guardado en: {output_file}")
        print(f"📊 Resumen: {matched_count} coinciden automáticamente. {missing_count} necesitan revisión manual.")
        print("👉 Abre el CSV y añade filas nuevas o edita los nombres para cubrir los 'SIN PAREJA'.")

if __name__ == "__main__":
    main()