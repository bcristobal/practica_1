import os
import time
import jwt
import requests
import pandas as pd
import pandera as pa
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
load_dotenv()

USER_EMAIL = os.getenv("USER_EMAIL", "b.cristobal@opendeusto.es")
PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", "Apikey/privateKey.pem")

# URLs Base
EUSKALMET_BASE = "https://api.euskadi.eus/euskalmet"
TRAFFIC_BASE = "https://api.euskadi.eus/traffic/v1.0/incidences"

# --- CONFIGURACIÓN DEL RANGO DE FECHAS ---
START_DATE = datetime(2025, 11, 1)
END_DATE = datetime(2025, 11, 30)

def get_euskadi_jwt():
    """Genera el token JWT leyendo siempre del archivo .pem para evitar errores de formato."""
    try:
        # Forzamos la lectura del archivo que sabemos que es correcto
        with open(PRIVATE_KEY_PATH, 'r') as f:
            private_key = f.read()
            
    except FileNotFoundError:
        # Solo si no existe el archivo, intentamos leer de la variable de entorno (para Docker)
        private_key = os.getenv("PRIVATE_KEY_EUSKALMET")
        if not private_key:
            print(f"❌ Error: No se encuentra la clave privada en {PRIVATE_KEY_PATH} ni en ENV.")
            return None
            
        # Si leemos de ENV, intentamos arreglar el formato si le faltan las cabeceras
        if "-----BEGIN PRIVATE KEY-----" not in private_key:
            private_key = f"-----BEGIN PRIVATE KEY-----\n{private_key}\n-----END PRIVATE KEY-----"

    current_time = int(time.time())
    payload = {
        "aud": "met01.apikey",
        "iss": "PythonScript_Integration",
        "exp": current_time + 3600,
        "iat": current_time,
        "version": "1.0.0",
        "email": USER_EMAIL
    }
    headers = {"alg": "RS256", "typ": "JWT"}
    
    try:
        return jwt.encode(payload, private_key, algorithm="RS256", headers=headers)
    except ValueError as e:
        print(f"❌ Error de formato en la clave privada: {e}")
        return None

def get_data(url, token=None):
    """Helper para peticiones HTTP."""
    headers = {"Accept": "application/json"}
    if token: headers["Authorization"] = f"Bearer {token}"
    try:
        r = requests.get(url, headers=headers)
        return r.json() if r.status_code == 200 else None
    except Exception as e:
        print(f"❌ Error HTTP: {e}")
        return None

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Valida el esquema del dataframe final acumulado."""
    print("🔬 (Calidad) Ejecutando validación de esquema con Pandera...")
    
    schema = pa.DataFrameSchema({
        "cityTown": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(min_value=1)),
        "incidenceType": pa.Column(pa.String, checks=pa.Check.isin(["Accidente"])),
        "temp_media": pa.Column(pa.Float, nullable=True, checks=[
            pa.Check.greater_than_or_equal_to(-15.0),
            pa.Check.less_than_or_equal_to(50.0)
        ]),
        "startDate": pa.Column(pa.String, nullable=False)
    })

    try:
        validated_df = schema.validate(df, lazy=True)
        print("✅ (Calidad) Datos validados correctamente.")
        return validated_df
    except pa.errors.SchemaErrors as err:
        print("\n⚠️ ALERTA DE CALIDAD DE DATOS (Se encontraron errores en algunas filas):")
        # print(err.failure_cases.head()) 
        return df

def fetch_traffic_incidences(date_obj):
    """Descarga incidencias de UN DÍA y filtra solo ACCIDENTES."""
    date_str = date_obj.strftime("%Y/%m/%d")
    
    all_incidences = []
    page = 1
    
    while True:
        url = f"{TRAFFIC_BASE}/byDate/{date_str}?_page={page}"
        data = get_data(url)
        
        if not data or 'incidences' not in data or not data['incidences']:
            break
            
        all_incidences.extend(data['incidences'])
        if page >= data.get('totalPages', 1):
            break
        page += 1
        
    df = pd.DataFrame(all_incidences)
    
    if df.empty:
        return df
        
    # Filtramos solo 'Accidente'
    df_filtered = df[df['incidenceType'] == 'Accidente'].copy()
    return df_filtered

def fetch_weather_forecast(mapping_df, date_obj):
    """Descarga clima solo para las ubicaciones presentes en mapping_df."""
    token = get_euskadi_jwt()
    if not token: return pd.DataFrame()

    date_path = date_obj.strftime("%Y/%m/%d")
    date_compact = date_obj.strftime("%Y%m%d")
    
    weather_records = []
    # Eliminamos duplicados para no pedir el mismo sitio dos veces
    unique_locations = mapping_df.drop_duplicates(subset=['location_id'])
    
    # Feedback visual
    print(f"   ☁️  Solicitando clima para {len(unique_locations)} municipios afectados...")
    
    for idx, row in unique_locations.iterrows():
        region = row['region_id']
        zone = row['region_zone_id']
        loc_id = row['location_id']
        
        url = (f"{EUSKALMET_BASE}/weather/regions/{region}/zones/{zone}/locations/{loc_id}/"
               f"forecast/at/{date_path}/for/{date_compact}")
        
        data = get_data(url, token)
        
        if data:
            temp = data.get('temperature', {}).get('value')
            min_t = data.get('temperatureRange', {}).get('min')
            max_t = data.get('temperatureRange', {}).get('max')
            desc = data.get('forecastText', {}).get('SPANISH')
            
            weather_records.append({
                'location_id': loc_id,
                'temp_media': temp,
                'temp_min': min_t,
                'temp_max': max_t,
                'clima_descripcion': desc
            })
            
    return pd.DataFrame(weather_records)

def process_day(current_date, df_mapping):
    """Procesa un solo día: descarga tráfico, filtra mapa y descarga clima optimizado."""
    date_str = current_date.strftime("%Y/%m/%d")
    print(f"\n📅 Procesando: {date_str}...")

    # 1. Tráfico
    df_traffic = fetch_traffic_incidences(current_date)
    if df_traffic.empty:
        print(f"   ℹ️ Sin accidentes para {date_str}")
        return pd.DataFrame()

    print(f"   🚗 Accidentes encontrados: {len(df_traffic)}")
    df_traffic['join_key'] = df_traffic['cityTown'].astype(str).str.strip().str.lower()

    # --- OPTIMIZACIÓN ---
    # 2. Filtrar Mapping: Identificar qué municipios necesitan clima
    affected_cities = df_traffic['join_key'].unique()
    
    # Nos quedamos solo con las filas del CSV que coinciden con los accidentes de hoy
    df_mapping_optimized = df_mapping[df_mapping['join_key'].isin(affected_cities)]
    
    if df_mapping_optimized.empty:
        print("   ⚠️ Los municipios de los accidentes no coinciden con el mapeo. No se descargará clima.")
        df_weather = pd.DataFrame()
    else:
        # 3. Clima: Llamada optimizada
        df_weather = fetch_weather_forecast(df_mapping_optimized, current_date)
    
    # 4. Cruce
    df_merged = pd.merge(df_traffic, df_mapping, on='join_key', how='left', suffixes=('', '_map'))
    
    if not df_weather.empty:
        df_final = pd.merge(df_merged, df_weather, on='location_id', how='left')
    else:
        df_final = df_merged
        
    return df_final

def main():
    # Cargar Mapeo
    csv_file = "mapping_municipios.csv" 
    if not os.path.exists(csv_file):
        print("❌ Falta mapping_municipios.csv")
        return
    
    # Usamos engine='python' y sep=None para detectar ; o , automáticamente    
    df_mapping = pd.read_csv(csv_file, sep=None, engine='python')
    df_mapping['join_key'] = df_mapping['municipio_trafico'].astype(str).str.strip().str.lower()

    # Bucle Mensual
    current_date = START_DATE
    monthly_results = []

    print(f"🚀 INICIANDO PROCESAMIENTO MENSUAL: {START_DATE.date()} a {END_DATE.date()}")

    while current_date <= END_DATE:
        try:
            df_day = process_day(current_date, df_mapping)
            if not df_day.empty:
                monthly_results.append(df_day)
        except Exception as e:
            print(f"❌ Error procesando {current_date}: {e}")
        
        # Avanzar al siguiente día
        current_date += timedelta(days=1)

    # Consolidación final
    if monthly_results:
        print("\n📦 Consolidando resultados del mes...")
        df_month = pd.concat(monthly_results, ignore_index=True)
        
        # Calidad
        df_month = validate_data(df_month)
        
        # Guardar
        output_file = f"reporte_mensual_{START_DATE.strftime('%Y%m')}.xlsx"
        
        # Seleccionar columnas limpias
        cols_to_keep = ['startDate', 'cityTown', 'incidenceType', 'cause', 
                        'municipio_trafico', 'temp_media', 'temp_min', 'temp_max', 
                        'clima_descripcion', 'latitude', 'longitude']
        
        final_cols = [c for c in cols_to_keep if c in df_month.columns]
        
        df_month[final_cols].to_excel(output_file, index=False)
        print(f"\n✅ ¡ÉXITO! Reporte mensual generado: {output_file}")
        print(f"   Total accidentes procesados: {len(df_month)}")
    else:
        print("\n⚠️ No se encontraron datos para generar el reporte.")

if __name__ == "__main__":
    main()