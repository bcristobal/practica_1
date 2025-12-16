from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import time
import jwt
import requests
import pandas as pd
import pandera as pa
from dotenv import load_dotenv

# --- CONFIGURACIÓN DE RUTAS EN DOCKER ---
# En Airflow Docker, las rutas son fijas según el docker-compose
BASE_PATH = "/opt/airflow"
PRIVATE_KEY_PATH = os.path.join(BASE_PATH, "Apikey/privateKey.pem")
MAPPING_CSV_PATH = os.path.join(BASE_PATH, "data/mapping_municipios.csv")
OUTPUT_PATH = os.path.join(BASE_PATH, "output")

# Cargar variables
load_dotenv()
USER_EMAIL = os.getenv("USER_EMAIL", "b.cristobal@opendeusto.es")

# URLs
EUSKALMET_BASE = "https://api.euskadi.eus/euskalmet"
TRAFFIC_BASE = "https://api.euskadi.eus/traffic/v1.0/incidences"

# --- TUS FUNCIONES AUXILIARES (Sin cambios lógicos) ---

def get_euskadi_jwt():
    try:
        with open(PRIVATE_KEY_PATH, 'r') as f:
            private_key = f.read()
    except FileNotFoundError:
        print(f"❌ Error: No encuentro clave en {PRIVATE_KEY_PATH}")
        return None

    current_time = int(time.time())
    payload = {
        "aud": "met01.apikey",
        "iss": "Airflow_Integration",
        "exp": current_time + 3600,
        "iat": current_time,
        "version": "1.0.0",
        "email": USER_EMAIL
    }
    headers = {"alg": "RS256", "typ": "JWT"}
    return jwt.encode(payload, private_key, algorithm="RS256", headers=headers)

def get_data(url, token=None):
    headers = {"Accept": "application/json"}
    if token: headers["Authorization"] = f"Bearer {token}"
    try:
        r = requests.get(url, headers=headers)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    print("🔬 Validando calidad con Pandera...")
    schema = pa.DataFrameSchema({
        "cityTown": pa.Column(pa.String, nullable=False),
        "incidenceType": pa.Column(pa.String, checks=pa.Check.isin(["Accidente"])),
        "temp_media": pa.Column(pa.Float, nullable=True, checks=[
            pa.Check.greater_than_or_equal_to(-15.0),
            pa.Check.less_than_or_equal_to(50.0)
        ]),
    })
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        print(f"⚠️ Errores de calidad detectados: {len(err.failure_cases)}")
        return df

def fetch_traffic_incidences(date_str):
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
    if df.empty: return df
    return df[df['incidenceType'] == 'Accidente'].copy()

def fetch_weather_forecast(mapping_df, date_obj):
    token = get_euskadi_jwt()
    if not token: return pd.DataFrame()

    date_path = date_obj.strftime("%Y/%m/%d")
    date_compact = date_obj.strftime("%Y%m%d")
    weather_records = []
    
    unique_locations = mapping_df.drop_duplicates(subset=['location_id'])
    print(f"☁️ Solicitando clima para {len(unique_locations)} municipios...")
    
    for idx, row in unique_locations.iterrows():
        region, zone, loc_id = row['region_id'], row['region_zone_id'], row['location_id']
        url = f"{EUSKALMET_BASE}/weather/regions/{region}/zones/{zone}/locations/{loc_id}/forecast/at/{date_path}/for/{date_compact}"
        data = get_data(url, token)
        if data:
            weather_records.append({
                'location_id': loc_id,
                'temp_media': data.get('temperature', {}).get('value'),
                'clima_descripcion': data.get('forecastText', {}).get('SPANISH')
            })
    return pd.DataFrame(weather_records)

# --- LA TAREA PRINCIPAL ---

def run_etl_logic(**kwargs):
    # Airflow nos inyecta 'logical_date' automáticamente
    execution_date = kwargs['logical_date']
    date_str = execution_date.strftime("%Y/%m/%d")
    
    print(f"🚀 [AIRFLOW] Iniciando ETL para el día: {date_str}")
    
    # 1. Cargar Mapping (usando separador automático)
    if not os.path.exists(MAPPING_CSV_PATH):
        raise FileNotFoundError(f"No encuentro: {MAPPING_CSV_PATH}")
    
    df_mapping = pd.read_csv(MAPPING_CSV_PATH, sep=None, engine='python')
    df_mapping['join_key'] = df_mapping['municipio_trafico'].astype(str).str.strip().str.lower()
    
    # 2. Obtener Tráfico
    df_traffic = fetch_traffic_incidences(date_str)
    if df_traffic.empty:
        print("ℹ️ No hubo accidentes este día. Fin.")
        return

    df_traffic['join_key'] = df_traffic['cityTown'].astype(str).str.strip().str.lower()
    
    # 3. Optimización (Filtrar mapa)
    affected_cities = df_traffic['join_key'].unique()
    df_map_opt = df_mapping[df_mapping['join_key'].isin(affected_cities)]
    
    if df_map_opt.empty:
        df_weather = pd.DataFrame()
    else:
        df_weather = fetch_weather_forecast(df_map_opt, execution_date)

    # 4. Cruce
    df_merged = pd.merge(df_traffic, df_mapping, on='join_key', how='left', suffixes=('', '_map'))
    if not df_weather.empty:
        df_final = pd.merge(df_merged, df_weather, on='location_id', how='left')
    else:
        df_final = df_merged

    # 5. Guardar
    df_final = validate_data(df_final)
    
    filename = f"reporte_diario_{execution_date.strftime('%Y%m%d')}.xlsx"
    full_path = os.path.join(OUTPUT_PATH, filename)
    
    cols = ['startDate', 'cityTown', 'incidenceType', 'municipio_trafico', 'temp_media', 'clima_descripcion']
    available_cols = [c for c in cols if c in df_final.columns]
    
    df_final[available_cols].to_excel(full_path, index=False)
    print(f"✅ Archivo generado: {full_path}")

# --- DEFINICIÓN DEL DAG ---

default_args = {
    'owner': 'estudiante_deusto',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_trafico_euskadi',
    default_args=default_args,
    description='Pipeline ETL oficial de la práctica',
    schedule_interval='@daily',       # Ejecutar diariamente
    start_date=datetime(2025, 11, 1), # FECHA DE INICIO DEL HISTÓRICO
    catchup=True,                     # True = Rellenar todo el mes de Noviembre automáticamente
    tags=['bigdata', 'practica'],
) as dag:

    tarea_principal = PythonOperator(
        task_id='etl_diaria',
        python_callable=run_etl_logic,
        provide_context=True,
    )

    tarea_principal