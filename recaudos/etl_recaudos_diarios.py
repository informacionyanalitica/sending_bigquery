import pandas as pd
import sys, os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas_gbq
from google.cloud import bigquery

load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process

def get_rango_semana_actual():
    hoy = datetime.now()
    lunes = (hoy - timedelta(days=hoy.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return lunes

def get_sql_semana_actual(lunes_str):
    return f"""
    SELECT *
    FROM reportes.recaudosDiariosUltimos2AnosView
    WHERE fechaPago >= '{lunes_str}'
      AND fechaPago < NOW();
    """

project_id_product = 'ia-bigquery-397516'
dataset_id_recaudos = 'recaudos'
table_name_recaudos = 'recaudosDiarios'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_recaudos}.{table_name_recaudos}'

def to_camel_case(s):
    parts = s.split('_')
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def columns_to_camel_case(df):
    df.columns = [to_camel_case(col) for col in df.columns]
    return df

def convert_date(df):
    if 'fechaPago' in df.columns:
        df['fechaPago'] = pd.to_datetime(df['fechaPago'], errors='coerce')
    return df

def convert_number(df):
    numeric_cols = [
        'valorCobrar', 'idPago', 'codigoTipoCobro', 'codigoTipoPago', 'codigoSedeCobro'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df

def log(msg, emoji="•"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {emoji} {msg}")

t_inicio = time.time()

print("=" * 60)
print(" ETL SEMANAL — recaudosDiarios")
print("=" * 60)

lunes = get_rango_semana_actual()
lunes_str = lunes.strftime('%Y-%m-%d %H:%M:%S')
log(f"Semana en curso: desde {lunes_str}", "📅")

log("Eliminando registros existentes de la semana en BigQuery...", "🗑️")
t0 = time.time()
client = bigquery.Client(project=project_id_product)
delete_query = f"""
DELETE FROM `{TABLA_BIGQUERY}`
WHERE fechaPago >= TIMESTAMP('{lunes_str}')
"""
client.query(delete_query).result()
log(f"Eliminación completada en {time.time() - t0:.2f}s", "✅")

log("Extrayendo datos de la semana en curso...", "🔌")
t0 = time.time()
df_recaudos_bd = func_process.load_df_server(get_sql_semana_actual(lunes_str), 'reportes')
log(f"Extracción completada en {time.time() - t0:.2f}s — {len(df_recaudos_bd):,} filas", "✅")

log("Normalizando columnas a camelCase...", "🔤")
df_recaudos_bd = columns_to_camel_case(df_recaudos_bd)

log("Convirtiendo tipos (fechas y numéricos)...", "🛠️")
df_recaudos_bd = convert_date(df_recaudos_bd)
df_recaudos_bd = convert_number(df_recaudos_bd)

log(f"Insertando en BigQuery → {TABLA_BIGQUERY} (append)...", "☁️")
t0 = time.time()
pandas_gbq.to_gbq(
    df_recaudos_bd,
    TABLA_BIGQUERY,
    project_id=project_id_product,
    if_exists='append'
)
log(f"Carga completada en {time.time() - t0:.2f}s", "✅")

print("-" * 60)
log(f"Proceso finalizado en {time.time() - t_inicio:.2f}s totales", "🏁")
print("=" * 60)

# --- Salida requerida por el DAG (NO MODIFICAR el orden ni formato) ---
print("Save successfully")
print(f"{len(df_recaudos_bd)},{TABLA_BIGQUERY},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")