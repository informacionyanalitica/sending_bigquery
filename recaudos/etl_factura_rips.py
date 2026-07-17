import os
import sys
import time
import glob
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import pandas_gbq
from google.cloud import bigquery

# Cargar variables de entorno
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process

# ==========================================
# CONFIGURACIÓN DE RUTAS Y BIGQUERY
# ==========================================
project_id_product = 'ia-bigquery-397516'
dataset_id_recaudos = 'recaudos'
table_name_recaudos = 'facturaRips'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_recaudos}.{table_name_recaudos}'

MESES_ESP = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

def log(msg, emoji="•"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {emoji} {msg}")

# ==========================================
# CÁLCULO DE FECHAS DINÁMICAS
# ==========================================
hoy = datetime.now()

primer_dia_mes_actual = hoy.replace(day=1)
mes_carpeta = primer_dia_mes_actual - pd.Timedelta(days=1)

year_str = mes_carpeta.strftime('%Y')                 
month_num = mes_carpeta.month                         
month_word = MESES_ESP[month_num]                     
folder_month_name = f"{month_num}. {month_word}"      

yyyymm_archivo = hoy.strftime('%Y%m')                 

PATH_DRIVE = os.environ.get("PATH_DRIVE")
PATH_FACTURA_RIPS = os.path.join(PATH_DRIVE, "BASES DE DATOS", year_str, folder_month_name, "FACTURA RIPS")

# ==========================================
# MAPEO EXPLICITO DEL ESQUEMA REAL (BigQuery)
# ==========================================
TIMESTAMPS_COLS = [
    'fechaNacimiento', 'fechaEmision', 'fechaImpresion', 'fechaVencimiento', 'fechaAtencionIvr'
]

INTEGERS_COLS = [
    'edad', 'cantidadUnidadesAut'
]

NUMERICS_COLS = [
    'vrCuotaMod', 'vrCopago'
]

def to_camel_case(s):
    s = s.strip().lower()
    parts = [p for p in s.split('_') if p]
    if not parts:
        return ""
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def columns_to_camel_case(df):
    df.columns = [to_camel_case(col) for col in df.columns]
    return df

def normalize_types_strict(df):
    for col in df.columns:
        # 1. Forzar Timestamps con localización UTC
        if col in TIMESTAMPS_COLS:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            else:
                df[col] = df[col].dt.tz_convert('UTC')
                
        # 2. Forzar Enteros
        elif col in INTEGERS_COLS:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            
        # 3. Forzar Numéricos
        elif col in NUMERICS_COLS:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            
        # 4. Todo lo demás es STRING estricto
        else:
            df[col] = df[col].astype(str).replace(r'\.0$', '', regex=True).replace(['nan', 'None', 'null'], '')
            
    return df

# ==========================================
# PROCESO PRINCIPAL (ETL)
# ==========================================
t_inicio = time.time()

print("=" * 60)
print(" ETL MENSUAL — facturaRips")
print("=" * 60)

log(f"Mes Carpeta: {month_word} {year_str} | Sufijo Archivo: {yyyymm_archivo}", "📅")

search_pattern = os.path.join(PATH_FACTURA_RIPS, f"*{yyyymm_archivo}.xlsx")
all_files = glob.glob(search_pattern)
valid_files = [f for f in all_files if not os.path.basename(f).startswith("~$")]

if not valid_files:
    raise FileNotFoundError(f"No se encontró ningún archivo de Excel válido para el periodo {yyyymm_archivo} en {PATH_FACTURA_RIPS}")

file_path = valid_files[0]
log(f"Archivo detectado: {os.path.basename(file_path)}", "📄")

# 1. Extracción
log("Leyendo archivo de Excel...", "🔌")
t0 = time.time()
df_rips = pd.read_excel(file_path) 
log(f"Lectura completada — {len(df_rips):,} filas", "✅") # <-- Corregido el formateador de miles

# 2. Transformación
log("Normalizando columnas a camelCase...", "🔤")
df_rips = columns_to_camel_case(df_rips)

log("Aplicando tipado estricto según esquema BigQuery...", "🛠️")
df_rips = normalize_types_strict(df_rips)

df_rips['periodoProceso'] = yyyymm_archivo

# 3. Carga
log(f"Eliminando registros anteriores para el periodo {yyyymm_archivo}...", "🗑️")
t0 = time.time()
client = bigquery.Client(project=project_id_product)

delete_query = f"""
DELETE FROM `{TABLA_BIGQUERY}`
WHERE periodoProceso = '{yyyymm_archivo}'
"""
try:
    client.query(delete_query).result()
    log("Eliminación completada", "✅")
except Exception as e:
    log(f"No se pudo limpiar la tabla: {e}", "⚠️")

log(f"Insertando en BigQuery → {TABLA_BIGQUERY} (append)...", "☁️")
t0 = time.time()

custom_schema = []
for col in df_rips.columns:
    if col in TIMESTAMPS_COLS:
        custom_schema.append({'name': col, 'type': 'TIMESTAMP'})
    elif col in INTEGERS_COLS:
        custom_schema.append({'name': col, 'type': 'INTEGER'})
    elif col in NUMERICS_COLS:
        custom_schema.append({'name': col, 'type': 'NUMERIC'})
    else:
        custom_schema.append({'name': col, 'type': 'STRING'})

pandas_gbq.to_gbq(
    df_rips,
    TABLA_BIGQUERY,
    project_id=project_id_product,
    if_exists='append',
    table_schema=custom_schema
)
log("Carga de datos completada con éxito", "✅")

print("-" * 60)
log(f"Proceso finalizado en {time.time() - t_inicio:.2f}s totales", "🏁")
print("=" * 60)

print("Save successfully")
print(f"{len(df_rips)},{TABLA_BIGQUERY},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")