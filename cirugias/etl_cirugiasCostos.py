import pandas as pd
import sys, os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)

import func_process
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame

convert_columns = convertColumnDataFrame()

project_id_product = 'ia-bigquery-397516'
dataset_id_cirugias = 'cirugias'
table_name_cirugias = 'cirugiasCostos'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_cirugias}.{table_name_cirugias}'

LIST_COLUMNS_STRING = [
    'id_registro','descripcion_tipo_identificacion','tipoIdentificacion','id_paciente',
    'primer_nombre','segundo_nombre','primer_apellido','segundo_apellido',
    'identificacion_paciente','sexo','descripcion_tipo_edad','telefono','celular','email',
    'nombre_municipio','ubicacion','quirofano','diagnostico','procedimiento',
    'codigo_cups','nombre_cups','codigo_autorizacion','profesional','nombre_medico',
    'especialidad','nombre_especialidad','nombre_entidad','requiere_anestesiologo',
    'tipo_anestesia','hora_cirugia','estado_actual','activo','cod_estado_anterior',
    'nombre_estado_anterior','cod_estado_nuevo','nombre_estado_nuevo',
    'observacion_historico','usuario_actualiza','observacion','observacion_oportunidad',
    'observacion_cancelacion','observacion_transitoria','usuario_ingreso',
    'tipoProcedimiento_Final'
]
LIST_COLUMNS_DATE = [
    'fecha_nacimiento','fecha_entrega_orden','fecha_vencimiento_orden',
    'fecha_agendamiento','fecha_actualizacion','nueva_fecha_actualizacion','fecha_ingreso'
]
LIST_COLUMNS_INT = ['edad', 'oportunidad']
LIST_COLUMNS_FLOAT = []

def get_rango_semana_actual():
    hoy = datetime.now()
    lunes = (hoy - timedelta(days=hoy.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return lunes

def get_sql_semana_actual(lunes_date_str):
    return f"""
    SELECT *
    FROM analitica.cirugiasCostosView AS a
    WHERE date(a.fecha_agendamiento) >= '{lunes_date_str}'
    """

def log(msg, emoji="•"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {emoji} {msg}")

t_inicio = time.time()
print("=" * 60)
print(" ETL SEMANAL — cirugiasCostos")
print("=" * 60)

lunes = get_rango_semana_actual()
lunes_date_str = lunes.strftime('%Y-%m-%d')
lunes_str = lunes.strftime('%Y-%m-%d %H:%M:%S')
log(f"Semana en curso: desde {lunes_date_str}", "📅")

log("Eliminando registros existentes de la semana en BigQuery...", "🗑️")
t0 = time.time()
client = bigquery.Client(project=project_id_product)
delete_query = f"""
DELETE FROM `{TABLA_BIGQUERY}`
WHERE DATE(fecha_agendamiento) >= '{lunes_date_str}'
"""
client.query(delete_query).result()
log(f"Eliminación completada en {time.time() - t0:.2f}s", "✅")

log("Extrayendo datos de la semana en curso...", "🔌")
t0 = time.time()
df_cirugias_costos = func_process.load_df_server(get_sql_semana_actual(lunes_date_str), 'analitica')
log(f"Extracción completada en {time.time() - t0:.2f}s — {len(df_cirugias_costos):,} filas", "✅")

if df_cirugias_costos is not None and not df_cirugias_costos.empty:
    cols_string = [c for c in LIST_COLUMNS_STRING if c in df_cirugias_costos.columns]
    cols_date = [c for c in LIST_COLUMNS_DATE if c in df_cirugias_costos.columns]
    cols_int = [c for c in LIST_COLUMNS_INT if c in df_cirugias_costos.columns]
    cols_float = [c for c in LIST_COLUMNS_FLOAT if c in df_cirugias_costos.columns]

    log("Convirtiendo tipos (string, fechas, enteros, float)...", "🛠️")
    df_cirugias_costos = convert_columns.convert_columns_string(df_cirugias_costos, cols_string)
    df_cirugias_costos = convert_columns.convert_columns_integer(df_cirugias_costos, cols_int)
    df_cirugias_costos = convert_columns.convert_columns_date(df_cirugias_costos, cols_date)
    df_cirugias_costos = convert_columns.convert_columns_float(df_cirugias_costos, cols_float)

    log(f"Insertando en BigQuery → {TABLA_BIGQUERY} (append)...", "☁️")
    t0 = time.time()
    loadbq.load_data_bigquery(df_cirugias_costos, TABLA_BIGQUERY)
    log(f"Carga completada en {time.time() - t0:.2f}s", "✅")
    total_registros = len(df_cirugias_costos)
else:
    log("Sin registros para la semana en curso", "⚠️")
    total_registros = 0

print("-" * 60)
log(f"Proceso finalizado en {time.time() - t_inicio:.2f}s totales", "🏁")
print("=" * 60)
print("Save successfully")
print(f"{total_registros},{TABLA_BIGQUERY},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")