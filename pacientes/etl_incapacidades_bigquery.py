import pandas as pd
import sys, os
from datetime import datetime
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

# Carga el archivo .env
load_dotenv()
PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)

import func_process
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame

# Configuración BigQuery
project_id_product = 'ia-bigquery-397516'
dataset_id = 'inasistencias'
table_name = 'incapacidades'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id}.{table_name}'

def to_camel_case(s):
    parts = s.lower().split('_')
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def columns_to_camel_case(df):
    df.columns = [to_camel_case(col) for col in df.columns]
    return df

print("="*60)
print(f"[{datetime.now()}] INICIO DEL PROCESO ETL MENSUAL DE INCAPACIDADES")
print(f"Destino BigQuery: {TABLA_BIGQUERY}")
print("="*60)

# Calcular mes anterior
now = datetime.now()
mes_anterior = now - relativedelta(months=1)
primer_dia = datetime(mes_anterior.year, mes_anterior.month, 1).strftime('%Y-%m-%d')
if mes_anterior.month == 12:
    siguiente_mes = datetime(mes_anterior.year + 1, 1, 1)
else:
    siguiente_mes = datetime(mes_anterior.year, mes_anterior.month + 1, 1)
ultimo_dia = (siguiente_mes - relativedelta(days=1)).strftime('%Y-%m-%d')

print(f"[{datetime.now()}] Verificando si ya existen datos de {mes_anterior.strftime('%Y-%m')} en BigQuery...")
query_check = f"""
SELECT COUNT(*) as cantidad FROM `{TABLA_BIGQUERY}`
WHERE fechaIngresoSistemaId >= '{primer_dia}' AND fechaIngresoSistemaId <= '{ultimo_dia}'
"""
df_check = loadbq.read_data_bigquery(query_check, TABLA_BIGQUERY)
if df_check is not None and not df_check.empty and df_check['cantidad'][0] > 0:
    print(f"[{datetime.now()}] ⚠ Ya existen datos para el mes {mes_anterior.strftime('%Y-%m')}, no se insertan registros.")
    print("="*60)
    print(f"[{datetime.now()}] PROCESO FINALIZADO")
    print(f"Resumen: 0 registros procesados.")
    print(f"Tabla destino: {TABLA_BIGQUERY}")
    print(f"Fecha de carga: {datetime.now().strftime('%Y-%m-%d')}")
    print("="*60)
    print(f"0,{TABLA_BIGQUERY},{datetime.now().strftime('%Y-%m-%d')}")
    exit(0)

# 2. Si no hay datos, traer de MariaDB
SQL = f"""
SELECT * FROM analitica.incapacidades_all
WHERE FECHA_INGRESO_SISTEMA_ID >= '{primer_dia}' AND FECHA_INGRESO_SISTEMA_ID <= '{ultimo_dia}'
"""
print(f"[{datetime.now()}] Ejecutando consulta SQL para el mes {mes_anterior.strftime('%Y-%m')}...")
df_incapacidades = func_process.load_df_server(SQL, 'analitica')

if df_incapacidades is not None and not df_incapacidades.empty:
    print(f"[{datetime.now()}] DataFrame cargado con éxito. Registros obtenidos: {len(df_incapacidades)}")
    print(f"[{datetime.now()}] Convirtiendo columnas a camelCase...")
    df_incapacidades = columns_to_camel_case(df_incapacidades)
    print(f"[{datetime.now()}] Enviando datos a BigQuery...")
    loadbq.load_data_bigquery(df_incapacidades, TABLA_BIGQUERY)
    print(f"[{datetime.now()}] ✓ Cargados {len(df_incapacidades)} registros nuevos en BigQuery.")
    total_registros = len(df_incapacidades)
else:
    print(f"[{datetime.now()}] ⚠ Sin registros para cargar para el mes {mes_anterior.strftime('%Y-%m')}.")
    total_registros = 0

fecha_cargue = datetime.now().strftime('%Y-%m-%d')
print("="*60)
print(f"[{datetime.now()}] PROCESO FINALIZADO")
print(f"Resumen: {total_registros} registros procesados.")
print(f"Tabla destino: {TABLA_BIGQUERY}")
print(f"Fecha de carga: {fecha_cargue}")
print("="*60)
print(f"{total_registros},{TABLA_BIGQUERY},{fecha_cargue}")