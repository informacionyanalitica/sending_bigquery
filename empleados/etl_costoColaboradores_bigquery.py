import pandas as pd
import sys, os
from datetime import datetime
from dotenv import load_dotenv

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
dataset_id = 'empleados'
table_name = 'costoColaboradores'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id}.{table_name}'

# Función para convertir columnas a camelCase
def to_camel_case(s):
    parts = s.split('_')
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def columns_to_camel_case(df):
    df.columns = [to_camel_case(col) for col in df.columns]
    return df

# Función para obtener los idCosto ya cargados en BigQuery
def get_loaded_ids():
    query = f"SELECT idCosto FROM `{TABLA_BIGQUERY}`"
    df = loadbq.read_sql_bigquery(query)
    if df is not None and not df.empty:
        return set(df['idCosto'].astype(str))
    return set()

# Función para cargar solo registros nuevos
def validate_and_load(df, tabla_bigquery):
    if df is not None and not df.empty:
        loadbq.load_data_bigquery(df, tabla_bigquery)
        print(f"✓ Cargados {len(df)} registros nuevos")
    else:
        print("⚠ Sin registros nuevos para cargar")

# Calcular la fecha del 15 del mes actual
now = datetime.now()
fecha_mes = datetime(now.year, now.month, 15).strftime('%Y-%m-%d')

# Solo traer registros de ese mes
SQL = f"SELECT * FROM analitica.costosCaloboradores WHERE fecha = '{fecha_mes}'"
df_costos = func_process.load_df_server(SQL, 'analitica')

# Convertir columnas a camelCase y filtrar duplicados
if df_costos is not None and not df_costos.empty:
    df_costos = columns_to_camel_case(df_costos)
    loaded_ids = get_loaded_ids()
    df_nuevos = df_costos[~df_costos['idCosto'].astype(str).isin(loaded_ids)]
    validate_and_load(df_nuevos, TABLA_BIGQUERY)
else:
    print("⚠ Sin registros para cargar")

# Línea para Airflow/email
total_registros = len(df_nuevos) if 'df_nuevos' in locals() and df_nuevos is not None else 0
fecha_cargue = datetime.now().strftime('%Y-%m-%d')
print(f"{total_registros},{TABLA_BIGQUERY},{fecha_cargue}")