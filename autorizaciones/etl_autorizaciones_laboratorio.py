import pandas as pd
import sys, os
from datetime import datetime
from dotenv import load_dotenv
import pandas_gbq

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process

def get_sql_last_24h():
    return """
    SELECT *
    FROM analitica.autorizacionesLaboratorioView
    WHERE fechaImpresion >= NOW() - INTERVAL 24 HOUR
      AND fechaImpresion < NOW();
    """

project_id_product = 'ia-bigquery-397516'
dataset_id_autorizaciones = 'autorizaciones'
table_name_historico = 'autorizacionesLaboratorio'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_autorizaciones}.{table_name_historico}'

def to_camel_case(s):
    parts = s.split('_')
    return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def columns_to_camel_case(df):
    df.columns = [to_camel_case(col) for col in df.columns]
    return df

def convert_date(df):
    if 'fechaImpresion' in df.columns:
        df.fechaImpresion = pd.to_datetime(df.fechaImpresion, errors='coerce')
    if 'fechaVencimientoAutorizacion' in df.columns:
        df.fechaVencimientoAutorizacion = pd.to_datetime(df.fechaVencimientoAutorizacion, errors='coerce')
    return df

def convert_number(df):
    if 'cantidadPrestacion' in df.columns:
        df.cantidadPrestacion.fillna(0, inplace=True)
        df.cantidadPrestacion = df.cantidadPrestacion.astype(int)
    if 'idCodigoTipoCobro' in df.columns:
        df.idCodigoTipoCobro.fillna(0, inplace=True)
        df.idCodigoTipoCobro = df.idCodigoTipoCobro.astype(int)
    return df

print("Iniciando extracción de datos...")
df_autorizaciones_bd = func_process.load_df_server(get_sql_last_24h(), 'analitica')
print(f"DF cargado con éxito. Filas: {len(df_autorizaciones_bd)}")

print("Transformando columnas a camelCase...")
df_autorizaciones_bd = columns_to_camel_case(df_autorizaciones_bd)

print("Convirtiendo tipos de datos...")
df_autorizaciones_bd = convert_date(df_autorizaciones_bd)
df_autorizaciones_bd = convert_number(df_autorizaciones_bd)

print(f"Cargando datos a BigQuery en {TABLA_BIGQUERY} (reemplazo total)...")
pandas_gbq.to_gbq(
    df_autorizaciones_bd,
    TABLA_BIGQUERY,
    project_id=project_id_product,
    if_exists='replace'
)

print(f"Se insertaron {len(df_autorizaciones_bd)} filas en {TABLA_BIGQUERY}.")
print("Save successfully")
print(f"Fecha y hora de carga: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")