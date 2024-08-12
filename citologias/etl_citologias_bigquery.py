import pandas as pd
import numpy as np 
import sys,os 
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

SQL_CITOLOGIAS_BD = """ SELECT *
                FROM reportes.citologias AS r
                WHERE r.fecha_real = '2024-08-08'
            """
               
SQL_BIGQUERY = """
                SELECT g.id_citologia
                FROM {} as g
                WHERE g.id_citologia IN {}
                """
                
project_id_product = 'ia-bigquery-397516'
dataset_id_citologias = 'citologias'
table_name_citologias = 'citologias'
validator_column = 'id_citologia'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_citologias}.{table_name_citologias}'

def converti_columns_date(df):
    df.fecha = pd.to_datetime(df.fecha, errors='coerce')
    df.fecha_expide_orden = pd.to_datetime(df.fecha_expide_orden, errors='coerce')
    df.fecha_real = pd.to_datetime(df.fecha_real, errors='coerce')
    return df

# Obtener datos no duplicados
def execution_load(df_citologias):
    try:
        if df_citologias.shape[0] > 0:
            valores_unicos = tuple(map(int, df_citologias[validator_column]))
            df_citologias_not_duplicates = loadbq.rows_not_duplicates(df_citologias,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)
            loadbq.load_data_bigquery(df_citologias_not_duplicates,TABLA_BIGQUERY)
        else:
            raise SystemExit
    except Exception as err:
        print(err)

# Leer datos
df_citologias_bd = func_process.load_df_server(SQL_CITOLOGIAS_BD, 'reportes')   
# Convertir fechas
df_citologias = converti_columns_date(df_citologias_bd)

# Cargar a bigquery
execution_load(df_citologias)

