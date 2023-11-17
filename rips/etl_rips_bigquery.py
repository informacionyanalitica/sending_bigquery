import pandas as pd
import numpy as np 
import sys,os 

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


SQL_RIPS_BD = """SELECT *
            FROM reportes.rips AS s
            WHERE s.fecha_cargue >= adddate(curdate(), interval -7 day)
                """
                
SQL_RIPS_BIGQUERY = """
                SELECT g.id_rip
                FROM {} as g
                WHERE g.id_rip IN {}
                """
                
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'rips'
table_name_rips = 'rips_partition'
validator_column = 'id_rip'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_rips}.{table_name_rips}'

# convertir fechas
def convertir_dates(df):
    df.fecha_cargue = pd.to_datetime(df.fecha_cargue, errors='coerce')
    df.fecha_nacimiento = pd.to_datetime(df.fecha_nacimiento, errors='coerce')
    df.hora_fecha = pd.to_datetime(df.hora_fecha, errors='coerce')
    return df

# Convertir valores numericos
def convertir_numbers(df):
    df.edad_anos = df.edad_anos.apply(lambda x: int(x) if x.isdigit() else np.nan)
    df.edad_meses = df.edad_meses.apply(lambda x: int(x) if x.isdigit() else np.nan)
    df.edad_dias = df.edad_dias.apply(lambda x: int(x) if x.isdigit() else np.nan)
    return df

# Leer datos
df_rips_bd = func_process.load_df_server(SQL_RIPS_BD, 'reportes')   

# Obtener datos no duplicados
valores_unicos = tuple(map(int,df_rips_bd[validator_column]))
df_rips_not_duplicates = loadbq.rows_not_duplicates(df_rips_bd,validator_column,SQL_RIPS_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

# Converitr tipos de datos
df_rips_not_duplicates_transform = convertir_dates(df_rips_not_duplicates)
df_rips_not_duplicates_transform = convertir_numbers(df_rips_not_duplicates)

# Cargar a bigquery
loadbq.load_data_bigquery(df_rips_not_duplicates_transform,TABLA_BIGQUERY)
