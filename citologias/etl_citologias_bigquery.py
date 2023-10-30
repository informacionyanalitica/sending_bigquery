import pandas as pd
import numpy as np 
import sys,os 

path = os.path.abspath('/data/compartida/etls/tools')
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

SQL_CITOLOGIAS_BD = """ SELECT *
                FROM reportes.citologias AS r
                WHERE r.fecha_real = '2023-10-28'
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

# Leer datos
df_citologias_bd = func_process.load_df_server(SQL_CITOLOGIAS_BD, 'reportes')   

# Convertir fechas
df_citologias = converti_columns_date(df_citologias_bd)

# Obtener datos no duplicados
valores_unicos = tuple(map(int, df_citologias[validator_column]))
df_citologias_not_duplicates = loadbq.rows_not_duplicates(df_citologias,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

# Cargar a bigquery
loadbq.load_data_bigquery(df_citologias_not_duplicates,TABLA_BIGQUERY)
