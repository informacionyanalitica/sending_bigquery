import pandas as pd
import sys
import os
from datetime import datetime

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


SQL_EMPLEADOS_BD = """SELECT *
            FROM reportes.empleados_2019
            """
SQL_BIGQUERY = """
                SELECT g.identificacion
                FROM {} as g
                WHERE g.identificacion IN {}
                """
project_id_product = 'ia-bigquery-397516'
dataset_id_turnos = 'empleados'
table_name_turnos = 'historicos'
validator_column = 'identificacion'   


TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_turnos}.{table_name_turnos}'

def convert_date_columns(df):
    df.fecha_ingreso = pd.to_datetime(df.fecha_ingreso, errors='coerce')
    df.fecha_retiro = pd.to_datetime(df.fecha_retiro, errors='coerce')
    return df

def validate_load(df_load):
    try:
        totalCargue = df_load.totalCargues[0]
        if totalCargue==0:
            # Leer datos
            df_historicos_bd = func_process.load_df_server(SQL_EMPLEADOS_BD, 'reportes')
            # Convert columns
            df_historicos_bd = convert_date_columns(df_historicos_bd)
            # Obtener datos no duplicados
            valores_unicos = tuple(map(str, df_historicos_bd[validator_column]))
            df_historicos_not_duplicates = loadbq.rows_not_duplicates(df_historicos_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)
            loadbq.load_data_bigquery(df_historicos_not_duplicates,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

# Cargar a bigquery
validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)
validate_load(validate_loads_logs)