import pandas as pd
import sys
import os
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


SQL_EMPLEADOS_BD = """SELECT *
            FROM reportes.empleados_activos
            """
SQL_BIGQUERY = """
                SELECT g.identificacion
                FROM {} as g
                WHERE g.identificacion IN {}
                """
project_id_product = 'ia-bigquery-397516'
dataset_id_turnos = 'empleados'
table_name_turnos = 'activos'
#validator_column = 'identificacion'    

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_turnos}.{table_name_turnos}'

def convert_number_columns(df):
    # Convertir vacios en 0
    df.hijos.replace('',0, inplace=True)
    df.salario.fillna(0,inplace=True)
    # Convertir numeros
    df.hijos=df.hijos.astype(int)
    df.salario=df.salario.astype(int)
    return df
def convert_date_columns(df):
    df.fecha_ingreso = pd.to_datetime(df.fecha_ingreso, errors='coerce')
    df.ultimo_contrato = pd.to_datetime(df.ultimo_contrato, errors='coerce')
    df.finalizacion_contrato = pd.to_datetime(df.finalizacion_contrato, errors='coerce')
    return df

def validate_load(df_load):
    try:
        total_cargues = df_load.totalCargues[0]
        if total_cargues==0:
            # Leer datos
            df_activos_bd = func_process.load_df_server(SQL_EMPLEADOS_BD, 'reportes')   
            df_activos_bd = convert_number_columns(df_activos_bd)
            df_activos_bd = convert_date_columns(df_activos_bd)    
            
            # Obtener datos no duplicados
            #valores_unicos = tuple(map(str, df_activos_bd[validator_column]))
            #df_activos_not_duplicates = loadbq.rows_not_duplicates(df_activos_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos) 
            # Load bigquery
            loadbq.load_data_bigquery(df_activos_bd,TABLA_BIGQUERY,'WRITE_TRUNCATE')
    except ValueError as err:
        print(err)


# Cargar a bigquery
validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)
validate_load(validate_loads_logs)

