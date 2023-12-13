import sys,os
import pandas as pd
from datetime import datetime,timedelta

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

# Fechas
today = func_process.pd.to_datetime(datetime.now() - timedelta(days=15))
fecha_capita =f"{today.year}-{today.month}-15"

# Parametros bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'rips'
table_name_ciediez = 'rips_ciediez'
table_maridb_ciediez = 'rips_cie10'

TABLA_BIGQUERY_CIEDIEZ = f'{project_id_product}.{dataset_id_rips}.{table_name_ciediez}'


SQL_EMPLEADOS = """SELECT identificacion AS identificacion_med, estado_activo FROM empleados_2019"""
SQL_RIPS = f"""SELECT 
                    fecha_capita, identificacion_pac, nombre_ips, sexo, edad_anos, identificacion_med, 
                    nombres_med, dx_principal, nombre_dx_principal, tipos_consulta
                FROM rips_auditoria_poblacion_2 
                WHERE (YEAR(fecha_capita) >= 2021) 
                    AND (tipos_consulta IN ('CONSULTA MEDICINA GENERAL', 'CONSULTA NO PROGRAMADA'))
                    AND (nombre_ips IN ('NORTE', 'CENTRO', 'AVENIDA ORIENTAL', 'CALASANZ', 'PAC'))
                ORDER BY fecha_capita;
            """
SQL_CIEDIEZ = """
                 SELECT *
                 FROM analitica.cie10
                 """

def validate_load(df_validate_load,df_load,tabla_bigquery,table_mariadb):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar mariadb
            func_process.save_df_server(df_load, table_mariadb, 'analitica')
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,tabla_bigquery)
    except ValueError as err:
        print(err)

def convert_columns_number(df):
    df.replace('nul','0',inplace=True)
    df.edad_anos.fillna(0, inplace=True)
    df.edad_anos = df.edad_anos.astype(int)
    return df

def convert_columns_date(df):
    df.fecha_capita = pd.to_datetime(df.fecha_capita, errors='coerce')
    return df

# Read data
rips = func_process.load_df_server(SQL_RIPS, 'analitica')
df_cie10 = func_process.load_df_server(SQL_CIEDIEZ, 'analitica')
df_empleados = func_process.load_df_server(SQL_EMPLEADOS, 'reportes')

# Transform
rips['fecha_capita'] = func_process.pd.to_datetime(rips['fecha_capita'])
rips_cie10 = rips.merge(df_empleados, 
                        on='identificacion_med', 
                        how='left').merge(df_cie10, 
                                          left_on='dx_principal', 
                                          right_on='cod_4', 
                                          how='left')
rips_cie10 = rips_cie10[rips_cie10.fecha_capita==fecha_capita]
rips_cie10 = convert_columns_date(rips_cie10)
rips_cie10 = convert_columns_number(rips_cie10)

# VALIDATE LOAD
validate_loads_logs_ciediez =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_CIEDIEZ)

# Load
validate_load(validate_loads_logs_ciediez,rips_cie10,TABLA_BIGQUERY_CIEDIEZ,table_maridb_ciediez)

