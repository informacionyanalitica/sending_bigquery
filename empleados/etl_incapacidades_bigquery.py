import pandas as pd
import sys
import os
from datetime import datetime,timedelta
import calendar

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

# GENERAMOS UNA FECHA PARA QUE EL INFORME TOME LOS DATOS DEL MES A PROCESAR
today = func_process.pd.to_datetime(datetime.now() - timedelta(days=15))
# Encuentra el último día del mes
ultimo_dia_del_mes = calendar.monthrange(today.year, today.month)[1]
fecha_inicial = today.strftime('%Y-%m-01')
fecha_final = today.strftime(f'%Y-%m-{ultimo_dia_del_mes}')

SQL_INCAPACIDADES_BD = """SELECT *
            FROM reportes.incapacidades_2021
            WHERE fecha_novedad_nomina between '{fecha_inicial}' and '{fecha_final}'
            """
SQL_BIGQUERY = """
                SELECT g.hv_id_incapacidad
                FROM {} as g
                WHERE g.hv_id_incapacidad IN {}
                """
project_id_product = 'ia-bigquery-397516'
dataset_id_empleados = 'empleados'
table_name_incapacidades = 'incapacidades'
validator_column = 'hv_id_incapacidad' 
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_empleados}.{table_name_incapacidades}'

def convert_date_columns(df):
    df.hv_fecha_inicial = pd.to_datetime(df.hv_fecha_inicial, errors='coerce')
    df.hv_fecha_final = pd.to_datetime(df.hv_fecha_final, errors='coerce')
    df.fecha_novedad_nomina = pd.to_datetime(df.fecha_novedad_nomina, errors='coerce')
    return df

def clean_values_na(df):
    df.costo_empresa = [row.replace('.00','') for row in df.costo_empresa]
    df.costo_empresa.replace('NaN','0',inplace=True)
    df.costo_empresa.replace('','0',inplace=True)
    
    df.hv_valor.fillna(0, inplace=True)
    df.costo_empresa.fillna(0, inplace=True)
    df.dias_incapacidad.fillna(0, inplace=True)
    return df
def convert_number_columns(df):
    df.hv_valor = df.hv_valor.astype(int)
    df.costo_empresa = df.costo_empresa.astype(int)
    df.dias_incapacidad = df.dias_incapacidad.astype(int)
    return df

# Leer datos
df_incapacidades_bd = func_process.load_df_server(SQL_INCAPACIDADES_BD.format(fecha_inicial=fecha_inicial,fecha_final=fecha_final), 'reportes')

# Convert columns
df_incapacidades_bd = convert_date_columns(df_incapacidades_bd)
df_incapacidades_bd = clean_values_na(df_incapacidades_bd)
df_incapacidades_bd = convert_number_columns(df_incapacidades_bd)

# Obtener datos no duplicados
valores_unicos = tuple(map(int, df_incapacidades_bd[validator_column]))
df_incapacidades_not_duplicates = loadbq.rows_not_duplicates(df_incapacidades_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if total_cargue == 0:
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

# Validate load
validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)

# Cargar a bigquery
validate_load(validate_loads_logs,df_incapacidades_not_duplicates)

