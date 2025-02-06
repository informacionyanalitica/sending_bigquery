import pandas as pd
import sys,os
from datetime import datetime,timedelta

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

today = datetime.now()
date_load = today.date()

SQL_AUTORIZACIONES_BD = f"""SELECT *
                FROM reportes.autorizacionesView as f
                where date(f.fechaImpresion) 
                    BETWEEN DATE_SUB('{date_load}', INTERVAL WEEKDAY('{date_load}') + 8 DAY)
                  AND DATE_ADD(DATE_SUB('{date_load}', INTERVAL WEEKDAY('{date_load}') + 8 DAY), INTERVAL 7 DAY);
                """
                
SQL_BIGQUERY = """
                SELECT concat(cc.numeroAutorizacion,'-',cc.codigoSuracupsPrestacion) as validator_column
                FROM {} as cc
                WHERE date(cc.fechaImpresion) >= DATE_SUB('{}', INTERVAL 1 MONTH)
                """

project_id_product = 'ia-bigquery-397516'
dataset_id_autorizaciones = 'autorizaciones'
table_name_historico = 'historicos'
VALIDATOR_COLUMN = 'validator_column'


TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_autorizaciones}.{table_name_historico}'
 
 # convertir fechas
def convert_date(df):
    df.fechaImpresion = pd.to_datetime(df.fechaImpresion, errors='coerce')
    df.fechaVencimientoAutorizacion = pd.to_datetime(df.fechaVencimientoAutorizacion, errors='coerce')
    return df

def convert_number(df):   
    df.cantidadPrestacion.fillna(0,inplace=True)
    df.id_codigoTipoCobro.fillna(0,inplace=True)
    df.cantidadPrestacion = df.cantidadPrestacion.astype(int)
    df.id_codigoTipoCobro = df.id_codigoTipoCobro.astype(int)
    return df

# Leer datos
df_autorizaciones_bd = func_process.load_df_server(SQL_AUTORIZACIONES_BD, 'reportes')   

# Transform columns 
df_autorizaciones_bd = convert_date(df_autorizaciones_bd)
df_autorizaciones_bd = convert_number(df_autorizaciones_bd)

def get_rows_not_duplicate(df_autorizaciones):
    try:
        # Obtener datos no duplicados
        df_autorizaciones[VALIDATOR_COLUMN] = df_autorizaciones.numeroAutorizacion.astype(str)+'-'+df_autorizaciones.codigoSuracupsPrestacion.astype(str)
        df_pagos_not_duplicates = loadbq.rows_duplicates_last_month(df_autorizaciones,VALIDATOR_COLUMN,SQL_BIGQUERY,TABLA_BIGQUERY)
        return df_pagos_not_duplicates
    except Exception as er:
        print(er)

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)


# Save data
df_validate_loads_logs =  loadbq.validate_loads_weekly(TABLA_BIGQUERY)
validate_load(df_validate_loads_logs,df_autorizaciones_bd)

