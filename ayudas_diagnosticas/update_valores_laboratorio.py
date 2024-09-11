import pandas as pd
import sys
import os
from datetime import datetime,timedelta
import time
from unicodedata import decimal
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
PATH_DRIVE =  os.environ.get("PATH_DRIVE")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

# Dates
today = pd.to_datetime("today")
date_final = (today - pd.DateOffset(days=today.weekday() + 1)).date()
date_initial =  (date_final - pd.DateOffset(weeks=1)).date()
# Name project BQ
project_id_product = 'ia-bigquery-397516'
# TABLAS
table_name_valores_tmp = 'update_valores_tmp'
dataset_id_ayudas_diagnosticas = 'ayudas_diagnosticas'
# ID BIGQUERY
TABLA_BIGQUERY_VALORES_TMP = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_valores_tmp}'

SQL_MERGE_VALORES = """
        MERGE `ia-bigquery-397516.ayudas_diagnosticas.laboratorio_clinico_partition` AS T
        USING `ia-bigquery-397516.ayudas_diagnosticas.update_valores_tmp` S
        ON T.ORDEN = S.ORDEN
        AND T.ORDEN_SEDE = S.ORDEN_SEDE
        AND T.CODIGO = S.CODIGO
        WHEN MATCHED THEN
          UPDATE SET T.VALORES = S.VALORES
"""
PATH_DOWNLOAD = f"{PATH_DRIVE}/tarifas laboratorio/Actualizar valores/estadisticas {date_initial}-{date_final}.xlsx"

def transform_df_tmp(df_estadisticas):
    df_estaditica_temporal = df_estadisticas[['ORDEN','ORDEN_SEDE','CODIGO','VALORES']]
    df_estaditica_temporal.ORDEN = df_estaditica_temporal.ORDEN.astype(str)
    return df_estaditica_temporal

def execution_update(df_estaditica_temporal):
    try:
        if df_estaditica_temporal.shape[0]>0:
            loadbq.load_data_bigquery(df_estaditica_temporal,TABLA_BIGQUERY_VALORES_TMP)
            time.sleep(2)
            loadbq.update_data_bigquery(SQL_MERGE_VALORES,TABLA_BIGQUERY_VALORES_TMP)
            time.sleep(2)
            loadbq.delete_table_bigquery(TABLA_BIGQUERY_VALORES_TMP)
    except Exception as err:
        print(err)

df_estadisticas = pd.read_excel(PATH_DOWNLOAD,sep=';')
df_estaditica_temporal = transform_df_tmp(df_estadisticas)
execution_update(df_estaditica_temporal)