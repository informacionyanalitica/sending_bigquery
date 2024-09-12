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
table_name_laboratorio_clinico= 'laboratorio_clinico_partition'
dataset_id_ayudas_diagnosticas = 'ayudas_diagnosticas'
# ID BIGQUERY
TABLA_BIGQUERY_LABORATORIO = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_laboratorio_clinico}'


PATH_SAVE_FILE = f"{PATH_DRIVE}/tarifas laboratorio/Actualizar valores/estadisticas {date_initial}_{date_final}.xlsx"

SQL_LABORATORIO_LAST_WEEK = f"""SELECT *
                            FROM  `ia-bigquery-397516.ayudas_diagnosticas.laboratorio_clinico_partition` as l
                            WHERE  date(l.FECHA) BETWEEN '{date_initial}' AND '{date_final}'"""


def save_file(df_estadisticas):
    try:
        if df_estadisticas.shape[0]>0:
            df_estadisticas.to_excel(PATH_SAVE_FILE,index=False)
            return print(date_initial,',',date_final)
        else:
            raise ValueError(f"El archivo no contiene datos")
    except Exception as err:
        print(err)

def remove_time_zone(df):
    try:
        if df.shape[0]>0:
            df.FECHA = df.FECHA.dt.tz_localize(None)
            df.FECHA_NACIMIENTO = df.FECHA_NACIMIENTO.dt.tz_localize(None)
            return df
        else:
            raise ValueError(f"El archivo no contiene datos")
    except Exception as err:
        print(err)

df_estadisticas = loadbq.read_data_bigquery(SQL_LABORATORIO_LAST_WEEK,TABLA_BIGQUERY_LABORATORIO)
df_estadisticas = remove_time_zone(df_estadisticas)
save_file(df_estadisticas)

