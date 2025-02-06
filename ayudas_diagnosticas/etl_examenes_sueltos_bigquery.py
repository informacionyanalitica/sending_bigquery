import pandas as pd
import sys
import os
from datetime import datetime,timedelta
import time
import numpy as np
from unicodedata import decimal
import extract_alergenos
from dotenv import load_dotenv
import locale
# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq
import cumplimientos_pyg as pyg
from convert_columns_dataframe import convertColumnDataFrame

locale.setlocale(locale.LC_TIME, "es_ES.utf8") 

# Fechas
today = datetime.now()
date_load = today.date()

# Instancias clase converColumnDatafrane
convert_columns = convertColumnDataFrame()
# Name project BQ
project_id_product = 'ia-bigquery-397516'
# DATASET AYUDAS DIAGNOSTICAS
dataset_id_ayudas_diagnosticas = 'ayudas_diagnosticas'
# TABLAS
table_name_examenes_sueltos = 'examenes_sueltos'
# ID BIGQUERY
TABLA_BIGQUERY_SUELTOS = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_examenes_sueltos}'

SQL_EXAMENES_SUELTOS = f""" SELECT r.id_result,r._order,r.orderType,r.patientId,r.name,r.lastName,r.dob,
                            r.gender,r.diagnostic,r.commentResult AS comment,r.entryDate,r.tipoDocumento,r.telefono,
                            r.eps,r.sede,r.empresa,r.medico,r.servicio,r.impresionDiagnostica,r.fecha_insercion,r.autorizacionSura,
                            r.nameSueltos AS nameSuelto, r.resultSueltos AS result,r.refminSueltos AS refmin,r.refmaxSueltos AS refmax,
                            r.fechaValidacionSueltos AS fechaValidacion,r.responsableSueltos AS responsable,r.tecnicSueltos AS tecnic,
                            r.areaSueltos AS area,r.commentSueltos as commentSuelto,r.pathologySueltos AS pathology,r.unitSueltos AS unit
                            FROM reportes.examenesLaboratorioView AS r
                            WHERE date(r.fecha_insercion)  BETWEEN DATE_SUB('{date_load}', INTERVAL WEEKDAY('{date_load}') + 8 DAY)
                                AND DATE_ADD(DATE_SUB('{date_load}', INTERVAL WEEKDAY('{date_load}') + 8 DAY), INTERVAL 7 DAY);
                            """
LIST_COLUMNS_DATE = ['dob','entryDate','fecha_insercion','fechaValidacion' ]
LIST_COLUMNS_STRING = ['id_result','_order','orderType','patientId','name','lastName','gender','diagnostic',
                       'comment','tipoDocumento','telefono','eps','sede','empresa','medico',
                       'servicio','impresionDiagnostica','autorizacionSura','nameSuelto','result','refmin',
                       'refmax','responsable','tecnic','area','commentSuelto','pathology','unit'
                       ]

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY_SUELTOS)
    except ValueError as err:
        print(err)

df_examenes_sueltos =func_process.load_df_server(SQL_EXAMENES_SUELTOS,'reportes')

# Convertir columnas string
df_examenes_sueltos = convert_columns.convert_columns_string(df_examenes_sueltos,LIST_COLUMNS_STRING)
# Convertir columnas date
df_examenes_sueltos = convert_columns.convert_columns_date(df_examenes_sueltos,LIST_COLUMNS_DATE)

# Save data
df_validate_loads_logs =  loadbq.validate_loads_weekly(TABLA_BIGQUERY_SUELTOS)
validate_load(df_validate_loads_logs,df_examenes_sueltos)
