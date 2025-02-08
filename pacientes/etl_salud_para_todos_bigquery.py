import pandas as pd
import sys,os
from datetime import datetime
import locale
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()
# Establecer Lenguaje y codificación 
locale.setlocale(locale.LC_TIME, "es_ES.utf8") 

PATH_DRIVE = os.environ.get("PATH_DRIVE")
PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame


today = datetime.now()
date_load = today.date()
name_month_load = date_load.strftime('%B').capitalize()
year_load = date_load.strftime('%Y')

# Instancias clase converColumnDatafrane
convert_columns = convertColumnDataFrame()

PATH_FILE = f'{PATH_DRIVE}/Agenda_Poliza/'
name_file = f'{name_month_load} {year_load}.xlsx'

# Project bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id = 'pacientes'
table_name = 'salud_para_todos'
# ID BIGQUERY
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id}.{table_name}'
# CONSTANT
COLUMNS_REQUIRED = ['fecha_atencion','hora_atencion','nombre_ips','nombre_profesional','tipo_documento',
                    'documento_identificacion','nombre_paciente','asistida','asistida_uno','prestacion',
                    'autorizacion','consecutivo_autorizacion','cups_autorizacion']
COLUMNS_RENAMED= ['fecha_atencion','hora_atencion','nombre_ips','nombre_profesional','tipo_documento',
                    'documento_identificacion','nombre_paciente','asistida','asistida_uno',
                    'autorizacion','novedades','consecutivo_autorizacion','prestacion','cups_autorizacion']
LIST_COLUMNS_STRING = ['documento_identificacion','cups_autorizacion','hora_atencion','nombre_ips',
                       'nombre_profesional','tipo_documento','nombre_paciente','asistida','asistida_uno',
                       'prestacion','autorizacion','consecutivo_autorizacion'
                       ]
LIST_COLUMNS_INTEGER = ['consecutivo_autorizacion','cups_autorizacion']
LIST_COLUMNS_DATE = ['fecha_atencion']

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

df_salud_para_todos = pd.read_excel(PATH_FILE+name_file, sheet_name='Salud para todos')
df_salud_para_todos.columns = COLUMNS_RENAMED
# Convertir columnas int
df_salud_para_todos = convert_columns.convert_columns_integer(df_salud_para_todos,LIST_COLUMNS_INTEGER)
# Convertir columnas string
df_salud_para_todos = convert_columns.convert_columns_string(df_salud_para_todos,LIST_COLUMNS_STRING)
# Convertir columnas date
df_salud_para_todos = convert_columns.convert_columns_date(df_salud_para_todos,LIST_COLUMNS_DATE)
# Column required
df_salud_para_todos = df_salud_para_todos[COLUMNS_REQUIRED]
# Save data
df_validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)
validate_load(df_validate_loads_logs,df_salud_para_todos)