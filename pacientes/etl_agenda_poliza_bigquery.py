import pandas as pd
import sys
import os
import calendar
import locale
from datetime import datetime
from dotenv import load_dotenv 

load_dotenv()

PATH_DRIVE = os.environ.get("PATH_DRIVE")
PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)

sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

# Establecer el idioma local a español
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

# VARIABLES DATE
fecha_execution = datetime.now()
fecha_cargue = fecha_execution.date()
number_month_capita = fecha_execution.month
year_capita = fecha_execution.year
name_month_capita = calendar.month_name[number_month_capita].capitalize()

COLUMNS_REQUIRED = ['fecha_atencion','hora_atencion','nombre_ips','nombre_profesional','identificacion_paciente',
                    'nombre_paciente','asistida','atendida_ipsa'  ]

PATH_FILE = f'{PATH_DRIVE}/Agenda_Poliza/'
NAME_FILE = name_month_capita+' '+str(year_capita)+'.xlsx'
SQL_BIGQUERY =  """
                SELECT concat(g.identificacion_paciente,'-',g.fecha_atencion,'-',g.hora_atencion) as column_validator
                FROM {} as g
                WHERE concat(g.identificacion_paciente,'-',g.fecha_atencion,'-',g.hora_atencion) IN {}
                """
project_id_product = 'ia-bigquery-397516'
dataset_id_pacientes = 'pacientes'
table_name_agendas = 'agendas_polizas'
validator_column = 'column_validator'    

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_pacientes}.{table_name_agendas}'


def split_identificacion_tipo(df):
    df['tipo_identificacion_paciente'] =[identificacion.split(' ')[0] for identificacion in df['identificacion_paciente']]
    df['identificacion_paciente'] =[identificacion.split(' ')[1] for identificacion in df['identificacion_paciente']]
    return df

def create_columns_validator(df):
    df['column_validator'] = df.identificacion_paciente.astype(str)+'-'+df.fecha_atencion.dt.date.astype(str)+'-'+df.hora_atencion.astype(str)
    return df

def convert_columns(df):
    df['fecha_atencion'] = pd.to_datetime(df['fecha_atencion'], errors='coerce')
    df['hora_atencion'] = df['hora_atencion'].astype(str)
    return df

def validate_load(df_load):
    try:
        total_cargues = df_load.totalCargues[0]
        if total_cargues==0:
            # Leer datos
            df_agenda_poliza = pd.read_excel(PATH_FILE+NAME_FILE)
            df_agenda_poliza.columns = COLUMNS_REQUIRED
            #df_agenda_poliza = df_agenda_poliza[df_agenda_poliza.fecha_atencion == pd.to_datetime(fecha_cargue)]
            df_agenda_poliza = convert_columns(df_agenda_poliza)
            df_agenda_poliza = split_identificacion_tipo(df_agenda_poliza)
            df_agenda_poliza = create_columns_validator(df_agenda_poliza)
            # Obtener datos no duplicados
            valores_unicos = tuple(map(str, df_agenda_poliza[validator_column]))
            df_agendas_not_duplicates = loadbq.rows_not_duplicates(df_agenda_poliza,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos) 
            df_agendas_not_duplicates.drop('column_validator',axis=1, inplace=True)
            
            # Load bigquery
            loadbq.load_data_bigquery(df_agendas_not_duplicates,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

# Cargar a bigquery
validate_loads_logs =  loadbq.validate_loads_daily(TABLA_BIGQUERY)
validate_load(validate_loads_logs)
