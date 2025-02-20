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

# DATE
today = datetime.now()
date_load = today.date()
date_capita = date_load.strftime('%Y-%m-15')
year_load =  date_load.strftime('%Y')
month_load =  date_load.strftime('%m')


# ID PROJECT BIGQUERY
project_id_product = 'ia-bigquery-397516'
table_name_capita_historico = 'capita_historico'
dataset_id_pacientes = 'pacientes'
# ID BIGQUERY
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_pacientes}.{table_name_capita_historico}'
# VARIABLES
PATH_FILES = f'{PATH_DRIVE}/Migracion Dropbox/CAPITA-MARCAS/'
NAME_FILES = 'BD_Poblacion_'
DICC_RENAME_COLUMNS = {
            'Tipo ID':'tipo_documento',
            'Numero ID':'numero_documento',
            'Primer apellido':'primer_apellido',
            'Segundo Apellido':'segundo_apellido',
            'Nombre':'nombre',
            'IPS':'ips',
            'Nombre IPS':'nombre_ips',
            'Genero':'genero',
            'Fecha nacimiento':'fecha_nacimiento',
            'Edad':'edad',
            'Cronicos':'cronicos',
            'RCV':'rcv',
            'Hipertension':'hipertension',
            'Diabetes':'diabetes',
            'Proteccion Renal':'proteccion_renal',
            'Dislipidemia':'dislipidemia',
            'Enf. Autoinmunes':'enf_autoinmunes',
            'Enf. Coagulacion':'enf_coagulacion',
            'Asma':'asma',
            'Epoc':'epoc',
            'Cancer de cervix':'cancer_cervix',
            'Cancer de mama':'cancer_mama',
            'VIH':'vih',
            'CPR':'cpr',
            'RCE':'rce',
            'Fragil Canguro':'fragil_canguro',
            'Oxigeno dependiente':'oxigeno_dependiente',
            'Sospecha abuso sexual':'sospecha_abuso_sexual',
            'TB':'tb',
            'Regimen':'regimen'
            }

COLUMNS_DROP = ['Tipo edad',
       'Documento medico', 'Medico de familia', 'Telefono 1', 'Telefono 2',
       'Telefono 3']
       
LIST_COLUMNS_REQUIRED = [
    'Tipo ID',
    'Numero ID',
    'Primer apellido',
    'Segundo Apellido',
    'Nombre',
    'IPS',
    'Nombre IPS',
    'Genero',
    'Fecha nacimiento',
    'Edad',
    'Cronicos',
    'RCV','Hipertension',
    'Diabetes',
    'Proteccion Renal','Dislipidemia',
    'Enf. Autoinmunes','Enf. Coagulacion',
    'Asma','Epoc',
    'Cancer de cervix',
    'Cancer de mama',
    'VIH',
    'CPR',
    'RCE',
    'Fragil Canguro',
    'Oxigeno dependiente',
    'Sospecha abuso sexual',
    'TB',
    'Regimen']

def validation_columns(df):
  for col in LIST_COLUMNS_REQUIRED:
    if col not in  df.columns:
      df[col] = 0
  df = df[LIST_COLUMNS_REQUIRED]
  return df


def rename_column(df):
  df.rename(DICC_RENAME_COLUMNS, inplace=True,axis=1)
  return df

def convert_column_string(df):
  COLUMNS_STRING = ['IPS','Cronicos','RCV','Hipertension','Diabetes','Proteccion Renal','Dislipidemia','Enf. Autoinmunes','Enf. Coagulacion',
          'Asma','Epoc','Cancer de cervix','Cancer de mama','VIH','CPR','RCE','Fragil Canguro',
      'Oxigeno dependiente','Sospecha abuso sexual','TB','Regimen']
  for col in COLUMNS_STRING:
    df[col] = df[col].astype(str)
  return df

def convert_date(df):
  df['Fecha nacimiento'] = pd.to_datetime(df['Fecha nacimiento'], errors='coerce')
  return df

def read_capita(anio,mes):
  try:
    df_capita = pd.read_excel(PATH_FILES+anio+'/'+NAME_FILES+mes+anio+'.xlsx', 
                              sheet_name='POBLACION_DETALLADA'                             
                                )
    return df_capita
  except Exception as err:
    print(err)

def load_bigquery(project_id,dataset_id,table_name_rips,df):
    bq_cloud = loadbq.CloudBigQuery(project_id, dataset_id, table_name_rips)
    bq_cloud.write_to_table(df)

def convert_string_force(df):    
    LIST_COLUMNS_STRING = df.select_dtypes(include=['object']).columns
    for col in LIST_COLUMNS_STRING:
        df[col] = df[col].astype(str)
    return df

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:           
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

df_file = read_capita(year_load,month_load)
df_capita_validation = validation_columns(df_file)
df_capita_convert = convert_column_string(df_capita_validation)
df_capita_convert = convert_date(df_capita_convert)
df_capita_rename = rename_column(df_capita_convert)
df_capita_rename['fecha_capita'] = pd.to_datetime(date_capita)
df_capita_rename = convert_string_force(df_capita_rename)

# Save data

df_validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)
validate_load(df_validate_loads_logs,df_capita_rename)
