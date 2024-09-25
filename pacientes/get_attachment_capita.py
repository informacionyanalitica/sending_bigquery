import sys,os
import pandas as pd
from datetime import datetime,timedelta
from dotenv import load_dotenv
import time
import requests
import zipfile
import os
from pathlib import Path
import locale
import glob
# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
PATH_DRIVE = os.environ.get("PATH_DRIVE")
PATH_ETL = os.environ.get("PATH_ETL")
PATH_API = os.environ.get("PATH_API")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

os.environ['LANG'] = 'es_ES.UTF-8'
os.environ['LC_ALL'] = 'es_ES.UTF-8'
locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
print(f"Locale configurado: {locale.getlocale()}")

today = datetime.now()
end_date = (today.date()+timedelta(days=1))
start_date = (today.date()-timedelta(days=1))
fecha_capita =today.strftime('%Y-%m-15')
month_name = today.strftime('%B')
year_capita = today.strftime('%Y')
month_number = today.strftime('%m')
name_subject = 'Fwd: Capitación'
name_subject_dynamic = f'{name_subject} {month_name} {year_capita}'
path_download_drive = f'{PATH_DRIVE}/BASES DE DATOS'
path_download_dynamic = f'{year_capita}/{int(month_number)}. {month_name.capitalize()}/CAPITA EPS SURA/'
path_download_api = f'/root/google-drive/BASES DE DATOS/{path_download_dynamic}'
NAME_ATTACHMENT = "4_800168083_UNICOPOS"
PATH_MESSAGE_READ = f'{PATH_ETL}/files/pacientes/'
NAME_MESSAGE_READ = 'message_read.csv'
# Bigquery
project_id_product = 'ia-bigquery-397516'

# DATASET AYUDAS DIAGNOSTICAS
dataset_id_pacientes = 'pacientes'
# TABLAS
table_name_capita_poblaciones = 'capitas_poblaciones'
TABLA_BIGQUERY_CAPITA_POBLACIONES = f'{project_id_product}.{dataset_id_pacientes}.{table_name_capita_poblaciones}'

# SQL
SQL_ID_SUBJECT = """SELECT  cc.nombreAsunto
                    JOIN apigmail.maestraasuntos AS ms ON ms.idAsunto = cc.idAsunto
                    WHERE ms.nombreAsunto = '{}'
                    
                    """

# Diccionario con parametros vacios para completar en el flujo
parameters_email_capita = {
            "name_subject":name_subject_dynamic,
            "message":''
        }
parameters_download_attachment =  {
    "destination_folder":'',
    "id_message":'',
    "name_attachment":NAME_ATTACHMENT
        }

def validate_load_capita():
    try:
        df_load_capita =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_CAPITA_POBLACIONES)
        totalCargues = df_load_capita.totalCargues[0]
        return totalCargues            
    except Exception as err:
        print(err)


def download_attachment(parameters_download_attachment):
    try:
        response = requests.post(f'{PATH_API}/email/download_attachment',json=parameters_download_attachment)
        path_file = response.json()
        return path_file['result']
    except ValueError as err:
        print(err)   


def unzip_zip(name_file, path_destination: str):
    name_file_zip = path_destination+'/'+name_file
    file_zip = Path(name_file_zip)
    path_destination = Path(path_destination)   
    if not file_zip.exists():
        raise FileNotFoundError(f"El archivo {file_zip} no existe.")
    # Asegurarse de que el directorio de destino exista
    path_destination.mkdir(parents=True, exist_ok=True)
    try:
        # Descomprimir el archivo zip
        with zipfile.ZipFile(file_zip, 'r') as zip_ref:
            zip_ref.extractall(path_destination)
        print(f'Archivo descomprimido exitosamente en: {path_destination}')
    except zipfile.BadZipFile:
        raise zipfile.BadZipFile(f"El archivo {file_zip} no es un archivo .zip válido.")
    
def validate_exist_path(path_folder,path_dynamic):
    list_path_folder = path_dynamic.split('/')
    try:
        for element in list_path_folder:
            path_validate = path_folder+'/'+element
            pattern = os.path.join(path_validate)
            folder_exists = glob.glob(pattern)
            if not folder_exists:
                os.mkdir(path_validate)
                path_folder =  path_validate
            else:
                path_folder =  path_validate
                continue
        path_download_validated = path_validate
        return path_download_validated
    except Exception as err:
        print(err)
        
def send_id_attachment(parameters_download_attachment,path_download_capita,list_id_email,path_download_api):
    try:
        parameters_download_attachment['destination_folder'] = path_download_api
        if len(list_id_email)>0:   
            for id_email in list_id_email:
                parameters_download_attachment['id_message'] = id_email
                time.sleep(3)
                filname = download_attachment(parameters_download_attachment)
                time.sleep(2)
                unzip_zip(filname,path_download_capita)
            return True
        else:
            return None
    except Exception as err:
            print(err)

def get_id_message_detected(name_subject):
    try:
        df_message_detected = pd.DataFrame()
        df_message_detected = func_process.load_df_server(SQL_ID_SUBJECT.format(name_subject),'api_gmail',server_to_connect='local') 
        if df_message_detected.shape[0]>0:
            list_id_email = df_message_detected.nombreAsunto.to_list()
            return list_id_email
    except Exception as err:
        print(err)

def check_email_capita():
    exists_email = None
    try:
        total_registros = validate_load_capita()
        if total_registros==0:
            list_id_email = get_id_message_detected(name_subject)
            path_download_validated = validate_exist_path(path_download_drive,path_download_dynamic)
            exists_email = send_id_attachment(parameters_download_attachment,path_download_validated,list_id_email,path_download_api)
        return exists_email
    except Exception as err:
        print(err)

   
