import sys,os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import time
import requests
import os
import locale
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
#end_date = (today.date()+timedelta(days=1))
#start_date = (today.date()-timedelta(days=1))
end_date = '2024-09-14'
start_date = '2024-09-12'
fecha_capita =today.strftime('%Y-%m-15')
month_name = today.strftime('%B')
year_capita = today.strftime('%Y')
month_number = today.strftime('%m')

# SQL
SQL_EMAILS_DETECTED = f"""
                    SELECT md.idAsunto,ma.nombreAsunto
                    FROM apigmail.mensajesdetectados AS md
                    JOIN apigmail.maestraasuntos AS ma ON ma.idAsunto = md.idAsunto
                            AND ma.activo = 1
                    WHERE YEAR(md.fechaCreacion) = {year_capita}
                    AND MONTH(md.fechaCreacion) = {month_number};
                    """
SQL_MESSAGE_READ = """ SELECT cc.id,cc.threadId
                FROM apigmail.mensajesleidos AS cc
                WHERE date(cc.fechaCreacion) >= date_sub(CURDATE(), INTERVAL 2 DAY)
                """

SQL_MAESTRA_ASUNTOS = """SELECT ma.idAsunto,ma.nombreAsunto 
                        FROM apigmail.maestraasuntos AS ma
                        WHERE ma.activo = 1"""

# Diccionario con parametros vacios para completar en el flujo
parameters_email_capita = {
            "name_subject":'',
            "message":''
        }

def read_email_detected():
    try:
        df_email_detected = func_process.load_df_server(SQL_EMAILS_DETECTED,'apiGmail',server_to_connect='local')
        return df_email_detected   
    except Exception as err:
        print(err)

def get_message_email(start_date,end_date):
    try:
        response = requests.get(f'{PATH_API}/email/range-date?start_date={start_date}&end_date={end_date}')
        message_email= response.json()  
        return message_email['results']
    except ValueError as err:
        print(err)    


def get_id_message_subject(parameters_email_subject,message_email,name_subject):
    try:
        list_id_capita = []
        if len(message_email)>0:
            parameters_email_capita['message'] = message_email
            parameters_email_capita['name_subject'] = name_subject
            response = requests.post(f'{PATH_API}/email/subject',json=parameters_email_subject)
            json_id_message = response.json()
            list_id_capita = json_id_message['result']
        return list_id_capita
    except ValueError as err:
        print(err)    
    
        
def get_message_read():
    try:
        df_message_read = pd.DataFrame(columns=['id','threadId'])
        df_message_read = func_process.load_df_server(SQL_MESSAGE_READ,'apiGmail',server_to_connect='local') # Read bd message_read
        return df_message_read
    except Exception as err:
        print(err)

def save_message_new(df_message_new):
    try:
        func_process.save_df_server(df_message_new,'mensajesLeidos','apiGmail',server_to_connect='local') # Save bd message_read
    except Exception as err:
        print(err)


def get_unchecked_messages(message_email):
    try:
        dict_unchecked_messages = {'id':'','threadId':''}
        if len(message_email)>0:
            df_message_new = pd.DataFrame(message_email)
            df_message_read = get_message_read()
            df_unchecked_messages = df_message_new[~df_message_new.id.isin(df_message_read.id.to_list())]
            dict_unchecked_messages = df_unchecked_messages.to_dict(orient='records')
            save_message_new(df_unchecked_messages)
        return dict_unchecked_messages
    except Exception as err:
        print(err)

def read_maestra_asunto():
    try:
        df_asuntos = func_process.load_df_server(SQL_MAESTRA_ASUNTOS,'apiGmail',server_to_connect='local')
        if df_asuntos.shape[0]>0:
            return df_asuntos
    except Exception as err:
        print(err)

def validate_message_subject(df_asuntos,parameters_email_capita,dict_unchecked_messages):
    try:
        list_subject = []
        for subject in df_asuntos.nombreAsunto:
            name_subject = f'{subject} {month_name} {year_capita}'
            list_id_email = get_id_message_subject(parameters_email_capita,dict_unchecked_messages,name_subject)
            id_master_subject = df_asuntos[df_asuntos.nombreAsunto == subject]['idAsunto'].values[0]
            list_master_subject = [id_master_subject,name_subject]
            list_subject.append(list_master_subject+[list_id_email])
            time.sleep(3)
        list_subject_detected = list_subject[0]
        df_message_detected = pd.DataFrame({'idAsunto':list_subject_detected[0],'idMensajeLeido':list_subject_detected[2]})
        return df_message_detected
    except Exception as err:
        print(err)
        
def load_message_detected(df_message_detected):
    try:
        if df_message_detected.shape[0]>0:
            func_process.save_df_server(df_message_detected,'mensajesDetectados','apiGmail',server_to_connect='local')
    except Exception as err:
        print(err)

def execution_detect_emails():
    try:
        df_email_detected = read_email_detected()
        total_email_detected = len(df_email_detected.idAsunto.unique())
        df_asuntos = read_maestra_asunto()
        if total_email_detected < df_asuntos.shape[0]:
            df_subject_no_detected = df_asuntos[~df_asuntos.idAsunto.isin(df_email_detected.idAsunto.unique())]
            message_email_result = get_message_email(start_date,end_date)
            dict_unchecked_messages = get_unchecked_messages(message_email_result)
            df_message_detected = validate_message_subject(df_subject_no_detected,parameters_email_capita,dict_unchecked_messages)
            load_message_detected(df_message_detected)
    except Exception as err:
        print(err)
