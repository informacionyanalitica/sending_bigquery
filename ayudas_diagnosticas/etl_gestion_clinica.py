import pandas as pd
import sys
import os
from datetime import datetime,timedelta
from unicodedata import decimal
import glob
from dotenv import load_dotenv
import locale 
# Carga el archivo .env
load_dotenv()

PATH_DRIVE = os.environ.get("PATH_DRIVE")
PATH_TOOLS = os.environ.get("PATH_TOOLS")
PATH_ETL = os.environ.get("PATH_ETL")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq
import extract_file_gdrive as filesGD

# LOCALE
locale.setlocale(locale.LC_TIME, "es_ES.utf8")  

date_load = (datetime.now() - timedelta(days=1))
date_execution = date_load.date()
MONTH_NAME = date_load.strftime('%B').capitalize()
PATH_GESTION_CLINICA = {"path_folder":"Gestión Clínica"} 
PATH_FILE_SAVE = f'{PATH_ETL}/etl/files/ayudas_diagnosticas/{MONTH_NAME}/'
NAME_FILE = str(date_execution)+'.xlsx'



RENAME_COLUMN_REQUIRED = ['identificacion_paciente','nombre_paciente','apellido_paciente','celular','email','orden_cltech','nombre_prueba','resultado',
                          'refmin','refmax','fecha_validacion','fecha_ingreso','orden_sura','identificacion_medico',
                          'nombre_medico','cargo_medico','sede_medico','email_medico'
                          ]
COLUMNS_REQUIRED = ['HISTORIA','nombre_paciente','apellido_paciente','celular','email','_order', 'name', 'result', 'refmin', 'refmax','fechaValidacion',
                        'entryDate', 'autorizacionSura','C_MEDICO', 'MEDICO', 'cargo_gestal', 'SEDE_MEDICO','email_medico']

# BIGQUERY
project_id_product = '`ia-bigquery-397516'
dataset_ayudas_diagnosticas = 'ayudas_diagnosticas' 
tabla_name_laboratorio_view = 'laboratorio_bd_view`'
TABLA_BIGQUERY_LABORATORIO_VIEW = f'{project_id_product}.{dataset_ayudas_diagnosticas}.{tabla_name_laboratorio_view}' 

# SQL
SQL_PERFILES_LABORATORIO =  f"""
                SELECT sl._order,sl.name,sl.result,sl.refmin,sl.refmax,sl.pathology,sl.fechaValidacion,
                sl.patientId,sl.entryDate,sl.autorizacionSura,sl.name as nombre_paciente,sl.lastName as apellido_paciente
                FROM reportes.perfilesExamenesView AS sl
                WHERE sl.pathology = 'true'
                and DATE(sl.fechaValidacion) = '{date_execution}'
                AND sl.result != 'MEMO' 
                AND sl.refmin != 'undefined'
                """
SQL_LABORATORIO = f"""SELECT distinct lb.ORDEN_SEDE,lb.HISTORIA,lb.NOMBRE,lb.C_MEDICO,lb.MEDICO,lb.cargo_gestal,lb.SEDE_MEDICO,
                        cp.celular,UPPER(cp.email) AS email,ea.email as email_medico
                    FROM `ia-bigquery-397516.ayudas_diagnosticas.laboratorio_bd_view` as lb
                    JOIN `ia-bigquery-397516.pacientes.capita` as cp on cp.identificacion_paciente = lb.HISTORIA
                    LEFT JOIN `ia-bigquery-397516.empleados.activos` as ea on ea.identificacion = lb.C_MEDICO
                    WHERE DATE(lb.FECHA) >= DATE_SUB('{date_execution}', INTERVAL 1 MONTH) 
                    AND lb.sede_medico != 'EXTERNO'
                    AND lb.ORDEN_SEDE != '1'"""

# FUNCTION
def read_dataset():
    try:
        df_laboratorio_view = loadbq.read_data_bigquery(SQL_LABORATORIO,TABLA_BIGQUERY_LABORATORIO_VIEW)
        df_perfiles = func_process.load_df_server(SQL_PERFILES_LABORATORIO,'reportes')
        df_laboratorio_sin_duplicados = df_laboratorio_view.drop_duplicates(subset=['HISTORIA','ORDEN_SEDE','C_MEDICO'])
        return df_laboratorio_sin_duplicados,df_perfiles
    except Exception as err:
        print(err)

def validate_folder():
    try:
        pattern = os.path.join(PATH_FILE_SAVE)
        folder_exists = glob.glob(pattern)
        if not folder_exists:
            os.mkdir(PATH_FILE_SAVE)
    except Exception as err:
        print(err)


def validate_save_file(df_gestion_medica):
    try:
        validate_folder()
        df_gestion_medica.to_excel(PATH_FILE_SAVE+NAME_FILE, index=False) 
        pattern_files = os.path.join(PATH_FILE_SAVE,NAME_FILE)
        files_exists = glob.glob(pattern_files)
        if not files_exists:
            raise ValueError(f"No se encontraron archivos con el patrón: {pattern_files}")
        else:
            print(MONTH_NAME)
    except Exception as err:
        print(err)
   
    
def get_merge(df_perfiles,df_laboratorio_sin_duplicados):
    try:
        df_perfiles_laboratorio = df_perfiles.merge(df_laboratorio_sin_duplicados, how='inner', left_on='autorizacionSura',right_on='ORDEN_SEDE')
        df_gestion_medica = df_perfiles_laboratorio[COLUMNS_REQUIRED]
        df_gestion_medica.columns = RENAME_COLUMN_REQUIRED
        return df_gestion_medica
    except Exception as err:
        print(err)




df_laboratorio_sin_duplicados,df_perfiles = read_dataset()
df_gestion_medica = get_merge(df_perfiles,df_laboratorio_sin_duplicados)
validate_save_file(df_gestion_medica)