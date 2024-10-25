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
print(PATH_TOOLS)
PATH_ETL = os.environ.get("PATH_ETL")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq
import extract_file_gdrive as filesGD

# LOCALE
locale.setlocale(locale.LC_TIME, "es_ES.utf8")  

today = datetime.now()
date_load = (today - timedelta(days=1))
date_execution = date_load.date()
MONTH_NAME = date_load.strftime('%B').capitalize()
PATH_GESTION_CLINICA = {"path_folder":"Gestión Clínica"} 
PATH_FILE_SAVE = f'{PATH_ETL}/etl/files/ayudas_diagnosticas/{MONTH_NAME}/'
pathology = sys.argv[1]

# SQL
SQL_VALIDATE_LOAD = """SELECT COUNT(*) AS totalCargues
                    FROM reportes.logsCarguesBigquery AS lg
                    WHERE lg.idBigquery = '{}'
                    AND DATE(lg.fechaCargue) = '{}'"""

# Name project BQ
project_id_product = 'ia-bigquery-397516'
# DATASET AYUDAS DIAGNOSTICAS
dataset_id_ayudas_diagnosticas = 'ayudas_diagnosticas'

# TABLAS
table_name_laboratorio_clinico = 'laboratorio_clinico_partition'

# ID BIGQUERY
TABLA_BIGQUERY_LABORATORIO_CLINICO = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_laboratorio_clinico}'
# ia-bigquery-397516.ayudas_diagnosticas.laboratorio_clinico_partition

RENAME_COLUMN_REQUIRED = ['identificacion_paciente','nombre_paciente','apellido_paciente','celular','email','orden_cltech','nombre_prueba','resultado',
                          'refmin','refmax','fecha_validacion','fecha_ingreso','orden_sura','identificacion_profesional',
                          'nombre_profesional','cargo_profesional','sede_profesional','email_profesional','rol'
                          ]
COLUMNS_REQUIRED = ['HISTORIA','nombre_paciente','apellido_paciente','celular','email','_order', 'nombre_prueba', 'result', 'refmin', 'refmax','fechaValidacion',
                        'entryDate', 'autorizacionSura','C_MEDICO', 'MEDICO', 'cargo_gestal', 'SEDE_MEDICO','email_medico','rol']

# BIGQUERY
project_id_product = '`ia-bigquery-397516'
dataset_ayudas_diagnosticas = 'ayudas_diagnosticas' 
tabla_name_laboratorio_view = 'laboratorio_bd_view`'
TABLA_BIGQUERY_LABORATORIO_VIEW = f'{project_id_product}.{dataset_ayudas_diagnosticas}.{tabla_name_laboratorio_view}' 

# SQL
SQL_PERFILES_LABORATORIO =  f"""
                SELECT sl._order,sl.nameSueltos AS nombre_prueba,sl.resultSueltos AS result,sl.refminSueltos AS refmin,sl.refmaxSueltos AS refmax,
                sl.pathologySueltos AS pathology,sl.fechaValidacionSueltos AS fechaValidacion,sl.patientId,sl.entryDate,
                sl.autorizacionSura,sl.name as nombre_paciente,sl.lastName as apellido_paciente
                FROM reportes.examenesLaboratorioView AS sl
                WHERE sl.pathologySueltos = '{pathology}'
                and DATE(sl.fechaValidacionSueltos) = '{date_execution}'
                AND sl.resultSueltos != 'MEMO' 
                AND sl.refminSueltos != 'undefined'
                UNION 
                SELECT pl._order,pl.name AS nombre_prueba,pl.result AS result,pl.refmin AS refmin,pl.refmax AS refmax,
	                pl.pathology AS pathology,pl.fechaValidacion AS fechaValidacion,pl.patientId,pl.entryDate,
	                pl.autorizacionSura,pl.namePatient as nombre_paciente,pl.lastaNamePatient as apellido_paciente
                FROM reportes.perfilesExamenesView AS pl
                WHERE pl.pathology = '{pathology}'
                and DATE(pl.fechaValidacion) = '{date_execution}'
                AND pl.result != 'MEMO' 
                AND pl.refmin != 'undefined';
                
                """
SQL_LABORATORIO = f"""SELECT distinct lb.ORDEN_SEDE,lb.HISTORIA,lb.NOMBRE,lb.C_MEDICO,lb.MEDICO,lb.cargo_gestal,lb.SEDE_MEDICO,
                        cp.celular,UPPER(cp.email) AS email,ea.email as email_medico,lb.rol
                    FROM `ia-bigquery-397516.ayudas_diagnosticas.laboratorio_bd_view` as lb
                    JOIN `ia-bigquery-397516.pacientes.capita` as cp on cp.identificacion_paciente = lb.HISTORIA
                    LEFT JOIN `ia-bigquery-397516.empleados.activos` as ea on ea.identificacion = lb.C_MEDICO
                    WHERE DATE(lb.FECHA) >= DATE_SUB('{date_execution}', INTERVAL 1 MONTH) 
                    AND lb.sede_medico != 'EXTERNO'
                    AND lb.ORDEN_SEDE != '1'"""

# FUNCTION
def validate_loads_daily():
    try:
        df_load_daily = func_process.load_df_server(SQL_VALIDATE_LOAD.format(TABLA_BIGQUERY_LABORATORIO_CLINICO,today.date()),'reportes')
        return df_load_daily
    except Exception as err:
        print(err)

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

def generate_file_name(pathology):
    name_file = 'Gestión Clínica '+str(date_load.date())+ ' Normales.xlsx'
    if pathology =='true':
        name_file = 'Gestión Clínica '+str(date_execution)+'.xlsx'        
    return name_file


def validate_save_file(df_gestion_medica,df_validate_load,pathology):
    try:
        validate_folder()
        totalCargues =df_validate_load.totalCargues[0]
        name_file = generate_file_name(pathology)
        if df_gestion_medica.shape[0] >0 and totalCargues>0:
            df_gestion_medica.to_excel(PATH_FILE_SAVE+name_file, index=False) 
        pattern_files = os.path.join(PATH_FILE_SAVE,name_file)
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


# VALIDATE LOAD LABORATORIO CLINICO
validate_loads_logs =  validate_loads_daily()
df_laboratorio_sin_duplicados,df_perfiles = read_dataset()
df_gestion_medica = get_merge(df_perfiles,df_laboratorio_sin_duplicados)
validate_save_file(df_gestion_medica,validate_loads_logs,pathology)
