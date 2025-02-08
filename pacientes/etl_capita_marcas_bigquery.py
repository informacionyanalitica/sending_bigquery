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


today= datetime.now()
date_load = today.date()


# ID PROJECT
project_id_product = 'ia-bigquery-397516'
dataset_id_pacientes = 'pacientes'
table_name_capita = 'capita'
# ID BIGQUERY
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_pacientes}.{table_name_capita}'
# NAME MARIADB
NAME_BD_MARIADB = 'reportes'
NAME_TABLE_MARIADB = 'capita'

# VARIABLES
capita_columns= ['tipo_identificacion', 'identificacion_paciente',
       'primer_apellido', 'segundo_apellido', 'primer_nombre',
       'segundo_nombre', 'sexo', 'telefono', 'edad', 'unidad_edad',
       'sede_atencion','nombre_sede','regimen','rango_salarial','identificacion_medico',
       'sw_cronicos', 'sw_rcv','sw_hipertension', 'sw_diabetes','fecha_ingreso_DM',
       'sw_proteccion_renal','sw_dislipidemia', 'sw_enfer_autoinmune', 'sw_enfer_coagulacion',
       'sw_asma', 'sw_epoc', 'sw_cancer_cervix', 'sw_cancer_mama', 'sw_vih',
       'sw_cpr', 'sw_rce', 'sw_fragil_canguro', 'sw_oxigeno_dependiente',
       'sw_sospecha_abuso_sexual', 'sw_tb','sw_obesidad','sw_puntaPiramide','sw_domiciliaria',
        'sw_traslado_sura','celular','direccion','email','fecha_nacimiento','mes_cargue','fecha_cargue']

name_month = today.strftime('%B').upper()
year = today.strftime('%Y')
num_month = today.strftime('%m')
format_string = "%Y%m%d"
# Path file
path = f"{PATH_DRIVE}/Scripts PYTHON/CAPITA/csv_in/{year}"
path_insulina = f"{PATH_DRIVE}/Migracion Dropbox/Marcas de Riesgo/{year}/{int(num_month)}. {name_month}"

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar mariadb
            func_process.save_df_server(df_load, NAME_TABLE_MARIADB, NAME_BD_MARIADB)
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

capita_mes = func_process.pd.read_csv(f'{path}/POBLACION EPS SURA {name_month}.txt',
                                   names= capita_columns,
                                   sep=';',
                                   dtype= {'identificacion_medico':'str'},
                                   low_memory=False,                        
                                   encoding='UTF-8'
                                   )
marca_insulina = func_process.pd.read_excel(f'{path_insulina}/Marca_insulina.xlsx',
                                            dtype= {'ID':'str'},
                                            ).dropna(axis=1)

capita_mes['fecha_cargue'] = func_process.pd.to_datetime("today").strftime("%Y-%m-%d")
capita_mes['mes_cargue'] = f"{today.strftime('%m')}"
capita_mes['fecha_cargue'] = func_process.pd.to_datetime(capita_mes['fecha_cargue'])
capita_mes.fecha_nacimiento =func_process.pd.to_datetime(capita_mes['fecha_nacimiento'])
capita_mes.fecha_ingreso_DM = func_process.pd.to_datetime(capita_mes['fecha_ingreso_DM'], format='%Y-%m-%d', errors='coerce')
# Add marca insulina
capita_mes['sw_insulina'] = 0
capita_mes['sw_insulina']  = capita_mes.identificacion_paciente.isin(marca_insulina.ID) 
#Convert column integer
capita_mes.sw_insulina.fillna(0, inplace=True)
capita_mes.sw_traslado_sura.fillna(0, inplace=True)
capita_mes.sw_traslado_sura = capita_mes.sw_traslado_sura.astype(int)
capita_mes.sw_insulina = capita_mes.sw_insulina.astype(int)
# Convert column String 
capita_mes.sede_atencion = capita_mes.sede_atencion.astype(str)
# Save data
df_validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)
validate_load(df_validate_loads_logs,capita_mes)
