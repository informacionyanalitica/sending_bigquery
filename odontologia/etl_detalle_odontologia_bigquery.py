import pandas as pd
import numpy as np 
import sys,os 
from dotenv import load_dotenv
from datetime import datetime
# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

today = datetime.now()
date_load = today.dt.date()

SQL_ODONTOLOGIA_DETALLE_VIEW = f"""SELECT *
                    FROM reportes.detalle_rips_odontologia_view AS r
                    WHERE r.fecha_consulta 
                        BETWEEN DATE_SUB({date_load}, INTERVAL WEEKDAY({date_load}) + 8 DAY)
                  AND DATE_ADD(DATE_SUB({date_load}, INTERVAL WEEKDAY({date_load}) + 8 DAY), INTERVAL 7 DAY);
                """
            
#df_odontologia_detalle_view = func_process.load_df_server(SQL_ODONTOLOGIA_DETALLE_VIEW, 'reportes')  

SQL_BIGQUERY = """
                SELECT g.id_detalle
                FROM {} as g
                WHERE g.id_detalle IN {}
                """

project_id_product = 'ia-bigquery-397516'
dataset_id = 'odontologia'
table_name = 'detalle_rips_odontologia_partition'
validator_column = 'id_detalle'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id}.{table_name}'

# Convertir fechas
def convert_dates(df):
    df.fecha_consulta = pd.to_datetime(df.fecha_consulta, errors='coerce')
    df.hora_cita = df.hora_cita.astype(str)
    df.hora_finaliza_cita = df.hora_finaliza_cita.astype(str)
    df.hora_cita = [row.replace('0 days ','') for row in df.hora_cita]
    df.hora_finaliza_cita = [row.replace('0 days ','') for row in df.hora_finaliza_cita]
    return df

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)
        
# Leer datos
df_detalle_odontologia_bd = func_process.load_df_server(SQL_ODONTOLOGIA_DETALLE_VIEW, 'reportes')   

# Convertir tipo de datos
df_detalle_odontologia_bd = convert_dates(df_detalle_odontologia_bd)

# Obtener datos no duplicados
valores_unicos = tuple(map(int, df_detalle_odontologia_bd[validator_column]))
df_detalle_odontologia_not_duplicates = loadbq.rows_not_duplicates(df_detalle_odontologia_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

# Save data
df_validate_loads_logs =  loadbq.validate_loads_weekly(TABLA_BIGQUERY)
validate_load(df_validate_loads_logs,df_detalle_odontologia_not_duplicates)

