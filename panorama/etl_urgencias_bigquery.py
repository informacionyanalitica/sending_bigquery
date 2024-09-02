import sys,os
import pandas as pd
from datetime import datetime,timedelta
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

project_id_product = 'ia-bigquery-397516'
dataset_id_panorama = 'panorama'
table_name_urgencias = 'urgencias'
TABLA_BIGQUERY= f'{project_id_product}.{dataset_id_panorama}.{table_name_urgencias}'
VALIDATOR_COLUMN = 'column_validator'

SQL_URGENCIAS_PANORAMA = """SELECT *
                FROM reportes.urgenciasPanoramaView AS tc
                WHERE date(tc.fecha_cargue) > '{last_date}'
                """
SQL_LAST_DATE_LOAD = """SELECT max(ct.fecha_cargue) AS last_date_load
                         FROM `ia-bigquery-397516.panorama.urgencias` as ct """


SQL_BIGQUERY_LAST_MONTH =  """
                SELECT concat(date(g.Fecha_Autorizacion),'-',g.Numero_de_documento,'-',g.Codigo_Diagnostico_EPS_Op) as column_validator
                    FROM {} as g
                    WHERE date(g.fecha_cargue) >= date_sub(current_date() , INTERVAL 1 MONTH)
                     """


def validate_rows_duplicate(df):
    try:
        df[VALIDATOR_COLUMN] = df.Fecha_Autorizacion.astype(str)+'-'+ df.Numero_de_documento.astype(str)+'-'+df.Codigo_Diagnostico_EPS_Op.astype(str) 
        valores_unicos = tuple(map(str, df[VALIDATOR_COLUMN]))
        df_rips_not_duplicates = loadbq.rows_duplicates_last_month(df,VALIDATOR_COLUMN,SQL_BIGQUERY_LAST_MONTH,TABLA_BIGQUERY) 
        df_rips_not_duplicates.drop(VALIDATOR_COLUMN, axis=1, inplace=True)
        if df_rips_not_duplicates.shape[0] == 0:
            raise SystemExit
        return df_rips_not_duplicates
    except Exception as err:
        print(err)

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if total_cargue == 0:
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

def read_date():
    try:
        last_date_load = loadbq.read_data_bigquery(SQL_LAST_DATE_LOAD,TABLA_BIGQUERY)['last_date_load'][0]
        print(last_date_load)
        df_urgencias_panorama = func_process.load_df_server(SQL_URGENCIAS_PANORAMA.format(last_date=last_date_load),'reportes')
        df_urgencias_panorama.Fecha_Autorizacion = pd.to_datetime(df_urgencias_panorama.Fecha_Autorizacion, errors='coerce')
        return df_urgencias_panorama
    except Exception as err:
        print(err)

# READ DATA
df_urgencias_panorama=read_date()
# VALIDATE LOGS LOAD
validate_loads_logs =  loadbq.validate_loads_weekly(TABLA_BIGQUERY)
# VALIDATE ROWS DUPLICATE
df_urgencias_not_duplicates = validate_rows_duplicate(df_urgencias_panorama)
#  Load data to server
validate_load(validate_loads_logs,df_urgencias_not_duplicates)
