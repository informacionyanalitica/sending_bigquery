import sys,os
import pandas as pd
from datetime import datetime,timedelta

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

project_id_product = 'ia-bigquery-397516'
dataset_id_panorama = 'panorama'
table_name_hospitalizaciones = 'hospitalizaciones'
TABLA_BIGQUERY= f'{project_id_product}.{dataset_id_panorama}.{table_name_hospitalizaciones}'
VALIDATOR_COLUMN = 'column_validator'


SQL_URGENCIAS_HOSPITALIZACIONES = """SELECT *
                FROM reportes.panorama_view AS tc
                WHERE date(tc.fecha_cargue) > '{last_date}'
                """
SQL_LAST_DATE_LOAD = """SELECT max(ct.fecha_cargue) AS last_date_load
                        FROM `ia-bigquery-397516.panorama.hospitalizaciones` as ct """


SQL_BIGQUERY_LAST_MONTH =  """
                SELECT concat(date(g.Fecha_Egreso_Afiliado),'-',g.Numero_de_documento,'-',g.consecutivo_evento) as column_validator
                    FROM {} as g
                    WHERE date(g.fecha_cargue) >= date_sub(current_date() , INTERVAL 1 MONTH)
                     """
LIST_COLUMNS_DATE = ['Fecha_Egreso_Afiliado','fecha_ingreso','fecha_cargue']
LIST_COLUMNS_INT = ['Numero_Dias_Estancia_Hospitalizacion','dias_uci','dias_uce']


def validate_rows_duplicate(df):
    try:
        df[VALIDATOR_COLUMN] = df.Fecha_Egreso_Afiliado.astype(str)+'-'+ df.Numero_de_documento.astype(str)+'-'+df.consecutivo_evento.astype(str) 
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

def convert_column_int(df):
    try:
        for col in LIST_COLUMNS_INT:
            df[col].fillna(0, inplace=True)
            df[col] = df[col].astype(int)
        return df
    except Exception as err:
        print(err)

def convert_column_date(df):
    try:
        for col in LIST_COLUMNS_DATE:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as err:
        print(err)

def convert_column_string(df):
    try:
        COLUMNS_NOT_STRING = LIST_COLUMNS_INT + LIST_COLUMNS_DATE
        for col in df.columns:
            if col not in COLUMNS_NOT_STRING:
                df[col] = df[col].astype(str)
        return df
    except Exception as err:
        print(err)

def read_date():
    try:
        last_date_load = loadbq.read_data_bigquery(SQL_LAST_DATE_LOAD,TABLA_BIGQUERY)['last_date_load'][0]
        df = func_process.load_df_server(SQL_URGENCIAS_HOSPITALIZACIONES.format(last_date=last_date_load),'reportes')
        if df.shape[0] == 0:
            raise SystemExit
        return df
    except Exception as err:
        print(err)

# READ DATA
df_hospitalizacion_panorama=read_date()
df_hospitalizacion_panorama = convert_column_date(df_hospitalizacion_panorama)
df_hospitalizacion_panorama = convert_column_int(df_hospitalizacion_panorama)
df_hospitalizacion_panorama = convert_column_string(df_hospitalizacion_panorama)
#  VALIDATE LOGS LOAD
validate_loads_logs =  loadbq.validate_loads_weekly(TABLA_BIGQUERY)
#  VALIDATE ROWS DUPLICATE
df_hospitalizacion_not_duplicates = validate_rows_duplicate(df_hospitalizacion_panorama)
#  Load data to server
validate_load(validate_loads_logs,df_hospitalizacion_not_duplicates)