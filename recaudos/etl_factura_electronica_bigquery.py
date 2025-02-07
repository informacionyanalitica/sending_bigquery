import pandas as pd
import sys,os
from datetime import datetime,timedelta
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame


today = datetime.now()
date_load = today.date()

# Instancias clase converColumnDatafrane
convert_columns = convertColumnDataFrame()

# Datos Bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'recaudos'
table_name_rips = 'facturas_electronicas'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_rips}.{table_name_rips}'

SQL_FE_VIEW = f"""
            SELECT *
            FROM reportes.facturasElectronicasView AS fe
            WHERE DATE(fe.fechaFactura) ='{date_load}'
        """
LIST_COLUMNS_STRING = ['idItemFactura','idPago']
LIST_COLUMNS_DATE = ['fechaFactura','fechaCreacionNotaCredito']
LIST_COLUMNS_INT = ['cantidad','precio','valorTotal']

def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)
        
df_factura_electronica = func_process.load_df_server(SQL_FE_VIEW,'reportes')
# Convertir columnas string
df_factura_electronica = convert_columns.convert_columns_string(df_factura_electronica,LIST_COLUMNS_STRING)
# Convertir columnas int
df_factura_electronica = convert_columns.convert_columns_integer(df_factura_electronica,LIST_COLUMNS_INT)
# Convertir columnas date
df_factura_electronica = convert_columns.convert_columns_date(df_factura_electronica,LIST_COLUMNS_DATE)
# Guardar datos
df_validate_loads_logs =  loadbq.validate_loads_daily(TABLA_BIGQUERY)
validate_load(df_validate_loads_logs,df_factura_electronica)
