import pandas as pd
import sys, os
from datetime import datetime
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame

# Instancias clase convertColumnDataFrame
convert_columns = convertColumnDataFrame()

# Datos BigQuery
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'recaudos'
table_name_rips = 'facturas_electronicas_1'  # Nueva tabla
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_rips}.{table_name_rips}'

# Obtener fecha actual
hoy = datetime.now()
dia = hoy.day

# Solo se ejecuta los días 1 y 16
if dia not in (1, 16):
    print("Este script solo debe ejecutarse los días 1 y 16 de cada mes")
    print(f"Hoy es día {dia} → no se procesa nada")
    # import sys; sys.exit(1)   # descomenta si quieres terminar la ejecución

else:
    if dia == 16:
        # Día 16 → primera quincena del MES ACTUAL
        año = hoy.year
        mes = hoy.month
        start_date = f"{año}-{mes:02d}-01"
        end_date   = f"{año}-{mes:02d}-15"
        quincena = "primera"
    
    else:  # dia == 1
        # Día 1 → segunda quincena del MES ANTERIOR
        mes_anterior = hoy - relativedelta(months=1)
        año = mes_anterior.year
        mes = mes_anterior.month
        
        start_date = f"{año}-{mes:02d}-16"
        
        # Último día del mes anterior
        ultimo_dia = (hoy.replace(day=1) - relativedelta(days=1)).day
        end_date = f"{año}-{mes:02d}-{ultimo_dia:02d}"
        
        quincena = "segunda"

    # Mostrar información (opcional)
    print(f"Fecha de ejecución: {hoy.strftime('%Y-%m-%d')}")
    print(f"Procesando {quincena} quincena → {start_date} a {end_date}")

SQL_FE_VIEW = f"""
    SELECT *
    FROM reportes.facturasElectronicasView_1 AS fe
    WHERE DATE(fe.fechaFactura) BETWEEN '{start_date}' AND '{end_date}'
"""
 #WHERE DATE(fe.fechaFactura) BETWEEN '{start_date}' AND '{end_date}'
LIST_COLUMNS_STRING = ['idItemFactura', 'idPago']
LIST_COLUMNS_DATE = ['fechaFactura', 'fechaCreacionNotaCredito']
LIST_COLUMNS_INT = ['cantidad', 'precio', 'valorTotal']

# Cargar datos desde MariaDB
df_factura_electronica = func_process.load_df_server(SQL_FE_VIEW, 'reportes')

# Verificar si hay datos
if not df_factura_electronica.empty:
        # Convertir columnas string
    df_factura_electronica = convert_columns.convert_columns_string(df_factura_electronica, LIST_COLUMNS_STRING)
        # Convertir columnas int
    df_factura_electronica = convert_columns.convert_columns_integer(df_factura_electronica, LIST_COLUMNS_INT)
        # Convertir columnas date
    df_factura_electronica = convert_columns.convert_columns_date(df_factura_electronica, LIST_COLUMNS_DATE)
    
    # Cargar a BigQuery sin validación adicional
    loadbq.load_data_bigquery(df_factura_electronica, TABLA_BIGQUERY)
    print(f"Datos cargados en {TABLA_BIGQUERY} para el rango {start_date} a {end_date}")
else:
    print("No se encontraron datos en el rango especificado")
    print(df_factura_electronica)

# Guardar el DataFrame filtrado en un archivo Excel
df_factura_electronica.to_excel('df_factura_electronica.xlsx', index=False)
