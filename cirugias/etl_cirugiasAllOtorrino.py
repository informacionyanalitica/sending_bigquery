import pandas as pd
import sys, os
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()
PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)

import func_process
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame

today = datetime.now() - timedelta(days=1)
date_load = today.date()

# Instancias clase convertColumnDataframe
convert_columns = convertColumnDataFrame()

# Datos Bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_cirugias = 'cirugias'
table_name_cirugias = 'cirugiasAllOtorrino'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_cirugias}.{table_name_cirugias}'

SQL_CIRUGIAS_VIEW = f"""
    SELECT *
    FROM analitica.cirugiasAllOtorrinoView AS a
    WHERE DATE(a.fecha_agendamiento) = '{date_load}'
"""

# Columnas STRING
LIST_COLUMNS_STRING = [
    'id_registro','descripcion_tipo_identificacion','tipoIdentificacion','id_paciente',
    'primer_nombre','segundo_nombre','primer_apellido','segundo_apellido',
    'identificacion_paciente','sexo','descripcion_tipo_edad','telefono','celular','email',
    'nombre_municipio','ubicacion','quirofano','diagnostico','procedimiento',
    'codigo_cups','nombre_cups','codigo_autorizacion','profesional','nombre_medico',
    'especialidad','nombre_especialidad','nombre_entidad','requiere_anestesiologo',
    'tipo_anestesia','hora_cirugia','estado_actual','activo','cod_estado_anterior',
    'nombre_estado_anterior','cod_estado_nuevo','nombre_estado_nuevo',
    'observacion_historico','usuario_actualiza','observacion','observacion_oportunidad',
    'observacion_cancelacion','observacion_transitoria','usuario_ingreso',
    'tipoProcedimiento_Final'
]

LIST_COLUMNS_DATE = [
    'fecha_nacimiento','fecha_entrega_orden','fecha_vencimiento_orden',
    'fecha_agendamiento','fecha_actualizacion','nueva_fecha_actualizacion','fecha_ingreso'
]
LIST_COLUMNS_INT = ['edad', 'oportunidad']
LIST_COLUMNS_FLOAT = []

def validate_load(df_validate_load, df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if total_cargue == 0:
            loadbq.load_data_bigquery(df_load, TABLA_BIGQUERY)
            print(f"✓ Cargados {len(df_load)} registros para {date_load}")
        else:
            print(f"⚠ Ya existe carga para {date_load}")
    except Exception as err:
        print(f"✗ Error: {err}")

df_cirugias_otorrino = func_process.load_df_server(SQL_CIRUGIAS_VIEW, 'analitica')

if df_cirugias_otorrino is not None and not df_cirugias_otorrino.empty:
    # Filtrar columnas presentes
    cols_string = [c for c in LIST_COLUMNS_STRING if c in df_cirugias_otorrino.columns]
    cols_date = [c for c in LIST_COLUMNS_DATE if c in df_cirugias_otorrino.columns]
    cols_int = [c for c in LIST_COLUMNS_INT if c in df_cirugias_otorrino.columns]
    cols_float = [c for c in LIST_COLUMNS_FLOAT if c in df_cirugias_otorrino.columns]

    # Convertir columnas
    df_cirugias_otorrino = convert_columns.convert_columns_string(df_cirugias_otorrino, cols_string)
    df_cirugias_otorrino = convert_columns.convert_columns_integer(df_cirugias_otorrino, cols_int)
    df_cirugias_otorrino = convert_columns.convert_columns_date(df_cirugias_otorrino, cols_date)
    df_cirugias_otorrino = convert_columns.convert_columns_float(df_cirugias_otorrino, cols_float)

    # Guardar datos
    df_validate_loads_logs = loadbq.validate_loads_daily(TABLA_BIGQUERY)
    validate_load(df_validate_loads_logs, df_cirugias_otorrino)
else:
    print(f"⚠ Sin registros para {date_load}")


# ===================================================================
# LÍNEA FINAL PARA EL CORREO: muestra cuántos registros se procesaron ese día
# ===================================================================
total_registros = 0
id_bigquery = TABLA_BIGQUERY
fecha_cargue = date_load.strftime('%Y-%m-%d')

if df_cirugias_otorrino is not None and not df_cirugias_otorrino.empty:
    total_registros = len(df_cirugias_otorrino)

# Esta línea es la que Airflow captura y usa para el email
print(f"{total_registros},{id_bigquery},{fecha_cargue}")