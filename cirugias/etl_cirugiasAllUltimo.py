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

# Fecha de carga: por defecto ayer. Opcional: pasar fecha como argumento
# para pruebas manuales -> python etl_cirugiasAllUltima.py 2026-09-23
if len(sys.argv) > 1:
    date_load = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
else:
    date_load = (datetime.now() - timedelta(days=1)).date()

# Instancias clase convertColumnDataframe
convert_columns = convertColumnDataFrame()

# Datos Bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_cirugias = 'cirugias'
table_name_cirugias = 'cirugiasAllUltima'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_cirugias}.{table_name_cirugias}'

# Rango semiabierto [dia, dia+1) para que MariaDB pueda usar índice en fecha_agendamiento
SQL_CIRUGIAS_VIEW = f"""
    SELECT *
    FROM analitica.cirugiasAllUltimaView AS a
    WHERE a.fecha_agendamiento >= '{date_load}'
      AND a.fecha_agendamiento <  '{date_load + timedelta(days=1)}'
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


def convertir_columnas(df):
    cols_string = [c for c in LIST_COLUMNS_STRING if c in df.columns]
    cols_date = [c for c in LIST_COLUMNS_DATE if c in df.columns]
    cols_int = [c for c in LIST_COLUMNS_INT if c in df.columns]
    cols_float = [c for c in LIST_COLUMNS_FLOAT if c in df.columns]

    # Cualquier columna de texto que no esté en las listas se manda como STRING
    # (evita errores de tipo en BigQuery si la vista tiene columnas extra)
    listadas = set(cols_string + cols_date + cols_int + cols_float)
    cols_extra = [c for c in df.columns if c not in listadas and df[c].dtype == 'object']
    cols_string += cols_extra

    df = convert_columns.convert_columns_string(df, cols_string)
    df = convert_columns.convert_columns_integer(df, cols_int)
    df = convert_columns.convert_columns_date(df, cols_date)
    df = convert_columns.convert_columns_float(df, cols_float)
    return df


def to_camel_case(nombre):
    """id_registro -> idRegistro, tipoProcedimiento_Final -> tipoProcedimientoFinal"""
    partes = [p for p in nombre.split('_') if p]
    if not partes:
        return nombre
    primera = partes[0][0].lower() + partes[0][1:]
    return primera + ''.join(p[0].upper() + p[1:] for p in partes[1:])


def columnas_camel_case(df):
    df.columns = [to_camel_case(c) for c in df.columns]
    return df


# Validación de duplicados contra la tabla destino en BigQuery (columnas en camelCase)
VALIDATOR_COLUMN = 'idRegistro'  # ya en camelCase
SQL_BIGQUERY_DUPLICADOS = """
                SELECT g.idRegistro
                FROM {} as g
                WHERE g.idRegistro IN {}
                """


def get_data_no_duplicate(df, validator_column, value_unique, sql_bigquery, tabla_bigquery):
    try:
        df_not_duplicates = pd.DataFrame()
        if df.shape[0] > 0:
            df_not_duplicates = loadbq.rows_not_duplicates(
                df, validator_column, sql_bigquery, tabla_bigquery, value_unique
            )
        return df_not_duplicates
    except ValueError as err:
        print(err)
        return pd.DataFrame()


def validate_load(df_validate_load, df_load):
    """Capa 1: log diario. Capa 2: descarta id_registro que ya estén en BigQuery.
    Devuelve el número de registros realmente cargados."""
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if total_cargue != 0:
            print(f"⚠ Ya existe carga para {date_load}")
            return 0

        valores_unicos = tuple(df_load[VALIDATOR_COLUMN].astype(str).unique())
        df_nuevos = get_data_no_duplicate(
            df_load, VALIDATOR_COLUMN, valores_unicos,
            SQL_BIGQUERY_DUPLICADOS, TABLA_BIGQUERY
        )

        if df_nuevos is None or df_nuevos.empty:
            print(f"⚠ Todos los registros de {date_load} ya existen en BigQuery")
            return 0

        loadbq.load_data_bigquery(df_nuevos, TABLA_BIGQUERY)
        descartados = len(df_load) - len(df_nuevos)
        print(f"✓ Cargados {len(df_nuevos)} registros para {date_load} "
              f"({descartados} duplicados descartados)")
        return len(df_nuevos)
    except Exception as err:
        print(f"✗ Error: {err}")
        return 0


total_registros = 0
df_cirugias_ultima = func_process.load_df_server(SQL_CIRUGIAS_VIEW, 'analitica')

if df_cirugias_ultima is not None and not df_cirugias_ultima.empty:
    df_cirugias_ultima = convertir_columnas(df_cirugias_ultima)
    df_cirugias_ultima = columnas_camel_case(df_cirugias_ultima)

    # Guardar datos
    df_validate_loads_logs = loadbq.validate_loads_daily(TABLA_BIGQUERY)
    total_registros = validate_load(df_validate_loads_logs, df_cirugias_ultima)
else:
    print(f"⚠ Sin registros para {date_load}")


# ===================================================================
# LÍNEA FINAL PARA EL CORREO: debe ser el último print (Airflow toma la última línea)
# Reporta los registros efectivamente cargados en BigQuery
# ===================================================================
print(f"{total_registros},{TABLA_BIGQUERY},{date_load.strftime('%Y-%m-%d')}")