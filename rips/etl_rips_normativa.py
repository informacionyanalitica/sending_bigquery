import pandas as pd
import sys
import os
import glob
import calendar
from datetime import datetime, date
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame

# ---------------------------------------------------------------------------
# Execution: se ejecuta el día 5 de cada mes y procesa el MES ANTERIOR.
# Para reprocesar un mes puntual: python etl_rips_normativa.py 2026-09
# ---------------------------------------------------------------------------
MESES = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
         7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'}

if len(sys.argv) > 1:
    year_int, month_int = (int(x) for x in sys.argv[1].split('-'))
else:
    hoy = date.today()
    year_int, month_int = (hoy.year - 1, 12) if hoy.month == 1 else (hoy.year, hoy.month - 1)

last_day = calendar.monthrange(year_int, month_int)[1]
date_i = date(year_int, month_int, 1)
date_f = date(year_int, month_int, last_day)
month_word = MESES[month_int]

# DETALLE DATASET BIGQUERY
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'rips'
table_name_rips_normativa = 'rips_normativa'
TABLA_BIGQUERY_RIPS_NORMATIVA = f'{project_id_product}.{dataset_id_rips}.{table_name_rips_normativa}'

# RUTA ARCHIVO
PATH_DRIVE = os.environ.get("PATH_DRIVE")
PATH_RIPS_NORMATIVA = f"{PATH_DRIVE}/BASES DE DATOS/{year_int}/{month_int}. {month_word}/RIPS NORMATIVA"
FILE_PREFIX = 'Consolidado_RIPS_NORMATIVA'
NAME_FILE = f'{FILE_PREFIX}_{date_i}_{date_f}.xlsx'   # ej. Consolidado_RIPS_NORMATIVA_2026-09-01_2026-09-30.xlsx
NAME_SHEET = 'consolidado'

SQL_VALIDATE_PERIODO = f"""
    SELECT COUNT(*) AS total
    FROM `{TABLA_BIGQUERY_RIPS_NORMATIVA}`
    WHERE DATE(fecha_ingreso) BETWEEN '{date_i}' AND '{date_f}'
"""

# Lista de columnas esperadas, en el orden del archivo
COLUMN_REQUIRED_RIPS = [
    'IPS', 'Tipo Identificación Paciente', 'Identificación Paciente',
    'Primer Nombre Paciente', 'Segundo Nombre Paciente', 'Primer Apellido Paciente',
    'Segundo Apellido Paciente', 'Código Tipo Afiliado', 'Descripción Tipo Usuario',
    'Fecha de Nacimiento', 'Sexo', 'Código País Residencia', 'Código Municipio',
    'Municipio', 'Zona', 'Código País Origen', 'Fecha Ingreso', 'Series',
    'Fecha Impresión', 'Código SURACUP', 'Código CUPS', 'Nombre Prestación',
    'Modalidad Grupo Servicio', 'Grupo Servicios', 'Código Servicio',
    'Finalidad Consulta', 'Finalidad Procedimiento', 'Código Causa Externa',
    'Nombre Causa Externa', 'Dx Principal', 'Nombre Dx Principal',
    'Dx Asociado 1', 'Dx Asociado 2', 'Dx Asociado 3', 'Tipo Dx',
    'Nombre Tipo Dx', 'Tipo Identificación Médico', 'Identificación Médico',
    'Nombres Médico', 'Concepto Recaudo', 'Valor Pago Moderador',
    'Vía Ingreso Servicio Salud', 'Código Complicación', 'Edad Años',
    'Edad Meses', 'Edad Dias', 'Código Departamento', 'Departamento',
    'Destino Alta', 'Destino Remisión', 'Destino Hospitalización',
    'Estado Salida', 'Horas Observación', 'Hora Salida'
]

# Lista de columnas renombradas, en el mismo orden
COLUMN_RENAME_RIPS = [
    'ips', 'tipo_identificacion_paciente', 'identificacion_paciente',
    'primer_nombre_paciente', 'segundo_nombre_paciente', 'primer_apellido_paciente',
    'segundo_apellido_paciente', 'codigo_tipo_afiliado', 'descripcion_tipo_usuario',
    'fecha_de_nacimiento', 'sexo', 'codigo_pais_residencia', 'codigo_municipio',
    'municipio', 'zona', 'codigo_pais_origen', 'fecha_ingreso', 'series',
    'fecha_impresion', 'codigo_suracup', 'codigo_cups', 'nombre_prestacion',
    'modalidad_grupo_servicio', 'grupo_servicios', 'codigo_servicio',
    'finalidad_consulta', 'finalidad_procedimiento', 'codigo_causa_externa',
    'nombre_causa_externa', 'dx_principal', 'nombre_dx_principal',
    'dx_asociado_1', 'dx_asociado_2', 'dx_asociado_3', 'tipo_dx',
    'nombre_tipo_dx', 'tipo_identificacion_medico', 'identificacion_medico',
    'nombres_medico', 'concepto_recaudo', 'valor_pago_moderador',
    'via_ingreso_servicio_salud', 'codigo_complicacion', 'edad_anios',
    'edad_meses', 'edad_dias', 'codigo_departamento', 'departamento',
    'destino_alta', 'destino_remision', 'destino_hospitalizacion',
    'estado_salida', 'horas_observacion', 'hora_salida'
]

LIST_COLUMNS_STRINGS = [
    'ips', 'tipo_identificacion_paciente', 'identificacion_paciente',
    'primer_nombre_paciente', 'segundo_nombre_paciente', 'primer_apellido_paciente',
    'segundo_apellido_paciente', 'codigo_tipo_afiliado', 'descripcion_tipo_usuario', 'sexo',
    'codigo_pais_residencia', 'codigo_municipio', 'municipio', 'zona', 'codigo_pais_origen', 'series',
    'codigo_suracup', 'codigo_cups', 'nombre_prestacion',
    'modalidad_grupo_servicio', 'grupo_servicios', 'codigo_servicio',
    'finalidad_consulta', 'finalidad_procedimiento', 'codigo_causa_externa',
    'nombre_causa_externa', 'dx_principal', 'nombre_dx_principal',
    'dx_asociado_1', 'dx_asociado_2', 'dx_asociado_3', 'tipo_dx',
    'nombre_tipo_dx', 'tipo_identificacion_medico', 'identificacion_medico',
    'nombres_medico', 'concepto_recaudo', 'via_ingreso_servicio_salud', 'codigo_complicacion',
    'codigo_departamento', 'departamento',
    'destino_alta', 'destino_remision', 'destino_hospitalizacion',
    'estado_salida'
]
LIST_COLUMNS_DATES = ['fecha_de_nacimiento', 'fecha_ingreso', 'fecha_impresion']
LIST_COLUMNS_INTEGER = ['valor_pago_moderador', 'edad_anios', 'edad_meses', 'edad_dias']


def find_file():
    """Busca el nombre exacto; si no está, toma el Consolidado_RIPS_NORMATIVA_*.xlsx más reciente."""
    path_exacto = f'{PATH_RIPS_NORMATIVA}/{NAME_FILE}'
    if os.path.isfile(path_exacto):
        return path_exacto

    candidatos = [f for f in glob.glob(f'{PATH_RIPS_NORMATIVA}/{FILE_PREFIX}_*.xlsx')
                  if not os.path.basename(f).startswith('~$')]
    if not candidatos:
        raise FileNotFoundError(f'No se encontró {NAME_FILE} en {PATH_RIPS_NORMATIVA}')
    elegido = max(candidatos, key=os.path.getmtime)
    print(f'AVISO: no está {NAME_FILE}, se usa {os.path.basename(elegido)}')
    return elegido


def read_rips_file(path_file_rips):
    print(f'Leyendo archivo: {path_file_rips}')
    hojas = pd.ExcelFile(path_file_rips).sheet_names
    hoja = next((h for h in hojas if h.strip().lower() == NAME_SHEET), hojas[0])
    print(f'Hoja: {hoja}')

    df_rips = pd.read_excel(path_file_rips, sheet_name=hoja, na_values=['', ' '])
    df_rips.columns = df_rips.columns.str.strip()

    existing_columns = [col for col in COLUMN_REQUIRED_RIPS if col in df_rips.columns]
    missing_columns = [col for col in COLUMN_REQUIRED_RIPS if col not in df_rips.columns]
    if missing_columns:
        print(f'Columnas esperadas pero no encontradas: {missing_columns}')
    if not existing_columns:
        raise ValueError('No se encontraron columnas esperadas en el archivo.')

    df_rips = df_rips[existing_columns]
    rename_dict = {old: new for old, new in zip(COLUMN_REQUIRED_RIPS, COLUMN_RENAME_RIPS) if old in existing_columns}
    df_rips = df_rips.rename(columns=rename_dict)
    print(f'Cantidad de registros: {len(df_rips)}')
    return df_rips


def convert_columns(df):
    convert = convertColumnDataFrame()
    df = convert.convert_columns_string(df, [c for c in LIST_COLUMNS_STRINGS if c in df.columns])
    df = convert.convert_columns_date(df, [c for c in LIST_COLUMNS_DATES if c in df.columns])
    df = convert.convert_columns_integer(df, [c for c in LIST_COLUMNS_INTEGER if c in df.columns])
    return df


def periodo_ya_cargado():
    df_validate = loadbq.read_data_bigquery(SQL_VALIDATE_PERIODO, TABLA_BIGQUERY_RIPS_NORMATIVA)
    return int(df_validate.total[0])


if __name__ == '__main__':
    print(f'=== RIPS NORMATIVA | periodo {date_i} a {date_f} | {datetime.now():%Y-%m-%d %H:%M:%S} ===')
    try:
        # VALIDATE: si el mes ya está en BigQuery no se vuelve a cargar (evita duplicados al re-ejecutar)
        total_existente = periodo_ya_cargado()
        if total_existente > 0:
            print(f'El periodo {date_i} a {date_f} ya tiene {total_existente} filas en {TABLA_BIGQUERY_RIPS_NORMATIVA}. No se carga.')
            print(0)  # última línea -> XCom (total cargado)
            sys.exit(0)

        # EXTRACT
        df_sheet_rips = read_rips_file(find_file())
        if df_sheet_rips.empty:
            raise ValueError('El archivo no trae registros.')

        # TRANSFORM
        df_sheet_rips = convert_columns(df_sheet_rips)

        # LOAD
        loadbq.load_data_bigquery(df_sheet_rips, TABLA_BIGQUERY_RIPS_NORMATIVA, 'WRITE_APPEND')
        print(len(df_sheet_rips))  # última línea -> XCom (total cargado)

    except Exception as error:
        print(f'Error ETL RIPS NORMATIVA: {error}')
        sys.exit(1)  # para que Airflow marque la tarea como fallida