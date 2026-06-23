import pandas as pd
import sys
import os
import logging
import traceback
import time
from dotenv import load_dotenv

load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BIGQUERY
# ---------------------------------------------------------------------------
project_id_product              = 'ia-bigquery-397516'
dataset_id_gestion_conocimiento = 'gestion_conocimiento'
table_name                      = 'especialidadesBasicas'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET   = '1ihjWM11s4wP_0OPKsBn3sJg65qzt2sJK281QnYbMx5E'
NAME_SHEET = 'Ingreso Auditoria'

# ---------------------------------------------------------------------------
# MAPEO DE COLUMNAS → camelCase
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Fecha (dd/mm/año)':                     'fecha',
    'Fecha en la que realizo la auditoria (dd/mm/año)': 'fechaRealizoAuditoria',
    'Mes Monitoreo':                         'mesMonitoreo',
    'Año':                                   'ano',
    'Especialidad':                          'especialidad',
    'Examen a Monitorear':                   'examenMonitorear',
    'Tipo Examen':                           'tipoExamen',
    'Sede':                                  'sede',
    'Rol Profesional':                       'rolProfesional',
    'Nombre del profesional':                'nombreProfesional',
    'Cedula':                                'cedula',
    'Historia clinica':                      'historiaClinica',
    'Condición de Salud':                    'condicionSalud',
    'Dx':                                    'diagnostico',
    'Percentil Riesgo':                      'percentilRiesgo',
    'Se revisan antecedentes personales y se gestionan en caso de ser necesario': 'antecedentesPersonales',
    'Se realiza interrogatorio mèdico completo': 'interrogatorioMedico',
    'se valida adherencia a manejos previos, prescripción adecuada de manejo farmacológico, dosificación y posología adecuada, tiempo de tratamiento, prescripción de medicamentos de primera línea. ( En este ítem se validan antibióticos basados en PROA, ciclos de analgesia, modulación del dolor, medicamentos específicos para patologías crónicas, entre otros)': 'adherenciaFarmacologica',
    'Se realiza exámen físico completo y adecuado': 'examenFisico',
    'Las ayudas diagnósticas se encuentran acorde a la condición de salud': 'ayudasDiagnosticas',
    'Las recomendaciones entregadas al paciente son acorde a lo establecido en guías protocolos': 'recomendacionesGuias',
    'Se maneja el paciente de manera integral: si requiere remisión a algún programa se realiza': 'manejoIntegral',
    'La condición en salud se manejó según guìa establecida del asegurador o protocolo institucional de Coopsana IPS (aplica sí está condiciòn en salud tiene guía establecida, si no es así colocar N/A).': 'condicionSegunGuia',
    'Sí el paciente volvió a reconsultar con el médico porque estuvo en urgencias y/o hospitalización, es adecuada la atención?': 'reconsultaAdecuada',
    'El médico resolvió de acuerdo a las guías establecidas la condición de salud del paciente mediante un enfoque integral?': 'resolucionGuias',
    'Sí se hizo remisiòn o el CCE a alguna especialización, es adecuada?': 'remisionAdecuada',
    'Se realiza gestión de los paraclínicos solicitados y del CEE si aplica.': 'gestionParaclinicos',
    'La referencia y contrareferencia es adecuada en caso de que aplique': 'referenciaContrareferencia',
    'indique si la reconsulta del paciente fue causa administrativa': 'reconsultaCausaAdministrativa',
    'Nota':                                  'nota',
    'Observaciones':                         'observaciones',
    '¿Datos completos?':                     'validacionDatos',
}

COLUMNS_REQUIRED = list(COLUMN_RENAME_MAP.values())

# ---------------------------------------------------------------------------
# TRANSFORMACIONES
# ---------------------------------------------------------------------------
def convert_dates(df):
    df['fecha']               = pd.to_datetime(df['fecha'], errors='coerce')
    df['fechaRealizoAuditoria'] = pd.to_datetime(df['fechaRealizoAuditoria'], errors='coerce')
    return df

def convert_numbers(df):
    try:
        df['ano'].replace('', '0', inplace=True)
        df['ano'].fillna(0, inplace=True)
        df['ano'] = df['ano'].astype(int)
        if 'nota' in df.columns:
            df['nota'].fillna('0', inplace=True)
            df['nota'] = df['nota'].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False)
            df['nota'].replace('', '0', inplace=True)
            df['nota'] = df['nota'].astype(float) / 100
    except Exception as err:
        log.error(f'convert_numbers: {err}\n{traceback.format_exc()}')
    return df

def get_columns_rows(df):
    try:
        df.rename(columns=COLUMN_RENAME_MAP, inplace=True)
        cols = [c for c in COLUMNS_REQUIRED if c in df.columns]
        missing = [c for c in COLUMNS_REQUIRED if c not in df.columns]
        if missing:
            log.warning(f'Columnas esperadas no encontradas en la hoja: {missing}')
        return df[cols]
    except Exception as err:
        log.error(f'get_columns_rows: {err}\n{traceback.format_exc()}')

def drop_rows_empty(df):
    try:
        if df.empty:
            return df
        mask = (
            df['fecha'].isna() | (df['fecha'].astype(str).str.strip() == '')
        ) & (
            df['examenMonitorear'].isna() | (df['examenMonitorear'].astype(str).str.strip() == '')
        )
        filas_antes = df.shape[0]
        df = df[~mask].reset_index(drop=True)
        descartadas = filas_antes - df.shape[0]
        if descartadas > 0:
            log.info(f'drop_rows_empty: {descartadas} filas vacías descartadas ({df.shape[0]} restantes)')
        return df
    except Exception as err:
        log.error(f'drop_rows_empty: {err}\n{traceback.format_exc()}')
        return df

def validate_load(df):
    if df.shape[0] > 0:
        try:
            import io
            log.info(f'Cargando {df.shape[0]} filas en {TABLA_BIGQUERY} ...')
            captured = io.StringIO()
            sys.stdout = captured
            loadbq.load_data_bigquery(df, TABLA_BIGQUERY, 'WRITE_TRUNCATE')
            sys.stdout = sys.__stdout__
            output = captured.getvalue().strip()
        except Exception as err:
            sys.stdout = sys.__stdout__
            log.error(f'validate_load: {err}\n{traceback.format_exc()}')
            return
        log.info(f'Carga exitosa → {df.shape[0]} filas insertadas en {TABLA_BIGQUERY}')
        if output:
            print(output)
    else:
        log.warning('DataFrame vacío, no se cargó nada a BigQuery.')

# ---------------------------------------------------------------------------
# EJECUCIÓN
# ---------------------------------------------------------------------------
def execution_load():
    inicio_total = time.time()
    log.info('=' * 60)
    log.info(f'Inicio del ETL: especialidadesBasicas')
    log.info(f'Tabla destino : {TABLA_BIGQUERY}')
    log.info(f'Sheet ID      : {ID_SHEET}')
    log.info('=' * 60)

    try:
        inicio_hoja = time.time()
        log.info(f'[{NAME_SHEET}] Leyendo hoja ...')

        df = func_process.get_google_sheet(ID_SHEET, NAME_SHEET)
        df.columns = df.columns.str.strip()
        log.info(f'[{NAME_SHEET}] Filas leídas desde Google Sheets: {df.shape[0]}')

        df = get_columns_rows(df)
        log.info(f'[{NAME_SHEET}] Columnas seleccionadas: {df.shape[1]}')

        df = drop_rows_empty(df)
        df = convert_dates(df)
        df = convert_numbers(df)
        df['auditor'] = NAME_SHEET

        duracion_hoja = time.time() - inicio_hoja
        log.info(f'[{NAME_SHEET}] Procesamiento completado en {duracion_hoja:.2f}s → {df.shape[0]} filas listas')

        df.drop(columns=[c for c in ['correo'] if c in df.columns], inplace=True)
        validate_load(df)

    except Exception as err:
        log.error(f'execution_load falló: {err}\n{traceback.format_exc()}')

    duracion_total = time.time() - inicio_total
    log.info(f'ETL finalizado en {duracion_total:.2f}s')
    log.info('=' * 60)

execution_load()