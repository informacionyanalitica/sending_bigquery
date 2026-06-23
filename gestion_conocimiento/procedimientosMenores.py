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
table_name                      = 'procedimientosMenores'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET   = '1iSvqN7lLXJ8hLpcdTc2DPjGtTjf_cn2bINQeK_ye4Ro'
NAME_SHEET = 'Ingreso'

# ---------------------------------------------------------------------------
# MAPEO DE COLUMNAS → camelCase
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Fecha (dd/mm/aaaa)':                    'fecha',
    'Fecha en la que realizo la auditoria (dd/mm/aaaa)': 'fechaRealizoAuditoria',
    'Examen a Monitorear':                   'examenMonitorear',
    'Mes Monitoreo':                         'mesMonitoreo',
    'Año':                                   'ano',
    'Sede':                                  'sede',
    'Nombre del profesional':                'nombreProfesional',
    'Cedula':                                'cedula',
    'Historia Clinica':                      'historiaClinica',
    'Identificación - Responsable o Acompañante': 'identificacionResponsable',
    'Identificación - Telefono':             'identificacionTelefono',
    'Identificación - Ocupacion':            'identificacionOcupacion',
    'Consentimiento Informado':              'consentimientoInformado',
    'Registra tipo de Procedimiento':        'registraTipoProcedimiento',
    'Examen fisico - Estado General':        'examenFisicoEstadoGeneral',
    'Registra Signos Vitales':               'registraSignosVitales',
    'Registra Medicamentos Aplicados':       'registraMedicamentosAplicados',
    'Dosis de medicamentos aplicados':       'dosisMedicamentos',
    'Reaccion a medicamentos Aplicados':     'reaccionMedicamentos',
    'Recomendaciones':                       'recomendaciones',
    'Administración de profilaxis - ¿Se le administró profilaxis antibiótica al paciente programado antes de la incisión quirúrgica?': 'profilaxisAdministrada',
    'Momento de administración - ¿ La administración de profilaxis antimicrobiana se realizó antes de la incisión (mínimo 120 minutos y dependiendo de la vida media del antibiótico)?': 'profilaxisMomento',
    'Antimicrobiano administrado - ¿La profilaxis antimicrobiana fue apropiada (antibiótico y dosis)?': 'profilaxisAntimicrobiano',
    'Redosificación intraoperatoria - Bajo las condiciones del paciente y las características del procedimiento, ¿ es necesaria la aplicación de dosis intra operatoria?': 'profilaxisRedosificacion',
    'Profilaxis posoperatoria - ¿Se prolongó la profilaxis antibiótica 24 horas despues de finalizado el procedimiento?': 'profilaxisPostoperatoria',
    'Pertinencia':                           'pertinencia',
    'Calificación Administrativos':          'calificacionAdministrativos',
    'Calificación Procedimiento Medico':     'calificacionProcedimientoMedico',
    'Calificación':                          'calificacion',
    'Calificación Pertinencia':              'calificacionPertinencia',
    'Observaciones':                         'observaciones',
    'Validación de la Base de Datos':        'validacionDatos',
}

COLUMNS_REQUIRED = list(COLUMN_RENAME_MAP.values())

PCT_COLS = [
    'calificacionAdministrativos', 'calificacionProcedimientoMedico',
    'calificacion', 'calificacionPertinencia'
]

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
        for col in PCT_COLS:
            if col in df.columns:
                df[col].fillna('0', inplace=True)
                df[col] = df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False)
                df[col].replace('', '0', inplace=True)
                df[col] = df[col].astype(float) / 100
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
    log.info(f'Inicio del ETL: procedimientosMenores')
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

        validate_load(df)

    except Exception as err:
        log.error(f'execution_load falló: {err}\n{traceback.format_exc()}')



execution_load()