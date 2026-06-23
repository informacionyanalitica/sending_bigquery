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
table_name                      = 'conceptosClinicosIPSBasicas'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET   = '1_YaNjqHtH-UlK3q-2xGRlkQo9Y2llCa-B0VFoUgSJjA'
NAME_SHEET = 'Ingreso'

# ---------------------------------------------------------------------------
# MAPEO DE COLUMNAS → camelCase
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Fecha':                                            'fecha',
    'Fecha en la que realizo la auditoria (dd/mm/año)': 'fechaRealizoAuditoria',
    'Diagnostico':                                       'diagnostico',
    'Servicio evaluado':                                 'servicioEvaluado',
    'Mes':                                                'mes',
    'Año':                                                'ano',
    'Sede':                                               'sede',
    'Medico':                                             'nombreProfesional',
    'Cedula':                                             'cedula',
    'Historia Clinica':                                   'historiaClinica',
    'Solicitud pertinente de ayudas disgnósticas':        'solicitudPertinenteAyudasDx',
    'Da Recomendaciones':                                 'daRecomendaciones',
    'Historia clínica completa':                          'historiaClinicaCompleta',
    'Solicita pertinencia de ayuda diagnóstica':          'solicitaPertinenciaAyudaDx',
    'Solicita valoración con la especialidad /definir conducta': 'solicitaValoracionEspecialidad',
    'La solicitud fue pertinente':                        'solicitudPertinente',
    'Calificación':                                       'calificacion',
    'Observaciones':                                      'observaciones',
    'Validación de la base de datos':                     'validacionDatos',
    'Envio de correo':                                    'correo',
}

COLUMNS_REQUIRED = list(COLUMN_RENAME_MAP.values())

PCT_COLS = ['calificacion']

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
            df['nombreProfesional'].isna() | (df['nombreProfesional'].astype(str).str.strip() == '')
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
    log.info(f'Inicio del ETL: conceptosClinicosIPSBasicas')
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



execution_load()