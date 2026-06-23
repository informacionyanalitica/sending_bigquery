import pandas as pd
import sys
import os
import logging
import traceback
import time
from datetime import datetime
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
table_name                      = 'cirugias'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET = '1iqgyastcdgMA0pcJ1dbP9ysTVg7kYHIX2vY6lc1yqas'

LIST_NAME_SHEET = [
    'Ingreso de auditorias',
    'Cirugia general',
]

# ---------------------------------------------------------------------------
# MAPEO POR PESTAÑA
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Ingreso de auditorias': {
        'Fecha (dd/mm/aaaa)':                    'fecha',
        'Fecha en la que realizo la auditoria (dd/mm/aaaa)': 'fechaRealizoAuditoria',
        'Guia Medica dx':                        'guiaMedicaDx',
        'Servicio evaluado':                     'servicioEvaluado',
        'Mes':                                   'mes',
        'Año':                                   'ano',
        'Sede':                                  'sede',
        'Medico':                                'nombreProfesional',
        'Cedula':                                'cedula',
        'Historia Clinica':                      'historiaClinica',
        'Identificación - Responsable o Acompañante': 'identificacionResponsable',
        'Identificación - Telefono':             'identificacionTelefono',
        'Identificación - Ocupacion':            'identificacionOcupacion',
        'Motivo de consulta':                    'motivoConsulta',
        'Examen fisico - Estado General':        'examenFisicoEstadoGeneral',
        'Examen fisico - Registro Signos Vitales': 'examenFisicoSignosVitales',
        'Descripción Operatoria':                'descripcionOperatoria',
        'Conducta - remisiones pertinentes':     'conductaRemisiones',
        'Conducta - incapacidad pertinente':     'conductaIncapacidad',
        'Recomendaciones Postquirurgicas':       'recomendacionesPostquirurgicas',
        'Adherencia a guías - medicamentos':     'adherenciaGuiasMedicamentos',
        'Administración de profilaxis - ¿Se le administró profilaxis antibiótica al paciente programado antes de la incisión quirúrgica?': 'profilaxisAdministrada',
        'Momento de administración - ¿ La administración de profilaxis antimicrobiana se realizó antes de la incisión (mínimo 120 minutos y dependiendo de la vida media del antibiótico)?': 'profilaxisMomento',
        'Antimicrobiano administrado - ¿La profilaxis antimicrobiana fue apropiada (antibiótico y dosis)?': 'profilaxisAntimicrobiano',
        '¿El análisis y plan son adecuados?':    'analisisYPlanAdecuados',
        'Aplicacion de guias medicas':           'aplicacionGuiasMedicas',
        'Calificación Administrativos':          'calificacionAdministrativos',
        'Calificación Enfermeria':               'calificacionEnfermeria',
        'Evaluacion de guias y practicas medica': 'evaluacionGuiasPracticas',
        'Observaciones':                         'observaciones',
        'Validación de la base de datos':        'validacionDatos',
        'Envio de correo':                       'correo',
    },
    'Cirugia general': {
        'Fecha (DD/MM/AAAA)':                    'fecha',
        'Fecha en la que realizo la auditoria (dd/mm/aaaa)': 'fechaRealizoAuditoria',
        'Mes':                                   'mes',
        'Año':                                   'ano',
        'Sede':                                  'sede',
        'Guia Medica dx':                        'guiaMedicaDx',
        'Nombre del profesional':                'nombreProfesional',
        'Número de documento':                   'cedula',
        'Número de historia Clínica':            'historiaClinica',
        'Se diligencian los item correspondientes a la identificación del usuario como ocupación, responsable, telefono': 'identificacionCompleta',
        'En la historia clínica se establece el diagnóstico presuntivo o confirmado de hernia de acuerdo a los criterios consignados en las guias.': 'diagnosticoHernia',
        'Se registra el procedimiento a realizar de acuerdo a las tecnicas consignadas en la guia.': 'procedimientoRegistrado',
        'En los hallazgos intraoperatorios se realiza la descripción de los mismos.': 'hallazgosIntraoperatorios',
        'Se realiza la descripción de la tecnica quirurgica empleada asi como materiales utilizados.': 'tecnicaQuirurgica',
        'En la lista de chequeo se registra información con respecto a los antecedentes medicos y tratamientos.': 'listaChequeoAntecedentes',
        'En la lista de verificación de seguridad se registran la previsión de sucesos criticos.': 'listaVerificacionSucesos',
        'En la lista de verificación de seguridad el cirujano registra procedimiento realizado y envio de muestras a patologia.': 'listaVerificacionProcedimiento',
        'En la lista de verificación de seguridad del paciente se registra incapacidad, cita de revisión, medicamentos ordenados.': 'listaVerificacionIncapacidad',
        'Cumple con el diligenciamiento de indicaciones, recomendaciones, signos de alarma con respecto a la cirugia realizada.': 'indicacionesRecomendaciones',
        'Administración de profilaxis - ¿Se le administró profilaxis antibiótica al paciente programado antes de la incisión quirúrgica?': 'profilaxisAdministrada',
        'Momento de administración - ¿ La administración de profilaxis antimicrobiana se realizó antes de la incisión (mínimo 120 minutos y dependiendo de la vida media del antibiótico)?': 'profilaxisMomento',
        'Antimicrobiano administrado - ¿La profilaxis antimicrobiana fue apropiada (antibiótico y dosis)?': 'profilaxisAntimicrobiano',
        'Redosificación intraoperatoria - Bajo las condiciones del paciente y las características del procedimiento, ¿ es necesaria la aplicación de dosis intra operatoria?': 'profilaxisRedosificacion',
        'Profilaxis posoperatoria - ¿Se prolongó la profilaxis antibiótica 24 horas despues de finalizado el procedimiento?': 'profilaxisPostoperatoria',
        'Cumple con la estructura de HC':        'estructuraHC',
        'Observaciones':                         'observaciones',
        'Calificación Administrativos':          'calificacionAdministrativos',
        'Calificación Enfermeria':               'calificacionEnfermeria',
        'Calificación Medico':                   'calificacionMedico',
        'Calificación de estructura':            'calificacionEstructura',
        'Calificación Guia + Estructura Medico': 'calificacionGuiaEstructuraMedico',
        'Validación de los datos':               'validacionDatos',
        'Envio de correo':                       'correo',
    },
}

# ---------------------------------------------------------------------------
# COLUMNAS DE PORCENTAJE POR PESTAÑA
# ---------------------------------------------------------------------------
PCT_COLS = {
    'Ingreso de auditorias': [
        'calificacionAdministrativos', 'calificacionEnfermeria', 'evaluacionGuiasPracticas'
    ],
    'Cirugia general': [
        'calificacionAdministrativos', 'calificacionEnfermeria',
        'calificacionMedico', 'calificacionEstructura', 'calificacionGuiaEstructuraMedico'
    ],
}

# ---------------------------------------------------------------------------
# TRANSFORMACIONES
# ---------------------------------------------------------------------------
def convert_dates(df):
    df['fecha']               = pd.to_datetime(df['fecha'], errors='coerce')
    df['fechaRealizoAuditoria'] = pd.to_datetime(df['fechaRealizoAuditoria'], errors='coerce')
    return df

def convert_numbers(df, pct_cols):
    try:
        df['ano'].replace('', '0', inplace=True)
        df['ano'].fillna(0, inplace=True)
        df['ano'] = df['ano'].astype(int)

        for col in pct_cols:
            if col in df.columns:
                df[col].fillna('0', inplace=True)
                df[col] = df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False)
                df[col].replace('', '0', inplace=True)
                df[col] = df[col].astype(float) / 100
    except Exception as err:
        log.error(f'convert_numbers: {err}\n{traceback.format_exc()}')
    return df

def drop_rows_empty(df):
    """Elimina filas donde fecha Y nombreProfesional estén vacíos simultáneamente."""
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

def get_columns_rows(df, rename_map):
    try:
        df.rename(columns=rename_map, inplace=True)
        cols = [c for c in rename_map.values() if c in df.columns]
        missing = [c for c in rename_map.values() if c not in df.columns]
        if missing:
            log.warning(f'Columnas esperadas no encontradas en la hoja: {missing}')
        return df[cols]
    except Exception as err:
        log.error(f'get_columns_rows: {err}\n{traceback.format_exc()}')

def validate_load(df):
    if df.shape[0] > 0:
        try:
            import io
            log.info(f'Cargando {df.shape[0]} filas en {TABLA_BIGQUERY} ...')
            # Captura el stdout de loadbq para imprimirlo de última
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
    log.info(f'Inicio del ETL: cirugias')
    log.info(f'Tabla destino : {TABLA_BIGQUERY}')
    log.info(f'Sheet ID      : {ID_SHEET}')
    log.info('=' * 60)

    try:
        df_total = pd.DataFrame()

        for name in LIST_NAME_SHEET:
            inicio_hoja = time.time()
            log.info(f'[{name}] Leyendo hoja ...')

            df = func_process.get_google_sheet(ID_SHEET, name)
            df.columns = df.columns.str.strip()  # elimina espacios al inicio y fin de los headers
            log.info(f'[{name}] Filas leídas desde Google Sheets: {df.shape[0]}')
            log.debug(f'[{name}] Columnas recibidas: {df.columns.tolist()}')

            df = get_columns_rows(df, COLUMN_RENAME_MAP[name])
            log.info(f'[{name}] Columnas seleccionadas: {df.shape[1]}')
            df = drop_rows_empty(df)

            df = convert_dates(df)
            df = convert_numbers(df, PCT_COLS[name])
            df['tipoFormulario'] = name

            duracion_hoja = time.time() - inicio_hoja
            log.info(f'[{name}] Procesamiento completado en {duracion_hoja:.2f}s → {df.shape[0]} filas listas')

            df_total = pd.concat([df_total, df], ignore_index=True)

        log.info(f'Total filas concatenadas de todas las hojas: {df_total.shape[0]}')
        df_total.drop(columns=[c for c in ['correo'] if c in df_total.columns], inplace=True)
        validate_load(df_total)

    except Exception as err:
        log.error(f'execution_load falló: {err}\n{traceback.format_exc()}')



execution_load()