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
table_name                      = 'planificacion'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET   = '1VOPsyeN3fV_DB49Zy0pA6sBCcwaz5N8qBdhKOSlOB-I'
NAME_SHEET = 'Ingreso de auditorias'

# ---------------------------------------------------------------------------
# MAPEO DE COLUMNAS → camelCase
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Fecha en la que realizo la auditoria (dd/mm/año)': 'fechaRealizoAuditoria',
    'Tipo de validación':                                'tipoValidacion',
    'Fecha':                                             'fecha',
    'Mes':                                                'mes',
    'Año':                                                'ano',
    'Sede':                                               'sede',
    'Nombre del Colaborador':                             'nombreProfesional',
    'Numero documento':                                   'cedula',
    'Numero de historia Clinica':                         'historiaClinica',
    'Percentil de Riesgo':                                'percentilRiesgo',
    'Edad de la paciente':                                'edadPaciente',
    'Sexo':                                                'sexo',
    'Motivo de consulta':                                 'motivoConsulta',
    'Cumple si en la historia clínica se indaga por edad de la menarca y sexarca': 'indagaMenarcaSexarca',
    'Cumple si en la historia clínica se indaga por preferencia sexual, número de compañeros sexuales y vida sexual activa al momento de la consulta': 'indagaPreferenciaSexual',
    'Cumple si en la historia clínica se indaga por antecedentes Ginecobstetricos de la paciente': 'indagaAntecedentesGinecobstetricos',
    'Cumple si están registrados los antecedentes personales y familiares, haciendo énfasis en enfermedad cardiovascular, cáncer de mama activo.': 'registraAntecedentesPersonalesFamiliares',
    'Cumple si están registrados los antecedentes anticonceptivos (aplica para ingresos e inicios de nuevo metodo para evaluar opciones en la paciente).': 'registraAntecedentesAnticonceptivos',
    'Cumple si están registrados los antecedentes toxicológicos con énfasis en consumo de licor, cigarrillo o sustancias psicoactivas. REALIZA INTERVENCIÓN': 'registraAntecedentesToxicologicos',
    'Cumple Si se evidencia el manejo de patologías asociadas.': 'manejoPatologiasAsociadas',
    'Cumple Si indaga los antecedentes farmacológicos de medicamentos de uso crónico. Na en persoNas Sin patologías crónicas. (Tener en cuenta los que pueden interactuar con el Método Anticonceptivo, disminuyendo su efectividad).': 'indagaAntecedentesFarmacologicos',
    'Cumple Si se registra examen físico completo por sistemas, signos vitales, medidas antropométricas y cálculo de IMC (Mínimo P/A e IMC) y estado de consciencia. Si el método elegido es DIU TCU. Sí sería obligatorio el examen genital con tacto bimanual al momento de la inserción. EN CASO DE ALTERACIONES DE IMC CUMPLE SI SE REALIZA INTERVENCIÓN (educación estilos de vida saludable y remisión a nutrición)': 'examenFisicoCompletoIMC',
    'Cumple Si se registra examen fíSico de mamas al menos una vez en el último año a partir de los 40 años Si hay antecedentes familiares de importancia o Si la paciente refiere alguNa alteración y desde los 40 años se debe realizar revisión una vez al año. Na Si la persoNa que aSiste a la consulta es de sexo masculiNo.': 'examenFisicoMamas',
    'Cumple Si se brindó Información sobre los métodos anticonceptivos disponibles (Sin excluSión alguNa): mecanismo de acción, forma de uso, ventajas, desventajas, riesgos, SigNos de alarma y efectividad de cada uNo de ellos.  (Aplica para ingresos, en metodos reverSibles asesorar en metodos de larga duración)': 'informacionMetodosAnticonceptivos',
    'Se definió Método según Criterios de Elegibilidad de la Organización Mundial de la Salud, brindando apoyo al usuario(a) para la elección del método. (Solo aplica para ingresos).': 'definicionMetodoOMS',
    'Metodo seleccionado (Tipo actual de metodo)':         'metodoSeleccionado',
    'Cumple Si se evidencia el consentimiento informado (Aplica solo para DIU).': 'consentimientoInformadoDIU',
    'Cumple Si la usuaria o usuario sale de la consulta, con la prescripción de un método de protección anticonceptivo. En caso de implante subdérmico, cumple Si el implante se realiza máximo a los 8 días después de la consulta en que lo eligió. En caso de DIU, cumple Si la inserción se realiza lo antes poSible con la certeza de No embarazo. En caso de método definitivo, cumple Si se realizó la remiSión inmediata al servicio. En los tres casos anteriores se le debe suministrar al usuario o usuaria un método temporal (condones u hormoNal temporal) mientras se inicia el método elegido.': 'prescripcionMetodoElegido',
    'Continuidad en el programa - Cumple Si se asigna cita de control o se fomenta actividad grupal de acuerdo al metodo seleccionado.  Los controles se realizan acorde a la periodicidad definida por la Norma según el método de Planificación familiar.': 'continuidadPrograma',
    'Cumple Si presenta indagatoria por Sintomas asociados a la intolerancia del metodo (aplica para controles)': 'indagatoriaIntoleranciaMetodo',
    'Cumple Si se registra la aplicación o consumo habitual de método elegido cuando aplica. (aplica para controles)': 'registraConsumoMetodo',
    'En mujeres entre los 25-29 y 69 años, con inicio de vida sexual. Cumple Si se evidencia citologías realizadas de acuerdo al esquema.': 'citologiasSegunEsquema',
    'Se realiza solicitud de mamografia Aplica en  mujeres de 50 años  hasta los 69 años.': 'solicitudMamografia',
    'Se realiza educacion del autoexamen de seNo':         'educacionAutoexamenSeno',
    'Se realiza remiSion de atencion a salud bucal':        'remisionSaludBucal',
    'Se realiza remiSion al programa de vacuNación':        'remisionVacunacion',
    'Educación para la prevención de ITS - Cumple si se educa a la usuaria acerca de la adecuada utilizacion del preservativo. (Se debe Siempre hablar del riesgo de Infecciones de TransmiSión Sexual (ITS) y de la neceSidad de usar Siempre doble protección)': 'educacionPrevencionITS',
    'Se identifica gestion de demanda inducida y seguimiento para la aSistencia de la paciente al programa según priorización de riesgos': 'gestionDemandaInducida',
    'Ecografía':                                            'ecografia',
    'Frotis de flujo vaginal':                              'frotisFlujoVaginal',
    'Prueba de embarazo':                                   'pruebaEmbarazo',
    'Si requirió paraclínicos para la atención, estos fueron pertinentes? (Se revisa Tamizajes para ITS o los que se derivan de la consulta preconcepcional en este ítem)': 'paraclinicosPertinentes',
    'Realiza captación a programa de RIAS':                 'captacionProgramaRIAS',
    'Analisis y plan':                                       'analisisYPlan',
    'Pertinencia de laboratorio':                            'pertinenciaLaboratorio',
    'ESTRUCTURA DE HISTORÍA CLINICA Se diligenció completamente el formato de historia clínica para la atención de pacientes de planificación familiar': 'estructuraHC',
    'OBSERVACIONES':                                         'observaciones',
    'Calificacion de enfermera(o)':                          'calificacionEnfermera',
    'Calificación guia de enfermeria':                       'calificacionGuiaEnfermeria',
    'Calificaciones de SI con puntuacion de 6.06':           'calificacionesSI606',
    'Calificaciones de NO con puntuacion de 6.06':           'calificacionesNO606',
    'Calificaciones de SI con puntuacion de 3.03':           'calificacionesSI303',
    'Calificaciones de NO con puntuacion de 3.03':           'calificacionesNO303',
    'Calificación estructura enfermeria':                    'calificacionEstructuraEnfermeria',
    'Calificación de pertinencia de examenes':               'calificacionPertinenciaExamenes',
    'Validacion de datos':                                   'validacionDatos',
}

COLUMNS_REQUIRED = list(COLUMN_RENAME_MAP.values())

PCT_COLS = [
    'calificacionEnfermera', 'calificacionGuiaEnfermeria',
    'calificacionEstructuraEnfermeria', 'calificacionPertinenciaExamenes'
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
                df[col] = df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                non_numeric_count = df[col].isna().sum()
                if non_numeric_count > 0:
                    log.warning(f'{col}: {non_numeric_count} valores no numéricos convertidos a NULL')
                df[col] = df[col] / 100
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
    log.info(f'Inicio del ETL: planificacion')
    log.info(f'Tabla destino : {TABLA_BIGQUERY}')
    log.info(f'Sheet ID      : {ID_SHEET}')
    log.info('=' * 60)

    try:
        inicio_hoja = time.time()
        log.info(f'[{NAME_SHEET}] Leyendo hoja ...')

        df = func_process.get_google_sheet(ID_SHEET, NAME_SHEET)
        df.columns = df.columns.str.strip().str.replace('\n', ' ', regex=False).str.replace('"', '', regex=False).str.strip()
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