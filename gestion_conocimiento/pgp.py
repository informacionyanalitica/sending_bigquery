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
table_name                      = 'pgp'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET = '1jSNQmvFtB3jI85QyDJD5DTHthrYS_8_SCh9DJvHzOUA'

LIST_NAME_SHEET = [
    'Procedimientos Terapeuticos',
    'Procedimientos DX',
    'Cirugia',
    'Consultas',
]

# ---------------------------------------------------------------------------
# MAPEO POR PESTAÑA
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Procedimientos Terapeuticos': {
        'Fecha (dd/mm/aaaa)':                    'fecha',
        'Fecha en la que realizó la auditoría (dd/mm/año)': 'fechaRealizoAuditoria',
        'Examen a Monitorear':                   'examenMonitorear',
        'Mes':                                   'mes',
        'Año':                                   'ano',
        'Sede':                                  'sede',
        'Concepto Clinico Virtual':               'conceptoClinicoVirtual',
        'Nombre del profesional':                'nombreProfesional',
        'Cedula':                                'cedula',
        'Historia Clinica':                      'historiaClinica',
        'Diagnóstico (Nombre)':                  'diagnostico',
        'Identificación - Responsable o Acompañante': 'identificacionResponsable',
        'Identificación - Telefono':             'identificacionTelefono',
        'Identificación - Ocupacion':            'identificacionOcupacion',
        'Consentimiento Informado':              'consentimientoInformado',
        'Registra tipo de Procedimiento':        'registraTipoProcedimiento',
        'Examen fisico - Estado General':        'examenFisicoEstadoGeneral',
        'Describe el Motivo de consulta':        'motivoConsulta',
        'Examen fisico -enfocado en la especialidad': 'examenFisicoEspecialidad',
        'Registra Signos Vitales':               'registraSignosVitales',
        'La orden fue realizada por el especialista': 'ordenRealizadaEspecialista',
        'Registra Medicamentos Aplicados':       'registraMedicamentosAplicados',
        'Dosis de medicamentos aplicados':       'dosisMedicamentos',
        'Reaccion a medicamentos Aplicados':     'reaccionMedicamentos',
        'Recomendaciones':                       'recomendaciones',
        'Administración de profilaxis - ¿Se le administró profilaxis antibiótica al paciente programado antes de la incisión quirúrgica?': 'profilaxisAdministrada',
        'Momento de administración - ¿ La administración de profilaxis antimicrobiana se realizó antes de la incisión (mínimo 120 minutos y dependiendo de la vida media del antibiótico)?': 'profilaxisMomento',
        'Antimicrobiano administrado - ¿La profilaxis antimicrobiana fue apropiada (antibiótico y dosis)?': 'profilaxisAntimicrobiano',
        'Redosificación intraoperatoria - Bajo las condiciones del paciente y las características del procedimiento, ¿ es necesaria la aplicación de dosis intra operatoria?': 'profilaxisRedosificacion',
        'Profilaxis posoperatoria - ¿Se prolongó la profilaxis antibiótica 24 horas despues de finalizado el procedimiento?': 'profilaxisPostoperatoria',
        'Se revisan antecedentes personales y se gestionan en caso de ser necesario': 'antecedentesPersonales',
        'Se realiza interrogatorio mèdico completo': 'interrogatorioMedico',
        'se valida adherencia a manejos previos, prescripción adecuada de manejo farmacológico, dosificación y posología adecuada, tiempo de tratamiento, prescripción de medicamentos de primera línea.': 'adherenciaFarmacologica',
        'Se realiza exámen físico completo y adecuado': 'examenFisico',
        'Las ayudas diagnósticas se encuentran acorde a la condición de salud': 'ayudasDiagnosticas',
        'Las recomendaciones entregadas al paciente son acorde a lo establecido en guías protocolos': 'recomendacionesGuias',
        'Se maneja el paciente de manera integral: si requiere remisión a algún programa se realiza': 'manejoIntegral',
        'La condición en salud se manejó según guìa establecida del asegurador o protocolo institucional de Coopsana IPS (aplica sí está condiciòn en salud tiene guía establecida, si no es así colocar N/A).': 'condicionSegunGuia',
        'El médico resolvió de acuerdo a las guías establecidas la condición de salud del paciente mediante un enfoque integral?': 'resolucionGuias',
        'Sí se hizo remisiòn o el CCE a alguna especialidad, es adecuada?': 'remisionAdecuada',
        'Se realiza gestión de los paraclínicos solicitados y del CEE si aplica.': 'gestionParaclinicos',
        'La referencia y contrareferencia es adecuada en caso de que aplique': 'referenciaContrareferencia',
        'indique si la reconsulta del paciente fue causa administrativa': 'reconsultaCausaAdministrativa',
        'Indiquie si el paciente tiene alguna exclusión del PGP': 'exclusionPGP',
        'Indique si la orden se gestionó de acuerdo al modelo': 'ordenSegunModelo',
        'Analisis y plan':                        'analisisYPlan',
        'Pertinencia Ayudas DX':                  'pertinenciaAyudasDx',
        'Calificación Administrativos':           'calificacionAdministrativos',
        'Calificación Final':                     'calificacionFinal',
        'Calificación Atención Completa del Medico': 'calificacionAtencionMedico',
        'Calificación Pertinencia Ayuda DX':      'calificacionPertinenciaAyudaDx',
        'Observaciones':                          'observaciones',
        'Validación de la Base de Datos':         'validacionDatos',
        'Envio de correo':                        'correo',
    },
    'Procedimientos DX': {
        'Fecha (dd/mm/aaaa)':                    'fecha',
        'Fecha en la que realizó la auditoría (dd/mm/año)': 'fechaRealizoAuditoria',
        'Examen a Monitorear':                   'examenMonitorear',
        'Mes Monitoreo':                         'mesMonitoreo',
        'Año':                                   'ano',
        'Sede':                                  'sede',
        'Concepto Clinico Virtual':               'conceptoClinicoVirtual',
        'Nombre del profesional':                'nombreProfesional',
        'Cedula':                                'cedula',
        'Historia Clinica':                      'historiaClinica',
        'Diagnóstico (Nombre)':                  'diagnostico',
        'Identificación - Responsable o Acompañante': 'identificacionResponsable',
        'Identificación - Telefono':             'identificacionTelefono',
        'Identificación - Ocupacion':            'identificacionOcupacion',
        'Consentimiento Informado':              'consentimientoInformado',
        'Registra tipo de Procedimiento':        'registraTipoProcedimiento',
        'Examen fisico - Estado General':        'examenFisicoEstadoGeneral',
        'Describe el Motivo de consulta':        'motivoConsulta',
        'Examen fisico -enfocado en la especialidad': 'examenFisicoEspecialidad',
        'Registra Signos Vitales':               'registraSignosVitales',
        'La orden fue realizada por el especialista': 'ordenRealizadaEspecialista',
        'Registra Medicamentos Aplicados':       'registraMedicamentosAplicados',
        'Dosis de medicamentos aplicados':       'dosisMedicamentos',
        'Reaccion a medicamentos Aplicados':     'reaccionMedicamentos',
        'Recomendaciones':                       'recomendaciones',
        'Administración de profilaxis - ¿Se le administró profilaxis antibiótica al paciente programado antes de la incisión quirúrgica?': 'profilaxisAdministrada',
        'Momento de administración - ¿ La administración de profilaxis antimicrobiana se realizó antes de la incisión (mínimo 120 minutos y dependiendo de la vida media del antibiótico)?': 'profilaxisMomento',
        'Antimicrobiano administrado - ¿La profilaxis antimicrobiana fue apropiada (antibiótico y dosis)?': 'profilaxisAntimicrobiano',
        'Redosificación intraoperatoria - Bajo las condiciones del paciente y las características del procedimiento, ¿ es necesaria la aplicación de dosis intra operatoria?': 'profilaxisRedosificacion',
        'Profilaxis posoperatoria - ¿Se prolongó la profilaxis antibiótica 24 horas despues de finalizado el procedimiento?': 'profilaxisPostoperatoria',
        'Se revisan antecedentes personales y se gestionan en caso de ser necesario': 'antecedentesPersonales',
        'Se realiza interrogatorio mèdico completo': 'interrogatorioMedico',
        'se valida adherencia a manejos previos, prescripción adecuada de manejo farmacológico, dosificación y posología adecuada, tiempo de tratamiento, prescripción de medicamentos de primera línea.': 'adherenciaFarmacologica',
        'Se realiza exámen físico completo y adecuado': 'examenFisico',
        'Las ayudas diagnósticas se encuentran acorde a la condición de salud': 'ayudasDiagnosticas',
        'Las recomendaciones entregadas al paciente son acorde a lo establecido en guías protocolos': 'recomendacionesGuias',
        'Se maneja el paciente de manera integral: si requiere remisión a algún programa se realiza': 'manejoIntegral',
        'La condición en salud se manejó según guìa establecida del asegurador o protocolo institucional de Coopsana IPS (aplica sí está condiciòn en salud tiene guía establecida, si no es así colocar N/A).': 'condicionSegunGuia',
        'El médico resolvió de acuerdo a las guías establecidas la condición de salud del paciente mediante un enfoque integral?': 'resolucionGuias',
        'Sí se hizo remisiòn o el CCE a alguna especialidad, es adecuada?': 'remisionAdecuada',
        'Se realiza gestión de los paraclínicos solicitados y del CEE si aplica.': 'gestionParaclinicos',
        'La referencia y contrareferencia es adecuada en caso de que aplique': 'referenciaContrareferencia',
        'indique si la reconsulta del paciente fue causa administrativa': 'reconsultaCausaAdministrativa',
        'Indiquie si el paciente tiene alguna exclusión del PGP': 'exclusionPGP',
        'Indique si la orden se gestionó de acuerdo al modelo': 'ordenSegunModelo',
        'Analisis y plan':                        'analisisYPlan',
        'Pertinencia Ayudas DX':                  'pertinenciaAyudasDx',
        'Calificación Administrativos':           'calificacionAdministrativos',
        'Calificación Final':                     'calificacionFinal',
        'Calificación Atención Completa del Medico': 'calificacionAtencionMedico',
        'Calificación Pertinencia Ayuda DX':      'calificacionPertinenciaAyudaDx',
        'Observaciones':                          'observaciones',
        'Validación de la Base de Datos':         'validacionDatos',
        'Envio de correo':                        'correo',
    },
    'Cirugia': {
        'Fecha (dd/mm/aaaa)':                    'fecha',
        'Fecha en la que realizó la auditoría (dd/mm/año)': 'fechaRealizoAuditoria',
        'Guia Medica dx':                        'guiaMedicaDx',
        'Servicio evaluado':                     'servicioEvaluado',
        'Mes':                                   'mes',
        'Año':                                   'ano',
        'Sede':                                  'sede',
        'Nombre Profesional que ordena la cirugía': 'nombreProfesionalOrdena',
        'Identificación Profesional que ordena la cirugía': 'cedulaProfesionalOrdena',
        'Nombre medico que opera':                'nombreMedicoOpera',
        'Identificación medico que opera':        'cedulaMedicoOpera',
        'Historia Clinica':                       'historiaClinica',
        'Identificación - Responsable o Acompañante': 'identificacionResponsable',
        'Identificación - Telefono':              'identificacionTelefono',
        'Identificación - Ocupacion':             'identificacionOcupacion',
        'Motivo de consulta':                     'motivoConsulta',
        'Examen fisico - Estado General':         'examenFisicoEstadoGeneral',
        'Examen fisico - Registro Signos Vitales': 'examenFisicoSignosVitales',
        'Descripción Operatoria':                 'descripcionOperatoria',
        'Conducta - remisiones pertinentes':      'conductaRemisiones',
        'Conducta - incapacidad pertinente':      'conductaIncapacidad',
        'Recomendaciones Postquirurgicas':        'recomendacionesPostquirurgicas',
        'Adherencia a guías - medicamentos':      'adherenciaGuiasMedicamentos',
        'Administración de profilaxis - ¿Se le administró profilaxis antibiótica al paciente programado antes de la incisión quirúrgica?': 'profilaxisAdministrada',
        'Momento de administración - ¿ La administración de profilaxis antimicrobiana se realizó antes de la incisión (mínimo 120 minutos y dependiendo de la vida media del antibiótico)?': 'profilaxisMomento',
        'Antimicrobiano administrado - ¿La profilaxis antimicrobiana fue apropiada (antibiótico y dosis)?': 'profilaxisAntimicrobiano',
        '¿El análisis y plan son adecuados?':     'analisisYPlanAdecuados',
        'Aplico la guia medica correspondiente?':  'aplicacionGuiasMedicas',
        'Calificación Administrativos':           'calificacionAdministrativos',
        'Calificación conducta':                  'calificacionConducta',
        'Evaluacion de guias y practicas medica':  'evaluacionGuiasPracticas',
        'Observaciones':                           'observaciones',
        'Validación de la base de datos':          'validacionDatos',
        'Envio de correo':                         'correo',
    },
    'Consultas': {
        'Fecha (dd/mm/aaaa)':                    'fecha',
        'Fecha en la que realizó la auditoría (dd/mm/año)': 'fechaRealizoAuditoria',
        'Servicio evaluado':                     'servicioEvaluado',
        'Mes':                                   'mes',
        'Año':                                   'ano',
        'Sede que genera la orden':               'sedeGeneraOrden',
        'Sede de la atencion':                    'sedeAtencion',
        'Medico':                                 'nombreProfesional',
        'Cedula':                                 'cedula',
        'Historia Clinica':                       'historiaClinica',
        'Modalidad de Consulta':                  'modalidadConsulta',
        'Identificación - Responsable o Acompañante': 'identificacionResponsable',
        'Identificación - Telefono':              'identificacionTelefono',
        'Identificación - Ocupacion':             'identificacionOcupacion',
        'Motivo de consulta':                     'motivoConsulta',
        'Enfermedad actual':                      'enfermedadActual',
        'Enfermedad actual -  Uso de siglas no Universales': 'enfermedadActualSiglas',
        'Elaboracion de revision por sistemas':   'revisionPorSistemas',
        'Registra Antecedentes':                  'registraAntecedentes',
        'Examen fisico - Estado General':         'examenFisicoEstadoGeneral',
        'Registra Signos Vitales':                'registraSignosVitales',
        'Examen fisico - Enfasis Especialidad':   'examenFisicoEspecialidad',
        'Registra Impresión diagnostica':         'registraImpresionDiagnostica',
        'Diagnóstico CIE 10 (Nombre)':             'diagnostico',
        'Conducta - remisiones pertinentes':      'conductaRemisiones',
        'A qué especialidad Remite':               'especialidadRemite',
        'Conducta - incapacidad pertinente':      'conductaIncapacidad',
        'Conducta - procedimientos pertinentes':  'conductaProcedimientos',
        'Tipo de procedimiento':                  'tipoProcedimiento',
        'Ayudas Diagnósticas':                    'ayudasDiagnosticas',
        'Tratameinto Médico':                     'tratamientoMedico',
        'El especialista es quien carga la orden de acuerdo al modelo?': 'ordenSegunModelo',
        'Conducta - recomendaciones pertinentes': 'conductaRecomendaciones',
        'Cumple con referencia contrareferencia': 'referenciaContrareferencia',
        'Adherencia a guías - ayudas diagnosticas': 'adherenciaAyudasDiagnosticas',
        '¿El análisis y plan son adecuados?':     'analisisYPlanAdecuados',
        'Aplico la guia medica correspondiente?': 'aplicacionGuiasMedicas',
        'Auditoria historia clinica':             'auditoriaHistoriaClinica',
        'Calificación Atención Medico':           'calificacionAtencionMedico',
        'Evaluacion de guias y practicas medica': 'evaluacionGuiasPracticas',
        'Observaciones':                          'observaciones',
        'Validación de la base de datos':         'validacionDatos',
        'Envio de correo':                        'correo',
    },
}

# ---------------------------------------------------------------------------
# COLUMNAS DE PORCENTAJE POR PESTAÑA
# ---------------------------------------------------------------------------
PCT_COLS = {
    'Procedimientos Terapeuticos': [
        'calificacionAdministrativos', 'calificacionFinal',
        'calificacionAtencionMedico', 'calificacionPertinenciaAyudaDx'
    ],
    'Procedimientos DX': [
        'calificacionAdministrativos', 'calificacionFinal',
        'calificacionAtencionMedico', 'calificacionPertinenciaAyudaDx'
    ],
    'Cirugia': [
        'calificacionAdministrativos', 'calificacionConducta', 'evaluacionGuiasPracticas'
    ],
    'Consultas': [
        'calificacionAtencionMedico', 'evaluacionGuiasPracticas'
    ],
}

# ---------------------------------------------------------------------------
# COLUMNA CLAVE PARA drop_rows_empty POR PESTAÑA (junto con 'fecha')
# ---------------------------------------------------------------------------
KEY_COL = {
    'Procedimientos Terapeuticos': 'nombreProfesional',
    'Procedimientos DX':           'nombreProfesional',
    'Cirugia':                     'nombreMedicoOpera',
    'Consultas':                   'nombreProfesional',
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

def drop_rows_empty(df, key_col):
    try:
        if df.empty:
            return df
        mask = (
            df['fecha'].isna() | (df['fecha'].astype(str).str.strip() == '')
        ) & (
            df[key_col].isna() | (df[key_col].astype(str).str.strip() == '')
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
    log.info(f'Inicio del ETL: pgp')
    log.info(f'Tabla destino : {TABLA_BIGQUERY}')
    log.info(f'Sheet ID      : {ID_SHEET}')
    log.info('=' * 60)

    try:
        df_total = pd.DataFrame()

        for name in LIST_NAME_SHEET:
            inicio_hoja = time.time()
            log.info(f'[{name}] Leyendo hoja ...')

            df = func_process.get_google_sheet(ID_SHEET, name)
            df.columns = df.columns.str.strip()
            log.info(f'[{name}] Filas leídas desde Google Sheets: {df.shape[0]}')

            df = get_columns_rows(df, COLUMN_RENAME_MAP[name])
            log.info(f'[{name}] Columnas seleccionadas: {df.shape[1]}')

            df = drop_rows_empty(df, KEY_COL[name])
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