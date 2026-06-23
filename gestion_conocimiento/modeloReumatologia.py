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
table_name                      = 'modeloReumatologia'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET = '1PEocc_Mcy-QNPlB5PUY1ZNAoYQm1kCEBw49jwlbn6RE'

LIST_NAME_SHEET = [
    'Medico y quimico',
    'Enfermería',
    'Nutrición y psicología',
]

# ---------------------------------------------------------------------------
# MAPEO POR PESTAÑA
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Medico y quimico': {
        'Tipo de validación':                    'tipoValidacion',
        'Fecha':                                  'fecha',
        'Fecha en la que realizo la auditoria (dd/mm/año)': 'fechaRealizoAuditoria',
        'Mes':                                    'mes',
        'Año':                                     'ano',
        'Sede':                                    'sede',
        'Historia Clinica':                        'historiaClinica',
        'Sexo':                                     'sexo',
        'Edad':                                     'edad',
        'Diagnostico':                              'diagnostico',
        'Perfil':                                   'perfil',
        'Nombre del profesional (Médico(a))':       'nombreProfesional',
        'Cedula médico':                            'cedulaMedico',
        'El medico ordena examenes - Hemograma  con VSG, Creatinina, Alanino amino transferasa (ALT), Proteína  C Reactiva , Radiografía de Manos y pies  comparativas, Anticuerpos  Anticitrulinas y Factor Reumatoideo': 'ordenaExamenesGenerales',
        'Verifica la marcación correcta del paciente': 'verificaMarcacionPaciente',
        'El medico ordena examen de Hemograma':     'ordenaHemograma',
        'El medico ordena examen de VSG':            'ordenaVSG',
        'El medico ordena examen de Creatinina':     'ordenaCreatinina',
        'El medico ordena examen de Alanino amino transferasa (ALT)': 'ordenaALT',
        'El medico ordena examen de Proteína  C Reactiva': 'ordenaProteinaCReactiva',
        'El medico ordena examen de Radiografía de Manos': 'ordenaRadiografiaManos',
        'El medico ordena examen de Radiografía de pies comparativas': 'ordenaRadiografiaPies',
        'El medico ordena examen de Anticuerpos  Anticitrulinas': 'ordenaAnticuerposAnticitrulinas',
        'El medico ordena examen de Factor Reumatoideo': 'ordenaFactorReumatoideo',
        'El medico ordena examen de uroanálisis':    'ordenaUroanalisis',
        'El medico ordena AST':                       'ordenaAST',
        'El medico ordena examen de DNA nativo anticuerpos': 'ordenaDNANativo',
        'El medico ordena examen de Complemento C3 y C4': 'ordenaComplementoC3C4',
        'El medico ordena examen de Proteínas en orina de 24 horas': 'ordenaProteinasOrina24h',
        'El medico ordena examen de Vitamina D':     'ordenaVitaminaD',
        'Ingreso al programa por médico  - Calcula el DAS 28.': 'calculaDAS28',
        'Ingreso al programa por médico  - Calcula el HAQ': 'calculaHAQ',
        'Cumple si calcula la tasa de filtración glomerular': 'calculaFiltracionGlomerular',
        'Calcula el nivel de actividad de lupus.':   'calculaActividadLupus',
        'Control Médico - Realiza anamnesis':        'realizaAnamnesis',
        'Control Médico - Revisa los exámenes de laboratorio y la historia clínica del paciente.': 'revisaExamenesHC',
        'Control Médico - Realiza el examen físico completo.': 'examenFisicoCompleto',
        'Control Médico - Solicitar exámenes complementarios cuando sea necesario.': 'solicitaExamenesComplementarios',
        'Control Médico - El médico evalúa las metas terapéuticas basado en la guía de práctica clínica, así como los indicadores intermedios (índice de masa corporal, actividad física, etc.) medicación, e indicaciones generales.': 'evaluaMetasTerapeuticas',
        'Control Médico - Genera remisión al equipo de apoyo (nutrición,  enfermería, actividad educativa de inmuno artropatías, químico farmacéutico, terapia física y/o psicología en caso de ser requerido).': 'generaRemisionEquipoApoyo',
        'remite según pertenencia a terapia física u Ocupacional': 'remiteTerapiaFisica',
        'Cumple si se indaga por FUM y si la mujer utiliza algún método anticonceptivo, (aplica en mujeres menores de 50 años y con vida sexual activa). Intervenir cuando se identifique riesgo': 'indagaFUMAnticonceptivo',
        'Programa el siguiente control de acuerdo con la periodicidad establecida en el programa.': 'programaSiguienteControl',
        'Entrega al usuario y su familia signos de alarma, recomendaciones generales y/o laborales si aplican.': 'entregaSignosAlarma',
        'Le recuerda al usuario los canales de contacto del programa (chat, correo electrónico, línea telefónica) para comunicarse en caso de alguna necesidad.': 'recuerdaCanalesContacto',
        'Ajusta el RAF según la necesidad, genera MIPRES de ser necesario.': 'ajustaRAFMipres',
        'Nombre químico farmacéutico':                'nombreQuimicoFarmaceutico',
        'Cedula químico farmacéutico':                'cedulaQuimicoFarmaceutico',
        'El químico farmacéutico realiza búsqueda activo o el personal asistencial remite a la consulta farmacéutica a pacientes, con diagnóstico de inmunoartropatía que no han tenido al menos una valoración en el último año': 'busquedaActivaConsultaFarmaceutica',
        'Durante la Consulta se diligencia el formato de Consulta Farmacéutica': 'diligenciaFormatoConsultaFarmaceutica',
        'Se realiza el análisis de la historia clínica y los datos obtenidos en el desarrollo de la consulta.': 'analisisHistoriaClinica',
        'Se determina el grado de control de la enfermedad         ( clinimetría) y se establecen estrategias para alcanzarlo': 'determinaGradoControl',
        'Establecer las sospechas de inseguridad que el paciente esté presentando  con el tratamiento: RNM,  PRM, efectos secundarios.': 'estableceSospechasInseguridad',
        'Sensibilización del paciente: Se explica al paciente las razones por la que fue remitido al Programa.': 'sensibilizacionPaciente',
        'Determinación de la adherencia terapéutica según el test de Morinsky Green': 'adherenciaMorinskyGreen',
        'Apoyo ( educación) para lograr la adherencia al tratamiento: se establecen estrategias para aumentar la adherencia (como la relación de la toma de medicamentos con actividades cotidianas realizadas por el paciente y la concientización acerca de la importancia de la adherencia)': 'apoyoAdherenciaTratamiento',
        'Educación sobre el uso correcto de los medicamentos: se educa a los pacientes en cuanto al uso correcto de la farmacoterapia, lo que incluye recomendaciones de uso racional de medicamentos y personalización del horario de la terapia medicamentosa de acuerdo a los requerimientos de cada paciente': 'educacionUsoMedicamentos',
        'Comprensión de la enfermedad: se explica los aspectos principales relacionados con la enfermedad por medio de respuesta a preguntas como: ¿Qué es?, ¿qué síntomas presenta?, ¿cuáles son las consecuencias de no controlarla? ¿para qué sirven los medicamentos?': 'comprensionEnfermedad',
        'Acordar con el médico un plan de actuación y desarrollar las intervenciones necesarias para resolver los RNM y/o PRM que el paciente pueda estar presentando. Debe registrarse la intervención en la historia clínica del paciente. Si se detectan RAM se debe reportar en el formato de Reporte de reacciones adversas del Programa de Farmacovigilancia': 'planActuacionRNMPRM',
        'Pertinencia de examenes':                   'pertinenciaExamenes',
        'Estructura de Historia Clinica - ¿Se diligenció completamente el formato de historia clínica para la atención de pacientes con reumatología?': 'estructuraHC',
        'Calificación guía medico(a)':                'calificacionGuiaMedico',
        'Guia si 2.13':                                'guiaSi213',
        'Guia no 2.13':                                'guiaNo213',
        'Guia si doble 4.26':                          'guiaSiDoble426',
        'Guia no doble 4.26':                          'guiaNoDoble426',
        'Calificación medico estructura':             'calificacionMedicoEstructura',
        'Calificación pertinencia de examenes':       'calificacionPertinenciaExamenes',
        'Promedio de calificación medico y quimico':  'promedioCalificacionMedicoQuimico',
        'Calificación quimico(a) farmacéutico':       'calificacionQuimicoFarmaceutico',
        'Observaciones':                              'observaciones',
        'Validación de datos':                        'validacionDatos',
        'Envío de correo':                            'correo',
    },
    'Enfermería': {
        'Tipo de validación':                    'tipoValidacion',
        'Fecha':                                  'fecha',
        'Fecha en la que realizo la auditoria (dd/mm/año)': 'fechaRealizoAuditoria',
        'Mes':                                     'mes',
        'Año':                                      'ano',
        'Sede':                                     'sede',
        'Nombre del profesional':                   'nombreProfesional',
        'Cedula':                                    'cedula',
        'Historia Clinica':                          'historiaClinica',
        'Sexo':                                      'sexo',
        'Edad':                                      'edad',
        'Diagnostico':                               'diagnostico',
        'Realiza anamnesis, el HAQ (si han pasado 6 o más meses) y consigna los datos en enfermedad actual.': 'realizaAnamnesisHAQ',
        'Realizar consulta de enfermería abordando los factores de riesgo,  verificando la adherencia al tratamiento y posibles complicaciones.': 'consultaEnfermeriaFactoresRiesgo',
        'Indaga por sintomatologia respiratoria y en caso de ser necesario solicita BK seriado': 'indagaSintomatologiaRespiratoria',
        'Repetir la medicación de las personas controladas y sin complicaciones.': 'repiteMedicacion',
        'La enfermera establece las metas relacionadas con el autocuidado de común acuerdo con el paciente. (Se deben establecer metas de corto plazo alcanzables y progresivas, que conduzcan, en el mediano y largo plazo, al logro de las metas terapéuticas y los indicadores intermedios.': 'estableceMetasAutocuidado',
        'Encaminar a los pacientes, según la periodicidad descrita en la guía de práctica clínica, de acuerdo con la especificidad de cada caso (con mayor frecuencia para individuos sin adherencia al tratamiento, los de difícil control, pacientes con terapia biológica o con co-morbilidades) para consulta con el médico del equipo.': 'encaminaPacientesPeriodicidad',
        'Entrega al usuario y su familia signos de alarma, recomendaciones generales y/o laborales si aplican (enfermero(a)).': 'entregaSignosAlarmaEnfermera',
        'Le recuerda al usuario los canales de contacto del programa (chat, correo electrónico, línea telefónica) para comunicarse en caso de alguna necesidad (enfermero(a))': 'recuerdaCanalesContactoEnfermera',
        'Se ordena examen de Hemograma':              'ordenaHemograma',
        'El medico ordena examen de VSG':              'ordenaVSG',
        'Se ordena examen de Creatinina':              'ordenaCreatinina',
        'Se ordena examen de Alanino amino transferasa (ALT)': 'ordenaALT',
        'Se ordena examen de Proteína  C Reactiva':     'ordenaProteinaCReactiva',
        'Se ordena examen de Radiografía de Manos':    'ordenaRadiografiaManos',
        'Se ordena examen de Radiografía de pies comparativas': 'ordenaRadiografiaPies',
        'Se ordena examen de Anticuerpos  Anticitrulinas': 'ordenaAnticuerposAnticitrulinas',
        'Se ordena examen de Factor Reumatoideo':      'ordenaFactorReumatoideo',
        'En mujeres entre los 18 y 69 años, con inicio de vida sexual. Cumple si se evidencia citologías realizadas de acuerdo al esquema.': 'citologiasSegunEsquema',
        'Cumple si se registra  en HC la mamografía. Aplica en  mujeres de 50 años  hasta los 69 años. (Máximo 2 años)': 'registraMamografia',
        'Cumple si se registra en la HC la educación del autoexamen de seno a partir de los 20 años': 'educacionAutoexamenSeno',
        'Cumple si se registra  en la HC la indagatoria de vida sexual activa, y recomendar el uso del preservativo para la prevención de ITS si aplica': 'indagatoriaVidaSexualActiva',
        'Cumple si se indaga  si la mujer utiliza algún método anticonceptivo, (aplica en mujeres menores de 50 años y con vida sexual activa). Intervenir cuando se identifique riesgo': 'indagaMetodoAnticonceptivo',
        'Solicita sangre oculta en heces a pacientes de 50 A 75 años cada 5 años': 'solicitaSangreOcultaHeces',
        'Cumple si solicito PSA (Aplica para hombres entre 50 a 75 cada 5 años)': 'solicitaPSA',
        'Cumple si se registra en la historia clínica los valores de presión arterial.': 'registraPresionArterial',
        'En caso de presión arterial alterada, cumple si se registra intervención.': 'intervencionPresionArterial',
        'Cumple si se registra el IMC,  examen físico completo incluido: revisión de cuello, cardiopulmonar y abdomen.': 'registraIMCExamenFisico',
        'Cumple si se registra intervención en caso de identificar el estado nutricional alterado': 'intervencionEstadoNutricional',
        'Estructura de Historia Clinica - ¿Se diligenció completamente el formato de historia clínica para la atención de pacientes con reumatología?': 'estructuraHC',
        'Promedio de calificación enfermera':          'promedioCalificacionEnfermera',
        'Guía calificación enfermera':                 'guiaCalificacionEnfermera',
        'Guia si 4.35':                                  'guiaSi435',
        'Guia no 4.35':                                  'guiaNo435',
        'Guia si doble 8.7':                             'guiaSiDoble87',
        'Guia no doble 8.7':                             'guiaNoDoble87',
        'Calificación estructura de HC':                'calificacionEstructuraHC',
        'Calificación de pertinencia de examenes':       'calificacionPertinenciaExamenes',
        'OBSERVACIONES':                                 'observaciones',
        'Validación de datos':                           'validacionDatos',
        'Envío de correo':                               'correo',
    },
    'Nutrición y psicología': {
        'Fecha':                                  'fecha',
        'Fecha en la que realizo la auditoria (dd/mm/año)': 'fechaRealizoAuditoria',
        'Mes':                                     'mes',
        'Año':                                      'ano',
        'Sede':                                     'sede',
        'Historia Clinica':                         'historiaClinica',
        'Sexo':                                      'sexo',
        'Edad':                                      'edad',
        'Diagnostico':                               'diagnostico',
        'Perfil':                                    'perfil',
        'CEDULA CONCATENADA':                         'cedulaConcatenada',
        'Nombre del nutricionista':                   'nombreNutricionista',
        'Cedula nutricionista':                       'cedulaNutricionista',
        'Realiza anamnesis alimentaria y realiza la toma de medidas antropométricas.': 'realizaAnamnesisAlimentaria',
        'Realiza el diagnóstico nutricional del paciente.': 'realizaDiagnosticoNutricional',
        'Establece la intervención nutricional de común acuerdo con el paciente. (Se deben establecer metas de corto plazo alcanzables y progresivas, que conduzcan, en el mediano y largo plazo, al logro de las metas terapéuticas y los indicadores intermedios.': 'estableceIntervencionNutricional',
        'Entrega al usuario y su familia signos de alarma, recomendaciones generales y/o laborales si aplican (nutricionista).': 'entregaSignosAlarmaNutricionista',
        'Le recuerda al usuario los canales de contacto del programa (chat, correo electrónico, línea telefónica) para comunicarse en caso de alguna necesidad (nutricionista).': 'recuerdaCanalesContactoNutricionista',
        'Nombre del psicologo':                        'nombrePsicologo',
        'Cedula psicologo':                            'cedulaPsicologo',
        'Realiza una aproximación diagnóstica':        'realizaAproximacionDiagnostica',
        'Plantea una propuesta de intervención sobre el problema o problemas detectados al paciente.': 'planteaPropuestaIntervencion',
        'Inicio de la intervención, puesta en marcha de las estrategias terapéuticas acordadas y evaluación durante su realización.': 'inicioIntervencion',
        'Finalización de la intervención y evaluación posterior de los resultados alcanzados.': 'finalizacionIntervencion',
        'Entrega al usuario y su familia signos de alarma, recomendaciones generales y/o laborales si aplican (psicologo).': 'entregaSignosAlarmaPsicologo',
        'Le recuerda al usuario los canales de contacto del programa (chat, correo electrónico, línea telefónica) para comunicarse en caso de alguna necesidad (psicologo).': 'recuerdaCanalesContactoPsicologo',
        'Estructura de Historia Clinica - ¿Se diligenció completamente el formato de historia clínica para la atención de pacientes con reumatología?': 'estructuraHC',
        'promedio de calificación':                    'promedioCalificacion',
        'Calificación nutricionista':                  'calificacionNutricionista',
        'Calificación psicologia':                     'calificacionPsicologia',
        'Calificación medico estructura':              'calificacionMedicoEstructura',
        'Observaciones':                               'observaciones',
        'Validación de datos':                         'validacionDatos',
        'Envío de correo':                             'correo',
    },
}

# ---------------------------------------------------------------------------
# COLUMNAS DE PORCENTAJE POR PESTAÑA
# ---------------------------------------------------------------------------
PCT_COLS = {
    'Medico y quimico': [
        'calificacionGuiaMedico', 'calificacionMedicoEstructura',
        'calificacionPertinenciaExamenes', 'promedioCalificacionMedicoQuimico',
        'calificacionQuimicoFarmaceutico'
    ],
    'Enfermería': [
        'promedioCalificacionEnfermera', 'guiaCalificacionEnfermera',
        'calificacionEstructuraHC', 'calificacionPertinenciaExamenes'
    ],
    'Nutrición y psicología': [
        'promedioCalificacion', 'calificacionNutricionista',
        'calificacionPsicologia', 'calificacionMedicoEstructura'
    ],
}

# ---------------------------------------------------------------------------
# COLUMNA CLAVE PARA drop_rows_empty POR PESTAÑA
# ---------------------------------------------------------------------------
KEY_COL = {
    'Medico y quimico':       'nombreProfesional',
    'Enfermería':             'nombreProfesional',
    'Nutrición y psicología': 'nombreNutricionista',
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
        if 'ano' in df.columns:
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
    log.info(f'Inicio del ETL: modeloReumatologia')
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