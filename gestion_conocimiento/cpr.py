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
table_name                      = 'cpr'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET = '1sy706dQHMnoIKBT57H6owpCepYxtwxEE3NUKcRQ0mbw'

LIST_NAME_SHEET = [
    'Modelo de remision',
    'Ingreso de auditorias',
    'Auditoría dirigida Sifilis Gestacional y MME',
]

# ---------------------------------------------------------------------------
# NOMBRES DE COLUMNAS POR POSICIÓN (igual patrón que RIAS/RCV)
# ---------------------------------------------------------------------------
COLUMN_NAMES_BY_POSITION = {
    'Modelo de remision': [
        'fechaRealizoAuditoria', 'tipoValidacion', 'fecha', 'mes', 'ano', 'sede',
        'tipoProfesional', 'nombreProfesional', 'cedula', 'historiaClinica', 'percentilRiesgo',
        'edadGestante', 'indagaFUM', 'solicitaTamizajePIE', 'reportaCGR', 'realizaMarcacionGestante',
        'asignaCitaIngreso8Dias', 'solicitaHemogramaCompleto', 'solicitaGlucosaAyuno',
        'solicitaUrocultivo', 'solicitaTamizajeSifilis', 'solicitaVIH', 'solicitaAgSHB',
        'solicitaIgGIgMToxoplasma', 'solicitaTamizajeElisaChagas', 'indagaVacunacionRubeola',
        'indagaHemoclasificacion', 'solicitaGramFlujoVaginal', 'realizaAsesoriaPrePostVIH',
        'cumpleProtocoloPruebaRapidaSifilis', 'indagaVidaSexualRiesgo', 'formulaPreservativos',
        'brindaEducacionPrevencionITS', 'registraEducacionSignosAlarma',
        'formulaMicronutrientes', 'brindaInformacionIVE', 'solicitaEcografia8a10Semanas',
        'analisisYPlan', 'pertinenciaLaboratorio', 'estructuraHC', 'observaciones',
        'calificacionPertinenciaLaboratorio', 'calificacionGuiaMedica',
        'calificacionMedicoEstructura', 'calificacionMedico', 'validacionDatos', 'correo',
    ],
    'Ingreso de auditorias': [
        'fechaRealizoAuditoria', 'fecha', 'tipoValidacion', 'mes', 'ano', 'sede',
        'tipoProfesional', 'diagnostico', 'nombreProfesional', 'cedula', 'historiaClinica',
        'percentilRiesgo', 'edadGestante', 'registraDatosContactoActualizados',
        'embarazoPlaneado', 'numeroControlesRealizados', 'semanasGestacionIngresoCPN',
        'registraValoracionRiesgoPsicosocial', 'causaIngresoTardio', 'indagaSiPlanificaba',
        'formuloSuplementacionDosisAdecuadas', 'registraConsumoToleranciaMicronutrientes',
        'indicacionesTempranaSuplementacion', 'hemogramaIngreso', 'hemogramaDespuesSemana28',
        'buscoCausasAnemia', 'prescripcionTratamientoHierro', 'ordenoHbControlPostratamiento',
        'resultadoCitologia', 'gramDirectoFlujoVaginalIngreso', 'registraRHNegativoMadre',
        'solicitudCoombsIndirecto', 'solicitudInmunoglobulinaAntiD',
        'registraInterveccionHipertensivas', 'administracionAspirinaPreeclampsia',
        'glucosaIngreso', 'pruebaToleranciaGlucosa', 'diagnosticoDiabetesGestacional',
        'urocultivoIngreso', 'tratamientoUrocultivoAnormal', 'urocultivoSeguimiento',
        'remisionObstetricaRecidiva', 'pruebasIgGIgMToxoplasma', 'seguimientoMensualIgM',
        'igGRubeolaIngreso', 'serologiaSifilisIngreso', 'pruebaTreponemicaSemana13a28',
        'vdrlPlanChoqueSemana28a32', 'cumpleProtocoloSifilisTercerTrimestre',
        'resultadoVIHAntesSemana28', 'asesoriaPruebaVoluntariaVIH', 'vihPrueba1Semana13a28',
        'resultadoVIHTercerTrimestre', 'resultadoAgsHB', 'verificaResultadoPositivoITS',
        'atencionEspecificaInfeccion', 'indagaRiesgosITS', 'gramFlujoVaginalSemana28a40',
        'cultivoRectovaginalEstreptococo', 'solicitaEcografiasGuiaCPR',
        'revisaEcografiaTamizajeGenetico', 'solicitaTSH', 'diligenciaTablaGananciaAlturaUterina',
        'prescripcionEcografiaControlAlteracion', 'indagaConsumoSPAAlcohol',
        'derivacionTrastornoSPA', 'evaluaExposicionViolencias', 'derivacionExposicionViolencias',
        'abordajeOtrasPatologias', 'asesoriaIVE', 'remiteRiesgoInminente',
        'identificaRiesgoDepresionPostparto', 'remisionGinecoobstetricia',
        'registraVacunacionTdInfluenza', 'verificoCriteriosTromboprofilaxis',
        'evaluaCavidadOral', 'direccionoVacunacionFaltante', 'valoracionOdontologo',
        'aplicaPruebaASSIST', 'calculaIMCInterpretacion', 'calculoIMCPregestacional',
        'establecesMetasGananciaPeso', 'evaluaGananciaPesoValoracion',
        'registraPatronAlimentario', 'remitidaNutricionista', 'remiteRiesgoPsicologia',
        'registraInformacionSignosAlarma', 'registraInformacionAnticoncepcion',
        'invitaCursoMaternidadPaternidad', 'diligenciaHistoriaClinicaPerinatal',
        'controlesAsignadosPeriodicidad', 'revisionesOportunasPostparto',
        'enfoquePostpartoSegunRiesgo', 'verificoRiesgoSeptico',
        'verificoOrdenoPlanificacionFamiliar', 'examenFisicoRecienNacido',
        'revisionRecienNacido3a5Dias', 'detectaCataratasCongenita',
        'indagaTipoAlimentacion', 'verificaLactanciaMaternaExclusiva',
        'verificoTecnicaLactanciaMaterna', 'estimuloMadreCorregirDificultades',
        'registraProgresionPesoRecienNacido', 'intervencionAlteracionAntropometrica',
        'registroTSHNeonatal', 'intervencionTSHAnormal', 'registroHemoclasificacionNeonatal',
        'validaAplicacionInmunoglobulinaAntiD', 'planAtencionAlteracionesNeonato',
        'revisionTamizajeCardiopatiaCongenita', 'recomendacionesMadreSignosAlarma',
        'suministroHierroRecienNacido', 'recomendacionesAlimentacionLibreDemanda',
        'derivacionRutaPrimeraInfancia', 'orientacionRegistroCivil', 'envioCarneControlPrenatal',
        'analisisYPlan', 'estructuraHC', 'hallazgoMasImportante',
        'calificacionGuiaMedica', 'calificacionesSi137', 'calificacionesNo137',
        'calificacionesSi068', 'calificacionesNo068', 'calificacionMedicoEstructura',
        'calificacionPertinenciaExamenes', 'calificacionMedico', 'validacionDatos', 'correo',
    ],
    'Auditoría dirigida Sifilis Gestacional y MME': [
        'fecha', 'fechaRealizoAuditoria', 'mesMonitoreo', 'ano', 'examenMonitorear',
        'tipoExamen', 'sede', 'rolProfesional', 'nombreProfesional', 'cedula',
        'historiaClinica', 'condicionSalud', 'diagnostico', 'percentilRiesgo',
        'antecedentesPersonales', 'interrogatorioMedico', 'adherenciaFarmacologica',
        'examenFisico', 'ayudasDiagnosticas', 'recomendacionesGuias', 'manejoIntegral',
        'condicionSegunGuia', 'reconsultaAdecuada', 'remisionAdecuada',
        'gestionParaclinicos', 'referenciaContrareferencia', 'observaciones', 'nota',
        'validacionDatos', 'correo',
    ],
}

# ---------------------------------------------------------------------------
# COLUMNAS DE PORCENTAJE POR PESTAÑA
# ---------------------------------------------------------------------------
PCT_COLS = {
    'Modelo de remision': [
        'calificacionPertinenciaLaboratorio', 'calificacionGuiaMedica',
        'calificacionMedicoEstructura', 'calificacionMedico'
    ],
    'Ingreso de auditorias': [
        'calificacionGuiaMedica', 'calificacionMedicoEstructura',
        'calificacionPertinenciaExamenes', 'calificacionMedico'
    ],
    'Auditoría dirigida Sifilis Gestacional y MME': ['nota'],
}

# ---------------------------------------------------------------------------
# COLUMNA CLAVE PARA drop_rows_empty POR PESTAÑA
# ---------------------------------------------------------------------------
KEY_COL = {
    'Modelo de remision':                            'nombreProfesional',
    'Ingreso de auditorias':                          'nombreProfesional',
    'Auditoría dirigida Sifilis Gestacional y MME':  'nombreProfesional',
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
                df[col] = df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                non_numeric_count = df[col].isna().sum()
                if non_numeric_count > 0:
                    log.warning(f'{col}: {non_numeric_count} valores no numéricos convertidos a NULL')
                df[col] = df[col] / 100
    except Exception as err:
        log.error(f'convert_numbers: {err}\n{traceback.format_exc()}')
    return df

def rename_by_position(df, expected_names, sheet_name):
    actual_count = df.shape[1]
    expected_count = len(expected_names)
    if actual_count != expected_count:
        log.warning(
            f'[{sheet_name}] Número de columnas distinto al esperado: '
            f'{actual_count} recibidas vs {expected_count} esperadas. '
            f'Se renombran solo las primeras {min(actual_count, expected_count)}.'
        )
    n = min(actual_count, expected_count)
    new_columns = list(expected_names[:n]) + list(df.columns[n:])
    df.columns = new_columns
    return df

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
    log.info(f'Inicio del ETL: cpr')
    log.info(f'Tabla destino : {TABLA_BIGQUERY}')
    log.info(f'Sheet ID      : {ID_SHEET}')
    log.info('=' * 60)

    try:
        df_total = pd.DataFrame()

        for name in LIST_NAME_SHEET:
            inicio_hoja = time.time()
            log.info(f'[{name}] Leyendo hoja ...')

            try:
                df = func_process.get_google_sheet(ID_SHEET, name)
            except Exception as err:
                log.error(f'[{name}] Falló la lectura desde Google Sheets: {err}\n{traceback.format_exc()}')
                log.warning(f'[{name}] Se omite esta hoja y se continúa con las demás.')
                continue

            log.info(f'[{name}] Filas leídas desde Google Sheets: {df.shape[0]}')

            df = rename_by_position(df, COLUMN_NAMES_BY_POSITION[name], name)
            log.info(f'[{name}] Columnas renombradas: {df.shape[1]}')

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