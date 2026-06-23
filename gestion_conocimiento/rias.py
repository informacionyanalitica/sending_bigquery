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
table_name                      = 'rias'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET = '1X041-O11SPjnmPjdsr-MoAq1I8UEnNuVGGmjSbyQtFk'

LIST_NAME_SHEET = [
    'Auditoria 12 a 17 años',
    'Auditoria 18 a 28 años',
    'Auditoria 29 a 59 años',
    'Auditoria mayor 60 años',
]

# ---------------------------------------------------------------------------
# NOMBRES DE COLUMNAS POR POSICIÓN (inmune a variaciones de espacios/texto)
# El orden de las preguntas en cada formulario es estable; en vez de hacer
# matching por texto exacto (frágil ante espacios dobles/pegados que varían
# entre lecturas de la API), renombramos directamente por posición.
# Las primeras 9 y últimas 7 posiciones usan nombres fijos descriptivos;
# las preguntas clínicas intermedias usan slugs generados automáticamente,
# verificados para no colisionar entre sí ni con mayúsculas/minúsculas
# (BigQuery trata los nombres de columna como insensibles a mayúsculas).
# ---------------------------------------------------------------------------
COLUMN_NAMES_BY_POSITION = {
    'Auditoria 12 a 17 años': [
        'fecha',
        'fechaRealizoAuditoria',
        'mes',
        'ano',
        'sede',
        'nombreProfesional',
        'cedula',
        'historiaClinica',
        'percentilRiesgo',
        'p09_RangoEdadParaAtencionMedica',
        'p10_RegistranAntecedentesExposicionHumoTabac',
        'p11_FumadorDeterminaEstadoFumadorExposicion',
        'p12_CasoConsumoCigarrilloRegistraConsejeria',
        'p13_RegistraEvaluacionSobrePresenciaTos',
        'p14_RegistraOrdenBaciloscopiaCasoSer',
        'p15_RegistraVacunacionJovenSegunPai',
        'p16_EvidencianFaltantesEsquemaVacunacionAdol',
        'p17_RegistraValoresHemoglobinaHematocritoRan',
        'p18_CasoNoEvidenciarResultadoHemoglobina',
        'p19_RegistraSituacionEscolarizacionDesempeno',
        'p20_RegistranRiesgosCuidadoExtraescolarAdole',
        'p21_RegistraInformacionRelacionadaActividadF',
        'p22_RegistraInformacionRelacionadaHigieneOra',
        'p23_RegistraInformacionRelacionadaAccionesCo',
        'p24_AplicaApgarFamiliarParaIdentificar',
        'p25_RegistraLaevaluacionFuncionesCognitivasI',
        'p26_RegistraIndagacionSobreRedesApoyo',
        'p27_RegistraExamenFisicoCompletoToma',
        'p28_RealizaEvaluacionCefalocaudalPorSistemas',
        'p29_RegistraEvaluacionAgudezaVisual',
        'p30_CasoIdentificarFactorRiesgoAlguna',
        'p31_RegistraEvaluacionFuncionesArticulacionV',
        'p32_IdentificaronAlteracionesAnormalidadesEv',
        'p33_DerivaValoracionOdontologicaAcuerdoPerio',
        'p34_RegistraExploracionRegionAnoGenital',
        'p35_CasoIdentificarFactorRiesgoAlguna',
        'p36_RegistraOrientacionSexualEIdentidad',
        'p37_RegistraInicioRelacionesSexualesNumero',
        'p38_RegistraUsoProteccionContraIts',
        'p39_CasoIdentificarActividadSexualRiesgo',
        'p40_RealizaAsesoriaPrePostestPara',
        'p41_CasoRetrasoMenstrualUOtros',
        'p42_CasoRetrasoMenstrualUOtros',
        'p43_AdolescenteTienePracticasSexualesCoitale',
        'p44_EvidenciaRealizacionConsultaAnticoncepci',
        'p45_RegistraSuministroMetodoAnticonceptivoSe',
        'p46_TeniendoCuentaMetodoPlanificacionFamilia',
        'p47_RegistraIdentificacionRiesgosConsumoAlim',
        'p48_CuandoRegistraAnalizaTomaMedidas',
        'p49_CasoIdentificarAlgunaAlteracionAnormalid',
        'p50_RegistraValoracionLesionesFisicasPor',
        'p51_CasoExposicionRiesgosPsicosocialesViolen',
        'p52_IndagaRegistraAntecedentesConductaSuicid',
        'p53_CasoIdentificarConductaSuicidaRegistra',
        'p54_CasoIdentificarAlgunRiesgoSalud',
        'p55_RegistranAntecedentesProblemasRendimient',
        'p56_CasoIdentificarAlgunoSiguientesRiesgos',
        'p57_RegistranAntecedentesPsicosocialesComoCo',
        'p58_CasoIdentificarTrastornosMentalesPadres',
        'p59_RegistraConsumoNoSustanciasPsicoactivas',
        'p60_CasoConsumoSustanciasPsicoactivasAplica',
        'p61_RegistraConsumoNoAlcohol',
        'p62_CasoDeconsumoAlcoholAplicaTest',
        'p63_CasoIdentificarseRiesgoConsumoAlcohol',
        'p64_RegistraRealizacionSesionesIndividualesS',
        'p65_IdentificaEducacionAcuerdoHallazgosAtenc',
        'p66_RegistraPlanCuidadosAcuerdoHallazgos',
        'p67_ConsiderandoTiempoLlevaAdolescenteConsul',
        'estructuraHC',
        'calificacionGuiaClinica',
        'calificacionEstructuraHC',
        'calificacionRIAS',
        'observaciones',
        'validacionDatos',
        'correo',
    ],
    'Auditoria 18 a 28 años': [
        'fecha',
        'fechaRealizoAuditoria',
        'mes',
        'ano',
        'sede',
        'nombreProfesional',
        'cedula',
        'historiaClinica',
        'percentilRiesgo',
        'p09_RangoEdadParaAtencionMedica',
        'p10_RegistranAntecedentesExposicionHumoTabac',
        'p11_FumadorDeterminaEstadoFumadorExposicion',
        'p12_CasoConsumoCigarrilloRegistraConsejeria',
        'p13_RegistraEvaluacionSobrePresenciaTos',
        'p14_SiseRegistraOrdenBaciloscopiaCaso',
        'p15_RegistraVacunacionJovenSegunLineamientos',
        'p16_EvidencianFaltantesEsquemaVacunacionRegi',
        'p17_RegistraInformacionRelacionadaActividadF',
        'p18_RegistraInformacionRelacionadaHigieneOra',
        'p19_RegistraInformacionRelacionadaEdadInicio',
        'p20_RegistraInformacionSobreExposicionFactor',
        'p21_CasoIdentificarFactorRiesgoAlguna',
        'p22_AplicaApgarFamiliarParaIdentificar',
        'p23_TomanRegistranTodosSignosVitales',
        'p24_RealizaEvaluacionCefalocaudalPorSistemas',
        'p25_RegistraEvaluacionAgudezaVisual',
        'p26_CasoIdentificarFactorRiesgoAlguna',
        'p27_RegistraEvaluacionFuncionesLaarticulacio',
        'p28_CasoIdentificarAlteracionesAnormalidadVa',
        'p29_DerivaValoracionOdontologicaAcuerdoPerio',
        'p30_RegistraOrientacionSexualIdentidadGenero',
        'p31_IndagaRegistranCambiosFisicosPsicologico',
        'p32_RegistraIndagacionSobreActividadSexual',
        'p33_CasoIdentificarActividadSexualRiesgo',
        'p34_RealizaAsesoriaPrePostestPara',
        'p35_RegistraConsejeriaEnfasisDerechosSexuale',
        'p36_CumplesiRegistraOfertaUsoEntrega',
        'p37_RegistraSolicitudCitologiaAplicaPara',
        'p38_RegistraResultadoCitologiaAplicaPara',
        'p39_CasoPresentarAlteracionesRegistraDerivac',
        'p40_RegistraInformacionSobreRegularidadPatro',
        'p41_CuandoRegistraTomaMedidasAntropometricas',
        'p42_CasoIdentificarRiesgosAlteracionesNutric',
        'p43_RegistraResultadodeGlicemiaBasalPerfil',
        'p44_RegistraAplicacionHerramientasFinnishRis',
        'p45_CasoIdentificarAlteracionResultadosAnter',
        'p46_RegistranAntecedentesExposicionDiversasF',
        'p47_CasoIdentificarSituacionesExposicionViol',
        'p48_RegistraExamenMentalValoracionEstrategia',
        'p49_CasoDeidentificarAlgunRiesgoSalud',
        'p50_RegistranAntecedentesConductaSuicidaCons',
        'p51_CasoIdentificarRiesgosComoTrastornos',
        'p52_CasoResultadoPositivoPruebasAnteriores',
        'p53_RegistraConsumoNoSustanciasPsicoactivas',
        'p54_RegistraConsumoNoAlcohol',
        'p55_CasoIdentificarConsumoSpaAplica',
        'p56_CasoPresentarAlteracionesAlgunoTest',
        'p57_IdentificaEducacionAcuerdoHallazgosAtenc',
        'p58_ConsiderandoTiempoLlevaJovenConsultando',
        'p59_UtilizaEscalaCorrespondienteSegunRiesgo',
        'p60_ValoracionContextoSocialRedesApoyo',
        'estructuraHC',
        'calificacionGuiaClinica',
        'calificacionEstructuraHC',
        'calificacionRIAS',
        'observaciones',
        'validacionDatos',
        'correo',
    ],
    'Auditoria 29 a 59 años': [
        'fecha',
        'fechaRealizoAuditoria',
        'mes',
        'ano',
        'sede',
        'nombreProfesional',
        'cedula',
        'historiaClinica',
        'percentilRiesgo',
        'p09_RangoEdadParaAtencionMedica',
        'p10_IndagaPorConsumoAlgunProducto',
        'p11_FumadorDeterminaEstadoFumadorExposicion',
        'p12_CasoConsumodeCigarrilloRegistraConsejeri',
        'p13_RegistraEvaluacionSobrePresenciaTos',
        'p14_RegistraOrdenBaciloscopiaCasoSer',
        'p15_RegistraSobreEsquemaVacunacionSegun',
        'p16_IdentificarAlgunBiologicoFaltanteRegistr',
        'p17_RegistraInformacionRelacionadaActividadF',
        'p18_RegistraInformacionRelacionadaHigieneOra',
        'p19_RegistraSobreFactoresRiesgoRelacionados',
        'p20_CasoIdentificarFactoresRiesgoLaborales',
        'p21_RegistraAplicacionApgarFamiliarPara',
        'p22_TomanYseRegistranTodosSignos',
        'p23_RealizaEvaluacionCefalocaudalPorSistemas',
        'p24_RegistraEvaluacionAgudezaVisual',
        'p25_CasoIdentificarFactorRiesgoAlguna',
        'p26_RegistraEvaluacionFuncionesArticulacionV',
        'p27_CasoIdentificarAlteracionesAlgunaAnormal',
        'p28_DerivaValoracionOdontologicaAcuerdoPerio',
        'p29_RegistranSignosSintomasRelacionadosSalud',
        'p30_RegistraIndagacionSobreActividadSexual',
        'p31_CasoIdentificarActividadSexualRiesgo',
        'p32_RealizaAsesoriaPrePostestPara',
        'p33_CasoIdentificarAntecedentesTransfusiones',
        'p34_RegistraResultadoPruebaRapidaPara',
        'p35_CuandoRegistraMujerUtilizaAlgun',
        'p36_RegistraRemisionParaAsesoriaAnticoncepci',
        'p37_RegistraRealizacionAsesoriaAnticoncepcio',
        'p38_AcuerdoHallazgosDeseosUsuarioSegun',
        'p39_RegistraEvaluacionClinicaSenoUltimo',
        'p40_RegistraSolicitudMamografiaBilateralDos',
        'p41_RegistraResultadoMamografiaBilateralDos',
        'p42_CasoResultadosAnormalesMamografiaDeriva',
        'p43_Mujeres3059AnosInicio',
        'p44_Mujeres3059AnosInicio',
        'p45_PresentarAlteracionEnelResultadoTamizaci',
        'p46_RegistraExamenClinicoProstataTacto',
        'p47_RegistraSolicitudAntigenoProstaticoSangr',
        'p48_RegistraResultadoAntigenoProstaticoSangr',
        'p49_IdentificaFactoresRiesgoIndividualHallaz',
        'p50_RegistraMedicionPesoTallaImc',
        'p51_IdentificaRegistraFactoresRiesgoAlteraci',
        'p52_RegistraValoracionRiesgoCardiovascularMe',
        'p53_CasoRegistrarEIdentificarRiesgo',
        'p54_RegistranResultadosExamenesLaboratorioGl',
        'p55_RegistranAntecedentesConductaSuicidaViol',
        'p56_CasoIdentificacionRiesgosSaludMental',
        'p57_IdentificanRiesgosPsicosocialesparaViole',
        'p58_RegistranAntecedentesConductaSuicidaCons',
        'p59_IdentificaRegistraTrastornoMentalFamilia',
        'p60_CasoPruebaTamizUtilizoPregunta',
        'p61_RegistraConsumoNoSustanciasPsicoactivas',
        'p62_CasoConsumoSustanciasPsicoactivasRegistr',
        'p63_RegistraConsumoNoAlcohol',
        'p64_CasoConsumoAlcoholRegistraAplicacion',
        'p65_CasoIdentificarseRiesgoConsumoAlcohol',
        'p66_RegistraInformacionSobreHallazgosPositiv',
        'p67_CuandoseRegistranConsultasPorSiguientes',
        'p68_TamizacionCancerColonSangreOculta',
        'p69_UtilizaEscalaCorrespondienteSegunRiesgo',
        'p70_ValoracionContextoSocialRedesApoyo',
        'estructuraHC',
        'calificacionGuiaClinica',
        'calificacionEstructuraHC',
        'calificacionRIAS',
        'observaciones',
        'validacionDatos',
        'correo',
    ],
    'Auditoria mayor 60 años': [
        'fecha',
        'fechaRealizoAuditoria',
        'mes',
        'ano',
        'sede',
        'nombreProfesional',
        'cedula',
        'historiaClinica',
        'percentilRiesgo',
        'p09_RangoEdadParaAtencionMedica',
        'p10_IndagaPorConsumoAlgunProducto',
        'p11_FumadorDeterminaEstadoFumadorExposicion',
        'p12_CasoConsumoCigarrilloRegistraConsejeria',
        'p13_RegistraEvaluacionSobrePresenciaTos',
        'p14_RegistraOrdenBaciloscopiaCasoSer',
        'p15_IndagaRegistraPresenciaNoInmovilidad',
        'p16_RegistraInspeccionRevisionAspectosGenera',
        'p17_CasoIdentificarFactorRiesgoAlguna',
        'p18_RegistranFactoresRiesgoRelacionadosSu',
        'p19_CasoIdentificarFactoresRiesgoLaborales',
        'p20_AplicaApgarFamiliarParaIdentificar',
        'p21_RegistraTomaTodosSignosVitales',
        'p22_RealizaEvaluacionCefalocaudalPorSistemas',
        'p23_RegistraEvaluacionAgudezaVisual',
        'p24_CasoIdentificarFactorRiesgoAlguna',
        'p25_RegistraEvaluacionFuncionesArticulacionV',
        'p26_CasoIdentificarAlteracionesAlgunaAnormal',
        'p27_DerivaValoracionOdontologicaAcuerdoPerio',
        'p28_RegistranSignosSintomasRelacionadosSalud',
        'p29_RegistraIndagacionSobreActividadSexual',
        'p30_CasoIdentificarActividadSexualRiesgo',
        'p31_RealizaAsesoriaPrePostestPara',
        'p32_CasoIdentificarAntecedentesTransfusiones',
        'p33_RegistraResultadoPruebaRapidaPara',
        'p34_RegistraEvaluacionclinicaMamaUltimoAno',
        'p35_RegistraSolicitudMamografiaBilateralDos',
        'p36_RegistraResultadoMamografiaBilateralDos',
        'p37_AnteResultadosAnormalesMamografiaBirads',
        'p38_IdentificaAlteracionLatamizacionRegistra',
        'p39_Mujeres6065AnosInicio',
        'p40_Enmujeres6065AnosInicio',
        'p41_RegistraExamenClinicoProstataTacto',
        'p42_RegistraSolicitudAntigenoProstaticoSangr',
        'p43_RegistraResultadoAntigenoProstaticoSangr',
        'p44_IdentificanFactoresRiesgoIndividualHalla',
        'p45_RegistraMedicionPesoTallaImc',
        'p46_RegistraMedicionPerimetroCircunferenciaM',
        'p47_RegistraSobreFactoresRiesgoAlteraciones',
        'p48_RegistraValoracionRiesgoCardiovascularMe',
        'p49_RegistraSolicitudExamenesLaboratorioPara',
        'p50_CuandoRegistranResultadosExamenesLaborat',
        'p51_RegistraValoracionLaaparienciaComportami',
        'p52_CasoIdentificarDeterioroCognitivoAplica',
        'p53_IdentificanAlteracionesResultadosNegativ',
        'p54_RegistraValoraciondeRiesgosPsicosociales',
        'p55_IdentificanRiesgosPsicosocialesParaViole',
        'p56_RegistraIdentificacionRiesgosComoTrastor',
        'p57_CasoIdentificarTrastornosMentalesPadres',
        'p58_CasoPruebaTamizUtilizoPregunta',
        'p59_CasoIdentificarRiesgosRelacionadosConsum',
        'p60_IdentificanAlteracionesRegistraDerivacio',
        'p61_RegistraInformacionSobreHallazgosPositiv',
        'p62_CuandoRegistranConsultasPorSiguientes',
        'p63_TamizacionCancerColonSangreOculta',
        'p64_AntiInfluenza',
        'p65_ValorarEstructuraDentoMaxilofacial',
        'p66_UtilizaEscalaCorrespondienteSegunRiesgo',
        'p67_ValoracionContextoSocialRedesApoyo',
        'estructuraHC',
        'calificacionGuiaClinica',
        'calificacionEstructuraHC',
        'calificacionRIAS',
        'observaciones',
        'validacionDatos',
        'correo',
    ],
}

# ---------------------------------------------------------------------------
# COLUMNAS DE PORCENTAJE (comunes a las 4 pestañas)
# ---------------------------------------------------------------------------
PCT_COLS_COMMON = ['calificacionGuiaClinica', 'calificacionEstructuraHC', 'calificacionRIAS']
PCT_COLS = {name: PCT_COLS_COMMON for name in LIST_NAME_SHEET}

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
    """Renombra columnas por posición. Si el número de columnas no coincide
    con lo esperado, lo reporta como warning para investigar manualmente."""
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
    log.info(f'Inicio del ETL: rias')
    log.info(f'Tabla destino : {TABLA_BIGQUERY}')
    log.info(f'Sheet ID      : {ID_SHEET}')
    log.info('=' * 60)

    try:
        df_total = pd.DataFrame()

        for name in LIST_NAME_SHEET:
            inicio_hoja = time.time()
            log.info(f'[{name}] Leyendo hoja ...')

            df = func_process.get_google_sheet(ID_SHEET, name)
            log.info(f'[{name}] Filas leídas desde Google Sheets: {df.shape[0]}')

            df = rename_by_position(df, COLUMN_NAMES_BY_POSITION[name], name)
            log.info(f'[{name}] Columnas renombradas: {df.shape[1]}')

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

    duracion_total = time.time() - inicio_total
    log.info(f'ETL finalizado en {duracion_total:.2f}s')
    log.info('=' * 60)

execution_load()