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
table_name                      = 'aiepi'
TABLA_BIGQUERY                  = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name}'

# ---------------------------------------------------------------------------
# GOOGLE SHEET
# ---------------------------------------------------------------------------
ID_SHEET   = '1JTPprj50vSoO5goGjGV3qkzt0YLAgVrPvLmc3MjJZJE'
NAME_SHEET = 'Ingreso de auditorias'

# ---------------------------------------------------------------------------
# MAPEO DE COLUMNAS → camelCase
# ---------------------------------------------------------------------------
COLUMN_RENAME_MAP = {
    'Tipo de validación':                      'tipoValidacion',
    'Fecha':                                   'fecha',
    'Fecha en la que realizo la auditoria':    'fechaRealizoAuditoria',
    'Mes':                                     'mes',
    'Año':                                     'ano',
    'Sede':                                    'sede',
    'Nombre del Colaborador':                  'nombreProfesional',
    'Numero de documento':                     'cedula',
    'Registre el número de la historia clínica evaluada.': 'historiaClinica',
    'Diagnostico':                             'diagnostico',
    'Edad en meses':                           'edadMeses',
    'Genero: Registre F (Femenino) o M (Masculino).': 'genero',
    'Cumple si se registra el nombre, teléfono y parentesco del acompañante o cuidador': 'p01_identificacionAcompanante',
    'Registra signos de peligro inminente de muerte: Verifique si el niño tenia alguno de los siguientes signos de peligro: no puede beber o tomar el pecho, vomita todo, está letárgico o inconsciente, ha tenido convulsiones. Cumple si se remitió u hospitalizó de inmediato..': 'p02_signosPeligroMuerte',
    'Verifique si el niño tenía tos o dificultad para respirar.  Si la respuesta es negativa pase a la pregunta 19 (Aplica para todos los casos con síntomas respiratorios).': 'p03_tosDificultadRespirar',
    'Si el niño tenía primer episodio de sibilancia, cuadro gripal en los 2 a 3 días anteriores, edad menor a 2 años y uno de los siguientes signos, cumple si se remitió u hospitalizó y se administró oxígeno  Signos:- Tiraje subcostal, - Respiración rápida, - Saturación de oxígeno < 92%, - Edad menor de 3 meses, - Edad menor de 6 meses y fue prematuro, - Apneas': 'p04_sibilanciaPrimerEpisodio',
    'Si el niño tenía tiraje subcostal o saturación de oxígeno < 92%, cumple si se remitió u hospitalizó y  se administró oxígeno y la primera dosis de un antibiótico apropiado.': 'p05_tiraje',
    'Si el niño tenía respiración rápida, cumple si se realizó manejo adecuado. (se prescribió antibiótico, se enseñó a la madre cuando volver de inmediato y se citó a revisión dos días después).': 'p06_respiracionRapida',
    'Si el niño tenía sibilancias, cumple si se registró si éstas eran de primera vez o recurrentes.': 'p07_sibilanciasRegistro',
    'Si las sibilancias eran recurrentes, cumple si se hizo la clasificación de la severidad (grave o no).': 'p08_sibilanciasRecurrentes',
    'Si era el primer episodio de sibilancias, cumple si se hizo manejo adecuado. (broncodilatador de acción rápida, se reclasificó y se citó a control en 2 días).': 'p09_sibilanciasPrimerEpisodioManejo',
    'Si el niño tenía tos o resfriado de más de 15 días de evolución, cumple si se descartó Tuberculosis.': 'p10_tosResfriado15Dias',
    'Si el niño tenía diagnóstico de rinofaringitis aguda o resfriado común, cumple si se tuvo en cuenta no prescribirle antihistamínicos como parte del tratamiento.': 'p11_rinofaringitis',
    'Si el niño tenía tos o resfriado, cumple si se indicó a la madre manejo de la fiebre, aliviar síntomas con lavados nasales y bebidas endulzadas, educación en signos de alarma y volver de inmediato si empeora o si no mejora volver a los 5 días.': 'p12_tosResfriadorEducacion',
    'Verifique si se registra que el  niño tenía diarrea.  Si la respuesta es negativa pase a la pregunta 29.': 'p13_diarrea',
    'Si el niño tenía deshidratación grave , cumple si se remitió o se hospitalizó y se inició reposición de líquidos según plan C de rehidratación.': 'p14_deshidratacionGrave',
    'Si tenía algún grado de deshidratación (sin otra clasificación grave), cumple si se inició Plan B y se ordenó control en 2 días,  o si se remitió si no hay posibilidad de atención.': 'p15_deshidratacionPlanB',
    'Si tenía alto riesgo de deshidratación (diarrea de alto gasto, vómito persistente o rechazo a la vía oral), cumple si se inició Plan A supervisado y se ordenó control en 2 días (si la diarrea continúa),  o si se remitió si no hay posibilidad de atención.': 'p16_altRiesgoDeshidratacion',
    'Si el niño tenía sangre en las heces, cumple si se realizó manejo adecuado: antibiótico apropiado  (Ac. Nalidíxico), se inició Plan según grado de deshidratación y control en dos días. Revisar en historia clínica plan de manejo (si aplica) y nota de revisión a las 48 horas de la primera atención.': 'p17_sangreHeces',
    'Si la diarrea tenía 14 días o más de evolución y el niño es menor de 6 meses o estaba deshidratado, cumple si se remitió u hospitalizó.': 'p18_diarrea14DiasMenor6M',
    'Si la diarrea es de 14 días o más de evolución y el niño no estaba deshidratado y es mayor de 6 meses, cumple si se dieron las instrucciones para: alimentación del niño con diarrea persistente, se prescribió vitamina A, enseña medidas preventivas, cuando volver si empeoraba y revisión en dos días.': 'p19_diarrea14DiasMayor6M',
    'Cumple cuando NO se le prescribió antidiarreicos como parte del tratamiento.': 'p20_noAntidiarreicos',
    'Si el niño tenía diarrea independiente de tener o no deshidratación, cumple si se prescribió zinc por 14 días.': 'p21_zinc14Dias',
    'Si la diarrea tenia menos de 14 días y el niño no estaba deshidratado, cumple si se dieron las instrucciones pertinentes: más líquidos de lo normal, continuar la alimentación, cuándo regresar y medidas preventivas de la enfermedad.': 'p22_diarreaMenos14Dias',
    'Verifique si el niño tenía fiebre. Si el niño no tenía fiebre pase a la pregunta 37.': 'p23_fiebre',
    'Si el niño tenía fiebre, cumple si se hizo la clasificación del riesgo (Alto, intermedio, bajo).': 'p24_fiebreClasificacion',
    'Si el niño tenía fiebre, cumple si se descartó enfermedad febril muy grave y se remitió u hospitalizó en caso de presentarlos.  (cualquier signo de peligro general, rigidez de nuca, aspecto tóxico, rash o eritema que no cede a la presión o manifestaciones focales de otras infecciones graves, menor de 3 meses y fiebre >= 38°, 3 a 6 meses de edad y fiebre >=39°).': 'p25_fiebreGrave',
    'Si la fiebre llevaba más de cinco días de duración, cumple si se realizó cuadro hemático, PCR y parcial de orina; si no es posible tomarlos lo refirió a un nivel superior o remitió para estudio.': 'p26_fiebre5Dias',
    'Verifique si está registrado que el menor procede o visitó en los últimos 15 días un área de riesgo de malaria.': 'p27_malaria',
    'Si procede o visitó en los últimos 15 días un área de riesgo de malaria y tiene cualquier signo de enfermedad febril de alto riesgo, cumple si se remitió u hospitalizó.': 'p28_malariaAltoRiesgo',
    'Si procede o visitó en los últimos 15 días un área de riesgo de malaria y no tiene ningún signo de MALARIA COMPLICADA, y tiene uno de los siguientes: fiebre y procede de un área rural o fiebre sin causa aparente y procede de un área urbana; cumple si se realiza u ordenó gota gruesa.': 'p29_malariaGotaGruesa',
    'Si el niño no fue remitido o hospitalizado por fiebre, cumple si se dieron instrucciones sobre el  tratamiento y cuidados del niño con fiebre en el hogar y cuándo debe volver de inmediato. (signos de peligro general, empeoramiento del cuadro clínico, exantemas, sangrados, rigidez de nuca).': 'p30_fiebreInstrucciones',
    'Verifique si el niño tenía problemas de oídos y/o garganta. Si el niño no tenía problema de oídos. Si el niño no tenía problema de oídos pase a la pregunta 42': 'p31_problemaOidos',
    'Si el niño tenía tumefacción dolorosa al tacto detrás de la oreja, cumple si se remitió u hospitalizó.  (Mastoiditis).': 'p32_mastoiditis',
    'Si el niño tenía problemas de oído,  cumple si se clasificó el problema y se prescribió el tratamiento correcto según cuadro de procedimientos AIEPI. (otitis medía aguda, otitis media  crónica o recurrente).': 'p33_otitis',
    'Si el niño tenía otitis media, cumple si se prescribió el antibiótico adecuado según cuadro de procedimientos.': 'p34_otitisAntibiotico',
    'Si el niño tenía problemas de garganta, cumple si se clasificó el problema y se prescribió el tratamiento según cuadro de procedimientos.': 'p35_garganta',
    'Cumple si se clasificó la salud bucal del niño.': 'p36_saludBucal',
    'Cumple si  se inició el manejo de acuerdo al cuadro de procedimientos Odontología': 'p37_odontologia',
    'Cumple si se registra la búsqueda de signos de maltrato y abuso sexual y se clasifica.': 'p38_maltrato',
    'Si el niño tenía signos de maltrato o abuso sexual. Cumple si se  activa protocolo de atención.  Si la respuesta es positiva, diligencie el instrumento de Violencia sexual.': 'p39_maltratoProtocolo',
    'Cumple si se clasificó el estado nutricional según la resolución resolución 2465 de 2016  (PC/E, T/E y P/T).': 'p40_estadoNutricional',
    'En caso de presentar alteraciones del estado nutricional. Cumple si se intervinieron las alteraciones en el estado nutricional según cuadro de procedimiento.': 'p41_alteracionNutricional',
    'Cumple si se clasificó la anemia. Por medio de signos clínicos: palidez palmar o conjuntival intensa, palidez palmar o conjuntival leve, no tiene palidez palmar ni conjuntival.': 'p42_anemia',
    'Si se identifico anemia.  Cumple si se realizó intervención según cuadro de procedimientos.': 'p43_anemiaIntervencion',
    'Si el niño no había recibido sulfato ferroso en los últimos seis meses. Cumple si se le prescribió. (Aplica para mayores de 6 meses). La dosis para prevención es 2 mg/kg de hierro elemental.': 'p44_sulfatoFerroso',
    'Si el niño no había recibido albendazol en los últimos seis meses, cumple si se  le prescribió. (Aplica para mayores de un año).  Hasta los dos años 200 mg, y por encima de los dos años la dosis completa de 400 mg cada seis meses.': 'p45_albendazol',
    'Cumple si se verificó el estado de vacunación del niño.': 'p46_vacunacion',
    'Si al niño le faltaba alguna vacuna. Cumple si se canalizó para su aplicación.': 'p47_vacunacionCanalizacion',
    'Cumple_si se registraron las instrucciones a la madre o al acompañante sobre cuándo volver de inmediato y cuidados en el hogar.': 'p48_instruccionesMadre',
    'Cumple si se dio una cita de revisión si estaba_indicada.': 'p49_citaRevision',
    'Cumple si se registra si el niño tenía alteraciones en su desarrollo y se realizó clasificación y tratamiento.': 'p50_desarrollo',
    'Si el niño(a) no estaba inscrito en el programa de detección temprana de alteraciones del crecimiento y desarrollo del menor de 10 años, cumple si fue remitido a dicho programa.': 'p51_programaCrecimiento',
    'Si el niño no había recibido suplemento con vitamina A en los últimos seis meses. Cumple si se le prescribió (aplica para mayores de seis meses). La dosis de seis meses a un año 100.000 unidades, del año en adelante 200.000 unidades cada seis meses.': 'p52_vitaminaA',
    'Cumple si se registraron  antecedentes  del embarazo que pueden afectar el bienestar del niño.': 'p53_antecedentesEmbarazo',
    'Cumple si se registraron  antecedentes del parto que pueden afectar el bienestar del niño.': 'p54_antecedentesParto',
    'Cumple si se evidencia verificación de signos de enfermedad muy grave o infección local.  Si responde no, pase a la pregunta 62. (No puede beber o tomar el pecho, vomita todo, está letárgico o inconsciente, ha tenido convulsiones).': 'p55_enfermedadGrave',
    'Si presentaba alguno de los signos de enfermedad grave o infección local, cumple si se remitió u hospitalizó de inmediato.': 'p56_enfermedadGraveHospitalizacion',
    'Se registra si el paciente presenta signos de infección local.': 'p57_infeccionLocal',
    'Si la anterior pregunta es Si, cumple si se realizó la clasificación, el manejo y tratamiento adecuado según cuadro de procedimientos AIEPI.': 'p58_infeccionLocalManejo',
    'Verifique si el niño tenía diarrea.  Si la respuesta es negativa pase a la pregunta 70.': 'p59_diarreaNeonato',
    'Si el niño estaba deshidratado o  tenía diarrea por siete días o más, cumple si se remitió u hospitalizó de inmediato.': 'p60_diarreaNeonato7Dias',
    'Si el niño no estaba deshidratado, cumple si se dieron las instrucciones pertinentes: más líquidos de lo normal, continuar la alimentación (especialmente lactancia), cita de control a los dos días y medidas preventivas de la enfermedad.': 'p61_diarreaNeonatoInstrucciones',
    'Si el niño tenía diarrea con sangre cumple si se administró dosis de vitamina k, primera dosis de los antibióticos recomendados y se remitió u hospitalizo de inmediato.': 'p62_diarreaSangre',
    'Si el niño tenía diarrea independiente de tener o no deshidratación, cumple si se prescribió zinc.': 'p63_zincNeonato',
    'Cumple si se realizó la clasificación nutricional del niño según la referencia de la OMS_(Resolución 2121 de 2010)?  (T/E y P/T).': 'p64_clasificacionNutricionalOMS',
    'Cumple si se determinó si el niño tenía problema de alimentación.  (AIEPI propone evaluar problema severo de alimentación o muy bajo peso, un problema de alimentación o bajo peso, o no tenía problemas con la alimentación)': 'p65_problemaAlimentacion',
    'Si el niño tenía un problema severo de alimentación, cumple si se remitió u hospitalizó de inmediato.': 'p66_alimentacionSevera',
    'Cumple si se indaga por el tipo de alimentación que recibía el niño.': 'p67_tipoAlimentacion',
    'Si la respuesta anterior es sí, verificar si el niño recibía lactancia materna exclusiva. * No afecta el porcentaje de cumplimiento de la institución.': 'p68_lactanciaMaterna',
    'Cumple si se verificó el estado de vacunación del niño.  Si no_trae el carné registrar No aplica.': 'p69_vacunacionNeonato',
    'Si al niño le faltaban vacunas, cumple si se canalizó para su aplicación. En menores prematuros tener en cuenta recomendación del pediatra.': 'p70_vacunacionNeonatoCanalizacion',
    'Cumple si se registraron las instrucciones a la madre o al acompañante sobre cuándo volver de inmediato y cuidados en el hogar.': 'p71_instruccionesMadreNeonato',
    'Cumple si se dio una cita de_revisión_si estaba indicada.': 'p72_citaRevisionNeonato',
    'Cumple si se registraron diagnósticos asociados, diferentes del motivo de consulta.': 'p73_diagnosticosAsociados',
    '¿El tratamiento es adecuado al diagnostico AIEPI? (medicamentos, recomendaciones, cuidados en el hogar, signos de alarma y cuando regresar)': 'p74_tratamientoAdecuado',
    'Si se solicitaron paraclínicos para atención de AIEPI fue pertinente': 'p75_paraclinicosAIEPI',
    'Cumple con la estructura de HC': 'p76_estructuraHC',
    'Observaciones':                  'observaciones',
    'Promedio de calificación medico': 'promedioCalificacionMedico',
    'Calificación guía medica':        'calificacionGuiaMedica',
    'Calificaciones de SI con puntuacion de 1,92':  'calificacionesSI192',
    'Calificaciones de NO con puntuacion de 1,92':  'calificacionesNO192',
    'Calificaciones de SI con puntuacion de 0,96':  'calificacionesSI096',
    'Calificaciones de NO con puntuacion de 0,96':  'calificacionesNO096',
    'Calificación estructura de historia clinica':   'calificacionEstructuraHC',
    'Validacion de datos':             'validacionDatos',
    'Correo':                          'correo',
}

COLUMNS_REQUIRED = list(COLUMN_RENAME_MAP.values())
PCT_COLS = ['promedioCalificacionMedico', 'calificacionGuiaMedica', 'calificacionEstructuraHC']

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
        df['edadMeses'].fillna(0, inplace=True)
        df['edadMeses'] = pd.to_numeric(df['edadMeses'], errors='coerce').fillna(0).astype(int)
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
            df['diagnostico'].isna() | (df['diagnostico'].astype(str).str.strip() == '')
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
    log.info(f'Inicio del ETL: aiepi')
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