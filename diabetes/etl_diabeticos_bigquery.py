import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import time
import locale
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
PATH_DRIVE = os.environ.get("PATH_DRIVE")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)

import func_process
import load_bigquery as loadbq

# Configurar el locale a español 
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

fecha_capita = pd.to_datetime(datetime.now() - timedelta(days=15))
fecha = func_process.pd.to_datetime(str(fecha_capita.year)+'-'+str(fecha_capita.month)+'-01')
fecha_cargue = func_process.pd.to_datetime(str(fecha_capita.year)+'-'+str(fecha_capita.month)+'-15')
mes = fecha.strftime('%B').lower()
mes_a = (fecha_capita - func_process.np.timedelta64(31,'D')).strftime('%B').lower()
fecha_six = (fecha - func_process.np.timedelta64(152,'D')).strftime('%Y-%m-%d')
fecha_seven = (fecha - func_process.np.timedelta64(212,'D'))
fecha_seven = pd.to_datetime((fecha_seven + func_process.np.timedelta64(1,'D')).strftime('%Y-%m-%d'))
fecha_last_year = pd.to_datetime((fecha - func_process.np.timedelta64(365,'D')).strftime('%Y-%m-%d'))
fecha_f = str(fecha.year)+'-'+str('{:02}'.format(fecha.month))+'-'+str(fecha.days_in_month)


# Parameters Bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_diabetes = 'diabetes'
dataset_id_rips = 'rips'
dataset_id_pacientes = 'pacientes'


# Name table bigquery
table_name_diabeticos_hipertensos = 'diabeticos_hipertensos'
table_name_diabeticos_hipertensos_totales = 'diabeticos_hipertensos_totales'
table_name_dm_all= 'dm_all'
table_name_cobertura_dm_totales = 'cobertura_dm_totales'
table_name_cobertura_hta = 'cobertura_hta'
table_name_cobertura_hta_totales = 'cobertura_hta_totales'
table_name_diabeticos_fu = 'diabeticos_fu'
table_name_rips_auditoria = 'rips_auditoria_poblacion_2'
table_name_capita = 'capita'
table_name_capita_poblaciones = 'capitas_poblaciones'

# Columns validator
validator_column = 'fecha_capita'


# ID Bigquery
TABLA_BIGQUERY_DIABETICOS_HIPERTENSOS = f'{project_id_product}.{dataset_id_diabetes}.{table_name_diabeticos_hipertensos}'
TABLA_BIGQUERY_DIABETICOS_HIPERTENSOS_TOTALES = f'{project_id_product}.{dataset_id_diabetes}.{table_name_diabeticos_hipertensos_totales}'
TABLA_BIGQUERY_DM_ALL = f'{project_id_product}.{dataset_id_diabetes}.{table_name_dm_all}'
TABLA_BIGQUERY_COBERTURA_DM_TOTALES = f'{project_id_product}.{dataset_id_diabetes}.{table_name_cobertura_dm_totales}'
TABLA_BIGQUERY_COBERTURA_HTA = f'{project_id_product}.{dataset_id_diabetes}.{table_name_cobertura_hta}'
TABLA_BIGQUERY_COBERTURA_HTA_TOTALES = f'{project_id_product}.{dataset_id_diabetes}.{table_name_cobertura_hta_totales}'
TABLA_BIGQUERY_DIABETICOS_FU = f'{project_id_product}.{dataset_id_diabetes}.{table_name_diabeticos_fu}'
TABLA_BIGQUERY_RIPS_AUDITORIA = f'{project_id_product}.{dataset_id_rips}.{table_name_rips_auditoria}'
TABLA_BIGQUERY_CAPITA = f'{project_id_product}.{dataset_id_pacientes}.{table_name_capita}'
TABLA_BIGQUERY_CAPITA_POBLACIONES = f'{project_id_product}.{dataset_id_pacientes}.{table_name_capita_poblaciones}'

LIST_TABLA_BIGQUERY = [TABLA_BIGQUERY_DIABETICOS_HIPERTENSOS,TABLA_BIGQUERY_DIABETICOS_HIPERTENSOS_TOTALES,
                       TABLA_BIGQUERY_DM_ALL,TABLA_BIGQUERY_COBERTURA_DM_TOTALES,TABLA_BIGQUERY_COBERTURA_HTA,
                       TABLA_BIGQUERY_COBERTURA_HTA_TOTALES,TABLA_BIGQUERY_DIABETICOS_FU]
LIST_TABLA_MARIADB = ['diabeticos_hipertensos','diabeticos_hipertensos_totales','dm_all',
                      'cobertura_dm_totales','cobertura_hta','cobertura_hta_totales','diabeticos_fu']
LIST_COLUMN_VALIDATOR = ['fecha_capita'] * len(LIST_TABLA_BIGQUERY)
LIST_IF_EXIST_BIGQUERY = ['WRITE_TRUNCATE','WRITE_APPEND','WRITE_TRUNCATE','WRITE_APPEND','WRITE_TRUNCATE','WRITE_APPEND','WRITE_APPEND']
LIST_IF_EXIST_MARIADB = ['replace','append','replace','append','replace','append','append']

# Diccionario para mapear los nombres de las sedes por medio del codigo (como texto)
n_ips = {
    '35':'CENTRO',
    '1013':'CALASANZ',
    '2136':'NORTE',
    '2715':'PAC',
    '115393':'AVENIDA ORIENTAL',
    '134189':'ARGENTINA',
    '133050':'PAC ESPECIALISTAS'    
}
cod_lab = ['HEMOGLOBINA GLICOSILADA']
cod_creatinina = ['creatinina', 'CREATININA EN SUERO', 'CREATININA - RIAS CURSO DE VIDA ADULTEZ', 'CREATININA EN SUERO -  RIAS CURSO DE VIDA VEJEZ']
cod_microalbuminuria = ['MICROALBUMINURIA (RELACION ALBUMINURIA/CREATINURIA)', 'Microalbuminuria en orina ocasional']

SQL_BIGQUERY = """
                SELECT g.fecha_capita
                FROM {} as g
                WHERE g.fecha_capita IN {}
                """

sql_lab_HMC = f"""
            SELECT DISTINCT
                patientId AS identificacion_paciente, 
                nameSueltos AS prueba,
                resultSueltos AS resultado,
                unitSueltos AS unidad, 
                fechaValidacionSueltos AS fecha_ingreso, 
                medico
            FROM examenesLaboratorioView 
            WHERE (            
                    (nameSueltos IN (
                        'HEMOGLOBINA GLICOSILADA', 'Microalbuminuria en orina ocasional',
                        'MICROALBUMINURIA (RELACION ALBUMINURIA/CREATINURIA)', 
                        'creatinina', 'CREATININA EN SUERO', 'CREATININA - RIAS CURSO DE VIDA ADULTEZ', 
                        'CREATININA EN SUERO -  RIAS CURSO DE VIDA VEJEZ'
                        ))
                AND
                    (fechaValidacionSueltos BETWEEN '{fecha_last_year}' AND '{fecha_f}')
                
                )
            ORDER BY fechaValidacionSueltos;
        """

sql_lab_PU = f"""
            SELECT 
                _order, 
                patientId AS identificacion_paciente, 
                panel AS prueba, 
                fechaValidacion AS fecha_ingreso, 
                medico 
            FROM perfilesExamenesView 
            WHERE (
                    (panel IN (
                        'PARCIAL DE ORINA', 
                        'UROANALISIS-RIAS CURSO DE VIDA', 
                        'UROANALISIS-RIAS CURSO DE VIDA ADULTEZ', 
                        'UROANALISIS-RIAS CURSO DE VIDA VEJEZ'
                        ))
                AND 
                    (fechaValidacion BETWEEN '{fecha_last_year}' AND '{fecha_f}')
                )
            GROUP BY _order, fechaValidacion, patientId
            ORDER BY fechaValidacion;
        """

sql_poblacion = f"""SELECT 
                    FECHA_CAPITA, 
                    NOMBRE_IPS as nombre_sede_atencion, 
                    MAYORES_DE_18_ANOS 
                    FROM `ia-bigquery-397516.pacientes.capitas_poblaciones`
                    WHERE FECHA_CAPITA >= '{str(fecha_cargue)}';"""

sql_dm_hta = f"""
            SELECT 
            identificacion_paciente, 
            primer_apellido, 
            segundo_apellido, 
            primer_nombre, 
            segundo_nombre, 
            sexo, 
            fecha_nacimiento, 
            edad, 
            sede_atencion, 
            sw_diabetes,
            sw_hipertension,
            mes_cargue 
            FROM `ia-bigquery-397516.pacientes.capita` 
            WHERE sw_diabetes = 1 OR sw_hipertension = 1;"""

sql_capita_reportes = """SELECT 
                        identificacion_paciente, 
                        telefono, 
                        celular 
                        FROM `ia-bigquery-397516.pacientes.capita`;"""

sql_rips_dm_hta = f"""SELECT * 
                        FROM `ia-bigquery-397516.rips.rips_auditoria_poblacion_2` as rp
                        WHERE rp.codigo_sura IN ('70302','8903056','70300','70301','8902031','8903018','3000052','3000054',
                                                '8903018','8903056','8902031','3000054','3000052','8903015','3000053','70201','70202','70200') 
                        AND fecha_capita >= '{fecha_last_year}'
                        ORDER BY fecha_capita ASC;
                        """

sql_hospitalizacion_dm = f"""SELECT 
                                * 
                            FROM panorama_view 
                            WHERE Codigo_Diagnostico_Egreso IN ('E100','E101','E102','E103','E104','E105','E106',
                                                                'E107','E108','E109','E110','E111','E112','E113',
                                                                'E114','E115','E116','E117','E118','E119','E120',
                                                                'E121','E122','E123','E124','E125','E126','E127',
                                                                'E128','E129','E130','E131','E132','E133','E134',
                                                                'E135','E136','E137','E138','E139','E140','E141',
                                                                'E142','E143','E144','E145','E146','E147','E148',
                                                                'E149','E15X','E160','E161','E162','R739','Z131');""" 

sql_hospitalizacion_hta = """
                            SELECT 
                                    * 
                            FROM panorama_view 
                            WHERE Codigo_Diagnostico_Egreso IN ('I10X','I110','I119','I120','I129',
                                                                'I130','I131','I132','I139','I150',
                                                                'I151','I152','I158','I159');""" 
sql_urgencias_dm = """SELECT 
                            * 
                        FROM urgenciasPanoramaView 
                        WHERE Codigo_Diagnostico_EPS_Op IN ('E100','E101','E102','E103','E104',
                                                    'E105','E106','E107','E108','E109','E110',
                                                    'E111','E112','E113','E114','E115','E116',
                                                    'E117','E118','E119','E120','E121','E122',
                                                    'E123','E124','E125','E126','E127','E128',
                                                    'E129','E130','E131','E132','E133','E134',
                                                    'E135','E136','E137','E138','E139','E140',
                                                    'E141','E142','E143','E144','E145','E146',
                                                    'E147','E148','E149','E15X','E160','E161',
                                                    'E162','R739','Z131');"""


sql_urgencias_hta = """SELECT 
                            * 
                        FROM urgenciasPanoramaView 
                        WHERE Codigo_Diagnostico_EPS_Op IN ('I10X','I110','I119','I120','I129',
                                                    'I130','I131','I132','I139','I150','I151',
                                                    'I152','I158','I159');"""

def validate_load(df_validate_load,df_load,tabla_bigquery,table_mariadb,if_exist_bigquery,if_exist_mariadb):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar mariadb
            func_process.save_df_server(df_load, table_mariadb, 'analitica',if_exist_mariadb)
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,tabla_bigquery,if_exist_bigquery)
    except ValueError as err:
        print(err)

# Obtener datos no duplicados
def validate_rows_duplicate(df,TABLA_BIGQUERY):
    try:
        valores_unicos = tuple(set(df[validator_column]))
        df_rows_not_duplicates = loadbq.rows_not_duplicates(df,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)
        return df_rows_not_duplicates
    except ValueError as err:
        print(err)

def execution_load(detalle_df_save):
    try:
        if  detalle_df_save.shape[0] > 0:
            for row in range(detalle_df_save.shape[0]):
                TABLA_BIGQUERY = detalle_df_save.tabla_bigquery.iloc[row]
                table_mariadb = detalle_df_save.tabla_mariadb.iloc[row]
                df = detalle_df_save.name_df.iloc[row]
                if_exist_bigquery =  detalle_df_save.if_exist_bigquery.iloc[row]
                if_exist_mariadb =  detalle_df_save.if_exist_mariadb.iloc[row]
                # VALIDATE DUPLICATE
                df_rows_not_duplicates = validate_rows_duplicate(df,TABLA_BIGQUERY)
                #VALIDATE LOGS
                validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)
                #Load
                validate_load(validate_loads_logs,df_rows_not_duplicates,TABLA_BIGQUERY,table_mariadb,if_exist_bigquery,if_exist_mariadb)
                time.sleep(5)
        else:
            raise SystemExit
    except Exception as error:
            print(error)
            

def creatinina(iden):
    if (df_datos_laboratorio_tamizaje['identificacion_paciente'].isin([iden]).sum()):
        if (df_datos_laboratorio_tamizaje[df_datos_laboratorio_tamizaje['identificacion_paciente'] == iden].prueba.isin(cod_creatinina).sum()):
            return 1
        else:
            return 0    
    else:
        return 0

# Ahora sacamos una columna para saber cuales tienes los tres resultados de laboratorio
def todos_examenes(cre, p_o, mic):
    if ((cre == 1) & (p_o == 1) & (mic == 1)):
        return 1
    else:
        return 0


def p_orina(iden):
    if (df_datos_laboratorio_PU['identificacion_paciente'].isin([iden]).sum()):
        if (df_datos_laboratorio_PU[df_datos_laboratorio_PU['identificacion_paciente'] == iden].prueba.sum()):
            return 1
        else:
            return 0    
    else:
        return 0

def microalbuminuria(iden):
    if (df_datos_laboratorio_tamizaje['identificacion_paciente'].isin([iden]).sum()):
        if (df_datos_laboratorio_tamizaje[df_datos_laboratorio_tamizaje['identificacion_paciente'] == iden].prueba.isin(cod_microalbuminuria).sum()):
            return 1
        else:
            return 0    
    else:
        return 0
def create_Hb1Ac_last_date(x_df):
    """Consulta todos los registros del df de diabeticos con Hb1ac y
    almacena en un nuevo dataframe el examen o registro mas reciente.
    
    Args:
        x_df: a DataFrame
        
    Returns:
        DataFrame
    """
    ids= x_df['identificacion_paciente'].unique()
    columns_df= x_df.columns
    Hb1Ac_last_date = []
    for i in ids:
        user = x_df[x_df['identificacion_paciente'] == i].sort_values(
                                                        by=['fecha_ingreso']
                                                    ).iloc[-1].values
        Hb1Ac_last_date.append(user)
        
    df_Hb1Ac_last_date = func_process.pd.DataFrame(Hb1Ac_last_date, columns=columns_df)
    return df_Hb1Ac_last_date

def validacion_examenes_hb1ac(df_diabeticos_hb1Ac):
    # Hb1Ac Mayor que 10 Mejorar 2 puntos
    df_mejoria_final = func_process.pd.DataFrame(columns=['identificacion_paciente', 'edad', 'prueba', 'resultado_prueba', 'unidad', 'fecha_ingreso', 'medico', 'compensados'])
    df_mejoria = pd.DataFrame()
    for i in cedulas_mejoria_clinica:
        df_diabetico = df_diabeticos_hb1Ac[df_diabeticos_hb1Ac.identificacion_paciente == i]
        if len(df_diabetico) > 1:
            
            #Validamos la primera codicion de mejoria clinica
            if df_diabetico.resultado_prueba.iloc[0] > 10:
                #Sacamos las dos fechas de los resultados y validamos si tienen diferencia mayor a 10 meses
                f1 = df_diabetico.fecha_ingreso.iloc[0]
                f2 = df_diabetico.fecha_ingreso.iloc[-1]
                f = (f2 - f1).days
                
                if f >= 335:
                    r1 = df_diabetico.resultado_prueba.iloc[0]
                    r2 = df_diabetico.resultado_prueba.iloc[-1]

                    if r1 > r2:
                        m = (r1 - r2)
                        if m >= 2:
                            
                            df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[1]})
                        else:                        
                            df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})            
                    
                    else:
                        df_mejoria =pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})

                    
                else:
                    df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})            

            #Validamos la segunda codicion de mejoria clinica
            elif (df_diabetico.resultado_prueba.iloc[0] >= 8) & (df_diabetico.resultado_prueba.iloc[0] <= 10):
                #Sacamos las dos fechas de los resultados y validamos si tienen diferencia mayor a 11 meses
                f1 = df_diabetico.fecha_ingreso.iloc[0]
                f2 = df_diabetico.fecha_ingreso.iloc[-1]
                f = (f2 - f1).days
                
                if f >= 335:
                    r1 = df_diabetico.resultado_prueba.iloc[0]
                    r2 = df_diabetico.resultado_prueba.iloc[-1]

                    if r1 > r2:
                        m = (r1 - r2)
                        if m >= 1:
                            df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[1]})
                        else:
                            df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})
                        
                    else:
                        df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})
                else:
                    df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})    
                    
            #Validamos la tercera codicion de mejoria clinica        
            elif df_diabetico.resultado_prueba.iloc[0] < 8:
                #Sacamos las dos fechas de los resultados y validamos si tienen diferencia mayor a 10 meses
                f1 = df_diabetico.fecha_ingreso.iloc[0]
                f2 = df_diabetico.fecha_ingreso.iloc[-1]
                f = (f2 - f1).days
                if f >= 335: 
                    r1 = df_diabetico.resultado_prueba.iloc[0]
                    r2 = df_diabetico.resultado_prueba.iloc[-1]

                    if r1 > r2:
                        m = (r1 - r2)
                        
                        if m >= 0.5:
                            df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[1]})
                        else:
                            df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})

                    else:
                        df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})
                
                else:
                    df_mejoria = pd.DataFrame({'identificacion_paciente':[i], 'edad':[df_diabetico.edad.iloc[-1]], 'prueba':[df_diabetico.prueba.iloc[-1]], 'resultado_prueba':[df_diabetico.resultado_prueba.iloc[-1]], 'unidad':[df_diabetico.unidad.iloc[-1]], 'fecha_ingreso':[df_diabetico.fecha_ingreso.iloc[-1]], 'medico':[df_diabetico.medico.iloc[-1]], 'compensados':[0]})
        df_mejoria_final = pd.concat([df_mejoria_final,df_mejoria])                    
    return df_mejoria_final
        
        
# Read data
diabeticos_mes_anterior = func_process.pd.read_csv(f"{PATH_DRIVE}/Scripts PYTHON/INDICADORES RCV/CSV_in/diabeticos_{mes_a}_{fecha_capita.year}.csv",
                                                    delimiter=';', 
                                                    dtype = {"identificacion_paciente" : "object"})

df_poblacion = loadbq.read_data_bigquery(sql_poblacion, TABLA_BIGQUERY_CAPITA_POBLACIONES)
df_diabeticos_hipertensos = loadbq.read_data_bigquery(sql_dm_hta,TABLA_BIGQUERY_CAPITA)
df_capita_reportes = loadbq.read_data_bigquery(sql_capita_reportes,TABLA_BIGQUERY_CAPITA)
df_rips_diabetes_hta = loadbq.read_data_bigquery(sql_rips_dm_hta,TABLA_BIGQUERY_RIPS_AUDITORIA)
df_datos_laboratorio = func_process.load_df_server(sql_lab_HMC, 'reportes')
df_datos_laboratorio_PU = func_process.load_df_server(sql_lab_PU, 'reportes')
df_hospitalizacion_dm = func_process.load_df_server(sql_hospitalizacion_dm, 'reportes')
df_hospitalizacion_hta = func_process.load_df_server(sql_hospitalizacion_hta, 'reportes')
df_urgencias_dm = func_process.load_df_server(sql_urgencias_dm, 'reportes')
df_urgencias_hta = func_process.load_df_server(sql_urgencias_hta, 'reportes')


# Convertir columnas fechas
df_poblacion['FECHA_CAPITA'] = func_process.pd.to_datetime(df_poblacion['FECHA_CAPITA'])
df_datos_laboratorio['fecha_ingreso'] = func_process.pd.to_datetime(df_datos_laboratorio['fecha_ingreso'])
df_datos_laboratorio_PU['fecha_ingreso'] = func_process.pd.to_datetime(df_datos_laboratorio_PU['fecha_ingreso'])
df_hospitalizacion_dm['Fecha_Egreso_Afiliado'] = func_process.pd.to_datetime(df_hospitalizacion_dm['Fecha_Egreso_Afiliado'])
df_hospitalizacion_hta['Fecha_Egreso_Afiliado'] = func_process.pd.to_datetime(df_hospitalizacion_hta['Fecha_Egreso_Afiliado'])
df_urgencias_dm['Fecha_Autorizacion'] = func_process.pd.to_datetime(df_urgencias_dm['Fecha_Autorizacion'])
df_urgencias_hta['Fecha_Autorizacion'] = func_process.pd.to_datetime(df_urgencias_hta['Fecha_Autorizacion'])

# conver tz_localize UTC
df_rips_diabetes_hta['fecha_capita'] = df_rips_diabetes_hta['fecha_capita'].dt.tz_localize(None)
df_rips_diabetes_hta['hora_fecha'] = df_rips_diabetes_hta['hora_fecha'].dt.tz_localize(None)

# Convertir diabetes e hipertension en Integer
df_diabeticos_hipertensos[['sw_diabetes', 'sw_hipertension']] = df_diabeticos_hipertensos[['sw_diabetes', 'sw_hipertension']].astype('int32')

# Pacientes Diabeticos
df_diabeticos = df_diabeticos_hipertensos[df_diabeticos_hipertensos['sw_diabetes'] == 1]
df_diabeticos.drop('sw_hipertension', axis=1, inplace=True)

# Pacientes Hipertensos
df_hipertensos = df_diabeticos_hipertensos[df_diabeticos_hipertensos['sw_hipertension'] == 1]
df_hipertensos.drop('sw_diabetes', axis=1, inplace=True)

# Filtrar por codigos
df_rips_diabeticos = df_rips_diabetes_hta[df_rips_diabetes_hta['codigo_sura'].isin(['70302','8903056','70300','70301','8902031','8903018','3000052','3000054'])]
df_rips_hta = df_rips_diabetes_hta[df_rips_diabetes_hta['codigo_sura'].isin(['8903018','8903056','8902031','3000054','3000052','8903015','3000053','70201','70202','70200'])]
        
# Guardar pacientes diabeticos del mes actual
diabeticos_mes = df_diabeticos[['identificacion_paciente','sw_diabetes']]
diabeticos_mes.to_csv(f"{PATH_DRIVE}/Scripts PYTHON/INDICADORES RCV/CSV_in/diabeticos_{mes}_{fecha_capita.year}.csv", sep=';', encoding='utf-8', index= False)

# Crear columnas pacientes diabeticos nuevos con respecto al mes anterior
df_diabeticos.insert(10, 'nuevo', df_diabeticos['identificacion_paciente'].mask(df_diabeticos['identificacion_paciente'].isin(diabeticos_mes_anterior['identificacion_paciente']), other= 0))
df_diabeticos.nuevo.mask(df_diabeticos.nuevo != 0, other= 1, inplace = True)

# Sacamos los resultados de laboratorio correspondientes a Hb1Ac y luego dejamos los de los ultimos 7 meses
df_datos_laboratorio_Hb1Ac = df_datos_laboratorio[df_datos_laboratorio.prueba == 'HEMOGLOBINA GLICOSILADA']
df_datos_laboratorio_Hb1Ac_7m = df_datos_laboratorio_Hb1Ac[df_datos_laboratorio_Hb1Ac.fecha_ingreso >= fecha_seven]

# cobertura_Hb1Ac son los pacientes DM que presentaron un 
# laboratorio de Hb1Ac en los ultimos 7 meses
df_diabeticos.insert(11, 
                    'cobertura_Hb1Ac', 
                    df_diabeticos['identificacion_paciente'].mask(df_diabeticos['identificacion_paciente'].isin(df_datos_laboratorio_Hb1Ac_7m['identificacion_paciente']),
                                                                    other= 1))
df_diabeticos.cobertura_Hb1Ac.mask(df_diabeticos.cobertura_Hb1Ac != 1,
                                    other= 0, 
                                    inplace = True)

# Agregamos la columna con el nombre de la sede
df_diabeticos['sede_atencion'] = df_diabeticos['sede_atencion'].astype('str')
df_diabeticos.insert(9, 'nombre_sede_atencion', df_diabeticos["sede_atencion"].map(n_ips))

#Sacamos Los resultados de laboratorio referentes a Hemoglobina Glicosilada del ultimo año
datos_laboratorio_Hb1Ac = df_datos_laboratorio[df_datos_laboratorio.prueba == 'HEMOGLOBINA GLICOSILADA']

#Sacamos los pacientes diabeticos activos en capita con algun examen de Hb1Ac en el ultimo año
df_diabeticos_hb1Ac = df_diabeticos.merge(datos_laboratorio_Hb1Ac, 
                                            on='identificacion_paciente', 
                                            how='inner')
df_diabeticos_hb1Ac = df_diabeticos_hb1Ac[['identificacion_paciente', 'edad', 'prueba', 'resultado', 'unidad', 'fecha_ingreso', 'medico']]
# Eliminamos algún resultado que nos haya salido con valor de MEMO
df_diabeticos_hb1Ac.drop(df_diabeticos_hb1Ac.loc[df_diabeticos_hb1Ac['resultado'] == 'MEMO'].index, inplace=True)

df_diabeticos_hb1Ac.resultado.replace('>18.50', '18.50', inplace = True)
df_diabeticos_hb1Ac.resultado.replace('>18.00', '18.00', inplace = True)
df_diabeticos_hb1Ac.resultado.replace('<18.50', '18.50', inplace = True)
df_diabeticos_hb1Ac.resultado.replace('>14.00', '14.00', inplace = True)

# Cambiamos el tipo de dato de la columna Resultado a Decimal o numerico, ahora de nombre *resultado_prueba*
# Borramos *resultado*
df_diabeticos_hb1Ac.insert(4, 'resultado_prueba', func_process.pd.to_numeric(df_diabeticos_hb1Ac['resultado']))
df_diabeticos_hb1Ac.drop('resultado', axis=1, inplace=True)

#Aplicamos procedimiento para dejar solo el ultimo resultado y asi validar MEJORIA CLINICA
df_Hb1Ac_last_date = create_Hb1Ac_last_date(df_diabeticos_hb1Ac)
df_Hb1Ac_last_date['edad'] = df_Hb1Ac_last_date['edad'].astype('int64')
df_Hb1Ac_last_date_70 = df_Hb1Ac_last_date[df_Hb1Ac_last_date['edad'] > 70]

pac_telemedicina_6M = df_rips_diabetes_hta[df_rips_diabetes_hta.codigo_sura.isin(['3000052','3000053', '3000054']) & (df_rips_diabetes_hta.hora_fecha >= fecha_six)].identificacion_pac
pac_presenciales = df_rips_diabetes_hta[~df_rips_diabetes_hta.codigo_sura.isin(['3000052','3000053', '3000054'])].identificacion_pac
df_Hb1Ac_last_date_70_no_telemedicina_no_presenciales = df_Hb1Ac_last_date_70[(~df_Hb1Ac_last_date_70['identificacion_paciente'].isin(pac_telemedicina_6M)) & (~df_Hb1Ac_last_date_70['identificacion_paciente'].isin(pac_presenciales))]




"""  Realizamos la validacion De las dos primeras condiciones
        1. Pacientes menores de **65** años **Hb1Ac** menor o igual **7.0** 
        2. Pacientes con **65** años o más **Hb1AC** menor o igual a **8.0**. """

Hb1Ac_last_date_compensados = df_Hb1Ac_last_date.loc[
    ((df_Hb1Ac_last_date['edad'] < 65) & (df_Hb1Ac_last_date['resultado_prueba'] <= 7.0)) | 
    ((df_Hb1Ac_last_date['edad'] >= 65) & (df_Hb1Ac_last_date['resultado_prueba'] <= 8.0))
]
Hb1Ac_last_date_compensados.loc[:,'compensados'] = 1

# Sacamos las cedulas de los pacientes para revisarles mejoria clinica
ids_diabeticos_hb1Ac = set(df_diabeticos_hb1Ac['identificacion_paciente'])
ids_diabeticos_compensados = set(Hb1Ac_last_date_compensados['identificacion_paciente'])
cedulas_mejoria_clinica = ids_diabeticos_hb1Ac.difference(ids_diabeticos_compensados)

""" El proceso toma la poblacion diabetica con uno o mas resultado de examen de **Hb1Ac**
        en el ultimo año que no haya salido compensada con las primeras dos validaciones y le realiza las otras tres validacion 
        """
df_mejoria = validacion_examenes_hb1ac(df_diabeticos_hb1Ac)
compensados_mejoria = func_process.pd.concat([Hb1Ac_last_date_compensados, df_mejoria], axis=0)
compensados_mejoria.drop(columns='edad', inplace= True)
diabeticos_all = df_diabeticos.merge(compensados_mejoria, on= 'identificacion_paciente', how='left')

""" SOLICITUD CRUCE
    * Diabéticos 
    * Con Hemoglobina glicosilada fuera de metas
    * No hayan tenido atenciones presenciales en el último año o virtuales en los últimos 6 meses.
    """
diabeticos_Hb1Ac_no_compensados = diabeticos_all[(diabeticos_all.cobertura_Hb1Ac == 1) & (diabeticos_all.compensados == 0)]
diabeticos_Hb1Ac_no_compensados_no_telemedicina_no_presenciales = diabeticos_Hb1Ac_no_compensados[(~diabeticos_Hb1Ac_no_compensados['identificacion_paciente'].isin(pac_telemedicina_6M)) & (~diabeticos_Hb1Ac_no_compensados['identificacion_paciente'].isin(pac_presenciales))]

# Resultados de Laboratorio del Ultimo años
df_datos_laboratorio_tamizaje = df_datos_laboratorio[~df_datos_laboratorio.prueba.isin(cod_lab)]
df_diabeticos_hipertensos.insert(0, 'fecha_capita', fecha_capita)
df_diabeticos_hipertensos.insert(10, 'nombre_sede_atencion', df_diabeticos_hipertensos["sede_atencion"].map(n_ips))
df_diabeticos_hipertensos.insert(13, 'creatinina', df_diabeticos_hipertensos['identificacion_paciente'].apply(creatinina)) 
df_diabeticos_hipertensos.insert(14, 'p_orina', df_diabeticos_hipertensos['identificacion_paciente'].apply(p_orina))
df_diabeticos_hipertensos.insert(15, 'microalbuminuria', df_diabeticos_hipertensos['identificacion_paciente'].apply(microalbuminuria))
df_diabeticos_hipertensos['sede_atencion'] = df_diabeticos_hipertensos['sede_atencion'].astype('str')
df_diabeticos_hipertensos['nombre_sede_atencion'] = df_diabeticos_hipertensos["sede_atencion"].map(n_ips)
df_diabeticos_hipertensos.insert(16, 'todos_examenes', df_diabeticos_hipertensos.apply(lambda x:  todos_examenes(x['creatinina'], x['p_orina'], x['microalbuminuria']), axis=1))
df_diabeticos_hipertensos_contacto = df_diabeticos_hipertensos.merge(df_capita_reportes, on='identificacion_paciente', how='left')
df_diabeticos_hipertensos_contacto['sede_atencion'] = df_diabeticos_hipertensos_contacto['sede_atencion'].astype('str')
df_diabeticos_hipertensos_contacto['nombre_sede_atencion'] = df_diabeticos_hipertensos_contacto['sede_atencion'].map(n_ips)
df_diabeticos_hipertensos.sede_atencion = df_diabeticos_hipertensos.sede_atencion.astype('str')
df_diabeticos_hipertensos['nombre_sede_atencion']= df_diabeticos_hipertensos["sede_atencion"].map(n_ips)
df_diabeticos_hipertensos[['sw_diabetes', 'sw_hipertension']] = df_diabeticos_hipertensos[['sw_diabetes', 'sw_hipertension']].astype('int64')

df_diabeticos_hipertensos_totales = df_diabeticos_hipertensos.groupby(['fecha_capita', 'nombre_sede_atencion']
                                                                      ).agg({'identificacion_paciente':'count', 
                                                                            'sw_diabetes':'sum', 
                                                                            'sw_hipertension':'sum', 
                                                                            'creatinina':'sum', 
                                                                            'p_orina':'sum', 
                                                                            'microalbuminuria':'sum', 
                                                                            'todos_examenes':'sum'}
                                                                          ).reset_index().rename(
                                                                                                  columns={'identificacion_paciente':'total_pacientes'}
                                                                                                )
list_diabeticos_hipertensos_totales = [fecha_capita,'COOPSANA IPS'] + df_diabeticos_hipertensos_totales.sum(numeric_only=True).tolist()
df_diabeticos_hipertensos_totales.loc[len(df_diabeticos_hipertensos_totales)] = list_diabeticos_hipertensos_totales

# df_diabeticos_hipertensos_totales['nombre_sede_atencion'].fillna('COOPSANA IPS', inplace = True)
# df_diabeticos_hipertensos_totales['fecha_capita'].fillna(fecha_capita, inplace = True)

df_diabeticos_hipertensos_totales['total_pacientes'] = df_diabeticos_hipertensos_totales['total_pacientes'].astype('int64')
df_diabeticos_hipertensos_totales['sw_diabetes'] = df_diabeticos_hipertensos_totales['sw_diabetes'].astype('int64')
df_diabeticos_hipertensos_totales['sw_hipertension'] = df_diabeticos_hipertensos_totales['sw_hipertension'].astype('int64')
df_diabeticos_hipertensos_totales['creatinina'] = df_diabeticos_hipertensos_totales['creatinina'].astype('int64')
df_diabeticos_hipertensos_totales['p_orina'] = df_diabeticos_hipertensos_totales['p_orina'].astype('int64')
df_diabeticos_hipertensos_totales['microalbuminuria'] = df_diabeticos_hipertensos_totales['microalbuminuria'].astype('int64')
df_diabeticos_hipertensos_totales['todos_examenes'] = df_diabeticos_hipertensos_totales['todos_examenes'].astype('int64')

df_poblacion = df_poblacion[df_poblacion['FECHA_CAPITA'] == fecha_cargue.strftime('%Y-%m-%d')]
df_diabeticos_hipertensos_totales_p = df_diabeticos_hipertensos_totales.merge(df_poblacion, on='nombre_sede_atencion')
df_diabeticos_hipertensos_totales_p.drop(columns='FECHA_CAPITA', inplace=True)
df_diabeticos_hipertensos_totales_p['fecha_capita'] = df_diabeticos_hipertensos_totales_p['fecha_capita'].astype('str')
df_diabeticos_hipertensos_totales_p.rename({'MAYORES_DE_18_ANOS':'mayores_18_anos'}, axis=1, inplace=True)
df_diabeticos_hipertensos_totales_p['fecha_capita'] = fecha_cargue

# Sacamos un atabla RIPS con los registros de diabeticos de los ultimos 7 meses.
rips_diabetes_seven_months = df_rips_diabeticos[(df_rips_diabeticos.hora_fecha > pd.to_datetime(fecha_seven)) &
                                                 (df_rips_diabeticos.hora_fecha < pd.to_datetime(fecha))]

diabeticos_all.insert(0, 'fecha_capita', fecha_capita)
diabeticos_all.insert(14, 'cobertura_dm', diabeticos_all['identificacion_paciente'].mask(diabeticos_all['identificacion_paciente'].isin(rips_diabetes_seven_months['identificacion_pac']), other= 1))
diabeticos_all.cobertura_dm.mask(diabeticos_all.cobertura_dm != 1, other= 0, inplace = True)
df_rips_diabeticos_mes_capita = df_rips_diabeticos[(df_rips_diabeticos.fecha_capita == fecha_cargue.strftime('%Y-%m-%d'))]

def c_atencion_enfermeria(iden):
        return df_rips_diabeticos_mes_capita[df_rips_diabeticos_mes_capita['identificacion_pac'] == iden].codigo_sura.isin(['70302', '8903056']).sum()


def c_atencion_medico(iden):
        return df_rips_diabeticos_mes_capita[df_rips_diabeticos_mes_capita['identificacion_pac'] == iden].codigo_sura.isin(['70300', '70301', '8902031', '8903018', '3000052', '3000054']).sum()

diabeticos_all.insert(15, 'c_atencion_enfermeria', diabeticos_all['identificacion_paciente'].apply(c_atencion_enfermeria))
diabeticos_all.insert(16, 'c_atencion_medico', diabeticos_all['identificacion_paciente'].apply(c_atencion_medico))

hospitalizacion_mes_dm = df_hospitalizacion_dm[(df_hospitalizacion_dm.Fecha_Egreso_Afiliado >= fecha.strftime('%Y-%m-%d')) & (df_hospitalizacion_dm.Fecha_Egreso_Afiliado <= fecha_f)]
urgencias_mes_dm = df_urgencias_dm[(df_urgencias_dm.Fecha_Autorizacion >= fecha.strftime('%Y-%m-%d')) & (df_urgencias_dm.Fecha_Autorizacion <= fecha_f)]

diabeticos_all.insert(17, 'hospitalizacion_dm', diabeticos_all['identificacion_paciente'].mask(diabeticos_all['identificacion_paciente'].isin(hospitalizacion_mes_dm['Numero_de_documento']), other= 1))
diabeticos_all.hospitalizacion_dm.mask(diabeticos_all.hospitalizacion_dm != 1, other= 0, inplace = True)

diabeticos_all.insert(18, 'urgencias_dm', diabeticos_all['identificacion_paciente'].mask(diabeticos_all['identificacion_paciente'].isin(urgencias_mes_dm['Numero_de_documento']), other= 1))
diabeticos_all.urgencias_dm.mask(diabeticos_all.urgencias_dm != 1, other= 0, inplace = True)


# Ahora vamos a contar los pacientes DM con alguna atencion el ultimo año ya sea por Medico o Enfermeria
def consultas_ultimo_ano_enfermeria(ide):
        return df_rips_diabeticos[df_rips_diabeticos['identificacion_pac'] == ide].codigo_sura.isin(['70302']).sum()
        
def consultas_ultimo_ano_medico(ide):
        return df_rips_diabeticos[df_rips_diabeticos['identificacion_pac'] == ide].codigo_sura.isin(['70300', '70301', '8902031', '8903018', '3000052', '3000054']).sum()
        
def consultas_ultimo_ano(ide):
        return len(df_rips_diabeticos[df_rips_diabeticos['identificacion_pac'] == ide].codigo_sura)

diabeticos_all.insert(17, 'consultas_ultimo_ano_e', diabeticos_all['identificacion_paciente'].apply(consultas_ultimo_ano_enfermeria))
diabeticos_all.insert(18, 'consultas_ultimo_ano_m', diabeticos_all['identificacion_paciente'].apply(consultas_ultimo_ano_medico))
diabeticos_all.insert(19, 'consultas_ultimo_ano', diabeticos_all['identificacion_paciente'].apply(consultas_ultimo_ano))

diabeticos_all_contacto = diabeticos_all.merge(df_capita_reportes, on='identificacion_paciente', how='left')
diabeticos_all_contacto.prueba.fillna('', inplace= True)
diabeticos_all_contacto.resultado_prueba.fillna(0, inplace= True)
diabeticos_all_contacto.unidad.fillna('', inplace= True)
diabeticos_all_contacto.fecha_ingreso.fillna('', inplace= True)
diabeticos_all_contacto.medico.fillna('', inplace= True)
diabeticos_all_contacto.compensados.fillna(0, inplace= True)

diabeticos_all_contacto.drop('fecha_capita', axis=1, inplace= True)
diabeticos_all_contacto.insert(0, 'fecha_capita', fecha_capita)
diabeticos_all_contacto.loc[diabeticos_all_contacto.loc[:, 'sexo'] == 'M']
diabeticos_all_contacto['fecha_capita'] = diabeticos_all_contacto['fecha_capita'].astype('str')
diabeticos_all_contacto['fecha_nacimiento'] = diabeticos_all_contacto['fecha_nacimiento'].astype('str')
diabeticos_all_contacto['fecha_ingreso'] = diabeticos_all_contacto['fecha_ingreso'].astype('str')
diabeticos_all_contacto.fecha_capita = fecha_cargue

diabeticos_all.sede_atencion = diabeticos_all.sede_atencion.astype('str')
diabeticos_all.drop(columns='nombre_sede_atencion', inplace= True)
diabeticos_all.insert(10, 'nombre_sede_atencion', diabeticos_all["sede_atencion"].map(n_ips))

# Sacamos los totales de las atenciones de enfermeria y medico de Pacientes DM compensados
diabeticos_c_atencion_totales = diabeticos_all[diabeticos_all.compensados == 1].groupby('nombre_sede_atencion').agg({'c_atencion_enfermeria':'sum', 'c_atencion_medico':'sum'}).reset_index().rename(columns={'c_atencion_enfermeria':'c_atencion_enfermeria_compensados', 'c_atencion_medico':'c_atencion_medico_compensados'})

# Sacamos los totales de las Hoapitalizaciones y Urgencias de Pacientes DM CON Cobertura
diabeticos_hosp_urge_totales_c = diabeticos_all[(diabeticos_all.cobertura_dm == 1)].groupby('nombre_sede_atencion').agg({'hospitalizacion_dm':'sum', 'urgencias_dm':'sum'}).reset_index().rename(columns={'hospitalizacion_dm':'hospitalizacion_dm_cubiertos', 'urgencias_dm':'urgencias_dm_cubiertos'})

# Sacamos los totales de las Hoapitalizaciones y Urgencias de Pacientes DM SIN Cobertura
diabeticos_hosp_urge_totales_no_c = diabeticos_all[(diabeticos_all.cobertura_dm == 0)].groupby('nombre_sede_atencion').agg({'hospitalizacion_dm':'sum', 'urgencias_dm':'sum'}).reset_index().rename(columns={'hospitalizacion_dm':'hospitalizacion_dm_no_cubiertos', 'urgencias_dm':'urgencias_dm_no_cubiertos'})

# Juntamos en una sola tabla eso totales
dm_atencion_hosp_urge_totales = diabeticos_c_atencion_totales.merge(diabeticos_hosp_urge_totales_c, on='nombre_sede_atencion').merge(diabeticos_hosp_urge_totales_no_c, on='nombre_sede_atencion')


#TABLA DE COBERTURA DM
cobertura_dm_totales = diabeticos_all.groupby(['fecha_capita', 
                                                'nombre_sede_atencion']).agg({'identificacion_paciente':'count', 
                                                                            'cobertura_Hb1Ac':'sum', 
                                                                            'cobertura_dm':'sum', 
                                                                            'compensados':'sum', 
                                                                            'c_atencion_enfermeria':'sum', 
                                                                            'c_atencion_medico':'sum', 
                                                                            'consultas_ultimo_ano_e':'sum',
                                                                            'consultas_ultimo_ano_m':'sum', 
                                                                            'consultas_ultimo_ano':'sum', 
                                                                            'hospitalizacion_dm':'sum', 
                                                                            'urgencias_dm':'sum'}).reset_index()
cobertura_dm_totales.rename({'identificacion_paciente':'poblacion_diabetes'},
                            axis=1, 
                            inplace=True)
cobertura_dm_totales = cobertura_dm_totales.merge(dm_atencion_hosp_urge_totales, 
                                                    on= 'nombre_sede_atencion', 
                                                    how='left')

cobertura_dm_totales[['cobertura_Hb1Ac','cobertura_dm','compensados','c_atencion_enfermeria','c_atencion_medico','consultas_ultimo_ano_e','consultas_ultimo_ano_m','consultas_ultimo_ano','hospitalizacion_dm','urgencias_dm','urgencias_dm_cubiertos','hospitalizacion_dm_cubiertos','hospitalizacion_dm_no_cubiertos','urgencias_dm_no_cubiertos']] = cobertura_dm_totales[['cobertura_Hb1Ac','cobertura_dm','compensados','c_atencion_enfermeria','c_atencion_medico','consultas_ultimo_ano_e','consultas_ultimo_ano_m','consultas_ultimo_ano','hospitalizacion_dm','urgencias_dm','urgencias_dm_cubiertos','hospitalizacion_dm_cubiertos','hospitalizacion_dm_no_cubiertos','urgencias_dm_no_cubiertos']].astype('int64')

list_cobertura_dm_totales = [fecha_capita,'COOPSANA IPS'] + cobertura_dm_totales.sum(numeric_only= True).tolist()
cobertura_dm_totales.loc[len(cobertura_dm_totales)] = list_cobertura_dm_totales                                      


# cobertura_dm_totales['fecha_capita'].fillna(fecha_capita, 
#                                             inplace = True)
# cobertura_dm_totales['nombre_sede_atencion'].fillna('COOPSANA IPS', 
#                                                     inplace = True)

cobertura_dm_totales['poblacion_diabetes'] = cobertura_dm_totales['poblacion_diabetes'].astype('int64')
cobertura_dm_totales['cobertura_Hb1Ac'] = cobertura_dm_totales['cobertura_Hb1Ac'].astype('int64')
cobertura_dm_totales['cobertura_dm'] = cobertura_dm_totales['cobertura_dm'].astype('int64')
cobertura_dm_totales['compensados'] = cobertura_dm_totales['compensados'].astype('int64')
cobertura_dm_totales['c_atencion_enfermeria'] = cobertura_dm_totales['c_atencion_enfermeria'].astype('int64')
cobertura_dm_totales['c_atencion_medico'] = cobertura_dm_totales['c_atencion_medico'].astype('int64')
cobertura_dm_totales['consultas_ultimo_ano_e'] = cobertura_dm_totales['consultas_ultimo_ano_e'].astype('int64')
cobertura_dm_totales['consultas_ultimo_ano_m'] = cobertura_dm_totales['consultas_ultimo_ano_m'].astype('int64')
cobertura_dm_totales['consultas_ultimo_ano'] = cobertura_dm_totales['consultas_ultimo_ano'].astype('int64')
cobertura_dm_totales['c_atencion_enfermeria_compensados'] = cobertura_dm_totales['c_atencion_enfermeria_compensados'].astype('int64')
cobertura_dm_totales['c_atencion_medico_compensados'] = cobertura_dm_totales['c_atencion_medico_compensados'].astype('int64')
cobertura_dm_totales['hospitalizacion_dm'] = cobertura_dm_totales['hospitalizacion_dm'].astype('int64')
cobertura_dm_totales['urgencias_dm'] = cobertura_dm_totales['urgencias_dm'].astype('int64')
cobertura_dm_totales['hospitalizacion_dm_cubiertos'] = cobertura_dm_totales['hospitalizacion_dm_cubiertos'].astype('int64')
cobertura_dm_totales['urgencias_dm_cubiertos'] = cobertura_dm_totales['urgencias_dm_cubiertos'].astype('int64')
cobertura_dm_totales['hospitalizacion_dm_no_cubiertos'] = cobertura_dm_totales['hospitalizacion_dm_no_cubiertos'].astype('int64')
cobertura_dm_totales['urgencias_dm_no_cubiertos'] = cobertura_dm_totales['urgencias_dm_no_cubiertos'].astype('int64')

cobertura_dm_totales_p = cobertura_dm_totales.merge(df_poblacion, on='nombre_sede_atencion')
cobertura_dm_totales_p.drop('FECHA_CAPITA', axis= 1, inplace=True)
cobertura_dm_totales_p['fecha_capita'] = cobertura_dm_totales_p['fecha_capita'].astype('str')
cobertura_dm_totales_p.rename({'MAYORES_DE_18_ANOS':'mayores_18_anos'}, axis=1, inplace=True)
cobertura_dm_totales_p.fecha_capita = fecha_cargue

df_rips_hta_seven_months = df_rips_hta[(df_rips_hta.hora_fecha > fecha_seven) & (df_rips_hta.hora_fecha <= fecha)]

df_hta = df_hipertensos
df_hta.sede_atencion = df_hta.sede_atencion.astype('str')
df_hta.insert(0, 'fecha_capita', fecha_cargue)
df_hta.insert(10, 'nombre_sede_atencion', df_hta["sede_atencion"].map(n_ips))
df_hta.insert(12, 'cobertura_hta', df_hta['identificacion_paciente'].mask(df_hta['identificacion_paciente'].isin(df_rips_hta_seven_months['identificacion_pac']), other= 1))
df_hta.cobertura_hta.mask(df_hta.cobertura_hta != 1, other= 0, inplace = True)

#Agregamos dos columnas donde marcamos los pacientes HTA que hayan tenido alguna hospitalizacion o urgencia en el mes
hospitalizacion_mes_hta = df_hospitalizacion_hta[(df_hospitalizacion_hta.Fecha_Egreso_Afiliado >= fecha.strftime('%Y-%m-%d')) & (df_hospitalizacion_hta.Fecha_Egreso_Afiliado <= fecha_f)]
urgencias_mes_hta = df_urgencias_hta[(df_urgencias_hta.Fecha_Autorizacion >= fecha) & (df_urgencias_hta.Fecha_Autorizacion <= fecha_f)]
df_hta['hospitalizacion_hta']= df_hta['identificacion_paciente'
                                     ].mask(df_hta['identificacion_paciente'
                                                  ].isin(hospitalizacion_mes_hta['Numero_de_documento']), other= 1)
df_hta.hospitalizacion_hta.mask(df_hta.hospitalizacion_hta != 1, other= 0, inplace = True)
df_hta['urgencias_hta']= df_hta['identificacion_paciente'
                               ].mask(df_hta['identificacion_paciente'
                                            ].isin(urgencias_mes_hta['Numero_de_documento']), other= 1)
df_hta.urgencias_hta.mask(df_hta.urgencias_hta != 1, other= 0, inplace = True)
df_hta_contacto = df_hta.merge(df_capita_reportes, 
                               on='identificacion_paciente', 
                               how= 'left')

df_hta_contacto.fillna('', inplace= True)
df_hta_contacto['fecha_capita'] = fecha_capita.strftime('%Y-%m-%d')
df_hta_contacto = df_hta_contacto[['fecha_capita', 'identificacion_paciente', 'primer_apellido', 'segundo_apellido', 'primer_nombre', 'segundo_nombre', 'sexo', 'fecha_nacimiento', 'edad', 'sede_atencion', 'nombre_sede_atencion', 'sw_hipertension', 'cobertura_hta', 'hospitalizacion_hta', 'urgencias_hta', 'mes_cargue', 'telefono', 'celular']]

# Sacamos los totales de las Hoapitalizaciones y Urgencias de Pacientes HTA CON Cobertura_HTA
hta_hosp_urge_totales_c = df_hta[(df_hta.cobertura_hta == 1)].groupby('nombre_sede_atencion').agg({'hospitalizacion_hta':'sum', 
                                                                                                   'urgencias_hta':'sum'}).reset_index()
hta_hosp_urge_totales_c.rename({'hospitalizacion_hta':'hospitalizacion_hta_cubiertos', 
                                'urgencias_hta':'urgencias_hta_cubiertos'},
                               axis=1,
                               inplace=True)

# Sacamos los totales de las Hospitalizaciones y Urgencias de Pacientes HTA Sin Cobertura_HTA
hta_hosp_urge_totales_no_c = df_hta[(df_hta.cobertura_hta == 0)].groupby('nombre_sede_atencion').agg({'hospitalizacion_hta':'sum', 
                                                                                                      'urgencias_hta':'sum'}).reset_index()
hta_hosp_urge_totales_no_c.rename({'hospitalizacion_hta':'hospitalizacion_hta_no_cubiertos', 
                                   'urgencias_hta':'urgencias_hta_no_cubiertos'},
                                  axis=1,
                                  inplace=True)
# Juntamos en una sola tabla eso totales
hta_hosp_urge_totales = hta_hosp_urge_totales_c.merge(hta_hosp_urge_totales_no_c, 
                                                      on='nombre_sede_atencion')

#TABLA DE COBERTURA HTA
df_hta_totales = df_hta.groupby(['fecha_capita', 'nombre_sede_atencion']).agg({'identificacion_paciente':'count', 
                                                                               'cobertura_hta':'sum', 
                                                                               'hospitalizacion_hta':'sum', 
                                                                               'urgencias_hta':'sum'}).reset_index()
df_hta_totales.rename({'identificacion_paciente':'poblacion_hta'}, 
                      axis=1, 
                      inplace=True)
df_hta_totales = df_hta_totales.merge(hta_hosp_urge_totales, 
                                      on='nombre_sede_atencion')

df_hta_totales[['cobertura_hta','hospitalizacion_hta','urgencias_hta','hospitalizacion_hta_cubiertos','urgencias_hta_cubiertos','hospitalizacion_hta_no_cubiertos','urgencias_hta_no_cubiertos']] = df_hta_totales[['cobertura_hta','hospitalizacion_hta','urgencias_hta','hospitalizacion_hta_cubiertos','urgencias_hta_cubiertos','hospitalizacion_hta_no_cubiertos','urgencias_hta_no_cubiertos']].astype('int64')

list_hta_totales = [fecha_capita,'COOPSANA IPS'] + df_hta_totales.sum(numeric_only= True).tolist()
df_hta_totales.loc[len(df_hta_totales)] = list_hta_totales

# df_hta_totales['fecha_capita'].fillna(fecha_capita, 
#                                       inplace = True)
# df_hta_totales['nombre_sede_atencion'].fillna('COOPSANA IPS', 
#                                               inplace = True)

df_hta_totales['poblacion_hta'] = df_hta_totales['poblacion_hta'].astype('int64')
df_hta_totales['cobertura_hta'] = df_hta_totales['cobertura_hta'].astype('int64')
df_hta_totales['hospitalizacion_hta'] = df_hta_totales['hospitalizacion_hta'].astype('int64')
df_hta_totales['urgencias_hta'] = df_hta_totales['urgencias_hta'].astype('int64')
df_hta_totales['hospitalizacion_hta_cubiertos'] = df_hta_totales['hospitalizacion_hta_cubiertos'].astype('int64')
df_hta_totales['urgencias_hta_cubiertos'] = df_hta_totales['urgencias_hta_cubiertos'].astype('int64')
df_hta_totales['hospitalizacion_hta_no_cubiertos'] = df_hta_totales['hospitalizacion_hta_no_cubiertos'].astype('int64')
df_hta_totales['urgencias_hta_no_cubiertos'] = df_hta_totales['urgencias_hta_no_cubiertos'].astype('int64')
df_hta_totales['fecha_capita'] = df_hta_totales['fecha_capita'].astype('str')

df_rips_diabetes_hta.drop(['poblacion_total', 'poblacion_total_coopsana'], axis=1, inplace= True)
df_rips_diabetes_hta.insert(16, 'atencion_enfermeria', df_rips_diabetes_hta['codigo_sura'].apply(lambda x: 1 if ((x == '70202') | (x =='70302') | (x =='8903056')) else 0))
df_rips_diabetes_hta.insert(17, 'atencion_medico', df_rips_diabetes_hta['codigo_sura'].apply(lambda x: 1 if ((x == '70200')| (x == '70201') | (x == '70300') | (x == '70301') | (x == '3000052') | (x == '3000054') | (x == '8902031') | (x == '8903018')) else 0))

fu_diabetes_hta_last_year = df_rips_diabetes_hta.groupby(['fecha_capita', 'nombre_ips', 'nombres_med']).agg({'atencion_enfermeria':'sum', 'atencion_medico':'sum', 'identificacion_pac':'count'}).rename(columns={'identificacion_pac':'total_atenciones'}).reset_index()
fu_diabetes_hta_last_year['nombres_med'] = fu_diabetes_hta_last_year['nombres_med'].str.replace('Á', 'A') 
fu_diabetes_hta_last_year['nombres_med'] = fu_diabetes_hta_last_year['nombres_med'].str.replace('É', 'E') 
fu_diabetes_hta_last_year['nombres_med'] = fu_diabetes_hta_last_year['nombres_med'].str.replace('Í', 'I') 
fu_diabetes_hta_last_year['nombres_med'] = fu_diabetes_hta_last_year['nombres_med'].str.replace('Ó', 'O') 
fu_diabetes_hta_last_year['nombres_med'] = fu_diabetes_hta_last_year['nombres_med'].str.replace('Ú', 'U') 
fu_diabetes_hta_last_year['nombres_med'] = fu_diabetes_hta_last_year['nombres_med'].str.replace('Ñ', 'N') 
fu_diabetes_hta_last_year_totales = fu_diabetes_hta_last_year.groupby('fecha_capita').agg({'atencion_enfermeria':'sum', 
                                                                                           'atencion_medico':'sum', 
                                                                                           'total_atenciones':'sum'}).reset_index()
fu_diabetes_hta_last_year_totales.insert(1, 'nombre_ips', 'COOPSANA IPS')
fu_diabetes_hta_last_year_totales.insert(2, 'nombres_med', '')
fu_diabetes_hta_last_year = func_process.pd.concat([fu_diabetes_hta_last_year, 
                                                    fu_diabetes_hta_last_year_totales]).sort_values(by= ['fecha_capita']).reset_index(drop=True)
fu_diabetes_hta_last_year['fecha_capita'] = fu_diabetes_hta_last_year['fecha_capita'].astype('str')

fu_diabetes_hta_last_year.fecha_capita = pd.to_datetime(fu_diabetes_hta_last_year.fecha_capita, errors='coerce')
df_diabeticos_hipertensos_totales_p.fecha_capita = pd.to_datetime(df_diabeticos_hipertensos_totales_p.fecha_capita, errors='coerce')
df_hta_totales.fecha_capita = pd.to_datetime(df_hta_totales.fecha_capita, errors='coerce')

#  Listas dataframe
list_dataframes = [df_diabeticos_hipertensos_contacto,df_diabeticos_hipertensos_totales_p,diabeticos_all_contacto,
                   cobertura_dm_totales_p,df_hta_contacto,df_hta_totales,fu_diabetes_hta_last_year]

detalle_df_save = pd.DataFrame({
            'tabla_bigquery':LIST_TABLA_BIGQUERY,
            'tabla_mariadb':LIST_TABLA_MARIADB,
            'column_validator':LIST_COLUMN_VALIDATOR,
            'name_df':list_dataframes,
            'if_exist_bigquery':LIST_IF_EXIST_BIGQUERY,
            'if_exist_mariadb':LIST_IF_EXIST_MARIADB
        })


# Save data Hb1Ac_70
df_Hb1Ac_last_date_70_no_telemedicina_no_presenciales.to_excel(f"Hb1Ac_70_{mes}.xlsx", index=False)
execution_load(detalle_df_save)

