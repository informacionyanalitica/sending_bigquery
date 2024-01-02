import sys,os
import pandas as pd
from datetime import datetime,timedelta

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

# Parametros bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'rips'
table_name_resumen_cpr = 'resumen_cpr'
table_name_indicador_cpr = 'rips_cpr_indicador'
table_name_rips_auditoria = 'rips_auditoria_poblacion_2'

TABLA_BIGQUERY_RESUMEN = f'{project_id_product}.{dataset_id_rips}.{table_name_resumen_cpr}'
TABLA_BIGQUERY_INDICADOR = f'{project_id_product}.{dataset_id_rips}.{table_name_indicador_cpr}'
TABLA_BIGQUERY_RIPS =  f'{project_id_product}.{dataset_id_rips}.{table_name_rips_auditoria}'

today = func_process.pd.to_datetime(datetime.now() - timedelta(days=15))
fecha = f"{today.year}-{today.month}-01"
fecha_f = f"{today.year}-{today.month}-{today.days_in_month} 23:59:59"
fecha_capita = func_process.pd.to_datetime(f"{today.year}-{today.month}-15").strftime("%Y-%m-%d")
mes = today.strftime('%B').upper()
ano = today.strftime('%Y')

sql_lab_PIE_pos = f"""
                SELECT 
                    patientId as identificacion_pac,
                    name as nombres,
                    lastName as apellidos,
                    sede as ips,
                    gender as sexo,                    
                    edad as edad_anos,
                    nameSueltos as nombre_prueba,
                    resultSueltos as resultado_prueba,
                    fechaValidacionSueltos as fecha_validacion,
                    medico as nombre_med
                FROM examenesLaboratorioView
                WHERE (nameSueltos = 'PRUEBA INMUNOLÓGICA DE EMBARAZO (HCG Cualitativa)')
                    AND (resultSueltos = 'POSITIVO') 
                    AND (fechaValidacionSueltos BETWEEN '{fecha}' AND '{fecha_f}')
                """
sql_rips_cpr = f"""
            SELECT 
                * 
            FROM rips_auditoria_poblacion_2 
            WHERE (codigo_sura IN ('70100','7000075','70101','7000076')) 
                AND (fecha_capita >= '{fecha_capita}');
            """
sql_rips_cpr_nine = f"""
                        SELECT * 
                        FROM rips_auditoria_poblacion_2 
                        WHERE (codigo_sura IN ('70100','70101','7000075','7000076'))
                            AND fecha_capita >= adddate(CURDATE(), INTERVAL -9 MONTH)
                        ORDER BY fecha_capita;
                    """
#Creamos funcion para agregar el nombre del programa
def n_programa(codigo_sura):
    if codigo_sura == '70100':
        return 'IngresoCPR'
    elif codigo_sura == '7000075':
        return 'IngresoCPR Telemedicina'
    elif codigo_sura == '70101':
        return 'ControlCPR'
    elif codigo_sura == '7000076':
        return 'ControlCPR Telemedicina'
    
def validate_load(df_validate_load,df_validate_rips,df_load,tabla_bigquery,table_mariadb):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        total_rips = df_validate_rips.totalCargues[0]
        if  total_cargue == 0 and total_rips >0:
            # Cargar mariadb
            func_process.save_df_server(df_load, table_mariadb, 'analitica')
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,tabla_bigquery)
    except ValueError as err:
        print(err)

def convert_columns_date(df):
    df.fecha_capita = pd.to_datetime(df.fecha_capita, errors='coerce')
    return df

def convert_columns_numeric(df):
    df.indicador = df.indicador.astype(float)
    return df
## CARGAMOS INFORMACION DE LA TABLA RIPS
lab_PIE = func_process.load_df_server(sql_lab_PIE_pos, 'reportes')
rips_cpr = func_process.load_df_server(sql_rips_cpr, 'analitica')
rips_cpr_nine = func_process.load_df_server(sql_rips_cpr_nine, 'analitica')

lab_PIE['fecha_capita'] = fecha_capita
rips_cpr.iloc[:, [1,23]] = rips_cpr.iloc[:, [1,23]].apply(func_process.pd.to_datetime, errors='coerce')
rips_cpr_nine.iloc[:, [1,23]] = rips_cpr_nine.iloc[:, [1,23]].apply(func_process.pd.to_datetime, errors='coerce')
rips_cpr = rips_cpr[rips_cpr['hora_fecha'] <= fecha_f]
rips_cpr_nine = rips_cpr_nine[rips_cpr_nine['hora_fecha'] <= fecha_f]

rips_cpr.insert(15, 'programa', rips_cpr['codigo_sura'].apply(n_programa))
ingreso_cpr = rips_cpr[['hora_fecha', 'identificacion_pac', 'ips', 'codigo_sura', 'primer_nombre_pac', 'primer_apellido_pac', 'segundo_apellido_pac', 'fecha_nacimiento', 'edad_anos', 'sexo']]
rips_cpr_resumen = rips_cpr.groupby(['fecha_capita', 'nombre_ips', 'codigo_sura', 'programa']).agg({'identificacion_pac':'count'}).reset_index().rename(columns={'identificacion_pac':'consultas'})
rips_cpr_resumen_totales = rips_cpr_resumen.groupby(['fecha_capita', 'codigo_sura', 'programa']).agg({'consultas':'sum'}).reset_index()
rips_cpr_resumen_totales.insert(1, 'nombre_ips', 'COOPSANA IPS')
resumen_cpr = func_process.pd.concat([rips_cpr_resumen, rips_cpr_resumen_totales]).sort_values(by=['fecha_capita', 'codigo_sura']).reset_index(drop=True)
resumen_cpr['fecha_capita'] = resumen_cpr['fecha_capita'].astype('str')

# Indicador
rips_cpr_nine.insert(15, 'programa', rips_cpr_nine['codigo_sura'].apply(n_programa))
rips_cpr_nine.insert(17, 'numerador', rips_cpr_nine['codigo_sura'].apply(lambda x: 1 if ((x == '70100') | (x =='70101') | (x =='7000075') | (x =='7000076')) else 0))
rips_cpr_nine.insert(18, 'denominador', rips_cpr_nine['codigo_sura'].apply(lambda x: 1 if ((x == '70100') | (x =='7000075')) else 0))
rips_cpr_nine_indicador = rips_cpr_nine.groupby(['nombre_ips']).agg({'numerador':'sum', 'denominador':'sum'}).reset_index()

row_total_coopsana = pd.Series({'nombre_ips':'COOPSANA IPS',
                          'numerador':rips_cpr_nine_indicador.numerador.sum(),
                          'denominador':rips_cpr_nine_indicador.denominador.sum()
                          })
rips_cpr_nine_indicador = pd.concat([rips_cpr_nine_indicador,row_total_coopsana.to_frame().T], ignore_index=True)
rips_cpr_nine_indicador['indicador'] = (rips_cpr_nine_indicador.numerador / rips_cpr_nine_indicador.denominador)
rips_cpr_nine_indicador.insert(0, 'fecha_capita', fecha_capita)
rips_cpr_nine_indicador.numerador = rips_cpr_nine_indicador.numerador.astype('int64')
rips_cpr_nine_indicador.denominador = rips_cpr_nine_indicador.denominador.astype('int64')
rips_cpr_nine_indicador['fecha_capita'] = rips_cpr_nine_indicador['fecha_capita'].astype('str')

# Convert columns
resumen_cpr = convert_columns_date(resumen_cpr)

rips_cpr_nine_indicador = convert_columns_date(rips_cpr_nine_indicador)
rips_cpr_nine_indicador = convert_columns_numeric(rips_cpr_nine_indicador)

# VALIDATE LOAD
validate_loads_logs_resumen_cpr =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_RESUMEN)
validate_loads_logs_indicador_cpr =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_INDICADOR)
validate_loads_logs_rips =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_RIPS)

# Load data to server
#-- Resumen
validate_load(validate_loads_logs_resumen_cpr,validate_loads_logs_rips,resumen_cpr,TABLA_BIGQUERY_RESUMEN,table_name_resumen_cpr)
#-- Indicador
validate_load(validate_loads_logs_indicador_cpr,validate_loads_logs_rips,rips_cpr_nine_indicador,TABLA_BIGQUERY_INDICADOR,table_name_indicador_cpr)

