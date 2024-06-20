import sys,os
import pandas as pd
from datetime import datetime,timedelta

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

today = func_process.pd.to_datetime(datetime.now() - timedelta(days=25))
capita_date = func_process.pd.to_datetime(today.strftime('%Y-%m-15'))
date_capita = capita_date.strftime('%Y-%m-%d')
date_i = f"{capita_date.strftime('%Y')}-{capita_date.strftime('%m')}-01 00:00:00"
date_f = f"{capita_date.strftime('%Y')}-{capita_date.strftime('%m')}-{str(capita_date.days_in_month)} 23:59:59"
mes_numero = str(capita_date.month)
ano = capita_date.strftime("%Y")
mes_letra = capita_date.strftime("%B").capitalize()

# Parametros bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'rips'
table_name_demografico = 'rips_poblaciones'
table_name_rips_auditoria = 'rips_auditoria_poblacion_2'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_rips}.{table_name_demografico}'
TABLA_BIGQUERY_RIPS = f'{project_id_product}.{dataset_id_rips}.{table_name_rips_auditoria}'

sql_rips = f"""SELECT 
                    hora_fecha, 
                    orden, 
                    ips, 
                    identificacion_pac, 
                    primer_nombre_pac, 
                    segundo_nombre_pac, 
                    primer_apellido_pac, 
                    segundo_apellido_pac, 
                    sexo, 
                    fecha_nacimiento, 
                    edad_anos, 
                    edad_meses, 
                    identificacion_med, 
                    nombres_med, 
                    codigo_sura, 
                    codigo_cups, 
                    nombre_prestacion, 
                    dx_principal, 
                    nombre_dx_principal, 
                    horas_observacion, 
                    fecha_cargue 
                FROM rips 
                WHERE (hora_fecha BETWEEN '{date_i}' AND '{date_f}')
                    AND codigo_sura IN ('50110', '7000026', '3000056', '50114', '3000056', '50114', '70000',
                      '70100', '71000', '71101', '70200', '70201', '70300', '70301', '70400', '71105', '70102', 
                      '7000075', '7000076', '3000052', '3000054', '8902031', '8903018', '8903056', '890101') 
                    AND ips NOT IN ('134189', '133050');"""

sql_poblaciones = f"""SELECT 
                            FECHA_CAPITA, 
                            NOMBRE_IPS, 
                            MENORES_A_5_ANOS_F, 
                            MENORES_A_5_ANOS_M, 
                            MENORES_A_5_ANOS, 
                            ENTRE_15_Y_44_ANOS_F, 
                            ENTRE_15_Y_44_ANOS_M, 
                            ENTRE_15_Y_44_ANOS, 
                            ENTRE_45_Y_59_ANOS_F, 
                            ENTRE_45_Y_59_ANOS_M, 
                            ENTRE_45_Y_59_ANOS, 
                            MAYOR_DE_59_ANOS_F, 
                            MAYOR_DE_59_ANOS_M, 
                            MAYORES_DE_59_ANOS 
                        FROM `ia-bigquery-397516.pacientes.capitas_poblaciones`
                        WHERE FECHA_CAPITA = '{date_capita}';"""

def load_rips(sql):
    rips_2021 = func_process.load_df_server(sql, 'reportes')
    rips_2021['hora_fecha'] = func_process.pd.to_datetime(rips_2021['hora_fecha'])
    rips_2021['fecha_nacimiento'] = func_process.pd.to_datetime(rips_2021['fecha_nacimiento'])
    rips_2021['fecha_cargue'] = func_process.pd.to_datetime(rips_2021['fecha_cargue'])
    rips_2021.drop(rips_2021[rips_2021['edad_anos'] == 'nul'].index, inplace=True)
    return rips_2021

def load_poblacion(sql):
    poblaciones_2021 = func_process.load_df_server(sql, 'analitica')
    poblaciones_2021['FECHA_CAPITA'] = func_process.pd.to_datetime(poblaciones_2021['FECHA_CAPITA'])
    poblaciones_2021.rename(columns={'FECHA_CAPITA':'fecha_capita', 'NOMBRE_IPS':'nombre_ips'}, inplace= True)
    return poblaciones_2021

def convert_columns_datetime(df):
    # convetir fechas
    df.fecha_capita = pd.to_datetime(df.fecha_capita, errors='coerce')
    df.fecha_cargue = pd.to_datetime(df.fecha_cargue, errors='coerce')
    df.fecha_nacimiento = pd.to_datetime(df.fecha_nacimiento, errors='coerce')
    return df

def conver_columns_integer(df):
    # Convertir numeros
    df.catidad_poblacion.fillna(0, inplace=True)
    df.poblacion_total.fillna(0, inplace=True)
    df.catidad_poblacion = df.catidad_poblacion.astype(int)
    df.poblacion_total =df.poblacion_total.astype(int)
    return df

def name_branch(x_df):
    def name(codigo):
        if codigo == '35':
            return 'CENTRO'
        elif codigo == '1013':
            return 'CALASANZ'
        elif codigo == '2136':
            return 'NORTE'
        elif codigo == '2715':
            return 'PAC'
        elif codigo == '115393':
            return 'AVENIDA ORIENTAL'
        
    x_df['nombre_ips'] = x_df['ips'].apply(name)
    return x_df
    
def type_attention(x_df):
    def tipos_consulta(cod_s):
        if ((cod_s == '50110') | (cod_s == '7000026')):
            return 'CONSULTA MEDICINA GENERAL'
        elif ((cod_s == '3000056') | (cod_s == '50114')):
            return 'CONSULTA NO PROGRAMADA'
        elif ((cod_s == '70000') | (cod_s == '70100') | (cod_s == '71000') | (cod_s == '71101') | (cod_s == '70200') | (cod_s == '70201') | (cod_s == '70300') | (cod_s == '70301') | (cod_s == '70400') | (cod_s == '71105') | (cod_s == '70102') | (cod_s == '7000075') | (cod_s == '7000076') | (cod_s == '3000052') | (cod_s == '3000054') | (cod_s == '8902031') | (cod_s == '8903018') | (cod_s == '8903056')):
            return 'PROMOCION Y PREVENCION'
        elif (cod_s == '890101'):
            return 'DOMICILIARIA'
        else:
            return 'OTRO'
    x_df['tipos_consulta'] = x_df['codigo_sura'].apply(tipos_consulta)
    return x_df

def fecha_capita(x_df):
    x_df.insert(0, 'fecha_capita', x_df['hora_fecha'].apply(
        lambda row: '{}{}{:02d}{}{}'.format(row.year, '-', row.month, '-', 15)
    ))
    x_df['fecha_capita'] = func_process.pd.to_datetime(x_df['fecha_capita'])
    return x_df

def create_age_group(x_df):
    x_df['edad_anos'] = x_df['edad_anos'].astype('int64')
    x_df['edad_meses'] = x_df['edad_meses'].astype('int64')    
    bins=[0, 5, 14, 44, 59, sys.maxsize]
    labels=['Menores de 6 anos', 'Entre 6 y 14 anos', 'Entre 15 y 44 anos', 'Entre 45 y 59 anos', 'Mayores de 59 anos']
    ageGroup=func_process.pd.cut(x_df['edad_anos'], bins=bins, labels=labels, include_lowest=True)
    x_df['poblaciones']=ageGroup
    return x_df

def df_poblacion(poblacion, df_poblacion, df_rips):
    df_poblacion.rename({'FECHA_CAPITA':'fecha_capita','NOMBRE_IPS':'nombre_ips'}, inplace=True, axis=1)
    df_f = df_rips[(df_rips.poblaciones == poblacion) & (df_rips.sexo == 'F')].merge(
        df_poblacion[list([df_poblacion.columns[0], df_poblacion.columns[1], df_poblacion.columns[2]])], 
        on= ['fecha_capita', 'nombre_ips'], how= 'left')
    df_f.rename(columns={df_f.columns[-1]:'catidad_poblacion'}, inplace= True)
    
    df_m = df_rips[(df_rips.poblaciones == poblacion) & (df_rips.sexo == 'M')].merge(
        df_poblacion[list([df_poblacion.columns[0], df_poblacion.columns[1], df_poblacion.columns[3]])], 
        on= ['fecha_capita', 'nombre_ips'], how= 'left')
    df_m.rename(columns={df_m.columns[-1]:'catidad_poblacion'}, inplace= True)
    
    df_f_m = func_process.pd.concat([df_f, df_m], ignore_index = True)
    
    df_final = df_f_m.merge(
        df_poblacion[list([df_poblacion.columns[0], df_poblacion.columns[1], df_poblacion.columns[4]])], 
        on= ['fecha_capita', 'nombre_ips'], how= 'left')
    df_final.rename(columns={df_final.columns[-1]:'poblacion_total'}, inplace= True)
    return df_final

def validate_load(df_validate_load,df_validate_load_rips,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        total_cargue_rips = df_validate_load_rips.totalCargues[0]
        if total_cargue == 0 and total_cargue_rips>0:
            # Cargar mariadb
            func_process.save_df_server(df_load,'rips_poblaciones','analitica')
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

rips_2021 = (
    load_rips(sql_rips)
    .pipe(name_branch)
    .pipe(type_attention)
    .pipe(fecha_capita)
    .pipe(create_age_group)
)

poblaciones_2021 = loadbq.read_data_bigquery(sql_poblaciones,TABLA_BIGQUERY)
poblaciones_2021.FECHA_CAPITA = pd.to_datetime(poblaciones_2021.FECHA_CAPITA,errors='coerce').dt.tz_convert(None)
poblaciones_2021_menores_6 = poblaciones_2021[['FECHA_CAPITA', 'NOMBRE_IPS', 'MENORES_A_5_ANOS_F', 'MENORES_A_5_ANOS_M', 'MENORES_A_5_ANOS']]
poblaciones_2021_entre_15_44 = poblaciones_2021[['FECHA_CAPITA', 'NOMBRE_IPS', 'ENTRE_15_Y_44_ANOS_F', 'ENTRE_15_Y_44_ANOS_M', 'ENTRE_15_Y_44_ANOS']]
poblaciones_2021_entre_45_59 = poblaciones_2021[['FECHA_CAPITA', 'NOMBRE_IPS', 'ENTRE_45_Y_59_ANOS_F', 'ENTRE_45_Y_59_ANOS_M', 'ENTRE_45_Y_59_ANOS']]
poblaciones_2021_mayores_59 = poblaciones_2021[['FECHA_CAPITA', 'NOMBRE_IPS', 'MAYOR_DE_59_ANOS_F', 'MAYOR_DE_59_ANOS_M', 'MAYORES_DE_59_ANOS']]
df_menores_6 = df_poblacion('Menores de 6 anos', poblaciones_2021_menores_6, rips_2021)
df_entre_15_44 = df_poblacion('Entre 15 y 44 anos', poblaciones_2021_entre_15_44, rips_2021)
df_entre_45_59 = df_poblacion('Entre 45 y 59 anos', poblaciones_2021_entre_45_59, rips_2021)
df_mayores_60 = df_poblacion('Mayores de 59 anos', poblaciones_2021_mayores_59, rips_2021)
rips_poblaciones_2021 = func_process.pd.concat([df_menores_6, df_entre_15_44, df_entre_45_59, df_mayores_60], ignore_index= True)
rips_poblaciones_2021 = convert_columns_datetime(rips_poblaciones_2021)
rips_poblaciones_2021 = conver_columns_integer(rips_poblaciones_2021)

# VALIDATE LOAD
validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY)
validate_loads_logs_rips =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_RIPS)

# Load data to server
validate_load(validate_loads_logs,validate_loads_logs_rips,rips_poblaciones_2021)
