import pandas as pd
import numpy as np 
import sys, os 
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq

fecha = func_process.pd.to_datetime(datetime.now() - timedelta(days=15)).strftime('%Y-%m')
fecha_i = func_process.pd.to_datetime(fecha + '-01')
fecha_capita = func_process.pd.to_datetime(fecha + '-15')
dias_del_mes = str(fecha_i.days_in_month)
fecha_f = func_process.pd.to_datetime(fecha + '-' + dias_del_mes)

mes_letra = fecha_capita.strftime('%B').capitalize()
mes_numero = str(fecha_capita.month)
ano = fecha_capita.strftime('%Y')

SQL_CAPITA = """SELECT g.identificacion_paciente,g.nombre_sede
                FROM reportes.capita as g
                where DATE_FORMAT(g.fecha_cargue,'%%Y-%%m') = '{}'
                            """
sql_detalle_odontologia = f"""
                            SELECT 
                                * 
                            FROM detalle_rips_odontologia_view
                            WHERE fecha_consulta BETWEEN '{fecha_i.strftime('%Y-%m-%d')}' AND '{fecha_f.strftime('%Y-%m-%d')}'
                            ORDER BY fecha_consulta
                        """

sql_gestion_horas = f"""
                SELECT 
                    documentoProfesional as identificacion_profesional,
                    horasClinica,
                    anio,
                    mes                    
                FROM gestionHorasOdontologiaView 
                WHERE anio = {ano} AND mes = {mes_numero}
            """

sql_capita_mes = "SELECT identificacion_paciente, fecha_nacimiento, edad FROM capita"

sql_poblaciones_odontologia = f"""
                                SELECT 
                                    `FECHA_CAPITA` AS fecha_capita,
                                    `NOMBRE_IPS` AS nombre_sede,
                                    Poblacion_mayor_2_anos,
                                    Poblacion_menor_igual_4_anos,
                                    Poblacion_entre_5_19_anos,
                                    Poblacion_entre_3_15_anos,
                                    Poblacion_mayor_12_anos
                                FROM analitica.poblaciones_odontologia
                                WHERE `FECHA_CAPITA` = '{fecha_capita.strftime('%Y-%m-%d')}' 
                                """
codigo_sedes = {
    'CENTRO': 35, 
    'AVENIDA ORIENTAL': 115393, 
    'PAC': 2715, 
    'NORTE': 2136,
    'CALASANZ': 1013
}

project_id_product = 'ia-bigquery-397516'
dataset_id = 'odontologia'
table_name = 'detalle_odontologia_capita'
table_maridb_capita_odontologia = 'detalle_odontologia_capita'

TABLA_BIGQUERY_CAPITA_ODONTOLOGIA = f'{project_id_product}.{dataset_id}.{table_name}'

# Funciones
def load_capita():
    capita = func_process.load_df_server(SQL_CAPITA.format(fecha), 'reportes')
    return capita

def subtitute_name_branch(capita):
    mapping = {
        'COOPSANA - CENTRO': 'CENTRO', 
        'COOPSANA CENTRO ARGENTINA': 'AVENIDA ORIENTAL', 
        'IPS PAC COOPSANA SURAMERICANA': 'PAC', 
        'COOPSANA NORTE': 'NORTE',
        'COOPSANA CALASANZ': 'CALASANZ'
    }
    capita['nombre_sede'] = capita['nombre_sede'].map(mapping)
    return capita

def codigo_ips(capita):
    mapping = {
        'CENTRO': 35, 
        'AVENIDA ORIENTAL': 115393, 
        'PAC': 2715, 
        'NORTE': 2136,
        'CALASANZ': 1013
    }
    capita['codigo_sede'] = capita['nombre_sede'].map(mapping)
    return capita

def poblacion_sedes(df_x):
    df_x_group = df_x.groupby(['fecha_capita', 'nombre_sede', 'codigo_sede']).agg({'identificacion_paciente': 'count'}).reset_index()
    df_x_group.rename(columns={'identificacion_paciente': 'poblacion_total'}, inplace=True)
    return df_x_group

def reg_sede(palabra):
    patron = '(-[0-9]+)'
    sede = re.search(patron, palabra)
    return sede.group().replace('-', '')

def convert_columns_date(df):
    df.fecha_nacimiento = pd.to_datetime(df.fecha_nacimiento, errors='coerce')
    return df

def clean_values_na(df):
    df.Poblacion_mayor_2_anos.fillna(0, inplace=True)
    df.Poblacion_menor_igual_4_anos.fillna(0, inplace=True)
    df.Poblacion_entre_5_19_anos.fillna(0, inplace=True)
    df.Poblacion_entre_3_15_anos.fillna(0, inplace=True)
    df.Poblacion_mayor_12_anos.fillna(0, inplace=True)
    df.poblacion_total.fillna(0, inplace=True)
    df.edad.fillna(0, inplace=True)
    return df

def convert_columns_number(df):
    df.Poblacion_mayor_2_anos = df.Poblacion_mayor_2_anos.astype(int)
    df.Poblacion_menor_igual_4_anos = df.Poblacion_menor_igual_4_anos.astype(int)
    df.Poblacion_entre_5_19_anos = df.Poblacion_entre_5_19_anos.astype(int)
    df.Poblacion_entre_3_15_anos = df.Poblacion_entre_3_15_anos.astype(int)
    df.Poblacion_mayor_12_anos = df.Poblacion_mayor_12_anos.astype(int)
    df.poblacion_total = df.poblacion_total.astype(int)
    df.edad = df.edad.astype(int)
    return df

def validate_load(df_validate_load, df_load, tabla_bigquery, table_mariadb):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if total_cargue == 0:
            func_process.save_df_server(df_load, table_mariadb, 'analitica')
            loadbq.load_data_bigquery(df_load, tabla_bigquery)
    except ValueError as err:
        print(err)

# Procesamiento
capita = (
    load_capita()
    .pipe(subtitute_name_branch)
    .pipe(codigo_ips)
)
capita['fecha_capita'] = fecha_capita
capita = capita.drop_duplicates(subset=['identificacion_paciente', 'fecha_capita'])
poblacion_capita = poblacion_sedes(capita)

detalle_odontologia = func_process.load_df_server(sql_detalle_odontologia, 'reportes')
detalle_odontologia = detalle_odontologia.drop_duplicates()
detalle_odontologia.drop(detalle_odontologia[detalle_odontologia['identificacion_paciente'] == '0'].index, axis=0, inplace=True)
detalle_odontologia['fecha_consulta'] = func_process.pd.to_datetime(detalle_odontologia['fecha_consulta'])

gestion_horas_odontologia = func_process.load_df_server(sql_gestion_horas, 'reportes')
gestion_horas_odontologia.anio = gestion_horas_odontologia.anio.astype('str')
gestion_horas_odontologia.identificacion_profesional = gestion_horas_odontologia.identificacion_profesional.astype('str')
gestion_horas_odontologia.mes = gestion_horas_odontologia.mes.astype('int64')
gestion_horas_odontologia['fecha_capita'] = gestion_horas_odontologia.apply(lambda x: f"{x['anio']}-{x['mes']:02}-15", axis=1)
gestion_horas_odontologia['fecha_capita'] = func_process.pd.to_datetime(gestion_horas_odontologia['fecha_capita'])
gestion_horas_odontologia = gestion_horas_odontologia.drop_duplicates(subset=['identificacion_profesional', 'fecha_capita'])

capita_mes = func_process.load_df_server(sql_capita_mes, 'reportes')
capita_mes = capita_mes.drop_duplicates(subset=['identificacion_paciente'])
capita_mes['identificacion_paciente'] = capita_mes['identificacion_paciente'].astype('str')

detalle_odontologia['fecha_capita'] = detalle_odontologia['fecha_consulta'].apply(lambda x: func_process.pd.to_datetime(x.strftime('%Y-%m-15')))

poblaciones_odontologia = func_process.load_df_server(sql_poblaciones_odontologia, 'reportes')
poblaciones_odontologia['fecha_capita'] = func_process.pd.to_datetime(poblaciones_odontologia['fecha_capita'])
poblacion_capita['fecha_capita'] = func_process.pd.to_datetime(poblacion_capita['fecha_capita'])
poblaciones_odontologia = poblaciones_odontologia.merge(poblacion_capita[['fecha_capita', 'nombre_sede', 'poblacion_total']],
                                                        on=['fecha_capita', 'nombre_sede'])
poblaciones_odontologia['codigo_sede'] = poblaciones_odontologia['nombre_sede'].map(codigo_sedes)
poblaciones_odontologia = poblaciones_odontologia.drop_duplicates(subset=['fecha_capita', 'codigo_sede'])  # Movido aquí

detalle_odontologia_horas = detalle_odontologia.merge(gestion_horas_odontologia[['identificacion_profesional', 'horasClinica', 'fecha_capita']],
                                                      how='left',
                                                      on=['identificacion_profesional', 'fecha_capita'])
detalle_odontologia_horas_capita = detalle_odontologia_horas.merge(capita,
                                                                  on=['identificacion_paciente', 'fecha_capita'], 
                                                                  how='left')
detalle_odontologia_horas_capita.drop('id_detalle', inplace=True, axis=1)
for i in detalle_odontologia_horas_capita[detalle_odontologia_horas_capita.nombre_sede.isnull()].index:
    id_pac = detalle_odontologia_horas_capita.iloc[i, 0]
    detalle_odontologia_horas_capita.at[i, 'codigo_sede'] = reg_sede(id_pac)
    
detalle_odontologia_horas_capita.codigo_sede = detalle_odontologia_horas_capita.codigo_sede.astype('int64')
detalle_odontologia_horas_capita.codigo_sede = detalle_odontologia_horas_capita.codigo_sede.astype('str')
poblaciones_odontologia.codigo_sede = poblaciones_odontologia.codigo_sede.astype('str')
detalle_odontologia_horas_capita_poblaciones = detalle_odontologia_horas_capita.merge(poblaciones_odontologia, 
                                                                                      on=['fecha_capita', 'codigo_sede'],
                                                                                      how='left')
detalle_odontologia_horas_capita_poblaciones['hora_cita'] = detalle_odontologia_horas_capita_poblaciones['hora_cita'].astype('str')
detalle_odontologia_horas_capita_poblaciones['hora_finaliza_cita'] = detalle_odontologia_horas_capita_poblaciones['hora_finaliza_cita'].astype('str')

# Reemplazar el bucle con operación vectorizada
detalle_odontologia_horas_capita_poblaciones['hora_cita'] = detalle_odontologia_horas_capita_poblaciones['hora_cita'].str[7:]
detalle_odontologia_horas_capita_poblaciones['hora_finaliza_cita'] = detalle_odontologia_horas_capita_poblaciones['hora_finaliza_cita'].str[7:]

detalle_odontologia_horas_capita_poblaciones.drop('nombre_sede_x', axis=1, inplace=True)
detalle_odontologia_horas_capita_poblaciones.rename({'nombre_sede_y': 'nombre_sede'}, axis=1, inplace=True)
detalle_odontologia_horas_capita_poblaciones_edad = detalle_odontologia_horas_capita_poblaciones.merge(capita_mes, 
                                                                                                      on='identificacion_paciente', 
                                                                                                      how='left')

# Eliminar duplicados finales
detalle_odontologia_horas_capita_poblaciones_edad = detalle_odontologia_horas_capita_poblaciones_edad.drop_duplicates()

# Convertir columnas 
detalle_odontologia_horas_capita_poblaciones_edad = convert_columns_date(detalle_odontologia_horas_capita_poblaciones_edad)
detalle_odontologia_horas_capita_poblaciones_edad = clean_values_na(detalle_odontologia_horas_capita_poblaciones_edad)
detalle_odontologia_horas_capita_poblaciones_edad = convert_columns_number(detalle_odontologia_horas_capita_poblaciones_edad)


# VALIDATE LOAD
validate_loads_logs_odontologia_capita =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_CAPITA_ODONTOLOGIA)

# Load
validate_load(validate_loads_logs_odontologia_capita,detalle_odontologia_horas_capita_poblaciones_edad,
                TABLA_BIGQUERY_CAPITA_ODONTOLOGIA,table_maridb_capita_odontologia)


#print(detalle_odontologia_horas_capita_poblaciones_edad)