import pandas as pd
import numpy as np
import sys
import os

path = os.path.abspath('/data/compartida/etls/tools')
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

SQL_TURNOS_BD = """ SELECT *
            FROM reportes.turnosAnioActual as t
            where date(t.fecha_turno) >= adddate(curdate(), interval -7 day)
            """
               
SQL_BIGQUERY = """
                SELECT g.id_turno
                FROM {} as g
                WHERE g.id_turno IN {}
                """
                
project_id_product = 'ia-bigquery-397516'
dataset_id_turnos = 'turnos'
table_name_turnos = 'datos_turnos_partition'
validator_column = 'id_turno'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_turnos}.{table_name_turnos}'

def convert_columns_str(df):
    df.tiempoEntreAtencion = df.tiempoEntreAtencion.astype(str)
    df.tiempoEntreAtencion = [row.replace('days','') for row in df.tiempoEntreAtencion]
    return df

def convert_columns_date(df):
    df.fecha_real = pd.to_datetime(df.fecha_real, errors='coerce')
    df.fecha_nacimiento = pd.to_datetime(df.fecha_nacimiento, errors='coerce')
    return df

def convert_columns_number(df):
    df.edad.fillna(0, inplace=True)
    df.atencion.fillna(0, inplace=True)
    df.t_taquilla.fillna(0, inplace=True)

    df.edad  = df.edad.astype(int)
    df.atencion = df.atencion.astype(int)
    df.t_taquilla = df.t_taquilla.astype(int)
    return df

# Leer datos
df_turnos_bd = func_process.load_df_server(SQL_TURNOS_BD, 'reportes')   

# Convert columns 
df_turnos_bd = convert_columns_str(df_turnos_bd)
df_turnos_bd = convert_columns_date(df_turnos_bd)
df_turnos_bd = convert_columns_number(df_turnos_bd)

# Obtener datos no duplicados
valores_unicos = tuple(map(int,df_turnos_bd[validator_column]))
df_turnos_not_duplicates = loadbq.rows_not_duplicates(df_turnos_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

# Cargar a bigquery
loadbq.load_data_bigquery(df_turnos_not_duplicates,TABLA_BIGQUERY)
