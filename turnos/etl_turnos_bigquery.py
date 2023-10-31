import pandas as pd
import numpy as np
import sys
import os

path = os.path.abspath('../tools')
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

        
# Leer datos
df_turnos_bd = func_process.load_df_server(SQL_TURNOS_BD, 'reportes')   

# Obtener datos no duplicados
valores_unicos = tuple(map(int,df_turnos_bd[validator_column]))
df_turnos_not_duplicates = loadbq.rows_not_duplicates(df_turnos_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

# Cargar a bigquery
loadbq.load_data_bigquery(df_turnos_not_duplicates,TABLA_BIGQUERY)
