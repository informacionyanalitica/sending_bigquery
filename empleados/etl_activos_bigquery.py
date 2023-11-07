import pandas as pd
import sys
import os

path = os.path.abspath('/data/compartida/etls/tools')
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

SQL_EMPLEADOS_BD = """SELECT *
            FROM reportes.empleados_activos
            """
SQL_BIGQUERY = """
                SELECT g.identificacion
                FROM {} as g
                WHERE g.identificacion IN {}
                """
project_id_product = 'ia-bigquery-397516'
dataset_id_turnos = 'empleados'
table_name_turnos = 'empleados_activos'
validator_column = 'identificacion'    

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_turnos}.{table_name_turnos}'

# Leer datos
df_turnos_bd = func_process.load_df_server(SQL_TURNOS_BD, 'reportes')              
