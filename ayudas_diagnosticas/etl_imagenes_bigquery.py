import pandas as pd
import sys
import os
from datetime import datetime
import time

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq

fecha_cargue = datetime.now().date()

project_id_product = 'ia-bigquery-397516'
dataset_id_ayudas_diagnosticas = 'ayudas_diagnosticas'

table_name_examenes = 'dim_examenes_imagenes'
table_name_pacientes = 'dim_pacientes_imagenes'
table_name_medicos = 'dim_medicos_imagenes'
table_name_facturas = 'facts_facturas_imagenes'  

validator_column_examenes = 'idExamenImagen'
validator_column_pacientes = 'idPacienteImagen'
validator_column_medicos = 'idMedicoImagen'
validator_column_facturas = 'idfacturaImagen'

tabla_bigquery_examenes = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_examenes}'
tabla_bigquery_pacientes = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_pacientes}'
tabla_bigquery_medicos = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_medicos}'
tabla_bigquery_facturas =f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_facturas}'

SQL_DIM_EXAMENES = """select *
            FROM analitica.dimExamenesImagenes AS w
            WHERE date(w.fechaCreacion) = '{fecha_cargue}'
            """
SQL_DIM_MEDICOS = """select *
            FROM analitica.dimMedicosImagenes AS w
            WHERE date(w.fechaCreacion) = '{fecha_cargue}'
            """
SQL_DIM_PACIENTES = """select *
            FROM analitica.dimPacientesImagenes w
            WHERE date(w.fechaCreacion) = '{fecha_cargue}'
            """
SQL_FACTS_FACTURAS = """select *
            FROM analitica.factsFacturasImagenes w
            WHERE date(w.fechaCreacion) = '{fecha_cargue}'
            """

SQL_BIGQUERY_EXAMENES = """
                SELECT g.idExamenImagen
                FROM {} as g
                WHERE g.idExamenImagen IN {}
                """

SQL_BIGQUERY_PACIENTES = """
                SELECT g.idPacienteImagen
                FROM {} as g
                WHERE g.idPacienteImagen IN {}
                """

SQL_BIGQUERY_MEDICOS = """
                SELECT g.idMedicoImagen
                FROM {} as g
                WHERE g.idMedicoImagen IN {}
                """

SQL_BIGQUERY_FACTURAS = """
                SELECT g.idfacturaImagen
                FROM {} as g
                WHERE g.idfacturaImagen IN {}
                """

def convert_date_examenes(df):
    df.fechaOrden = pd.to_datetime(df.fechaOrden, errors='coerce')
    return df

def convert_date_pacientes(df):
    df.fechaNacimiento = pd.to_datetime(df.fechaNacimiento, errors='coerce')
    return df

def convert_date_medicos(df):
    df.fechaCreacion = pd.to_datetime(df.fechaCreacion, errors='coerce')
    df.fechaActualizacion = pd.to_datetime(df.fechaActualizacion, errors='coerce')
    return df

def convert_date_factura(df):
    df.fechaCapita = pd.to_datetime(df.fechaCapita, errors='coerce')
    return df

# Obtener datos no duplicados
def get_data_no_duplicate(df,validator_column,sql_bigquery,tabla_bigquery):
    try:
        df_not_duplicates = pd.DataFrame()
        if df.shape[0] >0:
            valores_unicos = tuple(map(int, df[validator_column]))
            df_not_duplicates = loadbq.rows_not_duplicates(df,validator_column,sql_bigquery,tabla_bigquery,valores_unicos) 
        return df_not_duplicates
    except ValueError as err:
        print(err)


# Read data
df_examenes = func_process.load_df_server(SQL_DIM_EXAMENES.format(fecha_cargue=fecha_cargue), 'reportes')    
df_pacientes = func_process.load_df_server(SQL_DIM_PACIENTES.format(fecha_cargue=fecha_cargue), 'reportes')   
df_medicos = func_process.load_df_server(SQL_DIM_MEDICOS.format(fecha_cargue=fecha_cargue), 'reportes')   
df_facturas = func_process.load_df_server(SQL_FACTS_FACTURAS.format(fecha_cargue=fecha_cargue), 'reportes')   

# Transform
df_examenes = convert_date_examenes(df_examenes)
df_pacientes = convert_date_pacientes(df_pacientes)
df_medicos = convert_date_medicos(df_medicos)
df_facturas = convert_date_factura(df_facturas)

# Validate duplicate
df_not_duplicates_examenes = get_data_no_duplicate(df_examenes,validator_column_examenes,SQL_BIGQUERY_EXAMENES,tabla_bigquery_examenes)
time.sleep(5)
df_not_duplicates_pacientes = get_data_no_duplicate(df_pacientes,validator_column_pacientes,SQL_BIGQUERY_PACIENTES,tabla_bigquery_pacientes)
time.sleep(5)
df_not_duplicates_medicos = get_data_no_duplicate(df_medicos,validator_column_medicos,SQL_BIGQUERY_MEDICOS,tabla_bigquery_medicos)
time.sleep(5)
df_not_duplicates_facturas = get_data_no_duplicate(df_facturas,validator_column_facturas,SQL_BIGQUERY_FACTURAS,tabla_bigquery_facturas)

# Load bigquery
loadbq.load_data_bigquery(df_not_duplicates_examenes,tabla_bigquery_examenes)
loadbq.load_data_bigquery(df_not_duplicates_pacientes,tabla_bigquery_pacientes)
loadbq.load_data_bigquery(df_not_duplicates_medicos,tabla_bigquery_medicos)
loadbq.load_data_bigquery(df_not_duplicates_facturas,tabla_bigquery_facturas)





    
