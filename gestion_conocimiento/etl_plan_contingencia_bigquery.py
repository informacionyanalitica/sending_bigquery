import pandas as pd
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq

# Diccionario de mapeo de columnas originales a camelCase
COLUMN_RENAME_MAP = {
    'Fecha en la que realizo la auditoria (dd/mm/año)': 'fechaRealizoAuditoria',
    'Fecha': 'fecha',
    'Mes Monitoreo': 'mesMonitoreo',
    'Año': 'ano',
    'Sede': 'sede',
    'Nombre del profesional': 'nombreProfesional',
    'Cédula': 'cedulaProfesional',
    'Cédula del paciente': 'cedulaPaciente',
    'Examen a Monitorear': 'examenMonitorear',
    'Tipo de Examen': 'tipoExamen',
    'Condición de Salud': 'condicionSalud',
    'Diagnostico': 'diagnostico',
    'Nota': 'nota',
    'Observaciones': 'observaciones',
    'Validación de los datos': 'validacionDatos',
    'Correo': 'correo'
}

# Columnas requeridas en camelCase
COLUMNS_REQUIRED = [
    'fechaRealizoAuditoria', 'fecha', 'mesMonitoreo', 'ano', 'sede', 'nombreProfesional',
    'cedulaProfesional', 'cedulaPaciente', 'examenMonitorear', 'tipoExamen', 'condicionSalud',
    'diagnostico', 'nota', 'observaciones', 'validacionDatos', 'correo'
]

LIST_COLUMN_DROP = ['Analisis y plan', 'Pertinencia', 'Correo']

# DETALLE DATASET BIGQUERY
project_id_product = 'ia-bigquery-397516'
dataset_id_gestion_conocimiento = 'gestion_conocimiento'
table_name_auditores = 'planContingencia'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name_auditores}'

# DETALLE DE GOOGLE SHEET
ID_SHEET = '10xleugjBChU8mxk6Bx5apLXLBnvbD4_HtDcPUJjnFNc'
LIST_NAME_SHEET = [
    'LAURA AUDITORÍA CONCURRENTE',
    'JUAN DAVID AUDITORÍA CONCURRENTE',
    'CAMILA AUDITORÍA CONCURRENTE'
]

def convert_date(df):
    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    df['fechaRealizoAuditoria'] = pd.to_datetime(df['fechaRealizoAuditoria'], errors='coerce')
    return df

def convert_number(df):
    try:
        df['ano'].replace('', '0', inplace=True)
        df['ano'].fillna(0, inplace=True)
        df['ano'] = df['ano'].astype(int)
        df['nota'].fillna('0', inplace=True)
        df['nota'] = [str(val).replace('%', '') for val in df['nota']]
        df['nota'].replace('', '0', inplace=True)
        df['nota'] = df['nota'].astype(float)
        df['nota'] = [(val / 100) for val in df['nota']]
        return df
    except Exception as err:
        print(err)

def get_columns_rows(df):
    try:
        # Elimina columnas innecesarias si existen
        df.drop([col for col in LIST_COLUMN_DROP if col in df.columns], axis=1, inplace=True)
        # Renombra las columnas a camelCase
        df.rename(columns=COLUMN_RENAME_MAP, inplace=True)
        # Reordena las columnas según COLUMNS_REQUIRED (solo si existen en el DataFrame)
        df = df[[col for col in COLUMNS_REQUIRED if col in df.columns]]
        return df
    except Exception as err:
        print(err)

def drop_rows_empty(df):
    try:
        if df.empty:
            print("El DataFrame está vacío, no se puede procesar.")
            return df
        mask = (df['fecha'] == '') & (df['examenMonitorear'] == '')
        if mask.any():
            list_index_drop = df[mask].index[0]
            df = df.iloc[:list_index_drop]
        return df
    except Exception as err:
        print(f"Error en drop_rows_empty: {err}")
        return df

def validate_load(df_not_duplicate):
    if df_not_duplicate.shape[0] > 0:
        try:
            loadbq.load_data_bigquery(df_not_duplicate, TABLA_BIGQUERY, 'WRITE_TRUNCATE')
        except Exception as err:
            print(err)

def execution_load():
    try:
        df_auditores = pd.DataFrame()
        for name in LIST_NAME_SHEET:
            df_auditor = func_process.get_google_sheet(ID_SHEET, name)
            df_auditor = get_columns_rows(df_auditor)
            df_auditor_clean = drop_rows_empty(df_auditor)
            df_auditor_transform = convert_date(df_auditor_clean)
            df_auditor_transform = convert_number(df_auditor_transform)
            df_auditor_transform['auditor'] = name
            df_auditores = pd.concat([df_auditores, df_auditor_transform])
        validate_load(df_auditores)
    except Exception as err:
        print(err)

execution_load()