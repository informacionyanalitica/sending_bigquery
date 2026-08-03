import sys
import os
import time
import pandas as pd
from datetime import datetime
import requests
from google.cloud import bigquery
import pandas_gbq

# ---- Funciones de Google Drive y Sheets ----
def readFile(id, namePage="maestra"):
    """Lee una hoja específica de un Google Sheet"""
    try:
        url = f'https://apps.coopsana.co:7154/googleSheets/read/{id}/{namePage}'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if 'rows' not in data:
            print(f"✗ Error: La respuesta no contiene la clave 'rows'. Respuesta: {data}")
            return None
        return data
    except requests.exceptions.HTTPError as err:
        print(f"✗ Error HTTP: {err}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"✗ Error al leer Google Sheet: {err}")
        return None
    except ValueError as err:
        print(f"✗ Error al procesar JSON: {err}")
        return None

# ---- Clase Query para BigQuery ----
class Query:
    def __init__(self, sql=None, df=None, nameTB=None):
        self.sql = sql
        self.df = df
        self.nameTB = nameTB

    def _insert_bigquery(self, project_id, dataset_id, table_id, table_schema):
        """Inserta datos en BigQuery con esquema específico (truncate/replace)"""
        try:
            if self.df.shape[0] > 0:
                pandas_gbq.to_gbq(
                    self.df,
                    destination_table=f"{dataset_id}.{table_id}",
                    project_id=project_id,
                    if_exists='replace',
                    table_schema=table_schema
                )
                return True
            else:
                return False
        except Exception as er:
            return str(er)

def log(msg, emoji="•"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {emoji} {msg}")

# ---- CONFIGURACIÓN ----
GOOGLE_SHEET_ID = "1dZ6urdO1YD8EsMS9NlxQXKdQ5UjnHlUl5edtwp58Qcc"
SHEET_TAB = "CONSUMO POR CIRUGIA"

project_id_product = 'ia-bigquery-397516'
dataset_id_cirugias = 'cirugias'
table_name_cirugias = 'consumoPorCirugia'
TABLA_BIGQUERY_ID = table_name_cirugias
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_cirugias}.{table_name_cirugias}'

COLUMNAS_ORIGEN = ['NOMBRE DEL PROCEDIMIENTO', 'CODIGO', 'DESCRIPCION', 'CANTIDAD']
COLUMNAS_CAMEL = ['nombreDelProcedimiento', 'codigo', 'descripcion', 'cantidad']

SCHEMA_BIGQUERY = [
    {'name': 'nombreDelProcedimiento', 'type': 'STRING'},
    {'name': 'codigo', 'type': 'STRING'},
    {'name': 'descripcion', 'type': 'STRING'},
    {'name': 'cantidad', 'type': 'INTEGER'},
    {'name': 'fechaActualizacion', 'type': 'TIMESTAMP'}
]

t_inicio = time.time()

print("=" * 60)
print(" ETL — consumoPorCirugia (Google Sheets → BigQuery)")
print("=" * 60)

log(f"Leyendo hoja '{SHEET_TAB}' del Google Sheet...", "📄")
t0 = time.time()
data = readFile(GOOGLE_SHEET_ID, SHEET_TAB)

total_registros = 0

if data is None:
    log(f"No se pudo leer la hoja '{SHEET_TAB}'", "✗")
else:
    df = pd.DataFrame(data['rows'])
    log(f"Lectura completada en {time.time() - t0:.2f}s — {len(df):,} filas crudas", "✅")

    if df.shape[1] < len(COLUMNAS_ORIGEN):
        log(f"Se esperaban al menos {len(COLUMNAS_ORIGEN)} columnas, se encontraron {df.shape[1]}", "✗")
    else:
        log("Normalizando columnas a camelCase...", "🔤")
        df.columns = COLUMNAS_CAMEL[:df.shape[1]]
        df = df[COLUMNAS_CAMEL]

        log("Limpiando y transformando datos...", "🛠️")
        df = df.dropna(how='all')
        df = df.dropna(subset=['codigo'])

        df['nombreDelProcedimiento'] = df['nombreDelProcedimiento'].astype(str)
        df['codigo'] = df['codigo'].astype(str)
        df['descripcion'] = df['descripcion'].astype(str)
        df['cantidad'] = (
            df['cantidad']
            .astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
        )
        df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce').fillna(0).astype(int)
        df['fechaActualizacion'] = datetime.now()

        log(f"Filas procesadas: {len(df):,}", "✅")

        log(f"Cargando a BigQuery → {TABLA_BIGQUERY} (truncate/replace)...", "☁️")
        t0 = time.time()
        query = Query(None, df, TABLA_BIGQUERY_ID)
        resultado = query._insert_bigquery(
            project_id_product,
            dataset_id_cirugias,
            TABLA_BIGQUERY_ID,
            SCHEMA_BIGQUERY
        )

        if resultado == True:
            log(f"Carga completada en {time.time() - t0:.2f}s", "✅")
            total_registros = len(df)
        else:
            log(f"Error al insertar en BigQuery: {resultado}", "✗")

print("-" * 60)
log(f"Proceso finalizado en {time.time() - t_inicio:.2f}s totales", "🏁")
print("=" * 60)

# --- Salida requerida por el DAG (NO MODIFICAR el orden ni formato) ---
print("Save successfully")
print(f"{total_registros},{TABLA_BIGQUERY},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")