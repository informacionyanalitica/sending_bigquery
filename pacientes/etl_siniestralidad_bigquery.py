import pandas as pd
import sys, os
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import date, datetime
import time
import traceback

def log(msg, level="INFO"):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg}")

def log_separator():
    print("=" * 90)

# Inicio proceso
overall_start = time.time()
log_separator()
log("INICIO ETL SINIESTRALIDAD (FULL REFRESH)")
log_separator()

try:
    # Carga variables de entorno
    log("Cargando variables de entorno...")
    load_dotenv()
    PATH_TOOLS = os.environ.get("PATH_TOOLS")

    if not PATH_TOOLS:
        raise ValueError("La variable de entorno PATH_TOOLS no está definida.")

    tools_path = os.path.abspath(PATH_TOOLS)
    sys.path.insert(1, tools_path)
    log(f"PATH_TOOLS cargado: {tools_path}")

    import func_process
    log("Módulo func_process importado correctamente.")

    # Configuración BigQuery
    project_id = "ia-bigquery-397516"
    dataset_id = "pacientes"
    table_id = "siniestralidad"
    TABLA_BIGQUERY = f"{project_id}.{dataset_id}.{table_id}"

    log(f"Tabla destino BigQuery: {TABLA_BIGQUERY}")
    log("Modo de carga: WRITE_TRUNCATE (reemplaza contenido completo).")

    # Extraer datos de MariaDB/MySQL
    SQL = "SELECT * FROM analitica.sinestralidadUsuarios"
    log("Iniciando extracción de datos desde MariaDB/MySQL...")
    extract_start = time.time()

    df = func_process.load_df_server(SQL, "analitica")

    extract_elapsed = time.time() - extract_start
    if df is None:
        raise ValueError("La extracción retornó None en lugar de un DataFrame.")

    log(f"Extracción finalizada en {extract_elapsed:.2f}s.")
    log(f"Filas obtenidas: {len(df)} | Columnas obtenidas: {len(df.columns)}")

    if not df.empty:
        # Transformación mínima existente
        transform_start = time.time()
        df["fechaActualizacion"] = date.today()
        transform_elapsed = time.time() - transform_start

        log("Columna 'fechaActualizacion' agregada correctamente.")
        log(f"Transformación finalizada en {transform_elapsed:.2f}s.")
        log(f"Total columnas después de transformación: {len(df.columns)}")

        # Carga BigQuery
        log("Iniciando carga a BigQuery...")
        load_start = time.time()

        client = bigquery.Client()
        log("Cliente BigQuery inicializado.")

        job = client.load_table_from_dataframe(
            df,
            TABLA_BIGQUERY,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Reemplaza los datos de la tabla
                autodetect=True
            )
        )

        log(f"Job de carga enviado. Job ID: {job.job_id}")
        job.result()

        load_elapsed = time.time() - load_start
        log(f"Carga finalizada en {load_elapsed:.2f}s.")
        log(f"Tabla {TABLA_BIGQUERY} reemplazada correctamente.")
        log(f"Total filas cargadas: {len(df)}")
    else:
        log("No se encontraron datos para cargar.", level="WARNING")

    total_elapsed = time.time() - overall_start
    log_separator()
    log(f"PROCESO FINALIZADO OK en {total_elapsed:.2f}s")
    log_separator()

except Exception as e:
    total_elapsed = time.time() - overall_start
    log_separator()
    log(f"ERROR EN ETL: {str(e)}", level="ERROR")
    log("Detalle de excepción:", level="ERROR")
    print(traceback.format_exc())
    log(f"PROCESO FINALIZADO CON ERROR en {total_elapsed:.2f}s", level="ERROR")
    log_separator()
    raise