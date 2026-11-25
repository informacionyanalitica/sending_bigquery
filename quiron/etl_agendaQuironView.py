import os
import sys
import logging
from datetime import datetime

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("agendaQuiron")

TABLE = "agendaQuiron"
DATASET = "cirugias"
BQ_PROJECT = "ia-bigquery-397516"
KEYS = ["ClaveAgenda"]


def getSqlServer():
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
    return pyodbc.connect(
        f"DRIVER={{{driver}}};"
        f"SERVER={os.getenv('SQLSERVER_HOST')},{os.getenv('SQLSERVER_PORT')};"
        f"DATABASE={os.getenv('SQLSERVER_DB')};"
        f"UID={os.getenv('SQLSERVER_USER')};PWD={os.getenv('SQLSERVER_PASS', '')};"
        "TrustServerCertificate=yes;",
        timeout=30,
    )


def getMariaEngine():
    user = os.getenv("DB_USER_TRANSACTION")
    pwd = os.getenv("DB_PASSWORD_TRANSACTION")
    host = os.getenv("DB_HOST_TRANSACTION")
    port = os.getenv("DB_PORT_TRANSACTION", "3306")
    return create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/reportes")


def filterNew(df, existing, keys):
    merged = df.merge(existing, on=keys, how="left", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop(columns="_merge")


def insertIgnoreMaria(engine, df, chunk=20000):
    df = df.astype(object).where(pd.notnull(df), None)
    cols = ",".join(f"`{c}`" for c in df.columns)
    vals = ",".join(["%s"] * len(df.columns))
    sql = f"INSERT IGNORE INTO {TABLE} ({cols}) VALUES ({vals})"
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        for i in range(0, len(df), chunk):
            cur.executemany(sql, [tuple(r) for r in df.iloc[i:i + chunk].values])
            conn.commit()
    finally:
        conn.close()


try:
    log.info("Leyendo agendaView de Quiron...")
    conn = getSqlServer()
    df = pd.read_sql("SELECT * FROM dbo.agendaView", conn)
    conn.close()
    log.info(f"Filas leídas: {len(df)}")

    df["ClaveAgenda"] = df["ClaveAgenda"].astype("int64")
    df["NumeroOrden"] = df["NumeroOrden"].fillna(0).astype("int64")

    # ---------- MariaDB ----------
    engine = getMariaEngine()
    existing = pd.read_sql(f"SELECT ClaveAgenda FROM {TABLE}", engine)
    existing["ClaveAgenda"] = existing["ClaveAgenda"].astype("int64")
    newMaria = filterNew(df, existing, KEYS)
    if not newMaria.empty:
        insertIgnoreMaria(engine, newMaria)
    log.info(f"MariaDB: {len(newMaria)} filas nuevas")

    # ---------- BigQuery ----------
    bq = bigquery.Client(project=BQ_PROJECT)
    tableId = f"{BQ_PROJECT}.{DATASET}.{TABLE}"
    existingBq = bq.query(f"SELECT ClaveAgenda FROM `{tableId}`").to_dataframe()
    existingBq["ClaveAgenda"] = existingBq["ClaveAgenda"].astype("int64")
    newBq = filterNew(df, existingBq, KEYS)
    if not newBq.empty:
        bq.load_table_from_dataframe(
            newBq, tableId,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        ).result()
    log.info(f"BigQuery: {len(newBq)} filas nuevas")

    # Últimas líneas: ajustar al formato que espera el DAG
    print(f"{len(newBq)} , {TABLE} , {datetime.now():%Y-%m-%d}")

except Exception as e:
    log.error(f"Error: {e}")
    sys.exit(1)