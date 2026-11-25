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
log = logging.getLogger("datosRipsQuiron")

TABLE = "datosRipsQuiron"
DATASET = "rips"
BQ_PROJECT = "ia-bigquery-397516"
KEYS = ["NumeroIdentificacion", "HoraAtencion", "CUPS"]
CHUNK = 20000


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


def insertMaria(engine, df):
    dfm = df.astype(object).where(pd.notnull(df), None)
    cols = ",".join(f"`{c}`" for c in dfm.columns)
    vals = ",".join(["%s"] * len(dfm.columns))
    sql = f"INSERT IGNORE INTO {TABLE} ({cols}) VALUES ({vals})"
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        for i in range(0, len(dfm), CHUNK):
            cur.executemany(sql, [tuple(r) for r in dfm.iloc[i:i + CHUNK].values])
            conn.commit()
    finally:
        conn.close()


try:
    log.info("Leyendo datosRipsView de Quiron...")
    conn = getSqlServer()
    df = pd.read_sql("SELECT * FROM dbo.datosRipsView", conn)
    conn.close()
    log.info(f"Filas leídas: {len(df)}")

    # Fechas fuera de rango (p. ej. año 8904) pasan a NULL
    for c in ["FechaNacimiento", "FechaAtencion", "HoraAtencion"]:
        antes = df[c].notna().sum()
        df[c] = pd.to_datetime(df[c], errors="coerce")
        malas = antes - df[c].notna().sum()
        if malas:
            log.warning(f"{c}: {malas} fechas fuera de rango convertidas a NULL")

    df["CodigoPaciente"] = df["CodigoPaciente"].astype("Int64")
    df["NumeroIdentificacion"] = df["NumeroIdentificacion"].astype(str)

    # Duplicados de Quiron (misma atención registrada varias veces)
    dups = df.duplicated(subset=KEYS).sum()
    if dups:
        log.warning(f"{dups} filas con llave repetida descartadas (duplicados de Quiron)")
    df = df.drop_duplicates(subset=KEYS, keep="first")

    # ---------- MariaDB ----------
    engine = getMariaEngine()
    existing = pd.read_sql(
        f"SELECT NumeroIdentificacion, HoraAtencion, CUPS FROM {TABLE}", engine)
    existing["HoraAtencion"] = pd.to_datetime(existing["HoraAtencion"], errors="coerce")
    existing["NumeroIdentificacion"] = existing["NumeroIdentificacion"].astype(str)
    newMaria = filterNew(df, existing, KEYS)
    if not newMaria.empty:
        insertMaria(engine, newMaria)
    log.info(f"MariaDB: {len(newMaria)} filas nuevas")

    # ---------- BigQuery ----------
    bq = bigquery.Client(project=BQ_PROJECT)
    tableId = f"{BQ_PROJECT}.{DATASET}.{TABLE}"
    existingBq = bq.query(
        f"SELECT NumeroIdentificacion, HoraAtencion, CUPS FROM `{tableId}`"
    ).to_dataframe()
    existingBq["HoraAtencion"] = pd.to_datetime(existingBq["HoraAtencion"], errors="coerce")
    existingBq["NumeroIdentificacion"] = existingBq["NumeroIdentificacion"].astype(str)
    newBq = filterNew(df, existingBq, KEYS)
    if not newBq.empty:
        schema = bq.get_table(tableId).schema
        bq.load_table_from_dataframe(
            newBq, tableId,
            job_config=bigquery.LoadJobConfig(
                schema=schema, write_disposition="WRITE_APPEND"),
        ).result()
    log.info(f"BigQuery: {len(newBq)} filas nuevas")

    # Últimas líneas: ajustar al formato que espera el DAG
    print(f"{len(newBq)} , {TABLE} , {datetime.now():%Y-%m-%d}")

except Exception as e:
    log.error(f"Error: {e}")
    sys.exit(1)