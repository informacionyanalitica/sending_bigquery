import os
import sys
import logging
from datetime import datetime

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("agendaQuiron")

TABLE = "agendaQuiron"
DATASET = "cirugias"
BQ_PROJECT = "ia-bigquery-397516"
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


def refreshMaria(engine, df):
    """Borra y recarga en una sola transacción: si algo falla, queda lo anterior."""
    dfm = df.astype(object).where(pd.notnull(df), None)
    cols = ",".join(f"`{c}`" for c in dfm.columns)
    vals = ",".join(["%s"] * len(dfm.columns))
    sql = f"INSERT INTO {TABLE} ({cols}) VALUES ({vals})"
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {TABLE}")
        for i in range(0, len(dfm), CHUNK):
            cur.executemany(sql, [tuple(r) for r in dfm.iloc[i:i + CHUNK].values])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def refreshBigQuery(bq, df):
    tableId = f"{BQ_PROJECT}.{DATASET}.{TABLE}"
    schema = bq.get_table(tableId).schema  # conserva los tipos del DDL
    job = bq.load_table_from_dataframe(
        df, tableId,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()


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
    with engine.connect() as c:
        actual = c.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()

    # Protección: si la lectura vino vacía o muy incompleta, no tocar nada
    if df.empty or len(df) < actual * 0.5:
        raise RuntimeError(
            f"Lectura sospechosa de Quiron ({len(df)} filas vs {actual} en MariaDB). "
            "No se modificó ninguna tabla."
        )

    refreshMaria(engine, df)
    log.info(f"MariaDB: tabla recargada con {len(df)} filas")

    # ---------- BigQuery ----------
    bq = bigquery.Client(project=BQ_PROJECT)
    refreshBigQuery(bq, df)
    log.info(f"BigQuery: tabla recargada con {len(df)} filas")

    # Últimas líneas: ajustar al formato que espera el DAG
    print(f"{len(df)} , {TABLE} , {datetime.now():%Y-%m-%d}")

except Exception as e:
    log.error(f"Error: {e}")
    sys.exit(1)