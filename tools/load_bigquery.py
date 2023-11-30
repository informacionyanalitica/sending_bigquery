
import pandas as pd
import numpy as np 
import sys,os 
import func_process
from datetime import datetime

PATH_BIGQUERY = os.environ.get("PATH_BIGQUERY")
path = os.path.abspath(PATH_BIGQUERY)
sys.path.insert(1,path)
from cloud_bigquery import CloudBigQuery

FECHA_CARGUE = datetime.now()
YEAR = FECHA_CARGUE.year
MONTH = FECHA_CARGUE.month

SQL_VALIDATE_LOADS = """ SELECT COUNT(*) AS totalCargues
                        FROM reportes.logsCarguesBigquery AS lg
                        WHERE lg.idBigquery = '{idBigquery}'
                        AND year(lg.fechaCargue) = '{year}' AND MONTH(lg.fechaCargue)='{mes}'
                        """

def instanciar_cloud_bigquery(tabla_bigquery):
    project_id_product, dataset_id, table_name = tabla_bigquery.split('.')
    try:
        bq_cloud = CloudBigQuery(project_id_product, dataset_id, table_name)
        return bq_cloud
    except ValueError as err:
        print(err)

def insert_log_cargues_bigquery(total_registros:int,table_bigquery:str):
    """ Guardar datos sobre el cargue realizado """
    sql_insert = """
                INSERT INTO reportes.logsCarguesBigquery
                VALUES(0,NOW(),{},'{}')
            """.format(total_registros,table_bigquery)
    try:   
        return func_process.insert_rows(sql_insert)
    except Exception as err:
        print(err) 
        
def rows_not_duplicates(df_bd,column,sql_biquery,tabla_bigquery,valores_unicos):
    bq_cloud = instanciar_cloud_bigquery(tabla_bigquery)
    try:
        if len(valores_unicos)>1:
            sql_read = sql_biquery.format(tabla_bigquery,valores_unicos)
        else:
            valor_unico = '('+str(valores_unicos[0])+')'
            sql_read = sql_biquery.format(tabla_bigquery,valor_unico)
        registros_duplicados = bq_cloud.read_table(sql_read)
        # Obtener valores no duplicados
        df_save = df_bd[~df_bd[column].isin(registros_duplicados[column].to_list())]
        return df_save
    except ValueError as err:
        print(err)


def load_data_bigquery(df_save,tabla_bigquery):
    bq_cloud = instanciar_cloud_bigquery(tabla_bigquery)
    try:
        if not df_save.empty:
            response_save = bq_cloud.write_to_table_no_duplicates(df_save)
            insert_log_cargues_bigquery(response_save[0], response_save[1])
        else:    
            print('Dataframe sin datos')
    except Exception as err:
        print(err)

def validate_loads_monthly(tabla_bigquery):
    try:
        df_validate_loads = func_process.load_df_server(SQL_VALIDATE_LOADS.format(idBigquery=tabla_bigquery,year=YEAR,mes=MONTH),'reportes')
        return df_validate_loads
    except ValueError as err:
        print(err)