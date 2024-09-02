
import pandas as pd
import numpy as np 
import sys,os 
import func_process
from datetime import datetime

PATH_BIGQUERY = os.environ.get("PATH_BIGQUERY")
path = os.path.abspath(PATH_BIGQUERY)
sys.path.insert(1,path)
from cloud_bigquery import CloudBigQuery

FECHA_CARGUE = datetime.now().replace(microsecond=0)
YEAR = FECHA_CARGUE.year
MONTH = FECHA_CARGUE.month


SQL_VALIDATE_LOADS_MONTHLY = """ SELECT COUNT(*) AS totalCargues
                        FROM reportes.logsCarguesBigquery AS lg
                        WHERE lg.idBigquery = '{idBigquery}'
                        AND year(lg.fechaCargue) = '{year}' AND MONTH(lg.fechaCargue)='{mes}'
                        """


SQL_VALIDATE_LOADS_DAILY = """ SELECT COUNT(*) AS totalCargues
                        FROM reportes.logsCarguesBigquery AS lg
                        WHERE lg.idBigquery = '{idBigquery}'
                        AND date(lg.fechaCargue) = '{date_load}'
                        """

SQL_VALIDATE_LOADS_WEEKLY = """ SELECT COUNT(*) AS totalCargues
                        FROM reportes.logsCarguesBigquery AS lg
                        WHERE lg.idBigquery = '{idBigquery}'
                        AND DATE(lg.fechaCargue) >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY)
                        AND DATE(lg.fechaCargue) < DATE_ADD(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY), INTERVAL 7 DAY)
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
    df_save = pd.DataFrame()
    try:
        if len(valores_unicos) > 0:
            if len(valores_unicos)==1:
                valor_unico = "('"+str(valores_unicos[0])+"')"
                sql_read = sql_biquery.format(tabla_bigquery,valor_unico)
                registros_duplicados = bq_cloud.read_table(sql_read)
            elif len(valores_unicos)>1:
                sql_read = sql_biquery.format(tabla_bigquery,valores_unicos)
                registros_duplicados = bq_cloud.read_table(sql_read)
            # Obtener valores no duplicados
            df_save = df_bd[~df_bd[column].isin(registros_duplicados[column].to_list())]
        return df_save
    except ValueError as err:
        print(err)

def rows_duplicates_last_month(df_bd,column,sql_biquery,tabla_bigquery):
    bq_cloud = instanciar_cloud_bigquery(tabla_bigquery)
    df_save = pd.DataFrame()
    try:
        sql_read = sql_biquery.format(tabla_bigquery) 
        rows_save_bigquery = bq_cloud.read_table(sql_read)
        # Obtener valores no duplicados
        df_save = df_bd[~df_bd[column].isin(rows_save_bigquery[column].to_list())]
        return df_save
    except ValueError as err:
        print(err)

def load_data_bigquery(df_save,tabla_bigquery,if_exists='WRITE_APPEND'):
    bq_cloud = instanciar_cloud_bigquery(tabla_bigquery)
    try:
        if not df_save.empty:
            response_save = bq_cloud.write_to_table_no_duplicates(df_save,if_exists)
            insert_log_cargues_bigquery(response_save[0], response_save[1])
            print(response_save[0],',',response_save[1],',',FECHA_CARGUE)
        else:    
            print('Dataframe sin datos')
            print(0,',',tabla_bigquery,',',FECHA_CARGUE)
    except Exception as err:
        print(err)

def update_data_bigquery(sql_update,tabla_bigquery):
    bq_cloud = instanciar_cloud_bigquery(tabla_bigquery)
    try:
        response_update = bq_cloud.update_table(sql_update)
        return response_update
    except Exception as err:
        print(err)

def delete_table_bigquery(tabla_bigquery):
    bq_cloud = instanciar_cloud_bigquery(tabla_bigquery)
    try:
        bq_cloud.delete_table()
    except Exception as err:
        print(err)

def read_data_bigquery(sql_bigquery,tabla_bigquery):
    bq_cloud = instanciar_cloud_bigquery(tabla_bigquery)
    df_read = pd.DataFrame()
    try:
        df_read = bq_cloud.read_table(sql_bigquery)
        return df_read
    except Exception as err:
        print(err)

def validate_loads_monthly(tabla_bigquery):
    try:
        df_validate_loads = func_process.load_df_server(SQL_VALIDATE_LOADS_MONTHLY.format(idBigquery=tabla_bigquery,year=YEAR,mes=MONTH),'reportes')
        if df_validate_loads.totalCargues[0] == 0:
            return df_validate_loads
        else:
            raise ValueError("Ya se realizo el cargue para el día de hoy")
    except ValueError as err:
        print(err)

def validate_loads_daily(tabla_bigquery):
    try:
        df_validate_loads = func_process.load_df_server(SQL_VALIDATE_LOADS_DAILY.format(idBigquery=tabla_bigquery,
                                                                                        date_load=FECHA_CARGUE.date()),
                                                                                        'reportes')
        if df_validate_loads.totalCargues[0] == 0:
            return df_validate_loads
        else:
            raise ValueError("Ya se realizo el cargue para el día de hoy")
    except ValueError as err:
        print(err)

def validate_loads_weekly(tabla_bigquery):
    try:
        df_validate_loads = func_process.load_df_server(SQL_VALIDATE_LOADS_WEEKLY.format(idBigquery=tabla_bigquery),'reportes')
        if df_validate_loads.totalCargues[0] == 0:
            return df_validate_loads
        else:
            raise ValueError("Ya se realizo el cargue para el día de hoy")
    except ValueError as err:
        print(err)

