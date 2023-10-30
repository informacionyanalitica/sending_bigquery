from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from pandas_gbq import to_gbq
import os,sys

class CloudBigQuery:
    def __init__(self, project_id, dataset_id, table_name):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.client = bigquery.Client()

    def create_table(self, schema):
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_name)
        table = bigquery.Table(table_ref, schema=schema)
        try:
            table = self.client.create_table(table)  # Solo se crea si no existe
            print(f"Tabla {table.dataset_id}.{table.table_id} creada con éxito.")
        except Exception as e:
            print(f"Error al crear la tabla: {e}")

    def write_to_table(self, df, if_exists='append'):
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_name)
        try:
            to_gbq(df, destination_table=f'{self.project_id}.{self.dataset_id}.{self.table_name}', project_id=self.project_id, if_exists=if_exists)
        except ValueError as err:
            print(err)

    def write_to_table_no_duplicates(self,df):
        try:
            job_config  = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = self.client.load_table_from_dataframe(df,f'{self.project_id}.{self.dataset_id}.{self.table_name}', job_config=job_config)
            job.result()
            tabla_bigquery = f'{self.project_id}.{self.dataset_id}.{self.table_name}'
            print(f"Se insertaron {job.output_rows} filas en {tabla_bigquery}.")
            return [job.output_rows,tabla_bigquery]
        except Exception as err:
            print(err)
        
        
    def read_table(self, query):
        try:
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            return df
        except Exception as e:
            print(f"Error al leer la tabla: {e}")

    def delete_table(self):
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(self.table_name)
            self.client.delete_table(table_ref)
            print(f"Tabla {table_ref.path} eliminada con éxito.")
        except NotFound:
            print(f"La tabla {self.table_name} no existe.")
        except Exception as e:
            print(f"Error al eliminar la tabla: {e}")
