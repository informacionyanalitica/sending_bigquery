import sys
import os
import pandas as pd
from datetime import datetime
from numpy import errstate
import requests
from google.cloud import bigquery
import pandas_gbq
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ---- Configuración de MariaDB desde .env ----
MARIADB_CONFIG = {
    'host': os.getenv('DB_HOST_TRANSACTION'),
    'port': int(os.getenv('DB_PORT_TRANSACTION', 3306)),
    'user': os.getenv('DB_USER_TRANSACTION'),
    'password': os.getenv('DB_PASSWORD_TRANSACTION'),
    'database': 'analitica'
}

# ---- Funciones de Google Drive y Sheets ----
def getIdsGoogleSheet(path_folder):
    try:
        response = requests.post('http://localhost:5000/drive', json=path_folder)
        elementsDriveJson = response.json()
        return pd.DataFrame(elementsDriveJson, columns=['name', 'id', 'typeFile'])
    except ValueError as err:
        print(err)

def getIdFileSheet(elementsDrive, nameFile):
    try:
        elementsDrive = elementsDrive[elementsDrive['name'] == nameFile]
        if elementsDrive.shape[0] > 0:
            return elementsDrive
        else:
            return "No existen archivos que coincidan"
    except ValueError as err:
        print(err)

def readFile(id, namePage="Hoja 1"):
    try:
        url = f'https://apps.coopsana.co:7154/googleSheets/read/{id}/{namePage}'
        print(f"Leyendo: {url}")
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        if 'rows' not in data:
            print(f"Error: La respuesta no contiene la clave 'rows'. Respuesta: {data}")
            return None
        return data
    except requests.exceptions.HTTPError as err:
        print(f"Error HTTP: {err} - Código: {response.status_code} - Respuesta: {response.text}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"Error al leer Google Sheet: {err}")
        return None
    except ValueError as err:
        print(f"Error al procesar JSON: {err}")
        return None

# ---- Funciones para MariaDB ----
def connect_mariadb():
    """Conecta a MariaDB y retorna la conexión"""
    try:
        connection = mysql.connector.connect(**MARIADB_CONFIG)
        if connection.is_connected():
            print(f"✓ Conexión exitosa a MariaDB: {MARIADB_CONFIG['database']}")
            return connection
    except Error as e:
        print(f"✗ Error al conectar a MariaDB: {e}")
        return None

def insert_mariadb(df, table_name, connection):
    """Borra datos antiguos e inserta nuevos datos en MariaDB"""
    try:
        cursor = connection.cursor()
        
        # 1. Borrar datos existentes
        delete_query = f"DELETE FROM {table_name}"
        cursor.execute(delete_query)
        print(f"✓ Datos antiguos eliminados de {table_name}")
        
        # 2. Preparar INSERT
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # 3. Insertar datos fila por fila
        rows_inserted = 0
        for index, row in df.iterrows():
            values = []
            for value in row:
                if pd.isna(value):
                    values.append(None)
                elif isinstance(value, datetime):
                    values.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    values.append(str(value))
            
            cursor.execute(insert_query, tuple(values))
            rows_inserted += 1
        
        # 4. Confirmar transacción
        connection.commit()
        print(f"✓ {rows_inserted} filas insertadas en {table_name}")
        
        # 5. Verificar el conteo final
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"✓ Total de filas en {table_name}: {count}")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"✗ Error al insertar en MariaDB: {e}")
        connection.rollback()
        return False

# ---- Clase Query para BigQuery ----
class Query:
    def __init__(self, sql=None, df=None, nameTB=None):
        self.sql = sql
        self.df = df
        self.nameTB = nameTB
    
    def _insert_bigquery(self, project_id, dataset_id, table_id):
        try:
            if self.df.shape[0] > 0:
                pandas_gbq.to_gbq(
                    self.df,
                    destination_table=f"{dataset_id}.{table_id}",
                    project_id=project_id,
                    if_exists='replace',
                    table_schema=[
                        {'name': 'identificacion_med', 'type': 'STRING'},
                        {'name': 'nombre_med', 'type': 'STRING'},
                        {'name': 'cargoRoles', 'type': 'STRING'},
                        {'name': 'sedeRol', 'type': 'STRING'},
                        {'name': 'rol', 'type': 'STRING'},
                        {'name': 'rol2', 'type': 'STRING'},
                        {'name': 'sedeHoja', 'type': 'STRING'},
                        {'name': 'observaciones', 'type': 'STRING'},
                        {'name': 'fecha_actualizacion', 'type': 'TIMESTAMP'}
                    ]
                )
                # Verificar el número de filas después de la inserción
                client = bigquery.Client(project=project_id)
                query = f"SELECT COUNT(*) as total FROM `{project_id}.{dataset_id}.{table_id}`"
                query_job = client.query(query)
                result = query_job.result()
                for row in result:
                    print(f"✓ Filas en BigQuery ({table_id}): {row.total}")
                return True
            else:
                print("⚠ El DataFrame está vacío, no se insertaron datos en BigQuery.")
                return False
        except Exception as er:
            print(f"✗ Error en BigQuery: {er}")
            return str(er)

# ---- Código principal ----
def main():
    print("\n" + "="*60)
    print("CARGA DE ROLES MÉDICOS - GOOGLE SHEETS → MARIADB + BIGQUERY")
    print("="*60 + "\n")
    
    # Lista estática de subcarpetas e IDs
    subfolders = [
        {"name": "Avenida Oriental", "id": "1PfWP54HztgDHezRCYgmeEoeIqOhl3cBH-6D5TZI3rmI", "typeFile": "spreadsheet"},
        {"name": "Calasanz", "id": "1y3RWFKUkeCznWPqccEVjUUZjJ3Wb8tnrCawsLWJ30CY", "typeFile": "spreadsheet"},
        {"name": "Centro", "id": "1i888oFFG3iuEZUh-wHOCA8Umu9TPOEfXm2CYcPqd1Yk", "typeFile": "spreadsheet"},
        {"name": "CGR", "id": "1UMPKyutb1v9HtZPAgA20RM0-5XyKfwgiTRbQCf4qVZc", "typeFile": "spreadsheet"},
        {"name": "Norte", "id": "1Izt6vvSbUOPpEgcAzsfov6-VSRFjqICcYLIB5wlCqaw", "typeFile": "spreadsheet"},
        {"name": "PAC - Actualización ROL", "id": "1HZ1v-wL0R3fOuhJ0_2KOTYsL2eVSoT4EQWRbH9mSKG0", "typeFile": "spreadsheet"},
        {"name": "Tb", "id": "1A_nIOLgOxD2jDoo-61X6zQN8LhSFESeU0MNcIYxmGfI", "typeFile": "spreadsheet"},
    ]
    
    dfIdGoogleSheet = pd.DataFrame(subfolders, columns=['name', 'id', 'typeFile'])
    
    hoja = 'BD'
    required_columns = ['identificacion_med', 'nombre_med', 'cargoRoles', 'sedeRol', 
                       'rol', 'rol2', 'sedeHoja', 'observaciones', 'fecha_actualizacion']
    
    # Función para obtener datos
    def getDfRoles(dataDrive):
        sedesRoles = dataDrive.name
        dfRoles = pd.DataFrame()
        for sede in sedesRoles:
            try:
                dfIdFile = getIdFileSheet(dataDrive, sede)
                if isinstance(dfIdFile, str):
                    print(dfIdFile)
                    continue
                file_id = dfIdFile.id.iloc[-1]
                data = readFile(file_id, hoja)
                if data is None:
                    print(f"No se pudo leer datos para la sede {sede}")
                    continue
                dfFinal = pd.DataFrame(data['rows'])
                dfFinal.columns = ['identificacion_med', 'nombre_med', 'cargoRoles', 'sedeRol', 'rol', 'rol2', 'observaciones']
                dfFinal['sedeHoja'] = sede
                dfRoles = pd.concat([dfRoles, dfFinal], ignore_index=True)
            except Exception as err:
                print(f"Error procesando sede {sede}: {err}")
        if not dfRoles.empty:
            dfRoles['fecha_actualizacion'] = datetime.now()
            dfRoles = dfRoles[required_columns]
        return dfRoles
    
    # 1. Obtener datos de Google Sheets
    print("📊 PASO 1: Leyendo datos de Google Sheets...")
    dfRoles = getDfRoles(dfIdGoogleSheet)
    
    print(f"\n✓ Datos obtenidos: {len(dfRoles)} filas")
    print(f"✓ Columnas: {list(dfRoles.columns)}")
    
    if dfRoles.empty:
        print("\n⚠ No hay datos para procesar. Finalizando.")
        return
    
    # 2. Insertar en MariaDB automáticamente
    print("\n🗄️  PASO 2: Insertando en MariaDB...")
    connection = connect_mariadb()
    if connection:
        insert_mariadb(dfRoles, 'roles_medicos', connection)
        connection.close()
        print("✓ Conexión a MariaDB cerrada")
    else:
        print("✗ No se pudo conectar a MariaDB, saltando este paso")
    
    # 3. Insertar en BigQuery
    print("\n☁️  PASO 3: Insertando en BigQuery...")
    try:
        client = bigquery.Client()
        project_id = 'ia-bigquery-397516'
        dataset_id = 'empleados'
        dataset_id_full = f"{project_id}.{dataset_id}"
        dataset = bigquery.Dataset(dataset_id_full)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"✓ Dataset {dataset_id_full} verificado")
        
        table_id = 'roles'
        insertBD = Query(None, dfRoles, 'roles_medicos')
        result_bigquery = insertBD._insert_bigquery(project_id, dataset_id, table_id)
        
    except Exception as e:
        print(f"✗ Error en BigQuery: {e}")
    
    print("\n" + "="*60)
    print("✅ PROCESO COMPLETADO")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
