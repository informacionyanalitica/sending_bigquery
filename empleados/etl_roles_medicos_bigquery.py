import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from numpy import errstate
import requests
from google.cloud import bigquery
import pandas_gbq
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

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

# ---- Función para actualizar laboratorio_clinico_partition ----
def update_laboratorio_clinico(client, project_id):
    """
    Ejecuta todos los updates/merges en laboratorio_clinico_partition
    usando la fecha máxima de la tabla (se ejecuta después de laboratorio)
    """
    try:
        # MERGE 1: Actualizar MEDICO
        merge_medico = f"""
        MERGE `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition` AS l
        USING (
            SELECT identificacion, nombre
            FROM `{project_id}.empleados.activos_ultimos_meses_view`
        ) AS s
        ON l.C_MEDICO = s.identificacion
        WHEN MATCHED AND DATE(l.FECHA) = (SELECT MAX(DATE(FECHA)) FROM `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`) THEN
            UPDATE SET l.MEDICO = CAST(s.nombre AS STRING)
        """
        print(f"  Ejecutando MERGE 1: Actualizar MEDICO...")
        client.query(merge_medico).result()
        print(f"  ✓ MERGE 1 completado")
        
        # MERGE 2: Actualizar rol
        merge_rol = f"""
        MERGE `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition` AS l
        USING (
            SELECT identificacion_med, rol
            FROM `{project_id}.empleados.roles`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY identificacion_med ORDER BY rol) = 1
        ) AS s
        ON l.C_MEDICO = s.identificacion_med
        WHEN MATCHED AND DATE(l.FECHA) = (SELECT MAX(DATE(FECHA)) FROM `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`) THEN
            UPDATE SET l.rol = s.rol
        """
        print(f"  Ejecutando MERGE 2: Actualizar rol...")
        client.query(merge_rol).result()
        print(f"  ✓ MERGE 2 completado")
        
        # UPDATE 1: Normalizar valores
        update_normalize = f"""
        UPDATE `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition` AS l
        SET l.rol = CASE
            WHEN l.rol IN ('RIAS ', ' RIAS') THEN 'RIAS'
            WHEN l.rol IN ('Planificación Familiar', 'Planificacion', ' Planificación Familiar', 'Planificacion Familiar') THEN 'Planificación familiar'
            WHEN l.rol IN ('Medico gestor', 'Médico Gestor') THEN 'Medico Gestor'
            WHEN l.rol IN ('Ninguna', '') THEN 'Otro'
            ELSE l.rol
        END
        WHERE l.rol IN ('RIAS ', ' RIAS', 'Planificación Familiar', 'Planificacion', ' Planificación Familiar', 'Planificacion Familiar', 'Medico gestor', 'Médico Gestor', 'Ninguna', '')
        AND DATE(FECHA) = (SELECT MAX(DATE(FECHA)) FROM `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`)
        """
        print(f"  Ejecutando UPDATE 1: Normalizar rol...")
        client.query(update_normalize).result()
        print(f"  ✓ UPDATE 1 completado")

        # UPDATE 1.1: Cambiar a 'Consulta externa'
        update_consulta_externa = f"""
        UPDATE `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`
        SET rol = 'Consulta externa'
        WHERE rol IN ('Supernumeraria', 'Médico Gestor', 'Medico Gestor')
        AND DATE(FECHA) = (SELECT MAX(DATE(FECHA)) FROM `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`)
        """
        print(f"  Ejecutando UPDATE 1.1: Cambiar a 'Consulta externa'...")
        client.query(update_consulta_externa).result()
        print(f"  ✓ UPDATE 1.1 completado")

        # UPDATE 2: Establecer ESPECIALISTA
        update_especialista = f"""
        UPDATE `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`
        SET rol = 'ESPECIALISTA'
        WHERE rol = 'Otro'
        AND TRIM(cargo_gestal) IN (
            'MEDICO CIRUJANO','MEDICO CIRUJANO PLASTICO','MEDICO DEPORTOLOGO','MEDICO DERMATOLOGO',
            'MEDICO INTERNISTA','MEDICO LIDER REUMATOLOGIA','MEDICO ORTOPEDISTA','MEDICO OTORRINOLARINGOLOGO',
            'MEDICO PEDIATRA','MEDICO RADIOLOGO','MEDICO UROLOGO','DERMATOLOGA','REUMATOLOGO','NEUROLOGO',
            'REUMATÓLOGO INFANTIL','HEPATOLOGA','ANESTESIOLOGO','PSIQUIATRA','UROLOGO',
            'ESPECIALISTA EN CIRUGIA VASCULAR','MEDICO GINECOBSTETRA','ENDOCRINO'
        )
        AND DATE(FECHA) = (SELECT MAX(DATE(FECHA)) FROM `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`)
        """
        print(f"  Ejecutando UPDATE 2: Establecer ESPECIALISTA...")
        client.query(update_especialista).result()
        print(f"  ✓ UPDATE 2 completado")

        # UPDATE 3: Establecer CITOTECNOLOGO
        update_citotecnologo = f"""
        UPDATE `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`
        SET rol = 'CITOTECNOLOGO'
        WHERE TRIM(cargo_gestal) = 'CITOTECNOLOGA'
        AND DATE(FECHA) = (SELECT MAX(DATE(FECHA)) FROM `{project_id}.ayudas_diagnosticas.laboratorio_clinico_partition`)
        """
        print(f"  Ejecutando UPDATE 3: Establecer CITOTECNOLOGO...")
        client.query(update_citotecnologo).result()
        print(f"  ✓ UPDATE 3 completado")
        
        print("✓ Todos los updates completados exitosamente")
        return True
    except Exception as e:
        print(f"✗ Error en updates: {e}")
        return False

# ---- Código principal ----
def main():
    print("\n" + "="*60)
    print("CARGA DE ROLES MÉDICOS - GOOGLE SHEETS → BIGQUERY")
    print("="*60 + "\n")
    
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
    
    # 1. Obtener datos
    print("📊 PASO 1: Leyendo datos de Google Sheets...")
    dfRoles = getDfRoles(dfIdGoogleSheet)
    print(f"\n✓ Datos obtenidos: {len(dfRoles)} filas")
    
    if dfRoles.empty:
        print("\n⚠ No hay datos para procesar. Finalizando.")
        return
    
    # 2. Insertar en BigQuery
    print("\n☁️  PASO 2: Insertando en BigQuery...")
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
        insertBD._insert_bigquery(project_id, dataset_id, table_id)
        
        # 3. Ejecutar updates
        print("\n🔄 PASO 3: Actualizando tabla laboratorio_clinico_partition...")
        update_laboratorio_clinico(client, project_id)
        
    except Exception as e:
        print(f"✗ Error en BigQuery: {e}")
    
    print("\n" + "="*60)
    print("✅ PROCESO COMPLETADO")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()