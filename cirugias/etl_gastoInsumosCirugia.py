import sys
import os
import re
import time
import pandas as pd
from datetime import datetime
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas_gbq

# Carga el archivo .env y configura credenciales (bloque estándar)
load_dotenv()
PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq

# ---- Función de lectura Google Sheets ----
def readFile(id, namePage="BD_GASTO"):
    try:
        url = f'https://apps.coopsana.co:7154/googleSheets/read/{id}/{namePage}'
        response = requests.get(url, timeout=30)
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

def log(msg, emoji="•"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {emoji} {msg}")

def to_camel_case_safe(col, usados):
    tokens = re.findall(r'[A-Za-zÀ-ÿ0-9]+', str(col))
    if not tokens:
        base = 'col'
    else:
        first = tokens[0].lower()
        rest = ''.join(t.capitalize() for t in tokens[1:])
        base = first + rest
    if base[0].isdigit():
        base = 'c_' + base
    nombre = base
    contador = 2
    while nombre in usados:
        nombre = f"{base}_{contador}"
        contador += 1
    usados.add(nombre)
    return nombre

# ---- CONFIGURACIÓN ----
GOOGLE_SHEET_ID = "1I57sl6RielYQXZOsh1BWgMk1N_MYOVBFnR76uj5kTvk"
SHEET_TAB = "BD_GASTO"
FECHA_INICIO = "2026-07-01"

project_id_product = 'ia-bigquery-397516'
dataset_id_cirugias = 'cirugias'
table_name_cirugias = 'gastoInsumosCirugia'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_cirugias}.{table_name_cirugias}'

COLUMNAS_TOTALES = [
    'ID_CIRUGIA', 'FECHA_EXPORTACION', 'USUARIO', 'FECHA', 'IDENTIFICACION', 'PACIENTE', 'EPS',
    'ESPECIALIDAD', 'PROCEDIMIENTO_1', 'PROCEDIMIENTO_2', 'PROCEDIMIENTO_3', 'CIRUJANO',
    'ANESTESIOLOGO', 'TIPO_ANESTESIA', 'ESTADO_DESCARGUE',
    'ACIDO FUSIDICO 2% CREMA X 15 GR UNIDDAD', 'BUPIVACAINA 0,5% PESADA 20MG/4ML AMPOLLA',
    'CLORURO DE SODIO 0.9% 100 ML UNIDAD', 'CLORURO DE SODIO 0.9% 250 ML UNIDAD',
    'CLORURO DE SODIO 0.9% 500 ML UNIDAD', 'DEXAMETASONA 8MG/2ML AMPOLLA UNIDAD',
    'DICLOFENACO 75MG/3ML AMPOLLAS UNIDAD', 'DIPIRONA 1G/2ML AMPOLLAS UNIDAD',
    'HARTMAN BOLSA X 3000ML UNIDAD', 'OXIMETAZOLINA 0.025%  FRASCOX15ML UNIDAD',
    'TRAMADOL 100MG/2ML AMPOLLA UNIDAD', 'ADAPTADOR PRN ATI TERAPIA INTERMITENTE UNIDAD',
    'AGUJA DESECHABLE 18G X 1 1/2" UNIDAD', 'AGUJA DESECHABLE 25G X 1  UNIDAD',
    'AGUJA DESECHABLE 26G X 1/2 " UNIDAD', 'AGUJA ESPINAL PUNTA LAPIZ 25G 0.5MM X 90 MM WHITACRE UNIDAD',
    'AGUJA ESPINAL PUNTA LAPIZ 27G 0.4MM X 90 MM WHITACRE UNIDAD', 'ASEPTO JERINGA DESECHABLE UNIDAD',
    'CANULA DE GUEDEL N° 1 UNIDAD', 'CANULA DE GUEDEL N° 2 UNIDAD', 'CANULA DE GUEDEL N° 3 UNIDAD',
    'CANULA DE GUEDEL N° 4  UNIDAD', 'CANULA DE OXIGENO ADULTO 2 MT UNIDAD',
    'CANULA DE OXIGENO PEDIATRICA UNIDAD', 'CATETER INTRAVENOSO 22G X 1 1/4" UNIDAD',
    'CATETER INTRAVENOSO 24G X 1 1/4" UNIDAD', 'CAUCHO DE SUCCION 3,6 MM UNIDAD',
    'ELECTRODO MONITOREO UNIDAD', 'EQUIPO IRRIGACION EN Y BAXTER UNIDAD',
    'EXTENSION DE ANESTESIA UNIDAD', 'FILTRO ANTIBACTERIAL ADULTO UNIDAD',
    'FILTRO ANTIBACTERIAL PEDIATRICO UNIDAD', 'GASA ESTERIL 45X45 MEDICAL SUPPLIES PAQUETE X 5',
    'GASA ESTERIL 7.5 X7.5 CM X 5 UNID', 'HOJA PARA BISTURI N° 11 PARAMOUNT UNIDAD',
    'HOJA PARA BISTURI N°15 PARAMOUNT UNIDAD', 'JERINGA DESECHABLE 1 CC INSULINA UNIDAD',
    'JERINGA DESECHABLE 10 CC UNIDAD', 'JERINGA DESECHABLE 20 CC UNIDAD',
    'JERINGA DESECHABLE 3 CC UNIDAD', 'JERINGA DESECHABLE 5 CC UNIDAD',
    'LAPIZ ELECTROBISTURI UNIDAD', 'MASCARA ANESTESIA N°2 UNIDAD', 'MASCARA ANESTESIA N°3 UNIDAD',
    'MASCARA ANESTESIA N°4  UNIDAD', 'PLACA ELECTROBISTURI CON CABLE UNIDAD',
    'PRESERVATIVO EN LATEX UNIDAD', 'RECIPIENTE CITOQUIMICO  NACIONAL UNIDAD',
    'CATGUT CROMADO 2-0 SH 70CM REF. G123T JOHNSON & JOHNSON UNIDAD (CATGUT CROMADO 2-0 AGUJA V-20 )',
    'CATGUT CROMADO 3-0 SH 70CM REF. G122T JOHNSON & JOHNSON UNIDAD (CATGUT CROMADO 3-0 AGUJA V-20)',
    'CATGUT CROMADO 4-0 HR-15 75CM MC15 (CATGUT CROMADO 4-0 RB1 70CM)',
    'MONOCRYL  3-0 PS-2 70 CM REF.MCP427H JOHNSON & JOHNSON  UNIDAD (CAPROSYN 3-0 AGUJA P-12 75CM REF. SC-5638G CJ X 12 UND)',
    'MONOCRYL  4-0 PS-2 45 CM REF.MCP496G JOHNSON & JOHNSON  UNIDAD (CAPROSYN 4-0 AGUJA P-12 75CM REF. SC-5637-G CJ X 12 UND)',
    'NYLON 2-0 CT25 BLK 75CM MONOSOF UNIDAD', 'POLYGLACTIN  2-0 AGUJA MCR-37 90 CM (VICRYL 2-0 CT1)',
    'POLYGLACTIN 0 MCR26 UNIDAD 70CM (VICRYL 0 CT2)',
    'POLYGLACTIN 3 0 MC26 VIO X 70CM (VICRYL PLUS 3-0  HS ) (POLYSORB 3-0 V-20 75CM MEDTRONIC UNIDAD)',
    'POLYSORB 2-0 AGUJA GS22  75 CM (VICRYL 2-0 CT2)',
    'POLYSORB 4 0  C13  X75CM AGUJA CORTANTE (VICRYL RECUBIERTO  4-0 SC -20 70 CM)',
    'POLYSORB 5-0 AGUJA P-13 45CM REF.SL5687 CJ X 36',
    'SEDA NEGRA 2-0 SA 75CM REF. SA85T JOHNSON & JOHNSON UNIDAD',
    'SEDA NEGRA 2-0 SC26 45CM (SEDA SOFSILK 2-0 CT-25)', 'SEDA SOFSILK 2-0 AGUJA SC-2 75CM',
    'SURGIPRO  4-0 AGUJA P-12 X 45CM (PROLENE  4-0 PS-2 45 CM)',
    'SURGIPRO 0 AGUJA GS-21 75CM (PROLENE 0 CT-1)',
    'SURGIPRO 0 AGUJA GS-22  75CM UNIDAD (PROLENO 0 CT-2)',
    'SURGIPRO 2-0 AGUJA GS-22  75CM  (PROLENO 2 CT-2)',
    'SURGIPRO 6/0 18 P-10 BLUE X 45 CM (PROLENE 6-0 P-1 45 CM)',
    'SURGIPRO II 3-0 CT-25 75CM (PROLENE 3-0 SC-24)', 'TICRON 0 AGUJA GS-22 75CM UNIDAD (ETHIBOND 0 CT2 75CM)',
    'FRASCO FORMOL 80 ML', 'URL_PDF',
    'CATGUT CROMADO  5-0 RB-1 70 CM  REF U202T JOHNSON & JOHNSON  UNIDAD  (CATGUT CROMADO 5-0 AGUJA CV-23 75CM UNIDAD)'
]

COLUMNAS_METADATA = [
    'ID_CIRUGIA', 'FECHA_EXPORTACION', 'USUARIO', 'FECHA', 'IDENTIFICACION',
    'PACIENTE', 'EPS', 'ESPECIALIDAD', 'PROCEDIMIENTO_1', 'PROCEDIMIENTO_2', 'PROCEDIMIENTO_3',
    'CIRUJANO', 'ANESTESIOLOGO', 'TIPO_ANESTESIA', 'ESTADO_DESCARGUE'
]

COLUMNA_A_EXCLUIR = 'URL_PDF'  # no se carga a BigQuery

def salir_con_error(t_inicio, total_registros=0):
    print("-" * 60)
    log(f"Proceso finalizado en {time.time() - t_inicio:.2f}s totales (CON ERRORES)", "🏁")
    print("=" * 60)
    print("Save successfully")
    print(f"{total_registros},{TABLA_BIGQUERY},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(1)

t_inicio = time.time()

print("=" * 60)
print(f" ETL — gastoInsumosCirugia CRUDO ({FECHA_INICIO} → hoy, reemplazo total)")
print("=" * 60)

log(f"Leyendo hoja '{SHEET_TAB}' del Google Sheet...", "📄")
t0 = time.time()
data = readFile(GOOGLE_SHEET_ID, SHEET_TAB)

if data is None:
    log(f"No se pudo leer la hoja '{SHEET_TAB}'", "✗")
    salir_con_error(t_inicio)

df = pd.DataFrame(data['rows'])
log(f"Lectura completada en {time.time() - t0:.2f}s — {len(df):,} filas crudas, {df.shape[1]} columnas", "✅")

if df.shape[1] != len(COLUMNAS_TOTALES):
    log(f"⚠️ Advertencia: se esperaban {len(COLUMNAS_TOTALES)} columnas, llegaron {df.shape[1]} — revisar orden del sheet", "⚠️")

df.columns = COLUMNAS_TOTALES[:df.shape[1]]

log("Convirtiendo nombres reales a camelCase...", "🔤")
usados = set()
mapa_camel = {}
for col in df.columns:
    mapa_camel[col] = to_camel_case_safe(col, usados)
df = df.rename(columns=mapa_camel)

col_fecha_exportacion = mapa_camel['FECHA_EXPORTACION']

log(f"Excluyendo columna '{COLUMNA_A_EXCLUIR}' (no se carga)...", "🚫")
col_a_excluir_camel = mapa_camel.get(COLUMNA_A_EXCLUIR)
if col_a_excluir_camel and col_a_excluir_camel in df.columns:
    df = df.drop(columns=[col_a_excluir_camel])

log("Filtrando por fecha de exportación...", "📅")
df[col_fecha_exportacion] = pd.to_datetime(df[col_fecha_exportacion], errors='coerce')
df = df[df[col_fecha_exportacion] >= FECHA_INICIO]
log(f"Filas dentro del rango: {len(df):,}", "✅")

if df.empty:
    log("Sin registros dentro del rango", "⚠️")
    salir_con_error(t_inicio)

columnas_insumos_camel = [
    mapa_camel[c] for c in COLUMNAS_TOTALES
    if c not in COLUMNAS_METADATA and c != COLUMNA_A_EXCLUIR
]
log(f"Convirtiendo {len(columnas_insumos_camel)} columnas de insumos a numérico...", "🛠️")
for col in columnas_insumos_camel:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

log(f"Cargando crudo a BigQuery → {TABLA_BIGQUERY} (replace)...", "☁️")
t0 = time.time()
try:
    pandas_gbq.to_gbq(
        df,
        destination_table=f"{dataset_id_cirugias}.{table_name_cirugias}",
        project_id=project_id_product,
        if_exists='replace'
    )
    log(f"Carga completada en {time.time() - t0:.2f}s", "✅")
    total_registros = len(df)
except Exception as er:
    log(f"Error al insertar en BigQuery: {er}", "✗")
    salir_con_error(t_inicio)

print("-" * 60)
log(f"Proceso finalizado en {time.time() - t_inicio:.2f}s totales", "🏁")
print("=" * 60)
print("Save successfully")
print(f"{total_registros},{TABLA_BIGQUERY},{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")