import pandas as pd
import sys,os
from datetime import datetime,timedelta
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1, path)
import func_process
import load_bigquery as loadbq
from convert_columns_dataframe import convertColumnDataFrame

today = datetime.now() - timedelta(days=1)
date_load = today.date()

# Instancias clase convertColumnDataframe
convert_columns = convertColumnDataFrame()

# Datos Bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_autorizaciones = 'autorizaciones'
table_name_autorizaciones = 'autorizaciones_asesorias_virtuales'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_autorizaciones}.{table_name_autorizaciones}'

SQL_AUTORIZACIONES_VIEW = f"""
    SELECT *
    FROM analitica.autorizacionesAsesoriasVirtualesTodo AS a
    WHERE DATE(a.fechaSolicitud) = '{date_load}'
"""

# Columnas STRING (sin tipoIdentificacionPaciente)
LIST_COLUMNS_STRING = [
    'nroOrdenReemplazo','codigoPlan','tipoIdentificacionSucursalEmite',
    'numeroIdentificacionSucursalEmite','codigoSucursalEmite','nombreSucursalEmite',
    'tipoIdentificacionSucursalImprimio','numeroIdentificacionSucursalImprimio',
    'codigoSucursalImprimio','nombreSucursalImprimio','tipoIdentificacionUsuarioGenera',
    'numeroIdentificacionUsuarioGenera','loginUsuarioGenera','numeroIdentificacionUsuarioImprime',
    'loginUsuarioImprime','tipoIdentificacionProveedorAtencion','numeroIdentificacionProveedorAtencion',
    'nombreProveedorAtencion','tipoIdentificacionPrestador','numeroIdentificacionPrestador',
    'codigoSucursalPrestador','nombreSucursalPrestador','tipoIdentificacionRemitente',
    'numeroIdentificacionRemitente','nombreRemitente','codigoDiagnostico','numeroAutorizacion',
    'numeroIdentificacionPaciente','codigoSuracupsPrestacion','nombreManualTarifarioPrestacion',
    'id_codigoOrigenServicio','codigoorigenservicio','id_codigoLugarRecaudo','lugarRecaudo',
    'id_codigoTipoCobro','tipoCobro','id_codigoTipoContrato','tipoContrato','id_estadoOrden',
    'estadoOrden','id_tipoOrden','tipoOrden','debeRecaudar','cargoGestal','sedeGestal','rolMedico'
]

LIST_COLUMNS_DATE = ['fechaSolicitud','fechaImpresion','fechaVencimientoAutorizacion']
LIST_COLUMNS_INT = ['cantidadPrestacion']
LIST_COLUMNS_FLOAT = ['valorCobrar','porcentajeCopago']

def validate_load(df_validate_load, df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if total_cargue == 0:
            loadbq.load_data_bigquery(df_load, TABLA_BIGQUERY)
            print(f"✓ Cargados {len(df_load)} registros para {date_load}")
        else:
            print(f"⚠ Ya existe carga para {date_load}")
    except Exception as err:
        print(f"✗ Error: {err}")

df_autorizaciones_asesorias = func_process.load_df_server(SQL_AUTORIZACIONES_VIEW,'analitica')

if df_autorizaciones_asesorias is not None and not df_autorizaciones_asesorias.empty:
    # Filtrar columnas presentes
    cols_string = [c for c in LIST_COLUMNS_STRING if c in df_autorizaciones_asesorias.columns]
    cols_date = [c for c in LIST_COLUMNS_DATE if c in df_autorizaciones_asesorias.columns]
    cols_int = [c for c in LIST_COLUMNS_INT if c in df_autorizaciones_asesorias.columns]
    cols_float = [c for c in LIST_COLUMNS_FLOAT if c in df_autorizaciones_asesorias.columns]

    # Convertir columnas
    df_autorizaciones_asesorias = convert_columns.convert_columns_string(df_autorizaciones_asesorias, cols_string)
    df_autorizaciones_asesorias = convert_columns.convert_columns_integer(df_autorizaciones_asesorias, cols_int)
    df_autorizaciones_asesorias = convert_columns.convert_columns_date(df_autorizaciones_asesorias, cols_date)
    df_autorizaciones_asesorias = convert_columns.convert_columns_float(df_autorizaciones_asesorias, cols_float)

    # Guardar datos
    df_validate_loads_logs = loadbq.validate_loads_daily(TABLA_BIGQUERY)
    validate_load(df_validate_loads_logs, df_autorizaciones_asesorias)
else:
    print(f"⚠ Sin registros para {date_load}")