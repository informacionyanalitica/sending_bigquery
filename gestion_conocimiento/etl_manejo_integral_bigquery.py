import pandas as pd
import sys
import os
from datetime import datetime

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq



COLUMNS_REQUIRED = ['fecha','fecha_realizo_auditoria','mes','ano','examen_monitorear','tipo_examen','sede','rol_profesional','nombre_profesional',
 'cedula','historia_clinica','condicion_salud','dx','percentil_riesgo','antecedentes_personales_gestion_de_caso','interrogatorio_medico_completo',
 'adherencia_tratamiento_medicamentoso_actual','examen_fisico_completo_adecuado','ayudas_dx_acorde_condicion_salud',
 'recomendaciones_entregadas_acorde_guias_protocolos','maneja_paciente_manera_integral','condicion_salud_manejo_segun_guia',
 'paciente_reconsultar_urgencias_hospitalizacion','remision_o_CCE_alguna_especializacion','gestion_paraclinicos_solicitados',
 'referencia_contrareferencia_adecuada','analisis_plan','pertinencia','nota','observaciones','datos_completos']


# DETALLE DATASET BIGQUERY
project_id_product = 'ia-bigquery-397516'
dataset_id_gestion_conocimiento = 'gestion_conocimiento'
table_name_auditores = 'manejo_integral_pacientes'


TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_gestion_conocimiento}.{table_name_auditores}'

# DETALLE DE GOOGLE SHEET
ID_SHEET = '10o322VBkc8Lf_v8z7U6wxqsCUihhUPeOaHm_9wDlmW8'
LIST_NAME_SHEET = ['Dr Edwin','Dr Juan David','Jefe Camila','Dr Efrain']

# Date
def convert_date(df):
    # Convert columns date
    df.fecha = pd.to_datetime(df.fecha, errors='coerce')
    df.fecha_realizo_auditoria = pd.to_datetime(df.fecha_realizo_auditoria, errors='coerce')
    return df

# Number
def convert_number(df):
    try:
        # Convert year
        df.ano.fillna(0, inplace=True)
        df.ano = df.ano.astype(int)
        # Clean empty data and signs
        df.nota.fillna('0', inplace=True)  
        df.nota = [val.replace('%','') for val in df.nota]
        df.nota.replace('','0',inplace=True)
        df.nota = df.nota.astype(float)
        df.nota = [(val/100) for val in df.nota]
        return df
    except Exception as err:
        print(err)

def get_columns_rows(df):
    try:
        #df = df[pd.to_datetime(df['Fecha (dd/mm/año)']) == FECHA_CARGUE]
        df.drop(['Envío de Correo'], axis=1, inplace=True)
        df.columns = COLUMNS_REQUIRED
        return df
    except Exception as err:
        print(err)

def drop_rows_empty(df):
    try:
        list_index_drop = df[(df.fecha == '') & (df.examen_monitorear == '')].index[0]
        df = df.iloc[:list_index_drop]
        return df
    except Exception as err:
        print(err)


def validate_load(df_load_log,df_not_duplicate):
    total_cargues = df_load_log.totalCargues[0]
    if total_cargues==0:
        try:
            # Load bigquery
            loadbq.load_data_bigquery(df_not_duplicate,TABLA_BIGQUERY,'WRITE_TRUNCATE')
        except Exception as err:
            print(err)

def execution_load():
    try:
        df_auditores = pd.DataFrame()
        for name in LIST_NAME_SHEET:
            df_auditor = func_process.get_google_sheet(ID_SHEET, name)
            df_auditor = get_columns_rows(df_auditor)
            df_auditor_clean = drop_rows_empty(df_auditor)
            df_auditor_transform = convert_date(df_auditor_clean)
            df_auditor_transform = convert_number(df_auditor_clean)
            df_auditor_transform['auditor'] = name
            df_auditores = pd.concat([df_auditores,df_auditor_transform])
        # VALIDATE DATA
        validate_loads_logs =  loadbq.validate_loads_weekly(TABLA_BIGQUERY)
        validate_load(validate_loads_logs,df_auditores)
    except Exception as err:
        print(err)

execution_load()