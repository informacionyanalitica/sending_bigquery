import sys,os
import pandas as pd
from datetime import datetime,timedelta

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


fecha_capita = func_process.pd.to_datetime(datetime.now() - timedelta(days=15)).strftime('%Y-%m-15')
fecha = func_process.pd.to_datetime(fecha_capita).strftime('%Y-%m-01')
mes = func_process.pd.to_datetime(fecha_capita).strftime('%B').lower()

fecha_last_year = f"{(pd.to_datetime(fecha_capita).year-1)}-{pd.to_datetime(fecha_capita).strftime('%m-%d')}"
fecha_i = pd.to_datetime(fecha_capita).strftime('%Y-%m-%d')
fecha_f = f"{pd.to_datetime(fecha_capita).strftime('%Y-%m')}-{pd.to_datetime(fecha_capita).days_in_month}"

sql_poblacion_mujeres_14_49 = """
                                SELECT *
                                FROM `ia-bigquery-397516.pacientes.capita` as c 
                                WHERE (c.sexo = 'F') AND (c.edad BETWEEN 14 AND 49) ORDER BY c.edad;
                            """
sql_rips_salud_mental = """
                            SELECT 
                                hora_fecha, 
                                orden, 
                                ips, 
                                identificacion_pac, 
                                primer_nombre_pac, 
                                primer_apellido_pac, 
                                sexo, 
                                edad_anos, 
                                identificacion_med, 
                                nombres_med, 
                                codigo_sura, 
                                codigo_cups, 
                                nombre_prestacion, 
                                dx_principal, 
                                nombre_dx_principal, 
                                nombre_tipo_dx 
                            FROM rips WHERE (sexo = 'F') AND 
                                (dx_principal IN ('G400', 'G400', 'G401', 'G402', 'G403', 'G404', 'G405', 'G406', 'G409'));
                        """
sql_rips_planificacion_familiar = f"""
                                   SELECT 
                                        hora_fecha, 
                                        orden, 
                                        ips, 
                                        identificacion_pac, 
                                        primer_nombre_pac, 
                                        primer_apellido_pac, 
                                        sexo, 
                                        edad_anos, 
                                        identificacion_med, 
                                        nombres_med, codigo_sura, 
                                        codigo_cups, 
                                        nombre_prestacion, 
                                        dx_principal, 
                                        nombre_dx_principal, 
                                        nombre_tipo_dx 
                                    FROM rips  AS r
                                    LEFT join  reportes.empleados_2019 AS e on e.identificacion = identificacion_med 
                                    WHERE sexo = 'F' AND hora_fecha > '{fecha_last_year}' 
                                        AND e.cargo = 'ENFERMERA ASISTENCIAL';"""

sql_inactivos_planificacion_familiar = """  SELECT 
                                                * 
                                            FROM inactivosPlanificacionFamiliarView 
                                            WHERE motivo != 'Vasectomia' ORDER BY fecha_gestion DESC;"""

sql_gestion_planificacion_familiar = f"""   SELECT 
                                                identificacion_usuario, 
                                                fecha_gestion 
                                            FROM gestionPlanificacionFamiliarView 
                                            WHERE fecha_gestion BETWEEN '{fecha_i}' AND '{fecha_f}' ORDER BY fecha_gestion DESC;"""

# Parametros bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_pacientes = 'pacientes'

# Planificacion familiar
table_name_planificacion_familiar = 'planificacion_familiar'
table_maridb_planificacion_familiar = 'planificacion_familiar'
TABLA_BIGQUERY_PLANIFICACION_FAMILIAR = f'{project_id_product}.{dataset_id_pacientes}.{table_name_planificacion_familiar}'

# Rips
dataset_id_rips = 'rips'
table_name_rips_auditoria = 'rips_auditoria_poblacion_2'
TABLA_BIGQUERY_RIPS = f'{project_id_product}.{dataset_id_rips}.{table_name_rips_auditoria}'

# Capita
dataset_id_pacientes = 'pacientes'
table_name_capita = 'capita'
TABLA_BIGQUERY_PACIENTES = f'{project_id_product}.{dataset_id_pacientes}.{table_name_capita}'

#Creamos funcion para agregar el NOMBRE_IPS 
def n_ips(codigo_ips):
    if codigo_ips == '35':
        return 'CENTRO'
    elif codigo_ips == '1013':
        return 'CALASANZ'
    elif codigo_ips == '2136':
        return 'NORTE'
    elif codigo_ips == '2715':
        return 'PAC'
    elif codigo_ips == '115393':
        return 'AVENIDA ORIENTAL'
    elif codigo_ips ==  '134189':
        return 'ARGENTINA'

# El ultimo registro 
def control_planificacion_familiar(id):
    if (len (rips_planificacion_familiar[rips_planificacion_familiar['identificacion_pac'] == id]) > 0):
        return rips_planificacion_familiar[rips_planificacion_familiar['identificacion_pac'] == id].hora_fecha.max()

def validate_load(df_validate_load,df_validate_rips,df_load,tabla_bigquery,table_mariadb):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        total_cargue_rips = df_validate_rips.totalCargues[0]
        if  total_cargue == 0 and total_cargue_rips>0:
            # Cargar mariadb
            func_process.save_df_server(df_load, table_mariadb, 'analitica')
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,tabla_bigquery)
    except ValueError as err:
        print(err)


# Read data
poblacion_mujeres_14_49 = loadbq.read_data_bigquery(sql_poblacion_mujeres_14_49,TABLA_BIGQUERY_PACIENTES)
poblacion_mujeres_14_49['sede_atencion'] = poblacion_mujeres_14_49['sede_atencion'].astype('str')

rips_salud_mental = func_process.load_df_server(sql_rips_salud_mental, 'reportes')

rips_planificacion_familiar = func_process.load_df_server(sql_rips_planificacion_familiar, 'reportes')
rips_planificacion_familiar['hora_fecha'] = func_process.pd.to_datetime(rips_planificacion_familiar['hora_fecha'])

inactivos_planificacion_familiar = func_process.load_df_server(sql_inactivos_planificacion_familiar, 'reportes')

gestion_planificacion_familiar = func_process.load_df_server(sql_gestion_planificacion_familiar, 'reportes')
gestion_planificacion_familiar['fecha_gestion'] = func_process.pd.to_datetime(gestion_planificacion_familiar['fecha_gestion'])

# -- Transform   
poblacion_mujeres_14_49.drop(columns=['tipo_identificacion', 'segundo_apellido', 'segundo_nombre', 'unidad_edad', 'identificacion_medico', 'mes_cargue'], axis=1, inplace= True)
poblacion_mujeres_14_49['nombre_sede'] = poblacion_mujeres_14_49.sede_atencion.apply(n_ips)
poblacion_mujeres_14_49['salud_mental'] = poblacion_mujeres_14_49['identificacion_paciente'].mask(poblacion_mujeres_14_49['identificacion_paciente'].isin(rips_salud_mental['identificacion_pac']), other= 1)
poblacion_mujeres_14_49.salud_mental.mask(poblacion_mujeres_14_49.salud_mental != 1, other= 0, inplace = True)
    
poblacion_mujeres_14_49['planificacion_familiar'] = poblacion_mujeres_14_49['identificacion_paciente'].apply(control_planificacion_familiar)
poblacion_mujeres_14_49['inactiva'] = poblacion_mujeres_14_49['identificacion_paciente'].mask(poblacion_mujeres_14_49['identificacion_paciente'].isin(inactivos_planificacion_familiar['identificacion_usuario']), other= 1)
poblacion_mujeres_14_49.inactiva.mask(poblacion_mujeres_14_49.inactiva != 1, other= 0, inplace = True)

poblacion_mujeres_14_49['gestionada'] = poblacion_mujeres_14_49['identificacion_paciente'].mask(poblacion_mujeres_14_49['identificacion_paciente'].isin(gestion_planificacion_familiar['identificacion_usuario']), other= 1)
poblacion_mujeres_14_49.gestionada.mask(poblacion_mujeres_14_49.gestionada != 1, other= 0, inplace = True)

poblacion_mujeres_14_49['sede_atencion'] = poblacion_mujeres_14_49['sede_atencion'].astype('str')
poblacion_mujeres_14_49[['edad', 'sw_cronicos', 'sw_rcv', 'sw_hipertension', 'sw_diabetes', 'sw_proteccion_renal', 'sw_dislipidemia', 'sw_enfer_autoinmune', 'sw_enfer_coagulacion', 'sw_asma', 'sw_epoc', 'sw_cancer_cervix', 'sw_cancer_mama', 'sw_vih', 'sw_cpr', 'sw_rce', 'sw_fragil_canguro', 'sw_oxigeno_dependiente', 'sw_sospecha_abuso_sexual', 'sw_tb', 'sw_domiciliaria']] = poblacion_mujeres_14_49[['edad', 'sw_cronicos', 'sw_rcv', 'sw_hipertension', 'sw_diabetes', 'sw_proteccion_renal', 'sw_dislipidemia', 'sw_enfer_autoinmune', 'sw_enfer_coagulacion', 'sw_asma', 'sw_epoc', 'sw_cancer_cervix', 'sw_cancer_mama', 'sw_vih', 'sw_cpr', 'sw_rce', 'sw_fragil_canguro', 'sw_oxigeno_dependiente', 'sw_sospecha_abuso_sexual', 'sw_tb', 'sw_domiciliaria']].astype('int64')
poblacion_mujeres_14_49.drop(poblacion_mujeres_14_49.loc[
    (poblacion_mujeres_14_49['edad'] > 18) &
    (poblacion_mujeres_14_49['sw_cronicos'] == 0) &
    (poblacion_mujeres_14_49['sw_hipertension'] == 0) &
    (poblacion_mujeres_14_49['sw_diabetes'] == 0) &
    (poblacion_mujeres_14_49['sw_proteccion_renal'] == 0) &
    (poblacion_mujeres_14_49['sw_enfer_autoinmune'] == 0) &
    (poblacion_mujeres_14_49['sw_enfer_coagulacion'] == 0) &
    (poblacion_mujeres_14_49['sw_epoc'] == 0) &
    (poblacion_mujeres_14_49['sw_cancer_cervix'] == 0) &
    (poblacion_mujeres_14_49['sw_cancer_mama'] == 0) &
    (poblacion_mujeres_14_49['sw_rce'] == 0) &
    (poblacion_mujeres_14_49['sw_sospecha_abuso_sexual'] == 0) &
    (poblacion_mujeres_14_49['salud_mental'] == 0)
].index, inplace=True)


# VALIDATE LOAD
validate_loads_logs_planificacion_familiar =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_PLANIFICACION_FAMILIAR)
validate_loads_logs_rips =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_RIPS)

# Load
validate_load(validate_loads_logs_planificacion_familiar,
              validate_loads_logs_rips,
              poblacion_mujeres_14_49,
              TABLA_BIGQUERY_PLANIFICACION_FAMILIAR,
              table_maridb_planificacion_familiar)

