import pandas as pd
import sys
import os
from datetime import datetime,timedelta
from unicodedata import decimal
import extract_alergenos
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq
import cumplimientos_pyg as pyg
import locale

locale.setlocale(locale.LC_TIME, "es_ES.utf8") 

# Execution
fecha = (datetime.now() - timedelta(days=1))
capita_date = func_process.pd.to_datetime(fecha.strftime('%Y-%m')+'-15')
year = capita_date.strftime('%Y')
month_word = capita_date.strftime('%B').capitalize()
month_number = (capita_date.month)
last_day = str(capita_date.days_in_month)

PATH_DRIVE = os.environ.get("PATH_DRIVE")
PATH_TARIFAS = f"{PATH_DRIVE}/tarifas laboratorio/"
PATH_LABORATORIO = f"{PATH_DRIVE}/BASES DE DATOS/{year}/{month_number}. {month_word}/LABORATORIO"
nameFile = f'Estadistica {fecha.date()}.xlsx'
hoja = 'Hoja'


date_f = f"{year}-{month_number:02}-{last_day}"
date_i = f"{year}-{month_number:02}-01"

# Name project BQ
project_id_product = 'ia-bigquery-397516'

# DATASET AYUDAS DIAGNOSTICAS
dataset_id_ayudas_diagnosticas = 'ayudas_diagnosticas'
dataset_id_pacientes = 'pacientes'

# TABLAS
table_name_valores_laboratorio = 'valores_laboratorio'
table_name_laboratorio_clinico = 'laboratorio_clinico_partition'
table_name_capita_poblaciones = 'capitas_poblaciones'

# ID BIGQUERY
TABLA_BIGQUERY_VALORES = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_valores_laboratorio}'
TABLA_BIGQUERY_LABORATORIO_CLINICO = f'{project_id_product}.{dataset_id_ayudas_diagnosticas}.{table_name_laboratorio_clinico}'
TABLA_BIGQUERY_CAPITA_POBLACIONES = f'{project_id_product}.{dataset_id_pacientes}.{table_name_capita_poblaciones}'
# TABLA MARIADB
TABLA_MARIADB = 'laboratorio_bd'

COLUMNS_DELETE = ['Email', 'Diagnóstico Permanente','No Veri.', 'Rechazo','Nombre Usuario Ingreso', 'Comentario Orden','Apellido Usuario Ingreso',
       'Fecha Toma', 'Usuario Toma', 'Nombre Usuario Toma','Apellido Usuario Toma','Fecha Verificación', 'Usuario Verificación',
       'Nombre Usuario Verificación', 'Apellido Usuario Verificación','Fecha Resultado', 'Usuario Resultado', 'Nombre Usuario Resultado',
       'Apellido Usuario Resultado','Usuario Validación','Nombre Usuario Validación', 'Apellido Usuario Validación','Fecha Impresión', 
       'Usuario Impresión', 'Nombre Usuario Impresión','Apellido Usuario Impresión','tarifas']

COLUMNS_REQUIRED = ['ORDEN', 'HISTORIA', 'APELLIDO', 'SEGUNDO_APELLIDO', 'NOMBRE', 'SEXO',
       'FECHA_NACIMIENTO', 'EDAD', 'CODIGO', 'PRUEBA', 'RESULTADO', 'UNIDAD',
       'FECHA', 'EMPRESA', 'SEDE', 'ORDEN_SEDE', 'TELEFONO','IMPRESION_DIAGNOSTICA',
       'codigoDiagnostico', 'VALORES', 'SEDE_DE_LA_REMISION', 'C_MEDICO','MEDICO',
       'SEDE_MEDICO','SECCION','TARIFA_COSTO', 'POBLACION_SEDE', 'rol', 'rol_2',
       'estado_empleado', 'cumplimiento_pyg', 'cumplimiento_pyg_coopsana','cargo_gestal',
       'sw_traslado_dm','cumplimiento_pyg_sedes_totales','menosCuatroMeses','poblacionCoopsana',
       'condicionSalud','atendido','percentilRiesgo' ]

sql_autorizaciones = f"""
                        SELECT 
                            numeroAutorizacion AS 'ORDEN_SEDE', 
                            codigoDiagnostico, 
                            numeroidentificacionPaciente AS 'Historia', 
                            codigoSuracupsPrestacion AS CODIGO, 
                            fechaimpresion,
                            numeroidentificacionremitente,
                            nombreremitente
                        FROM autorizaciones_view 
                        WHERE year(fechaimpresion) = '{year}'
                        ORDER BY fechaimpresion;
                    """
SQL_CAPITA_POBLACIONES = f"""
                 SELECT 
                FECHA_CAPITA, 
            NOMBRE_IPS AS SEDE, 
            POBLACION_TOTAL
                FROM `ia-bigquery-397516.pacientes.capitas_poblaciones`
                WHERE extract(year from FECHA_CAPITA) = {year}   
                AND FECHA_CAPITA = (SELECT  max(FECHA_CAPITA) FROM `ia-bigquery-397516.pacientes.capitas_poblaciones`)
    """
    
sql_empleados_2019 =  """
                        SELECT 
                            identificacion,
                            concat(IFNULL(hv_nombres,''),' ',IFNULL(hv_apellidos,'')) AS nombre_medico,
                            estado_activo, 
                            sede as 'SEDE_MEDICO',
                            cargo as cargo_gestal,
                            fecha_ingreso,
                            ifnull(TIMESTAMPDIFF(month,fecha_ingreso,CURDATE()),0) AS tiempoMesIngreso
                        FROM empleados_2019;
                    """                
# SINIESTRALIDAD 
SQL_SINIESTRALIDAD = """SELECT DISTINCT cc.identificacion_paciente,cc.percentilRiesgo
            from analitica.sinestralidadUsuarios AS cc"""

# ACTUALIZAR ATENDIDO
SQL_RIPS = f""" SELECT DISTINCT c.identificacion_pac,c.identificacion_med
            FROM reportes.rips AS c
            WHERE YEAR(c.hora_fecha) = {year}
            and MONTH(c.hora_fecha) = {month_number}
            """

SQL_MAESTRA_CIEDIEZ= """SELECT * FROM analitica.maestraCiediez"""

SQL_MAESTRA_CONDICION_SALUD = """SELECT codigoDiagnostico,condicionSalud 
                                FROM analitica.maestraCondicioSalud
                                GROUP BY codigoDiagnostico """

SQL_VALORES_LABORATORIO = """ SELECT t.tarifas,max(t.valores) as VALORES
                                FROM `ia-bigquery-397516.ayudas_diagnosticas.valores_laboratorio`  as t
                                group by t.tarifas"""

def update_valores_alergenos(df_laboratorio,df_alergenos):
    try:
        df_laboratorio = df_laboratorio.merge(df_alergenos,left_on=['ORDEN','CODIGO'],right_on=['numero_orden','codigo'], how='left')
        df_laboratorio.loc[df_laboratorio['valor_total'].notnull(),'VALORES'] = df_laboratorio.valor_total
        df_laboratorio.drop(['numero_orden','codigo','valor_total'], axis=1, inplace=True)
        return df_laboratorio
    except ValueError as err:
        return 
    
def validate_load(df_validate_load,df_load,tabla_bigquery,table_mariadb):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar mariadb
            func_process.save_df_server(df_load, table_mariadb, 'analitica')
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,tabla_bigquery)
    except ValueError as err:
        print(err)

# CONVERT STRING
def convert_column_string(df):
    df.ORDEN = df.ORDEN.astype(str)
    df.menosCuatroMeses = df.menosCuatroMeses.astype(str)
    LIST_COLUMNS_STRING = df.select_dtypes(include=['object']).columns
    for col in LIST_COLUMNS_STRING:
        df[col] = df[col].astype(str)
    return df

# CONVERT INT
def convert_column_int(df):
    df.EDAD = df.EDAD.str.replace(r'\D', '', regex=True)
    df.EDAD = df.EDAD.astype(int)
    return df
# CONVERT FECHA
def convert_column_fecha(df):
    df.FECHA_NACIMIENTO = pd.to_datetime(df.FECHA_NACIMIENTO, errors='coerce')
    return df

# CONVERT FLOAT
def convert_column_float(df):
    df.poblacionCoopsana = df.poblacionCoopsana.astype(float)
    df.POBLACION_SEDE = df.POBLACION_SEDE.astype(float)
    df.sw_traslado_dm = df.sw_traslado_dm.astype(float)
    df.TARIFA_COSTO = df.TARIFA_COSTO.astype(float)
    return df


df_autorizaciones = func_process.load_df_server(sql_autorizaciones, 'reportes')
df_rips = func_process.load_df_server(SQL_RIPS,'reportes')
df_siniestralidad = func_process.load_df_server(SQL_SINIESTRALIDAD,'reportes')
df_capita_poblaciones = loadbq.read_data_bigquery(SQL_CAPITA_POBLACIONES, TABLA_BIGQUERY_CAPITA_POBLACIONES)
empleados = func_process.load_df_server(sql_empleados_2019, 'reportes')
df_maestra_condicion_salud = func_process.load_df_server(SQL_MAESTRA_CONDICION_SALUD,'analitica')
df_maestra_ciediez = func_process.load_df_server(SQL_MAESTRA_CIEDIEZ,'analitica')
df_estadistica = func_process.pd.read_excel(f"{PATH_LABORATORIO}/{nameFile}", 
                                    sheet_name=hoja)
tarifa_costo = func_process.pd.read_csv(f'{PATH_TARIFAS}tarifa_costo_todo_2.csv', sep=';')
valores = loadbq.read_data_bigquery(SQL_VALORES_LABORATORIO, TABLA_BIGQUERY_VALORES)

valores.TARIFAS = valores.tarifas.astype(str)
df_estadistica.Codigo = df_estadistica.Codigo.astype(str)
bd = df_estadistica.merge(valores, how='left',left_on='Codigo',right_on='tarifas')
bd.VALORES.fillna(0,inplace=True)
bd.drop(COLUMNS_DELETE,axis=1, inplace=True)
bd.rename({'ORDEN': 'ORDEN_SEDE','Codigo':'CODIGO','Fecha Ingreso':'Fecha'},
            axis=1,
            inplace= True)
bd.dropna(how= 'all',
            inplace= True)

format_ingreso = "%d/%m/%Y %H:%M:%S"
format_birth = "%d/%m/%Y"

bd.VALORES = bd.VALORES.astype(float)
bd['Fecha'] = func_process.pd.to_datetime(bd['Fecha'],format=format_ingreso,errors='coerce')
bd['Fecha Nacimiento'] = func_process.pd.to_datetime(bd['Fecha Nacimiento'], format='%d/%m/%Y',errors='coerce')
bd[['Historia','CODIGO','ORDEN_SEDE']] = bd[['Historia','CODIGO','ORDEN_SEDE']].astype('str')
df_autorizaciones[['Historia', 'CODIGO', 'ORDEN_SEDE', 'codigoDiagnostico','numeroidentificacionremitente','nombreremitente']] = df_autorizaciones[['Historia', 'CODIGO', 'ORDEN_SEDE', 'codigoDiagnostico','numeroidentificacionremitente','nombreremitente']].astype(str)
df_autorizaciones = df_autorizaciones[['Historia', 'CODIGO', 'ORDEN_SEDE', 'codigoDiagnostico','numeroidentificacionremitente','nombreremitente']]
empleados['identificacion'] = empleados['identificacion'].astype('str')

laboratorio_bd = bd.merge(df_autorizaciones,
                                        on=['Historia', 'CODIGO','ORDEN_SEDE'], 
                                        how='left')

# RELLENAMOS LOS NULOS
laboratorio_bd['codigoDiagnostico'].fillna(laboratorio_bd['IMPRESION DIAGNOSTICA'], 
                                            inplace = True)
laboratorio_bd['numeroidentificacionremitente'].fillna(laboratorio_bd['C. MEDICO'], 
                                            inplace = True)
laboratorio_bd['nombreremitente'].fillna(laboratorio_bd['MEDICO'], 
                                            inplace = True)
laboratorio_bd['Segundo Apellido'].fillna('', 
                                            inplace= True)
laboratorio_bd.fillna('NULL', 
                        inplace= True)
# ELIMINAR COLUMNAS 
laboratorio_bd.drop(['C. MEDICO','MEDICO'], axis=1, inplace=True)
laboratorio_bd.rename({'numeroidentificacionremitente':'C. MEDICO','nombreremitente':'MEDICO'}, axis=1,inplace=True)


# CARGAMOS ARCHIVO TARIFA COSTO
tarifa_costo['CODIGO'] = tarifa_costo['CODIGO'].astype('str')
laboratorio_bd_tarifa_costo = laboratorio_bd.merge(tarifa_costo[['CODIGO', 'SECCION', 'TARIFA COSTO']], 
                                                    on= 'CODIGO', 
                                                    how='left')
# AJUSTAMOS VALORES DE ALGUNOS EXAMENES QUE NO APARECEN EN EL ARCHIVO DE TARIFA COSTO
cod_906812 = laboratorio_bd_tarifa_costo.loc[laboratorio_bd_tarifa_costo['CODIGO'] =='906812']
cod_906812['TARIFA COSTO'].fillna(cod_906812['VALORES'], inplace= True)
cod_906812['SECCION'].fillna('EXAMENES REMISIONADO', inplace= True)
laboratorio_bd_tarifa_costo.drop(laboratorio_bd_tarifa_costo.loc[laboratorio_bd_tarifa_costo['CODIGO'] =='906812'].index, inplace=True)

cod_906834 = laboratorio_bd_tarifa_costo.loc[laboratorio_bd_tarifa_costo['CODIGO'] =='906834']
cod_906834['TARIFA COSTO'].fillna(cod_906834['VALORES'], inplace= True)
cod_906834['SECCION'].fillna('EXAMENES REMISIONADO', inplace= True)
laboratorio_bd_tarifa_costo.drop(laboratorio_bd_tarifa_costo.loc[laboratorio_bd_tarifa_costo['CODIGO'] =='906834'].index, inplace=True)

cod_908412 = laboratorio_bd_tarifa_costo.loc[laboratorio_bd_tarifa_costo['CODIGO'] =='908412']
cod_908412['TARIFA COSTO'].fillna(cod_908412['VALORES'], inplace= True)
cod_908412['SECCION'].fillna('EXAMENES REMISIONADO', inplace= True)
laboratorio_bd_tarifa_costo.drop(laboratorio_bd_tarifa_costo.loc[laboratorio_bd_tarifa_costo['CODIGO'] =='908412'].index, inplace=True)

laboratorio_bd_tarifa_costo_2 = func_process.pd.concat([laboratorio_bd_tarifa_costo, cod_906812, cod_906834, cod_908412], 
                                                        axis=0)
laboratorio_bd_tarifa_costo_2['SECCION'].fillna('N.A', 
                                                inplace= True)
laboratorio_bd_tarifa_costo_2['TARIFA COSTO'].replace(to_replace='NULL', 
                                                        value=0, 
                                                        inplace=True)
laboratorio_bd_tarifa_costo_2['TARIFA COSTO'].fillna(0, 
                                                    inplace= True)
laboratorio_bd_tarifa_costo_2['TARIFA COSTO'] = laboratorio_bd_tarifa_costo_2['TARIFA COSTO'].astype('int64')
laboratorio_bd_tarifa_costo_2['VALORES'].replace(to_replace='NULL', 
                                                value=0, 
                                                inplace=True)
laboratorio_bd_tarifa_costo_2['VALORES'] = laboratorio_bd_tarifa_costo_2['VALORES'].astype('int64')
laboratorio_bd_tarifa_costo_2.SEDE.replace(['aVENIDA ORIENTAL', 'AVENIDA ORIENTAL '], 'AVENIDA ORIENTAL', inplace= True)
laboratorio_bd_tarifa_costo_2.SEDE.replace('CENTRO ', 'CENTRO', inplace= True)
laboratorio_bd_tarifa_costo_2.SEDE.replace('NORTE ', 'NORTE', inplace= True)
laboratorio_bd_tarifa_costo_2.SEDE.replace('PRINCIPAL ', 'PRINCIPAL', inplace= True)
laboratorio_bd_tarifa_costo_2.SEDE.replace(['PLAN COMPLEMENTARIO', 'PLAN COMPLEMENTARIO SURA', 'PAC-SURAMERICANA'], 'PAC', inplace= True)
laboratorio_bd_tarifa_costo_2 = laboratorio_bd_tarifa_costo_2.merge(df_capita_poblaciones, 
                                                                    how='left', 
                                                                    on='SEDE')


laboratorio_bd_tarifa_costo_2.drop(columns=['FECHA_CAPITA'], axis= 1, inplace= True)
laboratorio_bd_tarifa_costo_2['POBLACION_TOTAL'].fillna(0, inplace= True)
laboratorio_bd_tarifa_costo_2['POBLACION_TOTAL'] = laboratorio_bd_tarifa_costo_2['POBLACION_TOTAL'].astype('int64')
laboratorio_bd_tarifa_costo_2['Fecha Nacimiento'] = laboratorio_bd_tarifa_costo_2['Fecha Nacimiento'].astype('str')
roles_sedes = func_process.get_roles_sedes(func_process.get_google_sheet)
roles_sedes.rename(columns={'Identificacion':'C. MEDICO'}, inplace=True)
medico_roles = roles_sedes[['C. MEDICO','Nombre','Cargo','Rol','Rol 2']]
medico_roles.drop_duplicates(subset='C. MEDICO',
                            keep='last',
                            inplace=True)
laboratorio_bd_tarifa_costo_2['C. MEDICO'] = laboratorio_bd_tarifa_costo_2['C. MEDICO'].astype('str')
medico_roles['C. MEDICO'] = medico_roles['C. MEDICO'].astype('str')


laboratorio_bd_tarifa_costo_2_rol= laboratorio_bd_tarifa_costo_2.merge(medico_roles[['C. MEDICO', 'Rol', 'Rol 2']], 
                                                                        on='C. MEDICO', 
                                                                        how= 'left')
laboratorio_bd_tarifa_costo_2_rol['Rol'].fillna('Otro', inplace=True) 
laboratorio_bd_tarifa_costo_2_rol['Rol 2'].fillna('Otro', inplace=True)
laboratorio_bd_tarifa_costo_2_rol.rename({'Orden':'ORDEN', 
                                        'Historia':'HISTORIA', 
                                        'Apellido':'APELLIDO', 
                                        'Segundo Apellido':'SEGUNDO_APELLIDO',
                                        'Nombre':'NOMBRE', 
                                        'Sexo':'SEXO',
                                        'Fecha Nacimiento':'FECHA_NACIMIENTO', 
                                        'Edad':'EDAD_2', 
                                        'Prueba':'PRUEBA', 
                                        'Resultado':'RESULTADO', 
                                        'Unidad':'UNIDAD', 
                                        'Fecha':'FECHA', 
                                        'IMPRESION DIAGNOSTICA':'IMPRESION_DIAGNOSTICA',
                                        'SEDE DESDE LA REMISION':'SEDE_DE_LA_REMISION', 
                                        'C. MEDICO':'C_MEDICO', 
                                        'TARIFA COSTO':'TARIFA_COSTO', 
                                        'POBLACION_TOTAL':'POBLACION_SEDE', 
                                        'Rol':'rol', 
                                        'Rol 2':'rol_2'}, 
                                        axis=1, 
                                        inplace= True )

laboratorio_bd_tarifa_costo_2_rol.insert(7, 'EDAD', laboratorio_bd_tarifa_costo_2_rol['EDAD_2'].apply(lambda x: x.replace('Años', '')))
laboratorio_bd_tarifa_costo_2_rol.drop(columns= 'EDAD_2', axis=1, inplace= True)
# Definir estado de profesional
def state_emp(id_):
    if (id_ in list(empleados[empleados['estado_activo'] == '1'].identificacion)):
        return 'ACTIVO'
    elif (id_ in list(empleados[empleados['estado_activo'] == '2'].identificacion)):
        return 'INACTIVO'
    elif (id_ in list(empleados[empleados['estado_activo'] == '3'].identificacion)):
        return 'INACTIVO'
    elif (id_ in list(empleados[empleados['estado_activo'].isnull()].identificacion)):
        return 'INACTIVO'
    else:
        return 'EXTERNO'
    
cumplimiento_pyg_sedes = pyg.get_cumplimiento_pyg_sedes()
cumplimiento_pyg_coopsana = pyg.get_cumplimiento_pyg_coopsana(cumplimiento_pyg_sedes)


laboratorio_bd_tarifa_costo_2_rol['estado_empleado'] = laboratorio_bd_tarifa_costo_2_rol['C_MEDICO'].apply(state_emp)
laboratorio_bd_tarifa_costo_2_rol.rename({'EMPRESA':'SEDE_DE_LA_REMISION'}, axis=1, inplace=True)
laboratorio_bd_tarifa_costo_2_rol.rename({'EPS':'EMPRESA'}, axis=1, inplace=True)
laboratorio_bd_tarifa_costo_2_rol = laboratorio_bd_tarifa_costo_2_rol[ 
                         ['ORDEN', 'HISTORIA', 'APELLIDO', 'SEGUNDO_APELLIDO', 'NOMBRE', 'SEXO',
                         'FECHA_NACIMIENTO', 'EDAD', 'CODIGO', 'PRUEBA', 'RESULTADO', 'UNIDAD',
                         'FECHA', 'EMPRESA', 'SEDE', 'ORDEN_SEDE', 'TELEFONO', 'IMPRESION_DIAGNOSTICA',  
                         'codigoDiagnostico', 'VALORES', 'SEDE_DE_LA_REMISION', 'C_MEDICO', 'MEDICO',
                         'SECCION', 'TARIFA_COSTO', 'POBLACION_SEDE', 'rol', 'rol_2', 'estado_empleado']
                    ]
cumplimientos_pyg = cumplimiento_pyg_sedes.merge(cumplimiento_pyg_coopsana,
                                                how='inner',
                                                on='fechaProceso')
cumplimientos_pyg = cumplimientos_pyg[cumplimientos_pyg.fechaProceso==month_word][['cumplimiento_pyg','centroCosto','cumplimiento_pyg_coopsana']]
laboratorio_bd_tarifa_costo_2_rol = laboratorio_bd_tarifa_costo_2_rol.merge(cumplimientos_pyg,how='left',left_on='SEDE',right_on='centroCosto')
laboratorio_bd_tarifa_costo_2_rol = laboratorio_bd_tarifa_costo_2_rol.merge(empleados,how='left',
                                                                            left_on='C_MEDICO',
                                                                            right_on='identificacion')
laboratorio_bd_tarifa_costo_2_rol['SEDE_MEDICO'].fillna('EXTERNO',inplace= True)
laboratorio_bd_tarifa_costo_2_rol['cargo_gestal'].fillna('EXTERNO', inplace=True)
laboratorio_bd_tarifa_costo_2_rol['sw_traslado_dm'] = 0
laboratorio_bd_tarifa_costo_2_rol.drop(['centroCosto','identificacion','estado_activo'], axis=1,inplace=True)

# Actualizar totales para alergenos
totales_valores_alergenos = extract_alergenos.execute_total_alergenos(capita_date)
laboratorio_bd_tarifa_costo_2_rol = update_valores_alergenos(laboratorio_bd_tarifa_costo_2_rol,totales_valores_alergenos)

# Empleados con menos de cuatro meses
laboratorio_bd_tarifa_costo_2_rol['menosCuatroMeses'] = 2
laboratorio_bd_tarifa_costo_2_rol['tiempoMesIngreso'].fillna(0,inplace=True)
laboratorio_bd_tarifa_costo_2_rol.tiempoMesIngreso = laboratorio_bd_tarifa_costo_2_rol.tiempoMesIngreso.astype(int)
menor_igual_cuatro_meses = laboratorio_bd_tarifa_costo_2_rol[laboratorio_bd_tarifa_costo_2_rol.tiempoMesIngreso <=4].index
laboratorio_bd_tarifa_costo_2_rol.loc[menor_igual_cuatro_meses, 'menosCuatroMeses'] = 1
# Actualizar nombre dianostico desde maestra ciediez
df_laboratorio_ciediez = laboratorio_bd_tarifa_costo_2_rol.merge(df_maestra_ciediez[['codigo','nombre']], how='left', left_on='codigoDiagnostico', right_on='codigo')
df_laboratorio_ciediez.drop('codigo', axis=1, inplace=True)
df_laboratorio_ciediez['nombre'].fillna(df_laboratorio_ciediez['IMPRESION_DIAGNOSTICA'],inplace=True)
df_laboratorio_ciediez.drop('IMPRESION_DIAGNOSTICA', axis=1, inplace=True)
df_laboratorio_ciediez.rename({'nombre':'IMPRESION_DIAGNOSTICA'}, axis=1,inplace=True)
# Transformar SEXO
df_laboratorio_ciediez['SEXO'] = df_laboratorio_ciediez['SEXO'].replace({'F': 'Femenino', 'M': 'Masculino','I':'Indefinido'})
# Nombre medicos en mayuscula
df_laboratorio_ciediez['MEDICO'] = df_laboratorio_ciediez['MEDICO'].str.upper()
# Poblacion coopsana    
df_laboratorio_ciediez['poblacionCoopsana'] = df_capita_poblaciones[df_capita_poblaciones.SEDE == 'COOPSANA IPS' ]['POBLACION_TOTAL'].values[0]
# Atendidos mes capita
df_laboratorio_rips = df_laboratorio_ciediez.merge(df_rips, how='left', 
                                                    left_on=['HISTORIA','C_MEDICO'],
                                                    right_on=['identificacion_pac','identificacion_med'] )
df_laboratorio_rips['atendido'] = 'NO'
mask_atendidos = df_laboratorio_rips[~df_laboratorio_rips.identificacion_pac.isna()].index
df_laboratorio_rips.loc[mask_atendidos,'atendido'] = 'SI'
# Percentil Riesgo
df_laboratorio_siniestralidad = df_laboratorio_rips.merge(df_siniestralidad, how='left', left_on='HISTORIA', right_on='identificacion_paciente')
df_laboratorio_siniestralidad['percentilRiesgo'].fillna(0, inplace=True)
# Convertir tipos de datos
df_laboratorio_load =convert_column_string(df_laboratorio_siniestralidad)
df_laboratorio_load =convert_column_int(df_laboratorio_siniestralidad)
df_laboratorio_load =convert_column_fecha(df_laboratorio_siniestralidad)
df_laboratorio_load =convert_column_float(df_laboratorio_siniestralidad)
# Crear condicion de salud
df_laboratorio_condicion_salud = df_laboratorio_siniestralidad.merge(df_maestra_condicion_salud, how='left', on='codigoDiagnostico')
mask_sin_condicion_salud = df_laboratorio_condicion_salud[df_laboratorio_condicion_salud.condicionSalud.isna()].index
df_laboratorio_condicion_salud.loc[mask_sin_condicion_salud, 'condicionSalud'] = 'OTRAS PATOLOGIAS'
# Crear columna en cumplimiento_pygs 0 
df_laboratorio_condicion_salud['cumplimiento_pyg_sedes_totales'] = 0
df_laboratorio_condicion_salud['cumplimiento_pyg'].fillna(0, inplace=True)
df_laboratorio_condicion_salud['cumplimiento_pyg_coopsana'].fillna(0, inplace=True)

# Seleccionar columnas necesarias
df_laboratorio_load = df_laboratorio_condicion_salud[COLUMNS_REQUIRED]
df_laboratorio_load = convert_column_string(df_laboratorio_load)


# VALIDATE LOAD
validate_loads_logs =  loadbq.validate_loads_daily(TABLA_BIGQUERY_LABORATORIO_CLINICO)

# Load
validate_load(validate_loads_logs,df_laboratorio_load,TABLA_BIGQUERY_LABORATORIO_CLINICO,TABLA_MARIADB)
