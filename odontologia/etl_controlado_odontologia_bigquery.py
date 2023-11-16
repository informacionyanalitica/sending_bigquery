import pandas as pd
import numpy as np 
import sys,os 

path = os.path.abspath('/data/compartida/etls/tools')
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq
from datetime import datetime, timedelta



project_id_product = 'ia-bigquery-397516'
dataset_id = 'odontologia'
table_name = 'datos_rips_odontologia_partition'
validator_column = 'id_atencion'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id}.{table_name}'


# CREAR FECHAS
today = datetime.now() - timedelta(days=20)
fecha_cargue = today.strftime('%Y-%m') 

fecha = func_process.pd.to_datetime(fecha_cargue+'-01')
fecha_first_day = fecha.strftime('%Y-%m-01')
fecha_last_day = f"{fecha.strftime('%Y')}-{fecha.strftime('%m')}-{fecha.days_in_month}"
str(fecha.year)+'-'+'{:02d}'.format(fecha.month)+'-'+str(fecha.days_in_month)
fecha_tres_meses = (fecha - func_process.np.timedelta64(2,'M'))
fecha_tres_meses = fecha_tres_meses.strftime('%Y-%m-01')

#CARGAMOS LA TABLAS TABLAS QUE VAMOS A ORGANIZAR
sql_datos_rips_odontologia = f"""
                                SELECT * 
                                FROM datos_rips_odontologia_view 
                                WHERE fecha_consulta BETWEEN '{fecha_first_day}' AND '{fecha_last_day}';
                            """
sql_detalle_rips_odontologia = "SELECT * FROM detalle_rips_odontologia_view;"
sql_empleados_activos = """
                            SELECT 
                                identificacion, 
                                nombre, 
                                cargo 
                            FROM empleados_activos;
                        """
                        
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

def total_atenciones(iden_pac):    
    return len(detalle_odontologia_3meses[detalle_odontologia_3meses.loc[:,'identificacion_paciente'] == iden_pac].drop_duplicates(subset =["identificacion_paciente","fecha_cita"]))

def hora(h):
    h = str(h).split(' ')[-1:][0]
    return h

def convert_columns_date(df):
    df.fecha_consulta = pd.to_datetime(df.fecha_consulta, errors='coerce')
    return df

def convert_columns_str(df):
    df.hora_cita = df.hora_cita.astype(str)
    df.hora_finaliza_cita = df.hora_finaliza_cita.astype(str)
    #Replace str
    df.hora_cita = [row.replace('0 days ','') for row in df.hora_cita]
    df.hora_finaliza_cita = [row.replace('0 days ','') for row in df.hora_finaliza_cita]
    return df

df_datos_rips_odontologia = func_process.load_df_server(sql_datos_rips_odontologia, 'reportes')
df_datos_rips_odontologia['hora_cita'] = df_datos_rips_odontologia['hora_cita'].astype('str')
df_datos_rips_odontologia['fecha_consulta'] = func_process.pd.to_datetime(df_datos_rips_odontologia['fecha_consulta'])
df_detalle_rips_odontologia = func_process.load_df_server(sql_detalle_rips_odontologia, 'reportes')
df_detalle_rips_odontologia.drop('id_detalle',inplace=True, axis=1)
df_empleados_activos = func_process.load_df_server(sql_empleados_activos, 'reportes')
odontologia_controlado = df_datos_rips_odontologia[(df_datos_rips_odontologia['fecha_consulta']>= fecha_first_day) &
                                                   (df_datos_rips_odontologia['fecha_consulta']<= fecha_last_day) &
                                                   (df_datos_rips_odontologia['paciente_controlado'] == '1')
                                                   ]

odontologia_controlado.drop(odontologia_controlado[(odontologia_controlado['identificacion_paciente'] == '0') 
                                                | (odontologia_controlado['identificacion_profesional'] == '')].index, 
                                                axis= 0, inplace= True
                            )
df_detalle_rips_odontologia.drop(df_detalle_rips_odontologia[(df_detalle_rips_odontologia.id_atencion == '')|
                                                             (df_detalle_rips_odontologia.id_atencion.isnull())].index, axis= 0, inplace=True)
detalle_odontologia = df_detalle_rips_odontologia['id_atencion'].str.split('-',expand=True)[[2,0,3,4,5,6]].rename(columns={0:'identificacion_profesional', 2:'identificacion_paciente'})
detalle_odontologia['fecha_cita'] = detalle_odontologia[3]+'-'+detalle_odontologia[4]+'-'+detalle_odontologia[5]
detalle_odontologia.drop(columns=[3,4,5,6], inplace = True)
df_detalle_rips_odontologia.drop(['identificacion_paciente', 'identificacion_profesional'], axis=1, inplace=True)
rips_detalle_odontologia = func_process.pd.concat([detalle_odontologia, df_detalle_rips_odontologia], axis=1)
rips_detalle_odontologia.drop(rips_detalle_odontologia[(rips_detalle_odontologia.identificacion_paciente == '')|(rips_detalle_odontologia.identificacion_paciente == '0')
                                                       |(rips_detalle_odontologia.identificacion_paciente == '00')|(rips_detalle_odontologia.identificacion_paciente == '000')
                                                       |(rips_detalle_odontologia.identificacion_paciente =='noencontrado')|(rips_detalle_odontologia.identificacion_paciente =='Noencontrado')].index, inplace= True)
detalle_odontologia_3meses = rips_detalle_odontologia[(rips_detalle_odontologia.fecha_cita >= fecha_tres_meses) &
                                                      (rips_detalle_odontologia.fecha_cita <= fecha_last_day)].drop_duplicates(subset =["identificacion_paciente","fecha_cita"])
detalle_odontologia_3meses[detalle_odontologia_3meses.loc[:, 'identificacion_paciente'] == '1025765454'].drop_duplicates(subset =["identificacion_paciente","fecha_cita"])


odontologia_controlado['total_atenciones'] = odontologia_controlado.loc[:,'identificacion_paciente'].apply(total_atenciones)
df_empleados_activos.rename(columns={'identificacion':'identificacion_profesional', 'nombre':'nombre_profesional'}, inplace = True)
df_empleados_activos['identificacion_profesional'] = df_empleados_activos['identificacion_profesional'].astype('str')

#Cargo las columnas de nombre y cargo del profesional
odontologia_controlado_med = odontologia_controlado.merge(df_empleados_activos, on='identificacion_profesional', how='left')

#Relleno los valores nulos
odontologia_controlado_med.fillna('', inplace= True)



#Agrego las columas de horas con el formato string
odontologia_controlado_med['hora_cita_2'] = odontologia_controlado_med['hora_cita'].apply(hora)
odontologia_controlado_med['hora_finaliza_cita_2'] = odontologia_controlado_med['hora_finaliza_cita'].apply(hora)

odontologia_controlado_med_f = odontologia_controlado_med.loc[:, ['id_atencion', 'identificacion_paciente', 'identificacion_profesional', 'nombre_profesional', 'cargo', 'estado_atencion', 'fecha_consulta', 'hora_cita_2', 'hora_finaliza_cita_2',   
                                                                  'entidad_prestadora', 'tipo_usuario', 'zona_residencia', 'sede', 'personal_atiende', 'paciente_controlado', 'total_atenciones']]

#Renombro las columnas de hora en string
odontologia_controlado_med_f.rename(columns={'hora_cita_2':'hora_cita', 'hora_finaliza_cita_2':'hora_finaliza_cita'}, inplace= True)
odontologia_controlado_med_f['fecha_consulta'] = odontologia_controlado_med_f['fecha_consulta'].astype('str')

# Convertir columnas fechas
odontologia_controlado_med_f = convert_columns_date(odontologia_controlado_med_f)

# Convert columnas string
odontologia_controlado_med_f = convert_columns_str(odontologia_controlado_med_f)


# Cargar datos
func_process.save_df_server(odontologia_controlado_med_f, 'odontologia_controlado_med', 'analitica')

# Save bigquery 
loadbq.load_data_bigquery(odontologia_controlado_med_f,TABLA_BIGQUERY)
