import sys,os
import pandas as pd
from datetime import datetime,timedelta

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


# GENERAMOS UNA FECHA PARA QUE EL INFORME TOME LOS DATOS DEL MES A PROCESAR
today = func_process.pd.to_datetime(datetime.now() - timedelta(days=15))
fecha_capita = func_process.pd.to_datetime(today.strftime('%Y-%m-15'))
#fecha_capita = func_process.pd.to_datetime(input('Escriba la fecha de la capita del mes a cargar, AAAA-MM-DD: '))

year = str(fecha_capita.year)
mes_letra = fecha_capita.strftime('%B').upper()
mes_numero = fecha_capita.strftime('%m')
last_day = str(fecha_capita.days_in_month)

fecha_i = f"{year}-{mes_numero}-01 00:00:00"
fecha_f = f"{year}-{mes_numero}-{last_day} 23:59:59"

fecha = fecha_capita.strftime("%Y-%m-%d")
# Diccionario para mapear los nombres de las sedes por medio del codigo (como texto)
n_ips = {
    '35':'CENTRO',
    '1013':'CALASANZ',
    '2136':'NORTE',
    '2715':'PAC',
    '115393':'AVENIDA ORIENTAL',
    '134189':'ARGENTINA',
    '133050':'PAC ESPECIALISTAS',
    '152932':'ESPECIALISTAS'
}   

list_poblacion_argentina = [['2019-01-15', 'ARGENTINA', 1871 ], ['2019-02-15', 'ARGENTINA', 1905], ['2019-03-15', 'ARGENTINA', 1813], 
                ['2019-04-15', 'ARGENTINA', 2328], ['2019-05-15', 'ARGENTINA', 2388], ['2019-06-15', 'ARGENTINA', 2559], 
                ['2019-07-15', 'ARGENTINA', 2533], ['2019-08-15', 'ARGENTINA', 2749], ['2019-09-15', 'ARGENTINA', 3237], 
                ['2019-10-15', 'ARGENTINA', 3273], ['2019-11-15', 'ARGENTINA', 3052], ['2019-12-15', 'ARGENTINA', 3317], 
                ['2020-01-15', 'ARGENTINA', 3184], ['2020-02-15', 'ARGENTINA', 3466], ['2020-03-15', 'ARGENTINA', 3684], 
                ['2020-04-15', 'ARGENTINA', 3897], ['2020-05-15', 'ARGENTINA', 3387], ['2020-06-15', 'ARGENTINA', 3478], 
                ['2020-07-15', 'ARGENTINA', 3613], ['2020-08-15', 'ARGENTINA', 3901], ['2020-09-15', 'ARGENTINA', 3985], 
                ['2020-10-15', 'ARGENTINA', 3869], ['2020-11-15', 'ARGENTINA', 3893], ['2020-12-15', 'ARGENTINA', 3417], 
                ['2021-01-15', 'ARGENTINA', 3489], ['2021-02-15', 'ARGENTINA', 3647], ['2021-03-15', 'ARGENTINA', 3830], 
                ['2021-04-15', 'ARGENTINA', 4009], ['2021-05-15', 'ARGENTINA', 3967], ['2021-06-15', 'ARGENTINA', 3788],
                ['2021-07-15', 'ARGENTINA', 4150], ['2021-08-15', 'ARGENTINA', 4229], ['2021-09-15', 'ARGENTINA', 4333],
                ['2021-10-15', 'ARGENTINA', 4568], ['2021-11-15', 'ARGENTINA', 4634], ['2021-12-15', 'ARGENTINA', 4815],
                ['2022-01-15', 'ARGENTINA', 4923], ['2022-02-15', 'ARGENTINA', 5045], ['2022-03-15', 'ARGENTINA', 5081],
                ['2022-04-15', 'ARGENTINA', 5127], ['2022-05-15', 'ARGENTINA', 5192], ['2022-06-15', 'ARGENTINA', 6022],
                ['2022-07-15', 'ARGENTINA', 5567], ['2022-08-15', 'ARGENTINA', 5631], ['2022-09-15', 'ARGENTINA', 5912],
                ['2022-10-15', 'ARGENTINA', 6207],['2022-11-15', 'ARGENTINA', 6517],['2022-12-15', 'ARGENTINA', 6842],
                ['2023-01-15', 'ARGENTINA', 7184],['2023-02-15', 'ARGENTINA', 7543]
                ]

list_poblacion_especialistas = [['2022-01-15', 'ESPECIALISTAS', 5796], ['2022-02-15', 'ESPECIALISTAS', 6101], ['2022-03-15', 'ESPECIALISTAS', 6422],
                ['2022-04-15', 'ESPECIALISTAS', 6760], ['2022-05-15', 'ESPECIALISTAS', 7115], ['2022-06-15', 'ESPECIALISTAS', 7489],
                ['2022-07-15', 'ESPECIALISTAS', 7883], ['2022-08-15', 'ESPECIALISTAS', 8297], ['2022-09-15', 'ESPECIALISTAS', 8733], 
                ['2022-10-15', 'ESPECIALISTAS', 9169], ['2022-11-15', 'ESPECIALISTAS', 9627], ['2022-12-15', 'ESPECIALISTAS', 10108],
                ['2023-01-15', 'ESPECIALISTAS', 10613], ['2023-02-15', 'ESPECIALISTAS', 11144]
                ]
#CARGAMOS LA TABLAS TABLAS QUE VAMOS A ORGANIZAR
sql_capita_poblaciones = f"""
                            SELECT 
                                FECHA_CAPITA, 
                                NOMBRE_IPS, 
                                POBLACION_TOTAL 
                            FROM capitas_poblaciones 
                            WHERE FECHA_CAPITA = '{fecha_capita}';
                        """

sql_rips_auditoria = f"""
                        SELECT 
                            hora_fecha, 
                            orden, 
                            ips, 
                            identificacion_pac, 
                            primer_nombre_pac, 
                            segundo_nombre_pac, 
                            primer_apellido_pac, 
                            segundo_apellido_pac, 
                            sexo, 
                            fecha_nacimiento, 
                            edad_anos, 
                            identificacion_med, 
                            nombres_med, 
                            codigo_sura, 
                            codigo_cups, 
                            nombre_prestacion, 
                            dx_principal, 
                            nombre_dx_principal, 
                            horas_observacion, 
                            fecha_cargue 
                        FROM reportes.rips 
                        WHERE  date(hora_fecha)= CURDATE();
                    """
sql_empleados = """
                    SELECT * 
                    FROM empleados_2019 
                    WHERE (cargo LIKE '%ENFERMERA ASISTENCIAL%' OR 
                            cargo LIKE '%MEDICO GENERAL%' OR 
                            cargo LIKE '%NUTRICIONISTA%' OR 
                            cargo LIKE '%PSICOLOG%' OR 
                            cargo LIKE '%QUIMICA FARMACEUTICA%' OR 
                            cargo LIKE '%REUMATOLOG%' OR 
                            cargo LIKE '%PEDIATRA%' OR 
                            cargo LIKE '%DERMATOLOG%' OR 
                            cargo LIKE '%GINECOOBSTETRA%' OR 
                            cargo LIKE '%INTERNISTA%' OR 
                            cargo LIKE '%REUMATOLOG%' OR 
                            cargo LIKE '%PSIQUIATRA%');
                """
sql_gestal = """
                SELECT 
                    identificacion as identificacion_med,                    
                    sede as sede_gestal,
                    cargo AS cargo_gestal
                FROM empleados_2019
            """



# Parametros bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_rips = 'rips'
table_name_rips_auditoria = 'rips_auditoria_poblacion_2'
TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_rips}.{table_name_rips_auditoria}'

def convert_dates(df):
    # Convertir fechas
    df.fecha_nacimiento = pd.to_datetime(df.fecha_nacimiento, errors='coerce')
    df.fecha_cargue = pd.to_datetime(df.fecha_cargue, errors='coerce')
    return df

def convert_numbers(df):
    # Convertir numeros
    df.replace('nul',0,inplace=True)
    df.edad_anos = df.edad_anos.astype(int)
    return df


      
# Agregar poblacion argentina cada mes 
def add_poblacion_mes(listPoblacion, sede):
    if len(listPoblacion) > 0:
        total_poblacion_last_month = list_poblacion_argentina[-1][-1]
        new_total_poblacion = int(total_poblacion_last_month + (total_poblacion_last_month*0.05))
        listPoblacion.append([fecha_capita, sede, new_total_poblacion])
        return listPoblacion
    
#Creamos funcion para agregar columna con el nombre de la Consulta
def n_prestacion(codigo_prestacion):
    if codigo_prestacion == '50110':
        return 'Consulta General'
    elif codigo_prestacion == '50114':
        return 'Consulta Prioritaria'
    elif codigo_prestacion == '50120' or codigo_prestacion == '50130' or codigo_prestacion == '50140' or codigo_prestacion == '50380' or codigo_prestacion == '50150' or codigo_prestacion == '50190':
        return 'Consulta Especialidades Basicas'
    
def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if total_cargue == 0:
            # Cargar mariadb
            func_process.save_df_server(df_load,'rips_auditoria_poblacion_2','analitica')
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY)
    except ValueError as err:
        print(err)

def read_dataset():
    try:
        df_rips_auditoria = func_process.load_df_server(sql_rips_auditoria, 'reportes')       
        if df_rips_auditoria.shape[0] ==0:
            raise SystemExit
        return df_rips_auditoria
    except Exception as err:
        print(err)

df_rips_auditoria = read_dataset()
list_poblacion_argentina = add_poblacion_mes(list_poblacion_argentina, 'ARGENTINA')
list_poblacion_especialistas = add_poblacion_mes(list_poblacion_especialistas, 'ESPECIALISTAS')
df_capita_poblaciones = func_process.load_df_server(sql_capita_poblaciones, 'analitica')
sede_gestal = func_process.load_df_server(sql_gestal, 'reportes')

#Sacamos la poblacion capitada en la Sede Argentina
poblacion_argentina = func_process.pd.DataFrame(list_poblacion_argentina,
    columns= ['FECHA_CAPITA', 'NOMBRE_IPS', 'POBLACION_TOTAL'])

poblacion_argentina['FECHA_CAPITA'] = func_process.pd.to_datetime(poblacion_argentina['FECHA_CAPITA'])
df_capita_poblaciones.FECHA_CAPITA = func_process.pd.to_datetime(df_capita_poblaciones.FECHA_CAPITA)

#Sacamos la poblacion capitada en la Sede Especialistas
poblacion_especialistas = func_process.pd.DataFrame(list_poblacion_especialistas,
                                                    columns= ['FECHA_CAPITA', 'NOMBRE_IPS', 'POBLACION_TOTAL'])

poblacion_especialistas['FECHA_CAPITA'] = func_process.pd.to_datetime(poblacion_especialistas['FECHA_CAPITA'])
df_capita_poblaciones.FECHA_CAPITA = func_process.pd.to_datetime(df_capita_poblaciones.FECHA_CAPITA)

#Dejamos solamente las poblaciones del mes a procesar
poblacion_argentina_mes = poblacion_argentina[poblacion_argentina.FECHA_CAPITA == fecha_capita.strftime('%Y-%m-%d')]
poblacion_especialistas_mes = poblacion_especialistas[poblacion_especialistas.FECHA_CAPITA == fecha_capita.strftime('%Y-%m-%d')]
df_capita_poblaciones_mes = df_capita_poblaciones[df_capita_poblaciones.FECHA_CAPITA == fecha_capita.strftime('%Y-%m-%d')]

df_capita_poblaciones_all = func_process.pd.concat([df_capita_poblaciones_mes, poblacion_especialistas_mes,
                                                    poblacion_argentina_mes]).sort_values('FECHA_CAPITA')

df_rips_auditoria.hora_fecha = func_process.pd.to_datetime(df_rips_auditoria.hora_fecha)
df_rips_auditoria['identificacion_pac'] = df_rips_auditoria['identificacion_pac'].astype('str')
df_rips_auditoria['identificacion_med'] = df_rips_auditoria['identificacion_med'].astype('str')
df_rips_auditoria['codigo_sura'] = df_rips_auditoria['codigo_sura'].astype('str')
df_rips_auditoria['ips'] = df_rips_auditoria['ips'].astype('str')
df_rips_auditoria.insert(0, 'FECHA_CAPITA', df_rips_auditoria['hora_fecha'].apply(lambda row: '{}{}{:02d}{}{}'.format(row.year, '-', row.month, '-', 15)))
df_rips_auditoria.insert(1, 'NOMBRE_IPS', df_rips_auditoria['ips'].map(n_ips))
df_rips_auditoria['FECHA_CAPITA'] = func_process.pd.to_datetime(df_rips_auditoria['FECHA_CAPITA'])
df_rips_auditoria_mes = df_rips_auditoria[df_rips_auditoria['FECHA_CAPITA'] == fecha_capita.strftime('%Y-%m-%d')]
df_rips_auditoria_mes_p = df_rips_auditoria_mes.merge(df_capita_poblaciones_all, 
                                                        how='left',
                                                        on=['FECHA_CAPITA','NOMBRE_IPS'])

df_rips_auditoria_mes_p['POBLACION_TOTAL_COOPSANA'] = df_capita_poblaciones_all[df_capita_poblaciones_all['NOMBRE_IPS'] == 'COOPSANA IPS'].iloc[0,2]

df_rips_auditoria_mes_p['tipos_consulta'] = func_process.np.where(
    df_rips_auditoria_mes_p['codigo_sura'].isin(['50110','7000026']), 'CONSULTA MEDICINA GENERAL',
    func_process.np.where(
        df_rips_auditoria_mes_p['codigo_sura'].isin(['3000056','50114']), 'CONSULTA NO PROGRAMADA',
        func_process.np.where(
            df_rips_auditoria_mes_p['codigo_sura'].isin(['70000','70100','71000','71101','70200','70201','70300','70301','70400','71105','70102','7000075','7000076','3000052','3000054','8902031','8903018','8903056']), 'PROMOCION Y PREVENCION',
            func_process.np.where(
                df_rips_auditoria_mes_p['codigo_sura'] == '890101', 'DOMICILIARIA', 'OTRO'
            )
        )
    )
)
inasistecias_rips = df_rips_auditoria_mes_p.groupby(
                                                ['FECHA_CAPITA', 
                                                'NOMBRE_IPS', 
                                                'identificacion_med', 
                                                'nombres_med']
                                            ).agg(
                                                {'codigo_sura':'count'}
                                            ).reset_index()
inasistecias_rips.rename({'codigo_sura':'total_citas_asistidas'},
                        axis=1, 
                        inplace=True)
inasistecias_rips_totales = inasistecias_rips.groupby(
                                                ['FECHA_CAPITA',
                                                'identificacion_med',
                                                'nombres_med']
                                            ).agg(
                                                {'total_citas_asistidas':'sum'}
                                            ).reset_index()
inasistecias_rips_totales.insert(1, 'NOMBRE_IPS', 'COOPSANA IPS')
df_rips_auditoria_mes_p['POBLACION_TOTAL'].fillna(0, inplace= True)
df_rips_auditoria_mes_p['POBLACION_TOTAL'] = df_rips_auditoria_mes_p['POBLACION_TOTAL'].astype('int64')
sede_gestal['identificacion_med'] = sede_gestal['identificacion_med'].astype('str')
df_rips_auditoria_mes_p_sede_gestal = df_rips_auditoria_mes_p.merge(sede_gestal, 
                                                                    on='identificacion_med', 
                                                                    how='left')

#Remplazamos los que quedan en nulo por EXTERNO
df_rips_auditoria_mes_p_sede_gestal.loc[df_rips_auditoria_mes_p_sede_gestal['sede_gestal'].isnull(), "sede_gestal"] = "EXTERNO"
df_rips_auditoria_mes_p_sede_gestal.loc[df_rips_auditoria_mes_p_sede_gestal['cargo_gestal'].isnull(), "cargo_gestal"] = "EXTERNO"
df_rips_auditoria_mes_p_sede_gestal = df_rips_auditoria_mes_p_sede_gestal[
    ['FECHA_CAPITA', 'NOMBRE_IPS', 'hora_fecha', 'orden', 'ips',
    'identificacion_pac', 'primer_nombre_pac', 'segundo_nombre_pac',
    'primer_apellido_pac', 'segundo_apellido_pac', 'sexo',
    'fecha_nacimiento', 'edad_anos', 'identificacion_med', 'nombres_med',
    'codigo_sura', 'codigo_cups', 'nombre_prestacion', 'dx_principal',
    'nombre_dx_principal', 'horas_observacion', 'tipos_consulta',
    'fecha_cargue', 'POBLACION_TOTAL', 'POBLACION_TOTAL_COOPSANA',
    'sede_gestal', 'cargo_gestal']
]


df_rips_auditoria_mes_p_sede_gestal = convert_dates(df_rips_auditoria_mes_p_sede_gestal)
df_rips_auditoria_mes_p_sede_gestal = convert_numbers(df_rips_auditoria_mes_p_sede_gestal)
df_rips_auditoria_mes_p_sede_gestal.columns = df_rips_auditoria_mes_p_sede_gestal.columns.str.lower()

# VALIDATE LOAD
validate_loads_logs =  loadbq.validate_loads_daily(TABLA_BIGQUERY)

# # Load data to server
validate_load(validate_loads_logs,df_rips_auditoria_mes_p_sede_gestal)

