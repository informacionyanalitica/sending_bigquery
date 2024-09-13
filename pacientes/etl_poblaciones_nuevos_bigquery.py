import sys,os
import pandas as pd
from datetime import datetime,timedelta
from dotenv import load_dotenv
import locale
# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


locale.setlocale(locale.LC_TIME, "es_ES.utf8") 
# VARIBLE - CONSTANTE
# Dates
today = datetime.now()
capita_date = pd.to_datetime(today.strftime('%Y-%m-15')).date()
capita_date_previous =(capita_date -timedelta(days=31))
mes_numero = str(capita_date.month)
ano = capita_date.strftime("%Y")
mes_letra = capita_date.strftime("%B").capitalize()

mes_numero_previous = str(capita_date_previous.month)
ano_previous = capita_date_previous.strftime("%Y")
mes_letra_previous = capita_date_previous.strftime("%B").capitalize()

# Path
PATH_DRIVE = os.environ.get("PATH_DRIVE")
path_capita = f"{PATH_DRIVE}/BASES DE DATOS/{ano}/{mes_numero}. {mes_letra}/CAPITA EPS SURA"
path_capita_previous = f"{PATH_DRIVE}/BASES DE DATOS/{ano_previous}/{mes_numero_previous}. {mes_letra_previous}/CAPITA EPS SURA"

# Parametros bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_pacientes = 'pacientes'

# Planificacion familiar
table_name_poblaciones_nuevos = 'capita_poblaciones_nuevos'
TABLE_MARIADB_POBLACIONES_NUEVOS = 'capita_poblaciones_nuevos'
TABLA_BIGQUERY_POBLACIONES_NUEVOS= f'{project_id_product}.{dataset_id_pacientes}.{table_name_poblaciones_nuevos}'

COLUMNS_REQUIRED = ['fecha_capita', 'nombre_ips', 'codigo_ips', 'poblacion_total',
       'poblacion_total_f', 'poblacion_total_m', 'menores_1_ano_f',
       'menores_1_ano_m', 'entre_1_y_4_anos_f', 'entre_1_y_4_anos_m',
       'menores_a_5_anos_f', 'menores_a_5_anos_m', 'entre_5_y_14_anos_f',
       'entre_5_y_14_anos_anos_m', 'entre_6_y_19_anos_f',
       'entre_6_y_19_anos_anos_m', 'entre_15_y_44_anos_f',
       'entre_15_y_44_anos_m', 'entre_45_y_59_anos_f', 'entre_45_y_59_anos_m',
       'mayor_de_59_anos_f', 'mayor_de_59_anos_m', 'menores_a_1_ano',
       'entre_1_y_4_anos', 'menores_a_5_anos', 'entre_5_y_14_anos',
       'entre_6_y_19_anos', 'entre_15_y_44_anos', 'entre_45_y_59_anos',
       'mayores_de_59_anos', 'entre_12_y_17_anos', 'entre_18_y_28_anos',
       'entre_29_y_59_anos', 'mujeres_entre_15_y_19_anos','menores_a_16_anos',
       'mujeres_entre_15_y_49_anos', 'mujeres_entre_10_y_49_anos',
       'mujeres_entre_10_y_14_anos', 'mujeres_entre_14_y_49_anos',
       'mayores_de_18_anos', 'entre_6_y_10_anos', 'entre_18_y_85_anos',
       'mayores_de_40_anos', 'menores_a_18_anos',
       'mujeres_mayores_igual_18_anos', 'menores_a_41_anos',
       'menores_a_3_anos', 'mayores_de_64_anos']
COLUMNS_INTEGER = ['POBLACION TOTAL',
       'POBLACION TOTAL - F', 'POBLACION TOTAL - M', 'Menores 1 ano - F',
       'Menores 1 ano - M', 'Entre 1 y 4 anos - F', 'Entre 1 y 4 anos - M',
       'Menores a 5 anos - F', 'Menores a 5 anos - M', 'Entre 5 y 14 anos - F',
       'Entre 5 y 14 anos - M', 'Entre 6 y 19 anos - F',
       'Entre 6 y 19 anos - M', 'Entre 15 y 44 anos - F',
       'Entre 15 y 44 anos - M', 'Entre 45 y 59 anos - F',
       'Entre 45 y 59 anos - M', 'Mayor de 59 anos - F',
       'Mayor de 59 anos - M', 'Menores a 1 ano', 'Entre 1 y 4 anos',
       'Menores a 5 anos', 'Entre 5 y 14 anos', 'Entre 6 y 19 anos',
       'Entre 15 y 44 anos', 'Entre 45 y 59 anos', 'Mayores de 59 anos',
       'Entre 12 y 17 anos', 'Entre 18 y 28 anos', 'Entre 29 y 59 anos',
       'Mujeres entre 15 y 19 anos', 'Mujeres entre 15 y 49 anos',
       'Mujeres entre 10 y 49 anos', 'Mujeres entre 10 y 14 anos',
       'Mujeres entre 14 y 49 anos', 'Mayores de 18 anos', 'Entre 6 y 10 anos',
       'Entre 18 y 85 anos', 'Mayores o igual de 40 anos',
       'Menores de 18 anos', 'Mujeres Mayores o igual de 18 anos',
       'Menores o igual de 40 anos', 'Menores o igual de 2 anos',
       'Menores de 16 anos', 'Mayores de 64 anos']

def convert_columns_integer(df):
    try:
        for col in COLUMNS_INTEGER:
            df[col] = df[col].astype('int64')
        return df
    except Exception as err:
        print(err)
# Funcion para cargar cada archivo TXT de capita
def load_branch(path_capita, file):
    branch = func_process.pd.read_csv(f"{path_capita}/{file}", sep=",", dtype={1:'object', 13:'object'}, encoding = "ISO-8859-1")
    branch.drop(branch.tail(2).index, inplace= True)
    return branch

def clear_space(df):
    try:
        df.columns = df.columns.str.replace('-','').str.replace('  ',' ')
        df.columns = df.columns.str.lower().str.replace(' ','_')
        return df
    except Exception as err:
        print(err)


def validate_load(df_validate_load,df_load):
    try:
        total_cargue = df_validate_load.totalCargues[0]
        if  total_cargue == 0:
            # Cargar mariadb
            func_process.save_df_server(df_load, TABLE_MARIADB_POBLACIONES_NUEVOS, 'analitica')
            # Cargar bigquery
            loadbq.load_data_bigquery(df_load,TABLA_BIGQUERY_POBLACIONES_NUEVOS)
    except ValueError as err:
        print(err)

#Funcion que carga cada archivo de la sede y lo junta en uno solo llamado capita
def load_capita():
    capita_115393 = load_branch(path_capita, "4_800168083_UNICOPOS_115393.TXT")
    capita_2715 = load_branch(path_capita, "4_800168083_UNICOPOS_2715.TXT")
    capita_2136 = load_branch(path_capita, "4_800168083_UNICOPOS_2136.TXT")
    capita_1013 = load_branch(path_capita, "4_800168083_UNICOPOS_1013.TXT")
    capita_35 = load_branch(path_capita, "4_800168083_UNICOPOS_35.TXT")    
    capita = func_process.pd.concat([capita_115393, capita_2715, capita_2136, capita_1013, capita_35], ignore_index=True)
    capita.rename(columns={'GENERO':'SEXO'}, inplace= True)
    return capita

#Creamos dos funciones para generar la edad en años y en meses
def age_months(born_date):
    """recibe la fecha de nacimiento la divide y cambia
    a formato datetime para pasarla a la funcion que calcula
    la edad en meses
    
    Args:
        born_date (pd.DateTime): fecha de nacimiento a la cual le 
        calcula la edad en meses respecto a la fecha de la capita
        
    Returns:
        months_age
    """    
    #Convertimos fechas a datetime
    c_d = func_process.datetime(capita_date.year, capita_date.month, capita_date.day)
    b_d = func_process.datetime(born_date.year, born_date.month, born_date.day)
    
    #Funcion que calcula Diferencia en meses
    def diff_month(current_date, born_date):
        return (current_date.year - born_date.year) * 12 + current_date.month - born_date.month
    
    months = diff_month(c_d, b_d)
    return months

def age_years(born_date):
    """recibe la fecha de nacimiento la divide y cambia
    a formato datetime para pasarla a la funcion que calcula
    la edad en años
    
    Args:
        born_date (pd.DateTime): fecha de nacimiento a la cual le 
        calcula la edad en años respecto a la fecha de la capita
        
    Returns:
        months_years
    """    
    c_d = func_process.datetime(capita_date.year, capita_date.month, capita_date.day)
    b_d = func_process.datetime(born_date.year, born_date.month, born_date.day)
    
    #Diferencia en años
    def diff_year(current_date, born_date):
        years = (current_date.year - born_date.year)
        years -= ((current_date.month, current_date.day) < (born_date.month, born_date.day))
        return years
    years = diff_year(c_d, b_d)
    return years



def load_capita_previous():
    capita_115393 = load_branch(path_capita_previous, "4_800168083_UNICOPOS_115393.TXT")
    capita_2715 = load_branch(path_capita_previous, "4_800168083_UNICOPOS_2715.TXT")
    capita_2136 = load_branch(path_capita_previous, "4_800168083_UNICOPOS_2136.TXT")
    capita_1013 = load_branch(path_capita_previous, "4_800168083_UNICOPOS_1013.TXT")
    capita_35 = load_branch(path_capita_previous, "4_800168083_UNICOPOS_35.TXT")    
    capita_previous = func_process.pd.concat([capita_115393, capita_2715, capita_2136, capita_1013, capita_35], ignore_index=True)
    capita_previous['NUEVO'] = 'NO'
    capita_previous = capita_previous[['NUMERO DE IDENTIFICACION','NUEVO']]    
    return capita_previous

# Sustituimos el nombre de cada sede por el que usanos frecuentemente
def subtitute_name_branch(capita):
    mapping = {
        'COOPSANA - CENTRO':'CENTRO', 
        'COOPSANA CENTRO ARGENTINA':'AVENIDA ORIENTAL', 
        'IPS PAC COOPSANA SURAMERICANA':'PAC', 
        'COOPSANA NORTE':'NORTE',
        'COOPSANA CALASANZ':'CALASANZ'
    }
    capita['NOMBRE IPS'] = capita['NOMBRE IPS'].map(mapping)
    return capita

#Creamos otra funcion para agregar el codigo de la ips que no trae la capita
def codigo_ips(capita):
    mapping = {
        'CENTRO':35, 
        'AVENIDA ORIENTAL':115393, 
        'PAC':2715, 
        'NORTE':2136,
        'CALASANZ':1013
    }
    capita['CODIGO IPS'] = capita['NOMBRE IPS'].map(mapping)
    return capita


capita_mes = (
    load_capita()
    .pipe(subtitute_name_branch)
    .pipe(codigo_ips)
)

format_string = "%d/%m/%Y"
capita_mes["FECHA NACIMIENTO"] = func_process.pd.to_datetime(capita_mes["FECHA NACIMIENTO"], format=format_string)

#invocamos la funcion en la columna  EDAD ANOS que estamos insertando
capita_mes['EDAD ANOS'] = capita_mes["FECHA NACIMIENTO"].apply(age_years)

#invocamos la funcion en la columna  EDAD MESES que estamos insertando
capita_mes['EDAD MESES'] = capita_mes["FECHA NACIMIENTO"].apply(age_months)

capita_anterior = load_capita_previous()
df_capita = capita_mes.merge(capita_anterior, on='NUMERO DE IDENTIFICACION', how='left')
capita = df_capita.loc[:,['NUMERO DE IDENTIFICACION', 'NOMBRE', 'PRIMER APELLIDO', 'SEGUNDO APELLIDO', 'NOMBRE IPS', 'CODIGO IPS', 'SEXO', 'FECHA NACIMIENTO', 'EDAD ANOS', 'EDAD MESES', 'NUMERO DE IDENTIFICACION COTIZANTE', 'NUEVO']]
capita['NUEVO'].fillna('SI', inplace = True)
# EXTRACT CAPITA NUEVOS
capita_nuevos = capita[capita['NUEVO'] == 'SI']
capita_nuevos = capita_nuevos.loc[:,['NUMERO DE IDENTIFICACION', 'NOMBRE IPS','CODIGO IPS','SEXO', 'EDAD ANOS', 'EDAD MESES', 'NUMERO DE IDENTIFICACION COTIZANTE']]
# SUB GRUPOS POBLACION NUEVOS EN CAPITA
poblacion_total_ips = capita_nuevos.groupby(['NOMBRE IPS','CODIGO IPS']).size().reset_index().rename(columns={0:'POBLACION TOTAL'})
# EDAD (GRUPOS MASCULINOS Y FEMENINO)
poblacion_total_ips_sexo = capita_nuevos.pivot_table(
    values = 'NUMERO DE IDENTIFICACION', 
    index= ['NOMBRE IPS'], 
    columns= ['SEXO'], 
    aggfunc= func_process.np.size
).reset_index().rename(columns={'F':'POBLACION TOTAL - F', 'M':'POBLACION TOTAL - M'})
#Segmentamos la poblacion
menores_1_ano_sexo = capita_nuevos[capita_nuevos['EDAD ANOS'] < 1]
#Agrupamos y contamos la poblacion
menores_1_ano_sexo_pivot = menores_1_ano_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Menores 1 ano - F', 'M':'Menores 1 ano - M'})
#Segmentamos la poblacion
entre_1_4_anos_sexo = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=1) & (capita_nuevos['EDAD ANOS'] <=4))]
#Agrupamos y contamos la poblacion
entre_1_4_anos_sexo_pivot = entre_1_4_anos_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Entre 1 y 4 anos - F', 'M':'Entre 1 y 4 anos - M'})
#Segmentamos la poblacion
menores_5_anos_sexo = capita_nuevos[(capita_nuevos['EDAD ANOS'] < 5)]
#Agrupamos y contamos la poblacion
menores_5_anos_sexo_pivot = menores_5_anos_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Menores a 5 anos - F', 'M':'Menores a 5 anos - M'})
#Segmentamos la poblacion
entre_5_14_anos_sexo = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=5) & (capita_nuevos['EDAD ANOS'] <=14))]
#Agrupamos y contamos la poblacion
entre_5_14_anos_sexo_pivot = entre_5_14_anos_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Entre 5 y 14 anos - F', 'M':'Entre 5 y 14 anos - M'})
#Segmentamos la poblacion
entre_6_19_anos_sexo = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=6) & (capita_nuevos['EDAD ANOS'] <=19))]
#Agrupamos y contamos la poblacion
entre_6_19_anos_sexo_pivot = entre_6_19_anos_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Entre 6 y 19 anos - F', 'M':'Entre 6 y 19 anos - M'})
#Segmentamos la poblacion
entre_15_44_anos_sexo = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=15) & (capita_nuevos['EDAD ANOS'] <=44))]
#Agrupamos y contamos la poblacion
entre_15_44_anos_sexo_pivot = entre_15_44_anos_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Entre 15 y 44 anos - F', 'M':'Entre 15 y 44 anos - M'})
#Segmentamos la poblacion
entre_45_59_anos_sexo = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=45) & (capita_nuevos['EDAD ANOS'] <=59))]
#Agrupamos y contamos la poblacion
entre_45_59_anos_sexo_pivot = entre_45_59_anos_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Entre 45 y 59 anos - F', 'M':'Entre 45 y 59 anos - M'})
#Segmentamos la poblacion
mayor_59_anos_sexo = capita_nuevos[capita_nuevos['EDAD ANOS'] >=60]
#Agrupamos y contamos la poblacion
mayor_59_anos_sexo_pivot = mayor_59_anos_sexo.pivot_table(
    index = ['NOMBRE IPS'],
    values = 'NUMERO DE IDENTIFICACION',    
    columns = ['SEXO'],
    aggfunc = func_process.np.size
).reset_index().rename(columns={'F':'Mayor de 59 anos - F', 'M':'Mayor de 59 anos - M'})
EDAD_GRUPOS_SEXO = poblacion_total_ips.merge(poblacion_total_ips_sexo, on= 'NOMBRE IPS').merge(menores_1_ano_sexo_pivot, on= 'NOMBRE IPS').merge(entre_1_4_anos_sexo_pivot, on= 'NOMBRE IPS').merge(menores_5_anos_sexo_pivot, on= 'NOMBRE IPS').merge(entre_5_14_anos_sexo_pivot, on= 'NOMBRE IPS').merge(entre_6_19_anos_sexo_pivot, on= 'NOMBRE IPS').merge(entre_15_44_anos_sexo_pivot, on= 'NOMBRE IPS').merge(entre_45_59_anos_sexo_pivot, on= 'NOMBRE IPS').merge(mayor_59_anos_sexo_pivot, on= 'NOMBRE IPS')
# EDAD (GRUPOS)
menores_1_ano_total= menores_1_ano_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Menores a 1 ano'})
entre_1_4_anos_total = entre_1_4_anos_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 1 y 4 anos'})
menores_5_anos_total = menores_5_anos_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Menores a 5 anos'})
entre_5_14_anos_total = entre_5_14_anos_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 5 y 14 anos'})
entre_6_19_anos_total = entre_6_19_anos_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 6 y 19 anos'})
entre_15_44_anos_total = entre_15_44_anos_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 15 y 44 anos'})
entre_45_59_anos_total = entre_45_59_anos_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 45 y 59 anos'})
mayor_59_anos_total = mayor_59_anos_sexo.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mayores de 59 anos'})
EDAD_GRUPOS_TOTAL = menores_1_ano_total.merge(entre_1_4_anos_total, on= 'NOMBRE IPS').merge(menores_5_anos_total, on= 'NOMBRE IPS').merge(entre_5_14_anos_total, on= 'NOMBRE IPS').merge(entre_6_19_anos_total, on= 'NOMBRE IPS').merge(entre_15_44_anos_total, on= 'NOMBRE IPS').merge(entre_45_59_anos_total, on= 'NOMBRE IPS').merge(mayor_59_anos_total, on= 'NOMBRE IPS')
#Segmentamos la poblacion
entre_12_17_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=12) & (capita_nuevos['EDAD ANOS'] <=17))]
entre_12_17_anos_total = entre_12_17_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 12 y 17 anos'})
entre_18_28_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=18) & (capita_nuevos['EDAD ANOS'] <=28))]
entre_18_28_anos_total = entre_18_28_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 18 y 28 anos'})
entre_29_59_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=29) & (capita_nuevos['EDAD ANOS'] <=59))]
entre_29_59_anos_total = entre_29_59_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 29 y 59 anos'})
EDAD_RIAS = entre_12_17_anos_total.merge(entre_18_28_anos_total, on= 'NOMBRE IPS').merge(entre_29_59_anos_total, on= 'NOMBRE IPS')
mujeres_entre_15_19_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=15) & (capita_nuevos['EDAD ANOS'] <=19) & (capita_nuevos['SEXO'] == 'F'))]
mujeres_entre_15_19_anos_total = mujeres_entre_15_19_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mujeres entre 15 y 19 anos'})
mujeres_entre_15_49_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=15) & (capita_nuevos['EDAD ANOS'] <=49) & (capita_nuevos['SEXO'] == 'F'))]
mujeres_entre_15_49_anos_total = mujeres_entre_15_49_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mujeres entre 15 y 49 anos'})
mujeres_entre_10_49_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=10) & (capita_nuevos['EDAD ANOS'] <=49) & (capita_nuevos['SEXO'] == 'F'))]
mujeres_entre_10_49_anos_total = mujeres_entre_10_49_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mujeres entre 10 y 49 anos'})
mujeres_entre_10_14_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=10) & (capita_nuevos['EDAD ANOS'] <=14) & (capita_nuevos['SEXO'] == 'F'))]
mujeres_entre_10_14_anos_total = mujeres_entre_10_14_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mujeres entre 10 y 14 anos'})
mujeres_entre_14_49_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=14) & (capita_nuevos['EDAD ANOS'] <=49) & (capita_nuevos['SEXO'] == 'F'))]
mujeres_entre_14_49_anos_total = mujeres_entre_14_49_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mujeres entre 14 y 49 anos'})
mayores_18_anos = capita_nuevos[(capita_nuevos['EDAD ANOS'] >=18)]
mayores_18_anos_total = mayores_18_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mayores de 18 anos'})
entre_6_10_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=6) & (capita_nuevos['EDAD ANOS'] <=10))]
entre_6_10_anos_total = entre_6_10_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 6 y 10 anos'})
INFORME_ESTADISTICO = mujeres_entre_15_19_anos_total.merge(mujeres_entre_15_49_anos_total, on= 'NOMBRE IPS').merge(mujeres_entre_10_49_anos_total, on= 'NOMBRE IPS').merge(mujeres_entre_10_14_anos_total, on= 'NOMBRE IPS').merge(mujeres_entre_14_49_anos_total, on= 'NOMBRE IPS').merge(mayores_18_anos_total, on= 'NOMBRE IPS').merge(entre_6_10_anos_total, on= 'NOMBRE IPS')
# GESTION DEL RIESGO
entre_18_85_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >=18) & (capita_nuevos['EDAD ANOS'] <=85))]
entre_18_85_anos_total = entre_18_85_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Entre 18 y 85 anos'})
mayores_40_anos = capita_nuevos[(capita_nuevos['EDAD ANOS'] >= 40)]
mayores_40_anos_total = mayores_40_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mayores o igual de 40 anos'})
menores_18_anos = capita_nuevos[(capita_nuevos['EDAD ANOS'] < 18)]
menores_18_anos_total = menores_18_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Menores de 18 anos'})
mujeres_mayores_igual_18_anos = capita_nuevos[((capita_nuevos['EDAD ANOS'] >= 18) & (capita_nuevos['SEXO'] == 'F'))]
mujeres_mayores_igual_18_anos_total = mujeres_mayores_igual_18_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mujeres Mayores o igual de 18 anos'})
menores_igual_45_anos = capita_nuevos[(capita_nuevos['EDAD ANOS'] <= 40)]
menores_igual_45_anos_total = menores_igual_45_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Menores o igual de 40 anos'})
menores_igual_2_anos = capita_nuevos[(capita_nuevos['EDAD ANOS'] <= 2)]
menores_igual_2_anos_total = menores_igual_2_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Menores o igual de 2 anos'})
GESTION_RIESGO = entre_18_85_anos_total.merge(mayores_40_anos_total, on= 'NOMBRE IPS').merge(menores_18_anos_total, on= 'NOMBRE IPS').merge(mujeres_mayores_igual_18_anos_total, on= 'NOMBRE IPS').merge(menores_igual_45_anos_total, on= 'NOMBRE IPS').merge(menores_igual_2_anos_total, on= 'NOMBRE IPS')
menores_16_anos = capita_nuevos[(capita_nuevos['EDAD ANOS'] < 16)]
menores_16_anos_total = menores_16_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Menores de 16 anos'})
mayores_64_anos = capita_nuevos[(capita_nuevos['EDAD ANOS'] > 64)]
mayores_64_anos_total = mayores_64_anos.groupby('NOMBRE IPS').size().reset_index().rename(columns={0:'Mayores de 64 anos'})
GRUPOS_ADICIONALES = menores_16_anos_total.merge(mayores_64_anos_total, on= 'NOMBRE IPS')
CAPITA_POBLACIONES = EDAD_GRUPOS_SEXO.merge(EDAD_GRUPOS_TOTAL, on= 'NOMBRE IPS').merge(EDAD_RIAS, on= 'NOMBRE IPS').merge(INFORME_ESTADISTICO, on= 'NOMBRE IPS').merge(GESTION_RIESGO, on= 'NOMBRE IPS').merge(GRUPOS_ADICIONALES, on= 'NOMBRE IPS')
CAPITA_POBLACIONES.insert(0, 'FECHA_CAPITA', capita_date)
CAPITA_POBLACIONES = convert_columns_integer(CAPITA_POBLACIONES)
CAPITA_POBLACIONES['CODIGO IPS'] = CAPITA_POBLACIONES['CODIGO IPS'].astype(str)
LIST_SUMA_TOTALES = CAPITA_POBLACIONES.sum(numeric_only=True).to_list()
CAPITA_POBLACIONES.loc[CAPITA_POBLACIONES.shape[0]] = [capita_date,'COOPSANA IPS',None]+LIST_SUMA_TOTALES
CAPITA_POBLACIONES['Entre 1 y 4 anos - F'].fillna(0,inplace=True)

CAPITA_POBLACIONES.FECHA_CAPITA = func_process.pd.to_datetime(CAPITA_POBLACIONES.FECHA_CAPITA, errors='coerce')
CAPITA_POBLACIONES = clear_space(CAPITA_POBLACIONES)
# RENOMBRAR COLUMNAS
CAPITA_POBLACIONES.rename({'entre_5_y_14_anos_m':'entre_5_y_14_anos_anos_m'}, axis=1,inplace=True)
CAPITA_POBLACIONES.rename({'entre_6_y_19_anos_m':'entre_6_y_19_anos_anos_m'}, axis=1,inplace=True)
CAPITA_POBLACIONES.rename({'mayores_o_igual_de_40_anos':'mayores_de_40_anos'}, axis=1,inplace=True) 
CAPITA_POBLACIONES.rename({'menores_de_18_anos':'menores_a_18_anos'}, axis=1,inplace=True) 
CAPITA_POBLACIONES.rename({'mujeres_mayores_o_igual_de_18_anos':'mujeres_mayores_igual_18_anos'}, axis=1,inplace=True)
CAPITA_POBLACIONES.rename({'menores_o_igual_de_40_anos':'menores_a_41_anos'}, axis=1,inplace=True)
CAPITA_POBLACIONES.rename({'menores_o_igual_de_2_anos':'menores_a_3_anos'}, axis=1,inplace=True)
CAPITA_POBLACIONES.rename({'menores_de_16_anos':'menores_a_16_anos'}, axis=1,inplace=True)

# ELGIR EL ORDEN DE LAS COLUMNAS REQUERIDAS
CAPITA_POBLACIONES = CAPITA_POBLACIONES[COLUMNS_REQUIRED]

# Cargar a bigquery
validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_POBLACIONES_NUEVOS)
validate_load(validate_loads_logs,CAPITA_POBLACIONES)
