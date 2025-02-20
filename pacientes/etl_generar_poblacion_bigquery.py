import sys,os
import pandas as pd
from datetime import datetime,timedelta
from dotenv import load_dotenv
import locale
# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
PATH_DRIVE = os.environ.get("PATH_DRIVE")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


locale.setlocale(locale.LC_TIME, "es_ES.utf8") 
# VARIBLE - CONSTANTE
today = pd.to_datetime(datetime.now().strftime('%Y-%m-15'))
month_number = int(today.month)
year = today.strftime("%Y")
month_word = today.strftime("%B").capitalize()

# Parametros bigquery
project_id_product = 'ia-bigquery-397516'
dataset_id_pacientes = 'pacientes'

COLUMNS_REQUIRED = ['FECHA_CAPITA', 'NOMBRE_IPS','SEXO','Poblacion_mayor_2_anos',
       'Poblacion_menor_igual_4_anos', 'Poblacion_entre_5_19_anos',   
       'Poblacion_entre_3_15_anos', 'Poblacion_mayor_12_anos']
# Planificacion familiar
table_name_poblaciones_nuevos = 'poblaciones_odontologia'
TABLE_MARIADB_POBLACIONES_NUEVOS = 'poblaciones_odontologia'
TABLA_BIGQUERY_POBLACIONES_NUEVOS= f'{project_id_product}.{dataset_id_pacientes}.{table_name_poblaciones_nuevos}'

# Funcion para cargar cada archivo TXT de capita
def load_branch(path_capita, file):
    branch = func_process.pd.read_csv(f"{path_capita}/{file}", sep=",", usecols=[1,7,10,11], dtype={1:'object', 13:'object'}, encoding = "ISO-8859-1")
    branch.drop(branch.tail(2).index, inplace= True)
    return branch

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


def poblacion_anos(df, tipo, capita_date, e_i, e_f=0, s=False):
    if tipo != False:
        if s == False:
            if tipo == 'entre':
                df = df[(df['EDAD AÑOS'] >= e_i) & (df['EDAD AÑOS'] <= e_f)]
                df = df.groupby(['NOMBRE IPS']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_entre_{e_i}_{e_f}_anos"})
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                # df = df.append(df.sum(numeric_only=True), ignore_index=True)
                # df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)                
                df.insert(0, 'FECHA CAPITA', capita_date)
                df.insert(2, 'SEXO', 'M - F')

                return df
            elif tipo == 'igual':
                df = df[df['EDAD AÑOS'] == e_i]
                df = df.groupby(['NOMBRE IPS']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_igual_{e_i}_anos"})
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)               
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                df.insert(0, 'FECHA CAPITA', capita_date)
                df.insert(2, 'SEXO', 'M - F')

                return df
            elif tipo == 'mayor':
                df = df[df['EDAD AÑOS'] > e_i]
                df = df.groupby(['NOMBRE IPS']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_mayor_{e_i}_anos"})
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)                
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                df.insert(0, 'FECHA CAPITA', capita_date)
                df.insert(2, 'SEXO', 'M - F')

                return df
            elif tipo == 'mayor igual':
                df = df[df['EDAD AÑOS'] >= e_i]
                df = df.groupby(['NOMBRE IPS']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_mayor_igual_{e_i}_anos"})
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)                
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                df.insert(0, 'FECHA CAPITA', capita_date)
                df.insert(2, 'SEXO', 'M - F')

                return df
            elif tipo == 'menor igual':
                df = df[df['EDAD AÑOS'] <= e_i]
                df = df.groupby(['NOMBRE IPS']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_menor_igual_{e_i}_anos"})
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)                
                #df.insert(0, 'FECHA CAPITA', capita_date)
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                df.insert(0, 'FECHA CAPITA', capita_date)
                df.insert(2, 'SEXO', 'M - F')

                return df
        else:
            s=s.upper()
            if tipo == 'entre':
                df = df[(df['EDAD AÑOS'] >= e_i) & (df['EDAD AÑOS'] <= e_f) & (df['SEXO'] == s)]
                df = df.groupby(['NOMBRE IPS', 'SEXO']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_entre_{e_i}_{e_f}_anos"})
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                df['SEXO'].fillna(s, inplace= True)                
                df.insert(0, 'FECHA CAPITA', capita_date)            

                return df
            elif tipo == 'igual':
                df = df[(df['EDAD AÑOS'] == e_i) & (df['SEXO'] == s)]
                df = df.groupby(['NOMBRE IPS', 'SEXO']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_igual_{e_i}_anos"})
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)
                df['SEXO'].fillna(s, inplace= True)                
                df.insert(0, 'FECHA CAPITA', capita_date)
                
                return df
            elif tipo == 'mayor':
                df = df[(df['EDAD AÑOS'] > e_i) & (df['SEXO'] == s)]
                df = df.groupby(['NOMBRE IPS', 'SEXO']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_mayor_{e_i}_anos"})
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)
                df['SEXO'].fillna(s, inplace= True)                
                df.insert(0, 'FECHA CAPITA', capita_date)
                
                return df
            elif tipo == 'mayor igual':
                df = df[(df['EDAD AÑOS'] >= e_i) & (df['SEXO'] == s)]
                df = df.groupby(['NOMBRE IPS', 'SEXO']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_mayor_igual_{e_i}_anos"})
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)
                df['SEXO'].fillna(s, inplace= True)                
                df.insert(0, 'FECHA CAPITA', capita_date)
                
                return df
            elif tipo == 'menor':
                df = df[(df['EDAD AÑOS'] < e_i) & (df['SEXO'] == s)]
                df = df.groupby(['NOMBRE IPS', 'SEXO']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_menor_{e_i}_anos"})
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)
                df['SEXO'].fillna(s, inplace= True)                
                df.insert(0, 'FECHA CAPITA', capita_date)
                
                return df
            elif tipo == 'menor igual':
                df = df[(df['EDAD AÑOS'] <= e_i) & (df['SEXO'] == s)]
                df = df.groupby(['NOMBRE IPS', 'SEXO']).agg({'NUMERO DE IDENTIFICACION':'count'}).reset_index().rename(columns={
                    "NUMERO DE IDENTIFICACION":f"Poblacion_menor_igual_{e_i}_anos"})
                LIST_SUMA_TOTALES = df.sum(numeric_only=True).to_list()
                df.loc[df.shape[0]] = ['COOPSANA IPS']+LIST_SUMA_TOTALES
                #df = df.append(df.sum(numeric_only=True), ignore_index=True)
                #df['NOMBRE IPS'].fillna('COOPSANA IPS', inplace= True)
                df['SEXO'].fillna(s, inplace= True)
                df.insert(0, 'FECHA CAPITA', capita_date)
                
                return df
    
    else:
        return print("""Debe seleccionar un tipo de poblacion:
        Ejemplo, mayor, menor, igual o entre""")    


def gen_poblacion(ano, tipo, mes_i, mes_f, edad_i, edad_f= 0, sexo=False):
    
    if tipo == 'entre':
        df_final = func_process.pd.DataFrame({"FECHA CAPITA":[], "NOMBRE IPS":[], "SEXO":[], f"Poblacion_entre_{edad_i}_{edad_f}_anos":[]})
    elif tipo == 'igual':
        df_final = func_process.pd.DataFrame({"FECHA CAPITA":[], "NOMBRE IPS":[], "SEXO":[], f"Poblacion_igual_{edad_i}_anos":[]})
    elif tipo == 'mayor':
        df_final = func_process.pd.DataFrame({"FECHA CAPITA":[], "NOMBRE IPS":[], "SEXO":[], f"Poblacion_mayor_{edad_i}_anos":[]})
    elif tipo == 'mayor igual':
        df_final = func_process.pd.DataFrame({"FECHA CAPITA":[], "NOMBRE IPS":[], "SEXO":[], f"Poblacion_mayor_igual_{edad_i}_anos":[]})
    elif tipo == 'menor':
        df_final = func_process.pd.DataFrame({"FECHA CAPITA":[], "NOMBRE IPS":[], "SEXO":[], f"Poblacion_menor_{edad_i}_anos":[]})
    elif tipo == 'menor igual':
        df_final = func_process.pd.DataFrame({"FECHA CAPITA":[], "NOMBRE IPS":[], "SEXO":[], f"Poblacion_menor_igual_{edad_i}_anos":[]})
    else:
        return print("""Debe seleccionar un tipo de poblacion:
        Ejemplo: tipo='mayor', tipo='menor', tipo='igual', tipo='menor igual', tipo='mayor igual' o tipo='entre'""")
    
    for i in range(mes_i, mes_f+1):         
        global capita_date
        global path_capita
        capita_date = func_process.pd.to_datetime(f"{ano}-{i:02d}-15")        
        path_capita = f"{PATH_DRIVE}/BASES DE DATOS/{capita_date.strftime('%Y')}/{str(capita_date.month)}. {capita_date.strftime('%B').capitalize()}/CAPITA EPS SURA"
        
        try:
            df_capita = load_capita()
            
            format_string = "%d/%m/%Y"
            df_capita["FECHA NACIMIENTO"] = func_process.pd.to_datetime(df_capita["FECHA NACIMIENTO"], format=format_string)
            
            #invocamos la funcion en la columna  EDAD ANOS que estamos insertando
            df_capita['EDAD AÑOS'] = df_capita["FECHA NACIMIENTO"].apply(age_years)
            print(f"** Capita de {i} de {ano} Cargada Correctamente **")
        except Exception as e:
            print(f"** Error al cargar capita de {i} - {e}**")
            
            
        
        df_capita = df_capita.rename(columns={'GENERO':'SEXO'})        
        
        df_capita['NOMBRE IPS'].replace('COOPSANA - CENTRO', 'CENTRO', inplace=True)
        df_capita['NOMBRE IPS'].replace('COOPSANA CENTRO ARGENTINA', 'AVENIDA ORIENTAL', inplace=True)
        df_capita['NOMBRE IPS'].replace('IPS PAC COOPSANA SURAMERICANA', 'PAC', inplace=True)
        df_capita['NOMBRE IPS'].replace('COOPSANA NORTE', 'NORTE', inplace=True)
        df_capita['NOMBRE IPS'].replace('COOPSANA CALASANZ', 'CALASANZ', inplace=True)        
        
        #Generamos el Df con la poblacion
        df_final_m = poblacion_anos(df_capita, tipo, capita_date, edad_i, edad_f, sexo)
        # print(df_final_m)
        
        #df_final = df_final.append(df_final_m, ignore_index = True)
        df_final =pd.concat([df_final,df_final_m])
    
    df_final[df_final.columns[-1]] = df_final[df_final.columns[-1]].astype('int64')
    return df_final

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

df_poblacion = gen_poblacion(ano=year, tipo='mayor', mes_i=month_number, mes_f=month_number, edad_i=2)
df_may_2 = df_poblacion
df_poblacion = gen_poblacion(ano=year, tipo='menor igual', mes_i=month_number, mes_f=month_number, edad_i=4)
df_men_igu_4 = df_poblacion
df_poblacion = gen_poblacion(ano=year, tipo='entre', mes_i=month_number, mes_f=month_number, edad_i=5, edad_f=19)
df_ent_5_19 = df_poblacion
df_poblacion = gen_poblacion(ano=year, tipo='entre', mes_i=month_number, mes_f=month_number, edad_i=3, edad_f=15)
df_ent_3_15 = df_poblacion
df_poblacion = gen_poblacion(ano=year, tipo='mayor', mes_i=month_number, mes_f=month_number, edad_i=12)
df_may_12 = df_poblacion
df_poblacion_final = df_may_2.merge(
                                    df_men_igu_4, 
                                    on=['FECHA CAPITA','NOMBRE IPS','SEXO']
                            ).merge(
                                    df_ent_5_19,
                                    on=['FECHA CAPITA','NOMBRE IPS','SEXO']
                            ).merge(
                                    df_ent_3_15,
                                    on=['FECHA CAPITA','NOMBRE IPS','SEXO']
                            ).merge(df_may_12,
                                    on=['FECHA CAPITA','NOMBRE IPS','SEXO']
                            )

# RENAME COLUMNS
df_poblacion_final.columns = COLUMNS_REQUIRED

# Cargar a bigquery
validate_loads_logs =  loadbq.validate_loads_monthly(TABLA_BIGQUERY_POBLACIONES_NUEVOS)
validate_load(validate_loads_logs,df_poblacion_final)



