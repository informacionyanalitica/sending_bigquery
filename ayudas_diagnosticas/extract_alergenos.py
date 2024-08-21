import sys,os
import pandas as pd
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process

#PATH_FILE = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQiFMqEJ41-d8T84B6EK1XMGpmD2kX5wqQEhEZeG-ZFE7g2CFRFHcTxYlnv9x-f11Ef9e8p4hNointC/pub?output=xlsx'
#PATH_FILE = 'G:\Mi unidad'

ID_SHEET ='1GsEAaS8YmClFLH4tu2QErYpRQi9mtr6xGyTbpLuiZZ4'
NAME_SHEET = 'ALERGENOS'




DICT_RENAME_COLUMN = {'Número de orden':'numero_orden',
                      'Fecha ingreso':'fecha_ingreso',
                        'Nombres':'nombre',
                        'Primer apellido':'primer_apellido',
                        'Segundo apellido':'segundo_apellido',
                        'Sede':'sede',
                        'Valor total':'valor_total',
                        'Nombre enterprise':'codigo'
                            }

def read_files_excel():
    try:
        #df_alergenos = pd.read_excel(path)
        df_alergenos = func_process.get_google_sheet(ID_SHEET, NAME_SHEET)
        return df_alergenos
    except ValueError as err:
        return err

def rename_columns(df):
    df.rename(DICT_RENAME_COLUMN, axis=1, inplace=True)
    return df

def format_columns(df):    
    try:
        df.valor_total = df.valor_total.astype(int)
        df.fecha_ingreso = pd.to_datetime(df.fecha_ingreso)
        df.numero_orden = df.numero_orden.astype(str)
        df.codigo = df.codigo.astype(str)
        df.fecha_ingreso = df.fecha_ingreso.dt.strftime('%Y-%m-15')
        df.numero_orden = [num.replace('.0','') for num in df.numero_orden]
        df.codigo = [cod.replace('.0','') for cod in df.codigo]
        return df
    except ValueError as err:       
        return err
    
def sum_values_orden(df):
    sum_total_orden = df.groupby(['numero_orden','codigo','fecha_ingreso']).agg({'valor_total':'sum'}).reset_index()
    sum_total_orden = sum_total_orden[sum_total_orden.numero_orden != 'nan']
    return sum_total_orden

def execute_total_alergenos(fecha_capita):
    try:
        data_alergenos = read_files_excel()
        df_alergenos = rename_columns(data_alergenos)
        df_alergenos = format_columns(df_alergenos)
        df_totales = sum_values_orden(df_alergenos)
        df_totales_mes_actual = df_totales[df_totales.fecha_ingreso == fecha_capita]
        df_totales_mes_actual.drop('fecha_ingreso', axis=1, inplace=True)
        return df_totales_mes_actual
    except ValueError as err:      
        return err
    
    
#print(execute_total_alergenos('2023-05-15'))