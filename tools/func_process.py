#Database management
from ast import Try
import mysql.connector as mariadb
from mysql.connector import Error
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Integer, String, Unicode, DateTime, Text
from sqlalchemy.orm import sessionmaker

#Data management
import numpy as np
import pandas as pd
import requests

#Dates management
import time
from datetime import date, timedelta, datetime
import os
import locale

# Nombre del mes en español
#import locale
#locale.setlocale(locale.LC_TIME, "es_CO") # Colombia

#Visualization
# import seaborn as sns
# import matplotlib.pyplot as plt

#Advertencias
import warnings
warnings.filterwarnings('ignore')

def timer(func):
    """A decorator that prints how long a function took to run."""
    # Define the wrapper function to return.
    def wrapper(*args, **kwargs):
        # When wrapper() is called, get the current time.
        t_start = time.time()
        # Call the decorated function and store the result.
        result = func(*args, **kwargs)
        # Get the total time it took to run, and print it.
        t_total = time.time() - t_start
        print('{} took {}s'.format(func.__name__, t_total))
        return result
    return wrapper

HOST = os.environ.get("DB_HOST")
PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
PASSWORD = os.environ.get("DB_PASSWORD")
DB_ANALITICA = 'analitica'
DB_REPORTES = 'reportes'


CONFIG_MARIADB = {'user':DB_USER, 'password':PASSWORD, 'host':HOST,'port':PORT}
# Funcion que consulta en la base de datos indicada la consulta relacionada
@timer
def load_df_server(sql, data_base):
    """Funcion que trae del servidor la tabla o df que se le consulte en sql,
    especificando el nombre de la base de datos a consultar data_base"""
    
    if (data_base != 'analitica' and data_base != 'reportes'):
        print('No se identifica base de datos destino: \n', data_base)
        return    
    try:
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{PASSWORD}@{HOST}:{PORT}/{data_base}")
    except SQLAlchemyError as e:
        print('Ocurrió un error con el engine_reportes: \n', e.__cause__)        
    try:
        with engine.connect() as cnx_engine:
            df = pd.read_sql(sql, cnx_engine)
            print('DF cargado con exito')
            return df
    except SQLAlchemyError as e:
        print('Ocurrió un error al realizar la consulta: \n', e.__cause__)
        return
    
def command_sql(sql, data_base):
    try:
        session = create_engine(f"mysql+pymysql://{DB_USER}:{PASSWORD}@{HOST}:{PORT}/{data_base}")
    except SQLAlchemyError as e:
        print('Ocurrió un error con el engine_reportes: \n', e.__cause__)        
    try:
        session.execute(sql)
        session.commit()
        print(f"La tabla {tabla} ha sido truncada con éxito.")
    except Exception as e:
        session.rollback()
        print(f"Error al truncar la tabla {tabla_a_truncar}: {e}")
    finally:
        session.close()

def insert_rows(sql_insert):
    try:
        
        engine = mariadb.connect(**CONFIG_MARIADB)
        conn = engine.cursor()
        conn.execute(sql_insert)
        engine.commit()
       
        # Cerra conexion
        conn.close()
        engine.close()
        return print('Save successfully') 
    except SQLAlchemyError as e:
        print('Ocurrió un error con el engine_reportes: \n', e.__cause__)        
    
    
    
@timer
def save_df_server(df, name, data_base, if_exist='append'):
    """Funcion que guarda en el servidor de bases de datos el df que se le asigne df:'DataFrame'
    especificando nombre name:'str', nombre de la base de datos data_base:'str' y que accion
    realizar en caso de que la tabla exista if_exist:'str' - defaul='append'"""
    
    if (data_base != 'analitica' and data_base != 'reportes' and data_base != 'facturaElectronica'):
        print('No se identifica base de datos destino: \n', data_base)
        return    
    try:
        engine = create_engine(f"mysql+pymysql://{DB_USER}:{PASSWORD}@{HOST}:{PORT}/{data_base}")
    except SQLAlchemyError as e:
        print('Ocurrió un error con el engine_reportes: \n', e.__cause__)    
    try:
        with engine.connect() as cnx_database:            
            df.to_sql(name, con=cnx_database, if_exists=if_exist, index= False, chunksize= 20000)
            return print('Save successfully')        
    except SQLAlchemyError as e:
        return print(e.__dict__['orig'])

@timer    
def load_df_mariadb_server(sql, data_base):
    """Funcion que trae del servidor la tabla o df que se le consulte en sql:'str',
    especificando el nombre de la base de datos a consultar data_base:'str'
    Este procedimiento lo ejecuta con la libreria de MariaDB"""
    
    if (data_base != 'analitica' and data_base != 'reportes'):
        print('No se identifica base de datos destino: \n', data_base)
        return
    try:    
        conexion_db = mariadb.connect(host=HOST,port=PORT,user=DB_USER,password=PASSWORD,database=data_base)
        try:        
            df = pd.read_sql(sql, conexion_db)       
            print("Conexion con éxito")
            return df
        finally:        
            conexion_db.close()        
    except Error as e:
        print("Ocurrió un error al conectar: ", e)
        
# Peticion y creacion del DataFrame con cada uno de los registros
def get_google_sheet(id_sheet, name_sheet):
    """Funcion que toma el id de la hoja de calculo de google drive y el nombre de la hoja
    y por medio de una api desarrollada crea el DF de dicha hoja de calculo"""
    
    try:
        response = requests.get(f'https://apps.coopsana.co:7154/googleSheets/read/{id_sheet}/{name_sheet}')
        data= response.json()    
        df_sheet = pd.DataFrame(data['rows'], columns = data['columns'])
        return df_sheet
    except:
        print(f"""{data['error']}
        {data['message']}
        {data['status']}""")

# Proceso en el cual consulto los archivos de Hojas de calculo de DRIVE 
# con los roles de los diferentes medicos

# Creacion de un solo DataFrame con los medicos y sus roles
def get_roles_sedes(get_google_sheet):
    """Funcion que toma todos los DF de roles de cada sede, llamados con la funcion mencionada
    y devuelve uno solo"""

    # Id y nombre de cada hoja en Drive
    id_ao = '1PfWP54HztgDHezRCYgmeEoeIqOhl3cBH-6D5TZI3rmI'
    id_ce = '1i888oFFG3iuEZUh-wHOCA8Umu9TPOEfXm2CYcPqd1Yk'
    id_ca = '1y3RWFKUkeCznWPqccEVjUUZjJ3Wb8tnrCawsLWJ30CY'
    id_no = '1Izt6vvSbUOPpEgcAzsfov6-VSRFjqICcYLIB5wlCqaw'
    id_pa = '1HZ1v-wL0R3fOuhJ0_2KOTYsL2eVSoT4EQWRbH9mSKG0'
    id_cgr = '1UMPKyutb1v9HtZPAgA20RM0-5XyKfwgiTRbQCf4qVZc'
    
    rol_ao = get_google_sheet(id_ao, 'BD')
    rol_ce = get_google_sheet(id_ce, 'BD')
    rol_ca = get_google_sheet(id_ca, 'BD')
    rol_no = get_google_sheet(id_no, 'BD')
    rol_pa = get_google_sheet(id_pa, 'BD')
    rol_cgr = get_google_sheet(id_cgr, 'BD')

    roles_sedes = pd.concat([rol_ao, rol_ce, rol_ca, rol_no, rol_pa, rol_cgr])
    return roles_sedes


def format_roles_sedes(df, rol):
    """Funcion que toma el DataFrame que viene de la otra funcion que o que toma todo los 
    DataFrame concatenados y filtra por el nombre del rol que se le indique como parametro
    Param1 - df: DataFrame --> df con todos los medicos y roles concatenados
    Param1 - rol: String --> nombre del rol
    """
    df = df[(df['Rol'] == rol) | (df['Rol 2'] == rol)]
    df.drop('Observaciones', axis=1, inplace=True)
    df['Identificacion'] = df['Identificacion'].astype('str')
    return df

def configure_locale():
    try:
        os.environ['LANG'] = 'es_ES.UTF-8'
        os.environ['LC_ALL'] = 'es_ES.UTF-8'
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
        print(f"Locale configurado: {locale.getlocale()}")
    except locale.Error:
        print("No se pudo configurar el locale. Usando el valor predeterminado.")
        locale.setlocale(locale.LC_ALL, '')  
        print(f"Locale predeterminado: {locale.getlocale()}")