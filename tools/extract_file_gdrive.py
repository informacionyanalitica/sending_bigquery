import requests
import pandas as pd
import sys, os


# path = os.path.abspath('../conexion')
# sys.path.insert(1, path)
# import conexion as BD

def getIdsGoogleSheet(path_folder):
    try:
        response = requests.post('http://localhost:5000/drive',json=path_folder)
        elementsDriveJson= response.json()  
        return pd.DataFrame(elementsDriveJson,columns=['name','id','typeFile'])
    except ValueError as err:
        print(err)    
        
def getIdFileSheet(elementsDrive,nameFile):
    try:
        elementsDrive = elementsDrive[elementsDrive['name']==nameFile]
        if elementsDrive.shape[0]>0:
            return elementsDrive
        else:
            return ("No existen archivos que coincidan")
    except ValueError as err:
        print(err)
        
def readFile(id,namePage="Hoja 1"):
    try:
        response = requests.get('https://apps.coopsana.co:7154/googleSheets/read/{}/{}'.format(id,namePage))
    except ValueError as err:
        print(err)
    data= response.json()  
    # df = pd.DataFrame(data['rows']) #.dropna(axis=1)
    # df.columns = data['columns']
    return data