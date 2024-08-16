import pandas as pd
import numpy as np
import sys,os

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


SQL_PAGOS_BD = """SELECT *
                FROM pagosHistoricosView AS p
            WHERE date(p.fechaPago) >= CURDATE()
            
                """
                
SQL_BIGQUERY = """
                SELECT g.factura
                FROM {} as g
                WHERE factura IN {}
                """

project_id_product = 'ia-bigquery-397516'
dataset_id_recaudo = 'recaudos'
table_name_pagos = 'pagos'
validator_column = 'factura'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_recaudo}.{table_name_pagos}'
 
# Leer datos
df_pagos_bd = func_process.load_df_server(SQL_PAGOS_BD, 'reportes')   

# Obtener datos no duplicados
valores_unicos = tuple(map(str,df_pagos_bd[validator_column]))
df_pagos_not_duplicates = loadbq.rows_not_duplicates(df_pagos_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

# Save data
loadbq.load_data_bigquery(df_pagos_not_duplicates,TABLA_BIGQUERY)


