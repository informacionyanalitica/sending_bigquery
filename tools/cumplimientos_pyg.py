
import func_process
import pandas as pd
import os, sys
from datetime import datetime 

# Variables
today = datetime.now()
year = today.year


# Extract data
id_presupuesto_2022 = '1kqVpSgTYEBIlxo12xRfZ56whqXFnh45va1XX4MFKIo8'
sheet_ce = 'Centro'
sheet_od_ce = 'Odontologia_Centro'
sheet_no = 'Norte'
sheet_od_no = 'Odontologia_Norte'
sheet_av = 'Avenida_Oriental'
sheet_od_av = 'Odontologia_Avenida_Oriental'
sheet_ca = 'Calasanz'
sheet_od_ca = 'Odontologia_Calasanz'
sheet_pa = 'PAC'
sheet_od_pa = 'Odontologia_PAC'
sheet_ce_es = 'Centro de Especialistas'
sheet_bo = 'Bolivia'
sheet_ay = 'Ayudas_DX'




def transform_columns_pyg(df):
    df['Cumplimiento PYG'] = df['Cumplimiento PYG'].str.replace('.','')
    df['Cumplimiento PYG'] = df['Cumplimiento PYG'].astype('int64')
    df['Mes'] = [mes.upper() for mes in df.Mes ]
    df.rename({'centro_de_costo':'centroCosto','Mes':'fechaProceso','Cumplimiento PYG':'cumplimiento_pyg' },axis=1,inplace=True)
    return df

def get_data_pyg():
    pre_ce = func_process.get_google_sheet(id_presupuesto_2022, sheet_ce)
    pre_ce = pre_ce[pre_ce['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_ce['centro_de_costo'] = 'CENTRO'
    
    pre_od_ce = func_process.get_google_sheet(id_presupuesto_2022, sheet_od_ce)
    pre_od_ce = pre_od_ce[pre_od_ce['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_od_ce['centro_de_costo'] = 'ODONTOLOGIA CENTRO'
    
    pre_no = func_process.get_google_sheet(id_presupuesto_2022, sheet_no)
    pre_no = pre_no[pre_no['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_no['centro_de_costo'] = 'NORTE'

    pre_od_no = func_process.get_google_sheet(id_presupuesto_2022, sheet_od_no)
    pre_od_no = pre_od_no[pre_od_no['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_od_no['centro_de_costo'] = 'ODONTOLOGIA NORTE'
        
    pre_av = func_process.get_google_sheet(id_presupuesto_2022, sheet_av)
    pre_av = pre_av[pre_av['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_av['centro_de_costo'] = 'AVENIDA ORIENTAL'
    
    pre_od_av = func_process.get_google_sheet(id_presupuesto_2022, sheet_od_av)
    pre_od_av = pre_od_av[pre_od_av['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_od_av['centro_de_costo'] = 'ODONTOLOGIA AVENIDA ORIENTAL'
    
    pre_ca = func_process.get_google_sheet(id_presupuesto_2022, sheet_ca)
    pre_ca = pre_ca[pre_ca['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_ca['centro_de_costo'] = 'CALASANZ'
    
    pre_od_ca = func_process.get_google_sheet(id_presupuesto_2022, sheet_od_ca)
    pre_od_ca = pre_od_ca[pre_od_ca['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_od_ca['centro_de_costo'] = 'ODONTOLOGIA CALASANZ'
    
    pre_pa = func_process.get_google_sheet(id_presupuesto_2022, sheet_pa)
    pre_pa = pre_pa[pre_pa['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_pa['centro_de_costo'] = 'PAC'
    
    pre_od_pa = func_process.get_google_sheet(id_presupuesto_2022, sheet_od_pa)
    pre_od_pa = pre_od_pa[pre_od_pa['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_od_pa['centro_de_costo'] = 'ODONTOLOGIA PAC'
    
    pre_ce_es = func_process.get_google_sheet(id_presupuesto_2022, sheet_ce_es)
    pre_ce_es = pre_ce_es[pre_ce_es['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_ce_es['centro_de_costo'] = 'CENTRO DE ESPECIALISTAS'
    
    pre_bo = func_process.get_google_sheet(id_presupuesto_2022, sheet_bo)
    pre_bo = pre_bo[pre_bo['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_bo['centro_de_costo'] = 'BOLIVIA'
    
    pre_ay = func_process.get_google_sheet(id_presupuesto_2022, sheet_ay)
    pre_ay_cumplimiento = pre_ay[pre_ay['Items'] == 'Total Ingresos Netos'][['Mes','Cumplimiento PYG']]
    pre_ay_ingresos_imagenes = pre_ay[pre_ay['Items'] == 'Ingreso Imágenes'][['Mes','Cumplimiento PYG']]
    pre_ay = pre_ay_cumplimiento.merge(pre_ay_ingresos_imagenes, how='left' ,on='Mes',)
    pre_ay['ingreso_imagenes'] = [0 if value is None else int(value.replace('.','')) for value in pre_ay['Cumplimiento PYG_y']]
    pre_ay['Cumplimiento PYG']  = pre_ay['Cumplimiento PYG_x'].str.replace('.','').astype(int)
    pre_ay['Cumplimiento PYG']  = pre_ay['Cumplimiento PYG'] .astype(str)
    pre_ay.drop(['Cumplimiento PYG_x','Cumplimiento PYG_y'],axis=1, inplace=True)
    pre_ay['centro_de_costo'] = 'AYUDAS DIAGNOSTICAS'
    
    
      
    centro_costo = pd.concat(
        [pre_ce,
        pre_od_ce,
        pre_no,
        pre_od_no,
        pre_av,
        pre_od_av,
        pre_ca,
        pre_od_ca,
        pre_pa,
        pre_od_pa,
        pre_ce_es,
        pre_bo,
        pre_ay
        ]
    )
    return centro_costo



def get_cumplimiento_pyg_sedes():
    data_pyg= get_data_pyg()
    cumplimiento_pyg_sedes = transform_columns_pyg(data_pyg)
    return cumplimiento_pyg_sedes

def get_cumplimiento_pyg_coopsana(df_pyg_sedes):
    df_pyg_sedes['ingreso_imagenes'] = df_pyg_sedes['ingreso_imagenes'].fillna(0)
    df_pyg_sedes['cumplimiento_pyg_sin_imagenes'] =df_pyg_sedes['cumplimiento_pyg'] - df_pyg_sedes['ingreso_imagenes']
    cumplimiento_pyg_coopsana = df_pyg_sedes.groupby(['fechaProceso']).agg({'cumplimiento_pyg_sin_imagenes':'sum'}).reset_index()
    cumplimiento_pyg_coopsana.rename({'cumplimiento_pyg_sin_imagenes':'cumplimiento_pyg_coopsana'}, axis=1, inplace=True)
    return cumplimiento_pyg_coopsana


def get_cumplimiento_pyg_sedes_totales(df_pyg):
    cumplimiento_pyg_sedes_principales = df_pyg[df_pyg.centroCosto.isin(['CENTRO','NORTE',
                                                                        'AVENIDA ORIENTAL','CALASANZ','PAC'])]
    cumplimiento_pyg_sedes_totales = cumplimiento_pyg_sedes_principales.groupby(['fechaProceso']).agg({'cumplimiento_pyg':'sum'}).reset_index()
    cumplimiento_pyg_sedes_totales.rename({'cumplimiento_pyg':'cumplimiento_pyg_sedes_totales'}, axis=1, inplace=True)
    return cumplimiento_pyg_sedes_totales

# Extract
df_pyg = get_cumplimiento_pyg_sedes()
df_pyg_coopsana = get_cumplimiento_pyg_coopsana(df_pyg)
df_pyg_sedes_totales = get_cumplimiento_pyg_sedes_totales(df_pyg)


#Merge
df_cumplimientos_pyg = df_pyg.merge(df_pyg_coopsana, how='inner', on='fechaProceso')
df_pyg_sedes_totales = df_cumplimientos_pyg.merge(df_pyg_sedes_totales, how='inner', on='fechaProceso')
df_pyg_sedes_totales.drop(['ingreso_imagenes','cumplimiento_pyg_sin_imagenes'],axis=1, inplace=True)

