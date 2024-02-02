import pandas as pd
import numpy as np
import sys,os

PATH_TOOLS = os.environ.get("PATH_TOOLS")
path = os.path.abspath(PATH_TOOLS)
sys.path.insert(1,path)
import func_process
import load_bigquery as loadbq


SQL_PAGOS_BD = """SELECT p.id_pago,p.factura,p.valorCobrar,p.numeroAutorizacion,
            p.numeroIdentificacionPaciente,p.estadoPago AS idEstadoPago,c.descripcion_estadoOrden AS estadoPago,
            p.tipo_pago,tp.tipo_pago AS tipoPago,p.resolucion,p.numero_adicional,
            p.codigoTipoCobro,cp.descripcion_codigoTipoCobro AS tipoCobro,dl.hv_identificacion AS documentoUsuarioCobra,
            p.usuarioCobra,p.codigo_sede_cobra AS codigoSedeCobra,ms.nombre_sede AS nombreSedeCobra,
            p.detalle_pago,p.id_servicio_pago_particular AS idServicioPagoParticular,csp.nombreServicioPagoParticular,
            p.fechaPago,p.fecha_cierre
            FROM recaudo_sura.pagos AS p 
            JOIN recaudo_sura.estadoOrden AS c ON c.id_estadoOrden = p.estadoPago
            JOIN central.maestra_tipo_pago AS tp ON tp.id_tipo_pago = p.tipo_pago
            JOIN recaudo_sura.codigoTipoCobro AS cp ON cp.id_codigoTipoCobro=p.codigoTipoCobro
            JOIN miturno.maestra_sedes AS ms ON ms.codigo_sede_interno = p.codigo_sede_cobra
            JOIN recaudo_sura.codigoServiciosPagosParticulares AS csp ON csp.idServicioPagoParticular = p.id_servicio_pago_particular
            JOIN intranet.usuariosAccesos ua ON ua.usuario = p.usuarioCobra
            JOIN hojas_vida.datos_laborales dl ON dl.hv_id_experiencia = ua.id_hv_experiencia
            WHERE date(p.fechaPago) >= adddate(curdate(), interval -7 day)
                """
                
SQL_BIGQUERY = """
                SELECT g.factura
                FROM {} as g
                WHERE factura IN {}
                """

project_id_product = 'ia-bigquery-397516'
dataset_id_recaudo = 'recaudos'
table_name_pagos = 'pagos_partition'
validator_column = 'factura'

TABLA_BIGQUERY = f'{project_id_product}.{dataset_id_recaudo}.{table_name_pagos}'
 
# Leer datos
df_pagos_bd = func_process.load_df_server(SQL_PAGOS_BD, 'reportes')   

# Obtener datos no duplicados
valores_unicos = tuple(map(str,df_pagos_bd[validator_column]))
df_pagos_not_duplicates = loadbq.rows_not_duplicates(df_pagos_bd,validator_column,SQL_BIGQUERY,TABLA_BIGQUERY,valores_unicos)

# Save data
loadbq.load_data_bigquery(df_pagos_not_duplicates,TABLA_BIGQUERY)


