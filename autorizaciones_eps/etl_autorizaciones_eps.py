"""
ETL Autorizaciones EPS
----------------------
Lee los reportes de autorizaciones de las EPS (Coosalud, Salud Total, Sanitas, SURA)
desde el Drive montado en el servidor, los limpia a un formato unico y los carga en
autorizacionesEps.autorizaciones (MariaDB). Registra cada archivo en autorizacionesEps.cargas.

Estructura esperada de carpetas (una subcarpeta por EPS):
    {PATH_DRIVE_EPS}/COOSALUD/
    {PATH_DRIVE_EPS}/SALUD_TOTAL/
    {PATH_DRIVE_EPS}/SANITAS/
    {PATH_DRIVE_EPS}/SURA/

Uso:
    python etl_autorizaciones_eps.py                 -> procesa archivos nuevos de PATH_DRIVE_EPS
    python etl_autorizaciones_eps.py /ruta/carpeta   -> procesa archivos nuevos de otra carpeta (pruebas)

Variables .env:
    PATH_DRIVE        Drive montado (Informacion y Analitica)
    PATH_DRIVE_EPS    (opcional) carpeta raiz de los reportes. Por defecto {PATH_DRIVE}/AUTORIZACIONES EPS
    DB_HOST_EPS       host de MariaDB (desarrollo o produccion)
    DB_PORT_EPS       puerto (por defecto 3306)
    DB_USER_EPS       usuario propio del ETL (solo permisos sobre autorizacionesEps)
    DB_PASSWORD_EPS   clave de ese usuario

Control de cargas y eventos (acordado con desarrollo):
    - cargas: un registro por archivo (OK / ERROR). Los archivos en ERROR se reprocesan solos
      en la siguiente ejecucion porque solo se omiten los que quedaron OK.
    - data_load_event: un evento PENDING por cada carga OK con registros nuevos o actualizados.
      Se crea al final, despues de homologacion y vigencia. Desarrollo lee los PENDING y cambia el estado.
"""
import os
import re
import sys
import time
import hashlib
import logging
import traceback
import uuid
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

load_dotenv()

# ---------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------
BASE_DATOS = "autorizacionesEps"
PATH_DRIVE = os.environ.get("PATH_DRIVE", "")
PATH_DRIVE_EPS = os.environ.get("PATH_DRIVE_EPS", f"{PATH_DRIVE}/AUTORIZACIONES EPS")

EXTENSIONES = (".xlsx", ".xls")

EVENT_TYPE = "data.load.completed"

# Tabla de eventos (la define desarrollo). Desarrollo esta pasando las columnas de snake_case
# a camelCase; el ETL detecta cual de las dos versiones tiene la tabla y usa esa.
TABLA_EVENTOS = "data_load_event"
COLUMNAS_EVENTOS = {
    "camelCase": {"eventId": "eventId", "idCarga": "idCarga", "eventType": "eventType"},
    "snake_case": {"eventId": "event_id", "idCarga": "id_carga", "eventType": "event_type"},
}

# ---------------------------------------------------------------------
# Logs: solo por consola (Airflow los guarda en el log de la tarea)
# ---------------------------------------------------------------------
def configurar_log():
    logger = logging.getLogger("etl_autorizaciones_eps")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    consola = logging.StreamHandler(sys.stdout)
    consola.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(consola)
    return logger


log = configurar_log()

COLUMNAS_DESTINO = [
    "eps", "numeroAutorizacion", "numeroSolicitud", "tipoIdentificacionPaciente",
    "numeroIdentificacionPaciente", "nombrePaciente", "cups", "codigoServicioEps",
    "descripcion", "especialidad", "llaveServicio", "cantidad", "estadoOrden",
    "diagnostico", "categoria", "numeroContrato", "fechaImpresion",
    "fechaVencimientoAutorizacion", "vencimientoCalculado", "fechaAtencion", "valorCobrar", "regimen",
    "nombreSucursalPrestador", "datosOriginales", "idCarga",
]

# Columnas que se actualizan si la autorizacion ya existe (llega en otro reporte)
COLUMNAS_ACTUALIZAR = [
    "numeroSolicitud", "tipoIdentificacionPaciente", "nombrePaciente", "cups",
    "codigoServicioEps", "descripcion", "cantidad", "estadoOrden", "diagnostico",
    "categoria", "numeroContrato", "fechaImpresion", "fechaVencimientoAutorizacion",
    "vencimientoCalculado", "fechaAtencion", "valorCobrar", "regimen", "nombreSucursalPrestador",
    "datosOriginales", "idCarga",
]

TIPOS_DOCUMENTO = {
    "CEDULA DE CIUDADANIA": "CC", "TARJETA IDENTIDAD": "TI", "TARJETA DE IDENTIDAD": "TI",
    "REGISTRO CIVIL": "RC", "CEDULA DE EXTRANJERIA": "CE", "PASAPORTE": "PA",
    "PERMISO POR PROTECCION TEMPORAL": "PT", "PERMISO ESPECIAL DE PERMANENCIA": "PE",
    "MENOR SIN IDENTIFICACION": "MS", "ADULTO SIN IDENTIFICACION": "AS",
}


# ---------------------------------------------------------------------
# Utilidades de limpieza
# ---------------------------------------------------------------------
def a_fecha(serie):
    """Convierte fechas en cualquier formato (texto, ISO, yyyy/mm/dd, dd/mm/yyyy,
    serial de Excel) a datetime. 9999-12-31 y valores sin fecha quedan vacios."""
    def convertir(valor):
        if pd.isna(valor):
            return pd.NaT
        v = str(valor).strip()
        if not v or v.startswith("9999"):
            return pd.NaT
        if v.replace(".", "", 1).isdigit():                     # serial de Excel (46300)
            n = float(v)
            return pd.Timestamp("1899-12-30") + pd.Timedelta(days=n) if 20000 < n < 80000 else pd.NaT
        if ":" in v and "-" not in v and "/" not in v:          # solo hora, sin fecha
            return pd.NaT
        dia_primero = len(v) >= 3 and v[2] == "/"               # dd/mm/yyyy
        return pd.to_datetime(v, errors="coerce", dayfirst=dia_primero)
    return pd.to_datetime(serie.map(convertir), errors="coerce")


def texto(serie):
    s = serie.astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    return s.where(~s.isin(["", "nan", "NaN", "None", "NaT"]), None)


def numero(serie):
    return pd.to_numeric(serie, errors="coerce")


def tipo_documento(serie):
    s = texto(serie)
    return s.map(lambda x: TIPOS_DOCUMENTO.get(str(x).upper(), str(x)[:3]) if x else None)


PATRONES_ESPECIALIDAD = [
    r"ESPECIALISTA EN (.+)$",                                   # ... POR ESPECIALISTA EN UROLOGIA
    r"^CONSULTA\b.*?\bPOR\s+(.+)$",                             # CONSULTA DE CONTROL POR REUMATOLOGIA
    r"^(?:CONSULTA|CONTROL)\s+(?:DE\s+\w+\s+)?EN\s+(.+)$",      # CONSULTA EN ORTOPEDIA (SURA)
]


def especialidad_desde_descripcion(desc):
    """Saca la especialidad de descripciones tipo 'CONSULTA ... POR ESPECIALISTA EN UROLOGIA'.
    Es solo un valor por defecto: la tabla homologacionServicios tiene prioridad."""
    if not desc:
        return None
    d = str(desc).upper()
    for patron in PATRONES_ESPECIALIDAD:
        m = re.search(patron, d)
        if m:
            return m.group(1).strip()[:100]
    return None


def llave_servicio(cups, descripcion):
    if cups:
        return str(cups)
    return hashlib.md5(str(descripcion or "").encode("utf-8")).hexdigest()


def datos_originales(df):
    return df.apply(lambda fila: fila.to_json(force_ascii=False, date_format="iso"), axis=1)


# ---------------------------------------------------------------------
# Limpieza por EPS (cada una devuelve un DataFrame con COLUMNAS_DESTINO sin idCarga)
# ---------------------------------------------------------------------
def limpiar_sanitas(ruta):
    df = pd.read_excel(ruta, sheet_name=0, dtype=str)           # la hoja 0 es "BD"; las demas son tablas dinamicas
    df = df.rename(columns={df.columns[0]: "numeroAutorizacion"}).dropna(subset=["numeroAutorizacion"])
    partes = df["Procedimiento / Medicamento"].str.split(" - ", n=1, expand=True)
    codigo = texto(partes[0])
    salida = pd.DataFrame({
        "numeroAutorizacion": texto(df["numeroAutorizacion"]),
        "numeroIdentificacionPaciente": texto(df["Identificacion Afiliado"]),
        "nombrePaciente": texto(df["Nombre Afiliado"]),
        "codigoServicioEps": codigo,
        "cups": codigo.where(codigo.str.fullmatch(r"\d{6}", na=False), None),
        "descripcion": texto(partes[1]) if 1 in partes.columns else None,
        "estadoOrden": texto(df["Estado"]),
        "fechaImpresion": a_fecha(df["Fecha expedicion"]),
        "fechaVencimientoAutorizacion": a_fecha(df["VIGENCIA HASTA"]),
        "nombreSucursalPrestador": texto(df["Nombre Prestador"]),
        "datosOriginales": datos_originales(df),
    })
    return salida


def limpiar_salud_total(ruta):
    df = pd.read_excel(ruta, sheet_name=0, dtype=str)
    codigo = texto(df["Código Servicio"])
    salida = pd.DataFrame({
        "numeroAutorizacion": texto(df["No. Solicitud"]),
        "numeroSolicitud": texto(df["IdRegistro"]),
        "tipoIdentificacionPaciente": tipo_documento(df["Tipo Documento"]),
        "numeroIdentificacionPaciente": texto(df["Número Documento"]),
        "nombrePaciente": texto(df["Nombre Protegido"]),
        "codigoServicioEps": codigo,
        "cups": codigo.str[:6],                                  # 10 digitos: los 6 primeros son el CUPS
        "descripcion": texto(df["Nombre Servicio"]),
        "cantidad": numero(df["Cantidad"]),
        "estadoOrden": texto(df["Estado"]),
        "categoria": texto(df["Clasificación"]),
        "numeroContrato": texto(df["Número de contrato"]),
        "fechaImpresion": a_fecha(df["Fecha Aprobación EPS"]),
        "fechaAtencion": a_fecha(df["Fecha Utilización IPS"]),
        "valorCobrar": numero(df["Valor de Cuota Moderadora o Copago"]),
        "regimen": texto(df["Regimen"]),
        "nombreSucursalPrestador": texto(df["Sede"]),
        "datosOriginales": datos_originales(df),
    })
    return salida


def limpiar_sura(ruta):
    df = pd.read_excel(ruta, sheet_name=0, dtype=str)           # .xls requiere xlrd
    codigo = texto(df["CODIGO PRESTACION"])
    # 6 digitos = CUPS; 5 digitos = CUPS que perdio el 0 inicial; 7 o mas = codigo propio SURA
    cups = codigo.map(lambda c: c.zfill(6) if c and c.isdigit() and len(c) in (5, 6) else None)
    salida = pd.DataFrame({
        "numeroAutorizacion": texto(df["ORDEN/EVENTO"]),
        "numeroSolicitud": texto(df["NRO SOLICITUD"]),
        "tipoIdentificacionPaciente": texto(df["TIPO ID AFILIADO"]),
        "numeroIdentificacionPaciente": texto(df["NUMERO IDENTIFICACION AFILIADO"]),
        "nombrePaciente": texto(df["NOMBRE AFILIADO"]),
        "codigoServicioEps": codigo,
        "cups": cups,
        "descripcion": texto(df["NOMBRE PRESTACION"]),
        "estadoOrden": texto(df["ESTADO ORDEN / EVENTO"]),
        "fechaImpresion": a_fecha(df["FECHA AUTORIZACION"]),
        "fechaVencimientoAutorizacion": a_fecha(df["FECHA VENCIMIENTO"]),
        "fechaAtencion": a_fecha(df["FECHA ATENCION"]),
        "datosOriginales": datos_originales(df),
    })
    return salida


def limpiar_coosalud(ruta):
    df = pd.read_excel(ruta, sheet_name=0, dtype=str)
    nombre = df[["Nombre del afilliado", "Segundo Nombre del afiliado",
                 "Apellido del afiliado", "Segundo apellido del afiliado"]].fillna("").agg(" ".join, axis=1)
    estado = texto(df["Estado"]).fillna("") + " / " + texto(df["Sub estado"]).fillna("")
    salida = pd.DataFrame({
        "numeroAutorizacion": texto(df["Numero del caso"]),
        "tipoIdentificacionPaciente": texto(df["Tipo de documento"]),
        "numeroIdentificacionPaciente": texto(df["Numero de documento"]),
        "nombrePaciente": texto(nombre),
        # El reporte no trae CUPS ni descripcion del servicio: se completan con homologacionServicios
        "estadoOrden": texto(estado.str.strip(" /")),
        "diagnostico": texto(df["Codigo del diagnostico"]),
        "categoria": texto(df["Categoria"]),
        "numeroContrato": texto(df["Numero Contracto"]),
        "fechaImpresion": a_fecha(df["Fecha de creacion"]),
        "fechaAtencion": a_fecha(df["Fecha de la Cita"]),
        "valorCobrar": numero(df["Valor a pagar"]),
        "regimen": texto(df["Regimen"]),
        "nombreSucursalPrestador": texto(df["Sede IPS atencion"]),
        "datosOriginales": datos_originales(df),
    })
    return salida


LIMPIEZA_POR_EPS = {
    "COOSALUD": limpiar_coosalud,
    "SALUD_TOTAL": limpiar_salud_total,
    "SANITAS": limpiar_sanitas,
    "SURA": limpiar_sura,
}

NOMBRE_EPS = {"COOSALUD": "COOSALUD", "SALUD_TOTAL": "SALUD TOTAL", "SANITAS": "SANITAS", "SURA": "SURA"}


def completar(df, eps):
    for col in COLUMNAS_DESTINO:
        if col not in df.columns:
            df[col] = None
    df["eps"] = eps
    df["vencimientoCalculado"] = 0                              # la fecha que llega es la de la EPS (o vacia)
    df["especialidad"] = df["descripcion"].map(especialidad_desde_descripcion)
    df["llaveServicio"] = [llave_servicio(c, d) for c, d in zip(df["cups"], df["descripcion"])]
    df = df.dropna(subset=["numeroAutorizacion", "numeroIdentificacionPaciente"])
    df = df.drop_duplicates(subset=["eps", "numeroAutorizacion", "llaveServicio"], keep="last")
    # NaN/NaT -> None para que MariaDB reciba NULL
    df = df.astype(object).where(pd.notnull(df), None)
    return df


# ---------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------
def engine():
    """Conexion con un usuario propio del ETL (no el de analitica)."""
    host = os.environ["DB_HOST_EPS"]
    port = os.environ.get("DB_PORT_EPS", "3306")
    user = os.environ["DB_USER_EPS"]
    password = quote_plus(os.environ["DB_PASSWORD_EPS"])     # por si la clave tiene @, :, / etc.
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{BASE_DATOS}",
                         pool_pre_ping=True)


def archivo_ya_cargado(cnx, eps, archivo):
    sql = text("SELECT COUNT(*) FROM cargas WHERE eps = :eps AND archivoOrigen = :archivo AND estado = 'OK'")
    return cnx.execute(sql, {"eps": eps, "archivo": archivo}).scalar() > 0


def iniciar_carga(cnx, eps, archivo):
    """Registra la carga con un UUID publico (loadId) que desarrollo comparte con Coco."""
    load_id = str(uuid.uuid4())
    r = cnx.execute(text("INSERT INTO cargas (loadId, eps, archivoOrigen) VALUES (:load_id, :eps, :archivo)"),
                    {"load_id": load_id, "eps": eps, "archivo": archivo})
    log.info(f"[{eps}] {archivo} | loadId {load_id}")
    return r.lastrowid


def cerrar_carga(cnx, id_carga, estado, leidas=None, cargadas=None, error=None, nuevas=None):
    actualizadas = None if cargadas is None or nuevas is None else cargadas - nuevas
    cnx.execute(text("""UPDATE cargas SET estado = :estado, filasLeidas = :leidas,
                        filasCargadas = :cargadas, filasNuevas = :nuevas, filasActualizadas = :actualizadas,
                        mensajeError = :error, fechaFin = NOW()
                        WHERE idCarga = :id"""),
                {"estado": estado, "leidas": leidas, "cargadas": cargadas,
                 "nuevas": nuevas, "actualizadas": actualizadas,
                 "error": (str(error)[:5000] if error else None), "id": id_carga})


def contar_autorizaciones(cnx, eps):
    return cnx.execute(text("SELECT COUNT(*) FROM autorizaciones WHERE eps = :eps"), {"eps": eps}).scalar()


def upsert_autorizaciones(cnx, df):
    columnas = ", ".join(COLUMNAS_DESTINO)
    valores = ", ".join(f":{c}" for c in COLUMNAS_DESTINO)
    actualizar = ", ".join(f"{c} = VALUES({c})" for c in COLUMNAS_ACTUALIZAR)
    sql = text(f"INSERT INTO autorizaciones ({columnas}) VALUES ({valores}) "
               f"ON DUPLICATE KEY UPDATE {actualizar}")
    registros = df[COLUMNAS_DESTINO].to_dict(orient="records")
    for i in range(0, len(registros), 1000):
        cnx.execute(sql, registros[i:i + 1000])
    return len(registros)


# Orden de menor a mayor prioridad: la ultima regla que aplique es la que queda.
# Una regla de una EPS especifica gana sobre una de 'TODAS'.
SQL_HOMOLOGAR = """
UPDATE autorizaciones a
JOIN homologacionServicios h
  ON h.activo = 1
 AND h.tipoLlave = :tipo
 AND h.eps = :eps_regla
 AND h.valorLlave = {campo}
SET a.especialidad   = h.especialidad,
    a.cups           = COALESCE(a.cups, h.cups),
    a.descripcion    = COALESCE(a.descripcion, h.descripcionCups),
    a.idHomologacion = h.idHomologacion
WHERE (:eps_regla = 'TODAS' OR a.eps = :eps_regla)
"""
LLAVES = [("CATEGORIA", "a.categoria"), ("CONTRATO", "a.numeroContrato"), ("CIE10", "a.diagnostico"),
          ("CODIGO_EPS", "a.codigoServicioEps"), ("CUPS", "a.cups")]


def homologar(eng):
    """Aplica homologacionServicios a TODAS las autorizaciones, para que una regla nueva
    tambien corrija lo que ya estaba cargado."""
    with eng.begin() as cnx:
        reglas = cnx.execute(text("SELECT COUNT(*) FROM homologacionServicios WHERE activo = 1")).scalar()
        if not reglas:
            log.info("Homologacion: no hay reglas activas en homologacionServicios")
            return
        reglas_eps = [r[0] for r in cnx.execute(text(
            "SELECT DISTINCT eps FROM homologacionServicios WHERE activo = 1 AND eps <> 'TODAS'"))]
        for eps_regla in ["TODAS"] + reglas_eps:
            for tipo, campo in LLAVES:
                r = cnx.execute(text(SQL_HOMOLOGAR.format(campo=campo)), {"tipo": tipo, "eps_regla": eps_regla})
                if r.rowcount:
                    log.info(f"Homologacion: regla {eps_regla}/{tipo} actualizo {r.rowcount} filas")
    log.info(f"Homologacion aplicada ({reglas} reglas activas)")


SQL_VIGENCIA = """
UPDATE autorizaciones a
JOIN vigenciaEps v ON v.eps = a.eps AND v.activo = 1
SET a.fechaVencimientoAutorizacion = DATE_ADD(a.fechaImpresion, INTERVAL v.mesesVigencia MONTH),
    a.vencimientoCalculado = 1
WHERE a.fechaImpresion IS NOT NULL
  AND (a.fechaVencimientoAutorizacion IS NULL OR a.vencimientoCalculado = 1)
"""


def calcular_vigencia(eng):
    """Para las EPS de vigenciaEps: vencimiento = fecha de autorizacion + N meses, solo donde
    la EPS no envio la fecha. Si cambia el numero de meses, se recalcula en la siguiente corrida."""
    with eng.begin() as cnx:
        reglas = cnx.execute(text(
            "SELECT eps, mesesVigencia FROM vigenciaEps WHERE activo = 1 ORDER BY eps")).fetchall()
        if not reglas:
            log.info("Vigencia: no hay reglas activas en vigenciaEps")
            return
        r = cnx.execute(text(SQL_VIGENCIA))
    detalle = ", ".join(f"{eps} {meses} meses" for eps, meses in reglas)
    log.info(f"Vigencia calculada ({detalle}) | filas actualizadas {r.rowcount}")


def resumen_calidad(eng):
    """Estado de la tabla al final de la ejecucion, por EPS."""
    with eng.connect() as cnx:
        filas = cnx.execute(text("""
            SELECT eps, COUNT(*) total,
                   SUM(especialidad IS NULL) sinEspecialidad,
                   SUM(cups IS NULL) sinCups,
                   SUM(fechaVencimientoAutorizacion >= CURDATE()) vigentes,
                   SUM(vencimientoCalculado = 1) vencCalculado
            FROM autorizaciones GROUP BY eps ORDER BY eps""")).fetchall()
    for eps, total, sin_esp, sin_cups, vigentes, calc in filas:
        log.info(f"Estado BD | {eps:<11} | total {total:>6} | vigentes {int(vigentes or 0):>6} | "
                 f"venc. calculado {int(calc or 0):>6} | sin especialidad {int(sin_esp or 0):>6} | "
                 f"sin CUPS {int(sin_cups or 0):>6}")


# ---------------------------------------------------------------------
# Proceso
# ---------------------------------------------------------------------
def procesar_archivo(eng, carpeta_eps, ruta):
    """Devuelve (estado, nuevas, actualizadas). estado: 'OMITIDO', 'OK' o 'ERROR'."""
    eps = NOMBRE_EPS[carpeta_eps]
    archivo = f"{carpeta_eps}/{os.path.basename(ruta)}"
    with eng.begin() as cnx:
        if archivo_ya_cargado(cnx, eps, archivo):
            log.info(f"[{eps}] {archivo} | ya cargado, se omite")
            return "OMITIDO", 0, 0
        id_carga = iniciar_carga(cnx, eps, archivo)
    inicio = time.time()
    log.info(f"[{eps}] {archivo} | inicia carga (idCarga {id_carga})")
    try:
        df = LIMPIEZA_POR_EPS[carpeta_eps](ruta)
        leidas = len(df)
        df = completar(df, eps)
        descartadas = leidas - len(df)
        df["idCarga"] = id_carga
        with eng.begin() as cnx:                                 # todo o nada por archivo
            antes = contar_autorizaciones(cnx, eps)
            cargadas = upsert_autorizaciones(cnx, df)
            nuevas = contar_autorizaciones(cnx, eps) - antes
            cerrar_carga(cnx, id_carga, "OK", leidas, cargadas, nuevas=nuevas)
        log.info(f"[{eps}] {archivo} | OK en {time.time() - inicio:.1f}s | leidas {leidas} | "
                 f"descartadas {descartadas} (repetidas o sin documento) | nuevas {nuevas} | "
                 f"actualizadas {cargadas - nuevas} | sin especialidad {int(df['especialidad'].isna().sum())}")
        return "OK", nuevas, cargadas - nuevas
    except Exception as e:
        log.error(f"[{eps}] {archivo} | ERROR en {time.time() - inicio:.1f}s -> {e!r}")
        log.error(traceback.format_exc())
        with eng.begin() as cnx:
            cerrar_carga(cnx, id_carga, "ERROR", error=traceback.format_exc())
        return "ERROR", 0, 0


# ---------------------------------------------------------------------
# Eventos para desarrollo
# ---------------------------------------------------------------------
SQL_EVENTOS = """
INSERT INTO {tabla} ({eventId}, {idCarga}, {eventType}, status, attempts)
SELECT UUID(), c.idCarga, :tipo, 'PENDING', 0
FROM cargas c
WHERE c.estado = 'OK'
  AND COALESCE(c.filasNuevas, 0) + COALESCE(c.filasActualizadas, 0) > 0
  AND NOT EXISTS (SELECT 1 FROM {tabla} e
                  WHERE e.{idCarga} = c.idCarga AND e.{eventType} = :tipo)
"""


def columnas_eventos(cnx):
    """Devuelve los nombres de columnas que tiene hoy la tabla de eventos (camelCase o snake_case)."""
    columnas = {r[0] for r in cnx.execute(text(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = :bd AND TABLE_NAME = :tabla"), {"bd": BASE_DATOS, "tabla": TABLA_EVENTOS})}
    for estilo, nombres in COLUMNAS_EVENTOS.items():
        if set(nombres.values()) <= columnas:
            return estilo, nombres
    raise RuntimeError(f"La tabla {TABLA_EVENTOS} no tiene las columnas esperadas: {sorted(columnas)}")


def crear_eventos(eng):
    """Un evento PENDING por cada carga OK con datos que aun no tenga evento.
    Si una ejecucion se cae despues de cargar, la siguiente crea los eventos que faltaron."""
    with eng.begin() as cnx:
        estilo, nombres = columnas_eventos(cnx)
        sql = SQL_EVENTOS.format(tabla=TABLA_EVENTOS, **nombres)
        creados = cnx.execute(text(sql), {"tipo": EVENT_TYPE}).rowcount
    log.info(f"Evento | tabla {TABLA_EVENTOS} con columnas en {estilo}")
    if creados:
        log.info(f"Evento | {creados} evento(s) {EVENT_TYPE} creados en PENDING")
    else:
        log.info("Evento | sin cargas nuevas con datos: no se genera evento")


def main(raiz):
    inicio = time.time()
    log.info("=" * 90)
    log.info(f"ETL autorizaciones EPS | carpeta: {raiz} | BD: {os.environ.get('DB_HOST_EPS')}/{BASE_DATOS}")
    eng = engine()
    resultados = {"OK": 0, "OMITIDO": 0, "ERROR": 0, "FILAS": 0, "NUEVAS": 0, "ACTUALIZADAS": 0}
    procesar_carpetas(eng, raiz, resultados)
    homologar(eng)
    calcular_vigencia(eng)
    crear_eventos(eng)
    resumen_calidad(eng)
    log.info(f"Fin | archivos cargados {resultados['OK']} | omitidos {resultados['OMITIDO']} | "
             f"con error {resultados['ERROR']} | duracion {time.time() - inicio:.1f}s")
    return resultados


def procesar_carpetas(eng, raiz, resultados):
    for carpeta_eps in LIMPIEZA_POR_EPS:
        carpeta = os.path.join(raiz, carpeta_eps)
        if not os.path.isdir(carpeta):
            log.warning(f"[{carpeta_eps}] no existe la carpeta {carpeta}, se omite")
            continue
        archivos = sorted(f for f in os.listdir(carpeta)
                          if f.lower().endswith(EXTENSIONES) and not f.startswith("~$"))
        if not archivos:
            log.info(f"[{NOMBRE_EPS[carpeta_eps]}] sin archivos en la carpeta")
        for nombre in archivos:
            estado, nuevas, actualizadas = procesar_archivo(eng, carpeta_eps, os.path.join(carpeta, nombre))
            resultados[estado] += 1
            resultados["NUEVAS"] += nuevas
            resultados["ACTUALIZADAS"] += actualizadas
            resultados["FILAS"] += nuevas + actualizadas


if __name__ == "__main__":
    resultados = main(sys.argv[1] if len(sys.argv) > 1 else PATH_DRIVE_EPS)
    # Ultima linea de la salida (formato estandar de los ETL: filas , tabla , fecha) -> la toma el DAG para el correo
    print(f"{resultados['FILAS']} , {BASE_DATOS}.autorizaciones , {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)