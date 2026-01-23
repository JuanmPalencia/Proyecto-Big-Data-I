# -*- coding: utf-8 -*-
"""
OECD ETL — SDMX REST (CSV con etiquetas)
Descarga datasets de la OECD y guarda CSV en data/raw/oecd/.

NUEVO:
- Pausa base entre descargas: 5 min (300s).
- Manejo 429 con "castigo" escalonado: 5m → 10m → 20m (por defecto).
- Respeta Retry-After si es mayor que el castigo.

Ejemplos:
  py ETL/script/oecd_extract.py                              # todos (pausas de 5m)
  py ETL/script/oecd_extract.py --only env_tax,gdp           # subset
  py ETL/script/oecd_extract.py --pause 60                   # pausa base 1m
  py ETL/script/oecd_extract.py --penalty-429 300,600,1200   # (por defecto)
"""


# Habilita el uso de "type hints" (anotaciones de tipo) modernos en Python
from __future__ import annotations

# Importa librerías del sistema: os (rutas), time (pausas), argparse (CLI)
import os, time, argparse

# Importa random para pausas aleatorias (jitter)
import random

# Importa "requests" para hacer peticiones HTTP (descargas web)
import requests

# Importa "pandas" para manipular los datos en tablas (DataFrames)
import pandas as pd

# Importa "StringIO" para leer texto (CSV) como si fuera un archivo
from io import StringIO

# Importa "datetime" u "UTC" para trabajar con fechas y zonas horarias
from datetime import datetime, UTC

# Importa "type hints" (Dict,List) para que el código sea más legible
from typing import Dict, List

# ======================================================
# CONFIGURACIÓN DE RUTAS
# ======================================================

# Obtiene la ruta absoluta del directorio donde se encuentra este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Carpeta de salida → /app/data/raw/oecd (en Docker) o ./data/raw/oecd (en local)
OUT_DIR = os.path.join(BASE_DIR, "..", "..", "data", "raw", "oecd")
# Crea el directorio de salida (y sus carpetas padre) si no existe
os.makedirs(OUT_DIR, exist_ok=True)

# ======================================================
# CONFIGURACIÓN BASE OECD
# ======================================================

# Define la URL base de la API SDMX de la OECD
BASE = "https://sdmx.oecd.org/public/rest/data"

# Lee la variable de entorno "OECD_START_YEAR"; si no existe, usa "2010"
DEFAULT_START = os.getenv("OECD_START_YEAR", "2010")

# Lee la variable de entorno "OECD_END_YEAR"; si no existe, usa "2023"
DEFAULT_END   = os.getenv("OECD_END_YEAR",   "2023")

# Lee el "timeout" (tiempo de espera) y lo convierte a entero (defecto: 90s)
DEFAULT_TIMEOUT = int(os.getenv("OECD_TIMEOUT", "90"))

# Lee el Nº de reintentos y lo convierte a entero (defecto: 3)
DEFAULT_MAX_RETRIES = int(os.getenv("OECD_MAX_RETRIES", "3"))

# Pausa BASE entre requests (por defecto 5 minutos)
DEFAULT_PAUSE = float(os.getenv("OECD_PAUSE_RETRY", "300"))

# Castigo por 429 (en segundos) — por defecto: 5m, 10m, 20m
DEFAULT_PENALTY_429 = os.getenv("OECD_PENALTY_429", "300,600,1200")


# ======================================================
# DEFINICIÓN DE DATASETS A DESCARGAR
# ======================================================


# Define un diccionario de plantillas de URL para cada dataset que queremos
DATA_URLS: Dict[str, str] = {

    # Define la plantilla de URL para "air_ghg"(emisiones de gases)
    "air_ghg": (
        f"{BASE}/OECD.ENV.EPI,DSD_AIR_GHG@DF_AIR_GHG,1.0/"
        "all?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla de URL para "env_tax"(impuestos ambientales)
    "env_tax": (
        f"{BASE}/OECD.ENV.EPI,DSD_ERTR@DF_ERTR,1.0/"
        "A..TAXREV._T._T.USD.?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "material_resources"(recursos materiales)
    "material_resources": (
        f"{BASE}/OECD.ENV.EPI,DSD_MATERIAL_RESOURCES@DF_MATERIAL_RESOURCES,1.0/"
        "OECD.A.DMC..TOT?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "material_footprint"(huella material)
    "material_footprint": (
        f"{BASE}/OECD.ENV.EPI,DSD_MATERIAL_RESOURCES@DF_MATERIAL_RESOURCES,1.0/"
        "OECD.A.MF..TOT?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "env_policy"(políticas ambientales)
    "env_policy": (
        f"{BASE}/OECD.ECO.MAD,DSD_EPS@DF_EPS,1.0/"
        ".A..EPS?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "green_growth"(crecimiento verde)
    "green_growth": (
        f"{BASE}/OECD.ENV.EPI,DSD_GG@DF_GREEN_GROWTH,1.1/"
        "AUS....?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),  # Cambia 'AUS....?' por 'all' si quieres todos los países.

    # Define la plantilla para "env_expenditure"(gasto ambiental)
    "env_expenditure": (
        f"{BASE}/OECD.ENV.EPI,DSD_EPEA@DF_EPEA,1.0/"
        ".A.NEEP...S1._T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    
    # "circular":  # añadir cuando el flow estable esté publicado.

    # 👩‍🔬 I+D, patentes, VC
    # Define la plantilla para "rd_expenditure"(gasto en I+D)
    "rd_expenditure": (
        f"{BASE}/OECD.STI.STP,DSD_RDS_GERD@DF_GERD_SEO,1.0/"
        ".A.._T....._T.XDC.?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "patents_ipc"(patentes por IPC)
    "patents_ipc": (
        f"{BASE}/OECD.STI.PIE,DSD_PATENTS@DF_PATENTS,1.0/"
        ".A...PRIORITY...INVENTOR..._T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "enviromental_ipc"(patentes ambientales)
    "enviromental_ipc": (
        f"{BASE}/OECD.STI.PIE,DSD_PATENTS@DF_PATENTS_ENVIROMENT,1.0/"
        ".A...PRIORITY...INVENTOR..._T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "vc_investment"(inversión en capital riesgo)
    "vc_investment": (
        f"{BASE}/OECD.SDD.TPS,DSD_VC@DF_VC_INV,1.0/"
        "...USD_EXC.A?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Macroeconomía y mercado laboral
    # Define la plantilla para "gdp"(PIB)
    "gdp": (
        f"{BASE}/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_I,1.0/"
        "A.AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B1GQ_R_GR.."
        "?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "productivity"(productividad)
    "productivity": (
        f"{BASE}/OECD.SDD.TPS,DSD_PDB@DF_PDB_ISIC4_I4,1.0/"
        ".A.EMP......?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "labour_force"(fuerza laboral)
    "labour_force": (
        f"{BASE}/OECD.SDD.TPS,DSD_ALFS@DF_SUMTAB,1.0/"
        ".LF.._Z._T....A?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Social / bienestar / salud
    # Define la plantilla para "bli_indicators"(Better Life Index)
    "bli_indicators": (
        f"{BASE}/BLI/"
        "all?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "regional_wellbeing"(bienestar regional)
    "regional_wellbeing": (
        f"{BASE}/REG_WELL_BEING/"
        "all?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "health_expenditure"(gasto en salud)
    "health_expenditure": (
        f"{BASE}/OECD.ELS.HD,DSD_SHA@DF_SHA,1.0/"
        ".A.EXP_HEALTH.PT_B1GQ._T.._T.._T...?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Otros ambientales
    # Define la plantilla para "air_pollutants"(contaminantes atmosféricos)
    "air_pollutants": (
        f"{BASE}/OECD.ENV.EPI,DSD_AIR_EMISSIONS@DF_AIR_EMISSIONS,1.0/"
        ".A.SOX.T_EM_MM.T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Define la plantilla para "energy_supply"(suministro energético)
    "energy_supply": (
        f"{BASE}/OECD.SDD.NAD.SEEA,DSD_PEFA@DF_PEFASUP,1.1/"
        "CRI.A..ATU_HH+ATU+A+B+C+D+E+F+G+H+I+J+K+L+M+N+O+P+Q+R+S+T+U+HH..SUP.?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
}

# -------------------- Utilidades --------------------
# Define una funcion para asegurar que la URL pida CSV
def _ensure_csv_format(url: str) -> str:
    # Si format esta en la URL, devuelve tal cual
    if "format=" in url:
        return url
    # Decide si usar "?" (si no hay parametros) o "&" (si ya hay)
    sep = "&" if "?" in url else "?"
    # Devuelve la URL con el formato CSV añadido
    return f"{url}{sep}format=csvfilewithlabels"


# Define una funcion para rellenar las plantillas de fecha
def _with_period(url_template: str, start: str, end: str) -> str:
    # Remplaza {start} y {end} en la URL con los valores dados
    return url_template.format(start=start, end=end)


# Define una función de pausa con jitter (variación aleatoria)
def _sleep_with_jitter(base_seconds: float):
    """
    Pausa base entre descargas con un pequeño jitter (±10%) para evitar
    sincronizarse con otros procesos.
    """
    # Calcula una variacion del 10% de la pausa base
    jitter = base_seconds * 0.1
    # Realiza la pausa: base +- variacion aleatorioa (minimo 0.0s)
    time.sleep(max(0.0, base_seconds + random.uniform(-jitter, jitter)))


# Define una funcion para convertir el string de penalizaciones en una lista de enteros
def _parse_penalties(s: str) -> List[int]:
    """
    Convierte '300,600,1200' -> [300, 600, 1200]
    """
    # Crea una lista vacia para guardar los numeros
    out: List[int] = []
    # Itera sobre cada parte del string separada por comas
    for part in (s or "").split(","):
        # Limpia espacion en blanco (ej. 300s)
        part = part.strip()
        # Si la parte está vacía la ignora
        if not part:
            continue
        # Intenta convertir la parte a entero y añadirla a la lista
        try:
            out.append(int(part))
        # Si falla la conversión, la ignora
        except ValueError:
            pass
    # Devuelve la lista de numeros, o una lista por defecto si estaba vacia
    return out or [300, 600, 1200]


# Define la funcion de descarta robusta (con reintentos)
def robust_get(url: str, timeout: int, max_retries: int, penalties_429: List[int]):
    """
    GET con manejo de 429 y 5xx:
      - Para 429 usa max(Retry-After, penalty[i]).
      - penalty[i] por defecto: 300s (5m), 600s (10m), 1200s (20m).
      - Si hay más de 3 consecutivos, repite el último (20m).
      - Para 5xx aplica backoff lineal: 60s, 120s, 180s… (amigable).
    """
    #Variable para guardar el ultimo error ocurrido
    last_exc = None
    # Contador para errores 429 (too many requests) consecutivos
    consec_429 = 0

    # Bucle que se ejecuta desde 1 hasta max_retries
    for attempt in range(1, max_retries + 1):
        # Inicia un bloque "try" para capturar errores
        try:
            # Realiza la petición GET
            resp = requests.get(url, timeout=timeout)

            # Comprueba si el código de estado es el 429 (too many requests)
            if resp.status_code == 429:
                # Aumenta el contador de 429 consecutivos
                consec_429 += 1
                # Intenta leer la cabecera Retry-After a un entero (0 si no existe)
                ra = resp.headers.get("Retry-After")
                # Convierte el valor de "Retry-After" a un entero (0 si no es válido)
                ra_val = int(ra) if ra and ra.isdigit() else 0
                # Elige el índice de penalización de nuestra lista
                idx = min(consec_429 - 1, len(penalties_429) - 1)
                # Obtiene nuestra penalizacion
                penalty = penalties_429[idx]
                # Elige el tiempo de espera más largo (el del servidor o el nuestro)
                wait_s = max(ra_val, penalty)
                print(f"   ⏳ 429 rate limit — esperando {wait_s}s (intento {attempt}/{max_retries}, #{consec_429} consecutivo)…")
                # Pone el script a "dormir"
                time.sleep(wait_s)
                # Vuelve al inicio del bucle para reintentar
                continue

            # Comprueba si es un error del servidor (500,502,503....)
            if 500 <= resp.status_code < 600:
                # Calcula una pausa de "backoff" lineal: 60s, 120s, 180s…
                wait_s = 60 * attempt
                print(f"   ⚠️ {resp.status_code} server error — reintento en {wait_s}s…")
                # Pone el script a "dormir"
                time.sleep(wait_s)
                # Vuelve al inicio del bucle para reintentar
                continue

            # Si no hubo errores 429 o 5xx, comprueba otros
            resp.raise_for_status()
            #Si "raise_for_status()" no da error devuelve la respuesta
            return resp


        # Captura errores HTTP (como 404) que "raise_for_status()" haya lanzado
        except requests.HTTPError as e:
            # Guarda el error
            last_exc = e
            # Si es el ultimo intento,
            if attempt == max_retries:
                # Se rinde y lanza la excepcion
                raise
            # Espera 5 segundos antes de reintentar
            time.sleep(5)
        # Captura cualquier otro error 
        except Exception as e:
            # Guarda el error
            last_exc = e
            # Si es el ultimo intento
            if attempt == max_retries:
                # Se rinde y lanza la excepcion
                raise
            # Calcula una pausa de backoff lineal: 30s, 60s, 90s…
            wait_s = 30 * attempt
            print(f"   ⚠️ Error de red — reintento en {wait_s}s…")
            # Pone el script a "dormir"
            time.sleep(wait_s)

    # Si el bucle "for" termina sin un "return", (todos los intentos fallaron)
    if last_exc:
        # Lanza el último error que se guardo
        raise last_exc


# Define una funcion para descargar y parsear el CSV
def fetch_csv(url: str, timeout: int, max_retries: int, penalties_429: List[int]) -> pd.DataFrame:
    # Llama a la funcion robusta para obtener la respuesta HTTP
    r = robust_get(url, timeout=timeout, max_retries=max_retries, penalties_429=penalties_429)
    # Lee el texto de la respuesta, lo pasa a StringIO (memoria) y lo lee con pandas
    return pd.read_csv(StringIO(r.text), low_memory=False)



# Define la funcion para eliminar duplicados de forma segura
def _drop_duplicates_safe(df: pd.DataFrame, note: str = "") -> pd.DataFrame:
    # Si el data frame esta vacio lo devuelve
    if df is None or df.empty:
        return df
    # Guarda el Nº de filas antes de limpiar
    before = len(df)
    # Elimina filas duplicadas y resetea el indice
    df = df.drop_duplicates(ignore_index=True)
    # Calcula cuantas filas se borraron
    removed = before - len(df)
    # Si se borro alguna imprime un mensaje y devuelve el dataframe limpio
    if removed > 0:
        print(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})")
    return df


# Define una funcion para guardar el DataFrame en un archivo CSV
def save_df(df: pd.DataFrame, name: str):
    # Llama a la funcion de eliminar duplicados
    df = _drop_duplicates_safe(df, note=f"{name}.csv")
    # Añade una nueva columna con la fecha de extraccion
    df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")
    # Construye la ruta completa del archivo de salida
    path = os.path.join(OUT_DIR, f"{name}.csv")
    #Guarda el DataFrame como CSV (sin indice, UTF-8)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"✅ {name}.csv  ({len(df):,} filas) -> {path}")

# -------------------- Función CLI principal --------------------


# Define la funcion principal que maneja la linea de comandos
def main():
    # Inicializa el parser de arguentos de la línea de comandos
    ap = argparse.ArgumentParser(description="OECD ETL — SDMX REST (CSV, 429-friendly)")

    # Añade el argumento --start (año de inicio)
    ap.add_argument("--start", default=DEFAULT_START)

    # Añade el argumento --end (año de fin)
    ap.add_argument("--end", default=DEFAULT_END)

    # Añade el argumento --only (datasets a descargar)
    ap.add_argument("--only", default="all", help="lista separada por comas; 'all' por defecto")

    # Añade el argumento --pause (pausa base entre descargas)
    ap.add_argument("--pause", type=float, default=DEFAULT_PAUSE, help="pausa base entre descargas (segundos)")

    # Añade el argumento --timeout (timeout por request)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="timeout por request (segundos)")

    # Añade el argumento --max-retries (Nº max de reintentos)
    ap.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)

    # Añade el argumento --penalty-429 (castigos por 429)
    ap.add_argument("--penalty-429", default=DEFAULT_PENALTY_429,
                    help="lista de segundos para castigo por 429, ej. '300,600,1200'")
    
    # Añade el argumento --overwrite (si existe el CSV, re-descargarlo)
    ap.add_argument("--overwrite", action="store_true", help="si existe el CSV, re-descargarlo")

    # Lee los argumentos que se paasaron por la terminal
    args = ap.parse_args()

    # Convierte el string de penalizaciones en una lisa de enteros
    penalties_429 = _parse_penalties(args.penalty_429)

    # Imprime el resumen de la configuracion de esta ejecucion
    print("🚀 OECD ETL — inicio")
    print(f"   Periodo:  {args.start}–{args.end}")
    print(f"   Carpeta:  {OUT_DIR}")
    print(f"   Timeout:  {args.timeout}s | Retries: {args.max_retries}")
    print(f"   Pausa base entre descargas: {int(args.pause)}s")
    print(f"   Castigos 429 (s): {penalties_429}\n")

    # Decide la lista de datasets a descargar
    targets = list(DATA_URLS.keys()) if args.only == "all" else [t.strip() for t in args.only.split(",") if t.strip()]

    # Inicia el bucle principal, iterando sobre cada dataset
    for key in targets:
        # Comprueba si el "key" (ej:"gdp") esta en nuestro catalogo "DATA_URLS""
        if key not in DATA_URLS:
            # Si no, imprime un error y salta al siguiente dataset
            print(f"❌ '{key}' no está definido. Opciones: {list(DATA_URLS.keys())}")
            continue

        # Contruye la ruta donde deberia estar el archivo CSV
        out_path = os.path.join(OUT_DIR, f"{key}.csv")
        # Comprueba si el archivo ya exuste Y si NO se ha pedido "--overwrite"
        if os.path.exists(out_path) and not args.overwrite:
            # Si se cumplen ambas, se salta la descarga
            print(f"⏭️  {key}: ya existe, usa --overwrite para re-descargar.")
            # Realiza la pausa de 5 mon igualmente para ser amable con la API
            _sleep_with_jitter(args.pause)
            # Salta al siguiente dataset
            continue

        # Rellena la plantilla de URL con los años de inicio y fin
        url = _with_period(DATA_URLS[key], args.start, args.end)
        # Comprueba si la URL ya tiene "format="
        if "format=" not in url:
            # Si no, decide si usar "?" o "&"
            sep = "&" if "?" in url else "?"
            # Y añade el formato CSV
            url = f"{url}{sep}format=csvfilewithlabels"

        print(f"🔄 {key} …")

        # Inicia un bucle "try" para capturar errores solo de este dataset
        try:
            # Llama a la funcion para descargar y parsear el CSV
            df = fetch_csv(url, timeout=args.timeout, max_retries=args.max_retries, penalties_429=penalties_429)
            # Comprueba si la descarga devolvio un DataFrame vacio
            if df.empty:
                print(f"⚠️  {key}: respuesta vacía.")
            # Si no esta vacio gurda el DataFrame como CSV
            else:
                save_df(df, key)
        # Captura si  hubo un error HTTP (ej:404 No encontrado)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "HTTP"
            print(f"❌ {key}: {code} {e}")
        # Captura cualquier otro error (ej: timeout)
        except Exception as e:
            print(f"❌ {key}: {e}")

        # Realiza la pausa larga (5 min por defecto) ANTES de empezar el siguiente dataset
        _sleep_with_jitter(args.pause)

    # Imprime un mensaje final cuando el bucle "for" ha terminado
    print(f"\n🕒 Fin: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")

if __name__ == "__main__":
    main()
