# -*- coding: utf-8 -*-
"""
Eurostat ETL — Green Turning Point (GTP)
Guarda CSVs en: data/raw/eurostat/

Uso ejemplos:
  py ETL/script/eurostat_extract.py --datasets all
  py ETL/script/eurostat_extract.py --datasets all --filters "time=2010:2024&geo=ES,PT,FR"
  py ETL/script/eurostat_extract.py --datasets gdp_per_capita,ghg_emissions --filters "time=2015:2024"
  # Grupos nuevos (se guardan como nombre__part1.csv, part2.csv, ...)
  py ETL/script/eurostat_extract.py --datasets egss,circular --filters "time=2015:2024"

Opcional:
  --eager-merge  -> fusiona automáticamente las partes de cada grupo en un único CSV (nombre.csv)
"""

import os # Para gestionar rutas de archivos y carpetas del sistema
import sys # Para gestionar algumentos del sistema y salir del script (sys.exit)
import argparse # Para crear la interfaz de línea de comandos (CLI) ej. --datasets all
import time # Para poner pausas (time.sleep) y ser amables con la API
import requests # El cliente HTTP para descargar los datos de la api
import pandas as pd # La librería principal para manipular los datos en tablas
from io import StringIO # Para leer cadenas de texto (el TSV) como si fueran archivos
from datetime import datetime, UTC # Para añadir marcas de tiempo (timestamps) a los datos


# ======================================================
# BLOQUE 1: CATÁLOGO DE DATOS (QUÉ DESCARGAR)
# ======================================================

# --- Mapa de datasets "simples" (con un nombre amigable -> un código de tabla) ---
# Esta es la configuración principal del script
DATASETS = {
    # --- ECONÓMICO (Eje X de la curva) ---
    "gdp_per_capita": "nama_10_pc", # Producto interior bruto
    "employment_by_sector": "nama_10_a64_e",   # Empleo por industria
    "gross_fixed_capital": "nama_10_an6",
    "productivity": "nama_10_lp_ulc",

    # --- DEMOGRÁfico (Contexto) ---
    "population_by_age": "demo_r_pjangrp3",
    "education_level": "edat_lfse_04",
    "population_density": "demo_r_d3dens",
    "migration": "demo_gind",
    "household_size": "ilc_lvph01",

    # --- AMBIENTAL (Eje Y de tu curva (el más importante))
    "ghg_emissions": "env_air_emis", # Emisiones de gases de efecto invernadero
    "renewable_energy": "nrg_ind_ren", # % de energías renovables
    #"energy_consumption": "nrg_bal_c",
    "environmental_expenditure": "env_ac_exp2", # Gasto en protección ambiental

    # --- SOCIAL (I+D), variables de control ---
    "poverty_rate": "ilc_peps01",
    "innovation_rd": "rd_e_gerdtot", # Gasto en I+D
}

# --- Grupos (Datasets Lógicos que unen varias tavlas de Eurostat) ---
DATASET_GROUPS = {
    # EGGS -- Sector de bienes y servicios ambientales (CLAVE)
    "egss": [
        "env_ac_egss1", # Producción, exportaciones...  
        "env_ac_egss2",  # Empleo en el sector verde
    ],
    #Indicadores de bienestar (proxy)
    "bli_indicators": [
        "ilc_pw01", # Satisfacción vital
        "hlth_silc_01", # Autopercepción de la salud
        "ilc_di12", # Ingresos netos
    ],
    #Bienestar Regional (Para análisis NUTS2)
    "regional_wellbeing": [
        "nama_10r_3gdp", # Pib Regional
        "lfst_r_lfu3rt", # Tasa de paro regional
        "edat_lfse_04", # Educación Terciaria
        # Si está disponible y lo quieres añadir:
        # "demo_r_mlifexp",  # life expectancy (NUTS2)
    ],
    # Economía circular
    "circular": [
        "cei_srm030", # Tasa de uso de material circular (CMU)
        "cei_srm020", # Tasa de reciclaje de basura municipal
        # "cei_srm030",   
    ],
}


# ======================================================
# BLOQUE 2: CPFIGURACIÓN DE RED
# ======================================================

# URL base de la nueva API (v1.0) de Eurostat
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"

# Ruta por defecto donde se guardarán los datos (reekativa al script runall)
DEFAULT_OUT = os.path.join("data", "raw", "eurostat")

# Parámetros de robustez de red
TIMEOUT = 90 # Tiempo máximo de espera por una respuesta
RETRIES = 3 # Número de intentos si falla la conexión
PAUSE_S = 0.8  # Pausa entre descargas (para no saturar a la API)


# ======================================================
# BLOQUE 3: LÓGICA DE PARSEO DE API (EL NÚCLEO)
# ======================================================


def parse_filters(filters_str: str) -> dict:
    """
    Convierte un string de filtros (ej. "time=2015:2024&geo=ES,PT")
    a un diccionario de python (ej. {"time":"2015:2024","geo":"ES,PT"})
    """
    if not filters_str:
        return {}
    
    # Divide el string por "&"
    pairs = [p for p in filters_str.replace(" ", "").split("&") if p]
    q = {}
    for p in pairs:
        # Divide cada par por el primer "="
        if "=" in p:
            k, v = p.split("=", 1)
            q[k] = v
    return q


# --------- Helpers (Ayudantes) Eurostat JSON-stat ---------
# El formato JSON de eurostat es complejo. No devuelve una tabla, sino un cubo de datos comprimido
# Estas funciones los "descomprimen".
def _build_inv_map(dim_obj: dict) -> dict:
    """
    Crea un mapa inverso: {posición:clave}
    La API nos dice "el valor en la posición 2 es 500".
    Esta función nos ayuda a saber qué significa la "posición 2".
    (ej. "ES" para España)
    """
    cat = dim_obj.get("category", {})

    # Intenta usar el "index" si existe (es el más fiable)
    if "index" in cat and isinstance(cat["index"], dict):
        # Invierte el diccionario {clave:pos}->{pos:clave}
        return {pos: key for key, pos in cat["index"].items()}
    
    # Si no, usa el orden de las "labels" (menos fiable pero funciona)
    labels = list((cat.get("label") or {}).keys())
    return {i: k for i, k in enumerate(labels)}


def _unravel_index(flat_idx: int, sizes: list[int]) -> list[int]:
    """
    "Descomprime" un índice plano.
    Eurostat nos da los valores en una lista plana [1,2,3,4,5,6]
    Esta función convierte un índice (ej:4) en sus coordenadas multidimensionales
    (ej: [España,2023,Industria]) basándose en los tamaños (sizes) de cada dimensión
    """
    idxs = []

    #Itera al revés por los tamaós de las dimensiones
    for size in reversed(sizes):
        idxs.append(flat_idx % size)
        flat_idx //= size
    return list(reversed(idxs))


def fetch_eurostat_table(dataset_code: str, params: dict | None = None) -> pd.DataFrame:
    """
    Descarga una tabla de Eurostat y la normaliza a un DataFrame de Pandas
    Esta función es de extracción
    """

    # Intento 1: Formato JSON (el preferido)
    url = BASE_URL + dataset_code
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            # 1. Petición HTTP GET
            r = requests.get(url, params=params or {}, timeout=TIMEOUT)
            r.raise_for_status()
            js = r.json()

            # 2. Reconstrucción del "Cubo de Datos"
            # Necesitamos saber el orden de las dimensiones (ej:[geo, time, sector])
            dim_ids = js.get("id") or list(js.get("dimension", {}).keys())
            dims = [] # Nombre de las dimensiones
            inv_maps = {} # Mapas{pos:clave} para cada dimensión
            sizes = [] # Tamaños de cada dimensión (ej: 27 paises, 10 años)

            # 3. Llenamos los metadatos de las dimensiones
            for d in dim_ids:
                dim_obj = js["dimension"][d]
                dims.append(d)
                inv_maps[d] = _build_inv_map(dim_obj)

                # Obtenemos el tamaño de la dimensión
                if "index" in dim_obj.get("category", {}):
                    sizes.append(len(dim_obj["category"]["index"]))
                elif "label" in dim_obj.get("category", {}):
                    sizes.append(len(dim_obj["category"]["label"]))
                else:
                    sizes.append(1)

            # 4. Procesamiento de los valores
            records = []
            values = js.get("value")

            # A) Si los valores vienen como un Diccionario (formato "sparse")
            if isinstance(values, dict):
                for key, val in values.items():
                    # "Key" es la posición (ej:"1:4:12" o un índice plano "42")
                    if key == "":
                        idxs = [0] * len(dims)
                    elif ":" in key:
                        idxs = [int(i) for i in key.split(":")]
                    else:
                        try:
                            # Convertimos el índice plano a coordenadas
                            idxs = _unravel_index(int(key), sizes)
                        except Exception:
                            idxs = [0] * len(dims)

                    # Rellenamos si faltan dimensiones (raro, pero seguro)
                    if len(idxs) < len(dims):
                        idxs = idxs + [0] * (len(dims) - len(idxs))

                    # Construimos el registro (la fila DataFrame)
                    rec = {d: inv_maps[d].get(idxs[i], None) for i, d in enumerate(dims)}
                    rec["value"] = val
                    records.append(rec)

            # B) Si los valores vienen como una lista (formato "flat")
            elif isinstance(values, list):
                for flat_i, val in enumerate(values):
                    #Convertimos el índice de la lista (flat_i) a coordenadas
                    idxs = _unravel_index(flat_i, sizes)
                    # Construimos la fila
                    rec = {d: inv_maps[d].get(idxs[i], None) for i, d in enumerate(dims)}
                    rec["value"] = val
                    records.append(rec)
            else:
                # Si "value" no es ni dict ni list, devolvemos tabla vacía
                return pd.DataFrame()

            if not records:
                return pd.DataFrame()

            # 5. Creación del DataFrame final
            df = pd.DataFrame(records)
            df["dataset_code"] = dataset_code
            df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")

            # Reordenamos columnas para que "value" y metadatos queden al final
            df = df[[*dims, "value", "dataset_code", "extraction_date"]]
            return df

        except Exception as e:

            # Si el JSON falla, lo guardamos y reintentamos
            last = e
            if attempt < RETRIES:
                time.sleep(0.7 * attempt)
            else:

                # Intento 2(FallBack): Formato TSV (Separado por Tabuladores)
                # Si el JSON falló 3 veces (ej: por ser demasiado grande)
                # Intentamos descargar el formato TSV, que es más simple
                try:
                    tsv_url = (BASE_URL + dataset_code)
                    if params:
                        # reconstruimos query string
                        extras = "&".join([f"{k}={v}" for k, v in params.items()])
                        tsv_url = f"{tsv_url}?{extras}&format=TSV"
                    else:
                        tsv_url = f"{tsv_url}?format=TSV"
                    r2 = requests.get(tsv_url, timeout=TIMEOUT)
                    r2.raise_for_status()

                    # Leemos el texto de la respuesta TSV directamente en Pandas
                    return pd.read_csv(StringIO(r2.text), sep="\t")
                except Exception:
                    # Si el TSV también falla, lanzamos el error original del JSON
                    raise last


# --------- Guardado (Load) ---------
def save_csv(df: pd.DataFrame, name: str, out_dir: str):
    """
    Guarda un DataFrame simple como "nombre.csv"
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"✅ {name}.csv → {path} ({len(df):,} filas)")


def save_part(df: pd.DataFrame, group_name: str, part_idx: int, out_dir: str, source_table: str | None = None):
    """
    Guarda una parte de un grupo (ej:"eggs_part1.csv")
    Esto es para descargas de grupos, antes de la fusion
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{group_name}__part{part_idx}.csv")

    # Añadimos la columna "source_table" para saber de qué tabla vino cada fila
    if source_table:
        df = df.copy()
        if "source_table" not in df.columns:
            df["source_table"] = source_table
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"✅ {group_name} :: part{part_idx} ({source_table or ''}) → {path} ({len(df):,} filas)")


def merge_parts(group_name: str, out_dir: str):
    """
    Busca todos los archivos "grupo:part*.csv" y los junta (concatena)
    y los guarda como "grupo.csv"
    """
    import glob # Librería para buscar archivos con patrones (ej:"*")

    pattern = os.path.join(out_dir, f"{group_name}__part*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"⚠️  {group_name}: no hay parts para fusionar.")
        return
    
    frames = []
    for f in files:
        df = pd.read_csv(f) # Lee cada parte del csv
        frames.append(df)

    # Concatena todos los DataFrames en uno solo
    merged = pd.concat(frames, ignore_index=True)
    path = os.path.join(out_dir, f"{group_name}.csv")
    merged.to_csv(path, index=False, encoding="utf-8")
    print(f"🤝 {group_name}.csv (merge) → {path} ({len(merged):,} filas)")


# --------- Orquestador de Grupos ---------

def download_group(group_key: str, params: dict, out_dir: str, eager_merge: bool):
    """
    Orquesta la descarga de un grupo de datasets (ej:"egss")
    """
    table_list = DATASET_GROUPS[group_key]
    
    # Añadimos un tracker de fallos
    part_failed = False
    
    for idx, table_code in enumerate(table_list, start=1):
        try:
            df = fetch_eurostat_table(table_code, params=params)
            if df is None or df.empty:
                print(f"⚠️  {group_key} / {table_code}: sin datos (revisa filtros).")
                part_failed = True  # Marcamos que esta parte falló
                continue
            save_part(df, group_key, idx, out_dir, source_table=table_code)
            
        except Exception as e:
            # Si fetch_eurostat_table lanza una excepción (ej. 404)
            print(f"❌ Error descargando parte {table_code} del grupo {group_key}: {e}")
            part_failed = True # Marcamos que esta parte falló
            
        finally:
            # Hacemos la pausa siempre, incluso si falló, para no martillear la API
            time.sleep(PAUSE_S)

    # Comprobamos el tracker antes de fusionar
    if eager_merge:
        if part_failed:
            print(f"🚫 {group_key}: Fusión omitida porque una o más partes fallaron.")
        else:
            print(f"👍 {group_key}: Todas las partes OK. Procediendo a fusionar.")
            merge_parts(group_key, out_dir)

# ======================================================
# BLOQUE 4: CLI (Punto de Entrada Principal)
# ======================================================

def main():

    # 1. Definición de la Interfaz de Línea de Comandos (CLI)
    ap = argparse.ArgumentParser(description="Eurostat ETL (GTP) con soporte de grupos")

    # Argumento obligatorio: qué descargar
    ap.add_argument("--datasets", required=True,
                    help="Lista separada por comas (ej. gdp_per_capita,ghg_emissions) o 'all'")
    
    # Argumento opcional: dónde guardar
    ap.add_argument("--out-dir", default=DEFAULT_OUT, help="Directorio de salida (default data/raw/eurostat)")

    # Argumento opcional, filtros de API
    ap.add_argument("--filters", default="",
                    help="Filtros API. Ej: \"time=2010:2024&geo=ES,PT,FR\"")
    
    # Argumento opcional (booleano): fusionar grupos
    ap.add_argument("--eager-merge", action="store_true",
                    help="Fusiona las partes de cada grupo en un único CSV (nombre.csv)")
    args = ap.parse_args()

    # 2. Resolución de Rutas (para compatibilidad con Docker)
    # Define las rutas de forma robusta, subiendo dos niveles (script->ETL->Proyecto)
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    DATA_DIR = os.path.join(BASE_DIR, "data", "eurostat")

    # Si el usuario no especific´´o una ruta, usamos la ruta calculada de Docker/local
    if not args.out_dir or args.out_dir == DEFAULT_OUT:
        args.out_dir = DATA_DIR
    os.makedirs(args.out_dir, exist_ok=True)

    # 3. Resolución de Tareas (qué se va a descargar)
    if args.datasets.lower() == "all":

        # Si piden "all", seleccionamos todo de ambas listas
        selected_simple = list(DATASETS.keys())
        selected_groups = list(DATASET_GROUPS.keys())
    else:

        # Si piden datasets específicos (ej:"gdp_per_capita,egss")
        req = [s.strip() for s in args.datasets.split(",") if s.strip()]
        selected_simple = [s for s in req if s in DATASETS] # Filtramos los simples
        selected_groups = [s for s in req if s in DATASET_GROUPS] # Filtramos los grupos

        # Validación de errores: ¿El usuario pidio algo que no existe?
        unknown = [s for s in req if s not in DATASETS and s not in DATASET_GROUPS]
        if unknown:
            print(f"❌ Dataset(s) no definidos: {unknown}")
            print(f"   Simples: {list(DATASETS.keys())}")
            print(f"   Grupos:  {list(DATASET_GROUPS.keys())}")
            sys.exit(1)

    # Parsea los filtros (ej:"time=2020:2024") al formato dict
    params = parse_filters(args.filters)

    if not params:
        print("⚠️  Descargando SIN filtros. Puede ser pesado y tardar más o agotar cuota.")
        print("   Recomendado: añade al menos 'time=YYYY:YYYY' y/o 'geo=PAISES'")
        print()

    # 4. Resumen de Ejecición (Logs)
    print("🚀 Eurostat ETL — inicio")
    print(f"   Simples: {selected_simple}")
    print(f"   Grupos:  {selected_groups}")
    print(f"   Filtros: {params if params else '(sin filtros)'}")
    print(f"   Salida:  {args.out_dir}\n")

    # 5. Bucles de Ejecución - Simples
    for name in selected_simple:
        code = DATASETS[name]
        try:
            print(f"🔄 Descargando {name} [{code}] ...")
            df = fetch_eurostat_table(code, params=params)
            if df is None or df.empty:
                print(f"⚠️ {name}: sin datos devueltos (revisa filtros).")
                continue
            save_csv(df, name, args.out_dir)

        # Captura de errores por dataset (para que uno no rompa todo)
        except requests.HTTPError as e:
            print(f"❌ HTTP {name}: {e}")
        except requests.ConnectionError as e:
            print(f"❌ Conexión {name}: {e}")
        except Exception as e:
            print(f"❌ Error {name}: {e}")

    # 6. Bucle de ejecución - Grupos
    for gname in selected_groups:
        try:
            print(f"🔄 Descargando grupo {gname} …")
            download_group(gname, params=params, out_dir=args.out_dir, eager_merge=args.eager_merge)
        except requests.HTTPError as e:
            print(f"❌ HTTP {gname}: {e}")
        except requests.ConnectionError as e:
            print(f"❌ Conexión {gname}: {e}")
        except Exception as e:
            print(f"❌ Error {gname}: {e}")

    print("\n🕒 Fin:", datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"))

# Punto de entrada estándar de Python:
# Si ejecutamos "python este_script.py", se llama a la función main()
if __name__ == "__main__":
    main()
