#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinel-2 — Descargar ZIP completo (.SAFE) y extraer solo TCI/Bxx/SCL del interior.
...
"""

# Habilita "type hints" (anotaciones de tipo) modernos en python
from __future__ import annotations
# Importa librerias del sistema: os(rutas), sys(sistema), time(pausas), json(datos)
# arfparse(CLI), fnmatch(patrones de archivos), zipfile(manejo ZIP)
import os, sys, time, json, argparse, fnmatch, zipfile
# Importa librerias para fechas y URLs
from datetime import date, timedelta
from urllib.parse import urlencode
# Importa la funcion para cargar el archivo .env (variables de entorno)
from dotenv import load_dotenv
# Importa requests para hacer peticiones HTTP (descargas)
import requests
# Importa pandas para manejar tablas de datos
import pandas as pd
# Importa una función específica de pandas para "aplanar" JSONs
from pandas import json_normalize
# Importa libreria de imagenes "Pillow" (PIL)
from PIL import Image 

# Elimina el límite de tamaño de pixeles de Pillow para evitar errores con imagenes satelitales grandes
Image.MAX_IMAGE_PIXELS = None 
# Define la URL base de la API de metadatos (el catálogo)
CAT_BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
# Define la URL del servidor de auntenticacion (OAuth2)
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"


# ======================================================
# BLOQUE 1: Funciones auxiliares
# ======================================================

# Convierte un objeto "date" a un string en formato ISO(YYYY-MM-DD)
def iso_day(d: date) -> str:
    return d.strftime("%Y-%m-%d")

# Devuelve la fecha de hoy y la fecha de "hace X dias" en formato ISO
def today_and_start(days_back: int):
    today = date.today()
    start = today - timedelta(days=days_back)
    return iso_day(today), iso_day(start)

# Comprueba si una variable de entorno (del .env) existe y la devuelve
def ensure_env(var: str) -> str:
    v = os.getenv(var) # Lee la variable
    if not v: #Si no existe, lanza error
        raise RuntimeError(f"Falta variable de entorno: {var}")
    return v

# ======================================================
# BLOQUE 2: Funciones de API (Auntenticacion y Busqueda)
# ======================================================

# Obtiene el token de acceso de Copernicus (Keycloak)
def get_keycloak(username: str, password: str) -> str:
    # Prepara los datos del formulario de login
    data = {"client_id": "cdse-public", "username": username, "password": password, "grant_type": "password"}
    # Envia la peticion POST para loguearse
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    try:
        # Si la erspuesta es 4xx o 5xx, lanza un error
        r.raise_for_status()
    except Exception: # Si falla el login
        try: payload = r.json() # Intenta leer el error en JSON
        except Exception: payload = r.text # Si no, lee el error en texto
        raise RuntimeError(f"Fallo creando token. Respuesta: {payload}")
    # Devuelve el "access_token" extraido del JSON de respuesta
    return r.json()["access_token"]

# Construye el string del filtro OData para la API
def make_filter(collection: str, start_iso: str, end_iso: str, wkt: str | None,
                only_l2a: bool, tile: str | None) -> str:
    # Filtro base: Colección (SENTINEL-2) y rango de fechas
    base = (
        f"Collection/Name eq '{collection}' "
        f"and ContentDate/Start gt {start_iso}T00:00:00.000Z "
        f"and ContentDate/Start lt {end_iso}T00:00:00.000Z"
    )
    # Añade el filtro de geolocalizacion (WKT Polygon) si se proporciona
    if wkt:
        base += f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
    # Añade filtro para excluir productos L1C (solo queremos L2A, procesados)
    if only_l2a:
        base += " and not contains(Name,'L1C')"
    # Añade filtro por "tile" (zonas MGrS especifica) si se proporciona
    if tile:
        base += f" and contains(Name,'{tile}')"
    return base

# Descarga una unica pagina de resultados de la API
def fetch_page(params: dict) -> dict:
    # Realiza la peticion GET con los parametros (ej:$filter=...,$top=50)
    r = requests.get(f"{CAT_BASE}?{urlencode(params)}", timeout=90)
    r.raise_for_status() # Coprueba errores
    return r.json() # Devuelve la pagina como un diccionatio

# Orquesta la descarga de todas las paginas de resultados (paginacion)
def fetch_all(collection: str, start_iso: str, end_iso: str, wkt: str | None,
            top: int, max_pages: int, orderby: str, include_count: bool,
            only_l2a: bool, tile: str | None, select: str | None) -> dict:
    # Prepara los parametros base para la API
    params = {
        "$filter": make_filter(collection, start_iso, end_iso, wkt, only_l2a, tile),
        "$orderby": orderby, # Ordena resultados
        "$top": str(top), # Cuantos resultados por pagina
    }
    # Añade parametros opcionales si existen
    if include_count: params["$count"] = "true" #Pedir Nº total de resultados
    if select: params["$select"] = select # Pedir solo columnas especificas

    # Inicializa variables para el bucle de paginacion
    all_items, count, skip = [], None, 0
    # Bucle que se ejecuta "max_pages" veces como maximo
    for _ in range(max_pages):
        page_params = dict(params) #Copia los parametros base
        if skip: page_params["$skip"] = str(skip) # Añade $skip si no es la pag. 1
        js = fetch_page(page_params) # Descarga la pagina
        #Si es la primera pagina y pedimos "$count", guarda el total
        if "@odata.count" in js and count is None: count = js["@odata.count"]
        # Obtiene la lista de "value" (los productos) de la pagina
        items = js.get("value", [])
        # Añade los productos de esta pagina a la lista total
        all_items.extend(items)
        # Si la API devuelve menos productos de los pedidos, es la ultima pagina
        if len(items) < top: break
        # Prepara el "skip" para la siguiente pagina
        skip += top
        time.sleep(0.3) # Pausa breve para ser "amable" con la API
    # Prepara el diccionario de salida final
    out = {"value": all_items}
    if count is not None: out["@odata.count"] = count
    return out

# Convierte un JSON de  metadatos (de "fetch_all") en un DataFrame de pandas
def to_flat_df(js: dict) -> pd.DataFrame:
    # Usa "json_normalize" para "aplanar" el JSON anidado en una tabla
    df = json_normalize(js.get("value", []))
    if not df.empty: # Si el DataFrame no esta vacio
        # Define un orden de columnas preferiodo (las mas importantes primero)
        first_cols = [c for c in [
            "Id", "Name", "ContentDate.Start", "ContentDate.End",
            "ContentType", "ContentLength", "OriginDate", "GeoFootprint"
        ] if c in df.columns]
        # Obtiene el resto de columnas
        other_cols = [c for c in df.columns if c not in first_cols]
        # Reordena el DataFrame
        df = df[first_cols + other_cols]
    return df


# --- Funciones de Descarga de Archivos ---

# Sigue las redirecciones HTTP (301,302,...) para encontrar el archivo real
def follow_redirects(session: requests.Session, url: str, max_hops: int = 10) -> requests.Response:
    # Realiza la peticion inicial SIN seguir redirecciones
    resp = session.get(url, allow_redirects=False, timeout=120)
    hops = 0
    # Bucle: mientras la respuesta sea una redireccion (30x) y no superemos el limite
    while resp.status_code in (301, 302, 303, 307, 308) and hops < max_hops:
        loc = resp.headers.get("Location") # Obtiene la nueva URL de la cabecera
        if not loc: break # Si no hay "Location", rompe el bucle
        # Pide la nueva URL (el servidor de S3/almacenamiento)
        resp = session.get(loc, allow_redirects=False, timeout=300)
        hops += 1
    return resp


# Descarga el archivo ZIP completo (el .SAFE)
def download_product_zip(session: requests.Session, product_id: str, identifier: str, out_dir: str) -> str:
    """
    Descarga el .SAFE (ZIP) completo:
      GET https://catalogue.dataspace.copernicus.eu/odata/v1/Products(<GUID>)/$value
    """
    # Crea carpeta de salida si no existe
    os.makedirs(out_dir, exist_ok=True)
    # Define la ruta del archivo ZIP de salida
    out_zip = os.path.join(out_dir, f"{identifier}.zip")
    # Si el ZIP ya existe se salta la descarga
    if os.path.exists(out_zip):
        print(f"⏭️  ZIP ya existe, omitiendo descarga: {out_zip}")
        return out_zip

    # Contruye la URL de descarga del producto (usando el "product_id"/GUID)
    url = f"{CAT_BASE}({product_id})/$value"
    # Llama a la funcion que sigue las redirecciones
    resp = follow_redirects(session, url)
    resp.raise_for_status() # Comprueba errores en la descarga final
    # Escribe el contenido de la respuesta (los bytes del ZIP) en el archivo
    with open(out_zip, "wb") as f:
        f.write(resp.content)
    return out_zip

# --- Funciones de Extracción (ZIP) ---

# Construye una lista de patrones de busqueda (ej:"*TCI*.jp2") segun el modo y bandas
def build_patterns(mode: str, bands: list[str] | None) -> list[str]:
    mode = (mode or "").lower() # Pasa el modo a minusculas
    pats: list[str] = []
    # Si el modo es "tci" (TRUE COLOR IMAGE)
    if mode == "tci":
        # Define patrones para encontrar el TCI en cualquier carpeta de resolucion
        pats = [
            "*IMG_DATA*/R10m/*TCI*.jp2",
            "*IMG_DATA*/R20m/*TCI*.jp2",
            "*IMG_DATA*/R60m/*TCI*.jp2",
            "*IMG_DATA_R10m*/*TCI*.jp2",
            "*IMG_DATA_R20m*/*TCI*.jp2",
            "*IMG_DATA_R60m*/*TCI*.jp2",
            "*IMG_DATA*/*TCI*.jp2",
            "*/*TCI*.jp2",
        ]
    # Si el modo es "scl" (Scene Classification Layer)
    elif mode == "scl":
        # Define patrones para encontrar el SCL (suele estar en R20m)
        pats = [
            "*IMG_DATA*/R20m/*SCL*.jp2",
            "*IMG_DATA_R20m*/*SCL*.jp2",
            "*IMG_DATA*/*SCL*.jp2",
            "*/*SCL*.jp2",
        ]
    # Si el modo es "bands" (Bandas cientificas)
    elif mode == "bands" and bands:
        # Limpia la lisa de bandas
        b = [x.strip().upper() for x in bands]
        # Itera sobre cada banda solicitada
        for band in b:
            # Añade patrones para buscar esa banda en todas las carpetas posibles
            pats += [
                f"*IMG_DATA*/R10m/*{band}*10m*.jp2",
                f"*IMG_DATA*/R20m/*{band}*20m*.jp2",
                f"*IMG_DATA*/R60m/*{band}*60m*.jp2",
                f"*IMG_DATA_R10m*/*{band}*.jp2",
                f"*IMG_DATA_R20m*/*{band}*.jp2",
                f"*IMG_DATA_R60m*/*{band}*.jp2",
                f"*IMG_DATA*/*{band}*.jp2",
                f"*/*{band}*.jp2",
            ]
    return pats

# Extrae selectivamente archivos de un ZIP usando los patrones
def extract_selected_from_zip(zip_path: str, mode: str, bands: list[str] | None, out_dir: str) -> list[str]:
    # Obtiene la lista de patrones
    pats = build_patterns(mode, bands)
    extracted: list[str] = [] # Lista para guardar los archivos extraidos
    # Abre el archivo ZIP en modo lectura ("r")
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Obtiene una lista de todos los archivos dentro del ZIP
        members = zf.namelist()

        # Bloque de diagnostico(solo para TCI)
        if mode.lower() == "tci":
            # Busca archivos que contengan "TCI" para ayudar a depurar
            tci_candidates = [m for m in members if "TCI" in m.upper()]
            if tci_candidates:
                print("🔎 Posibles TCI detectados en el ZIP (muestras):")
                for x in tci_candidates[:20]:
                    print("  -", x)

        # Lógica de filtrado
        to_get = set() # Un "set" para guardar los archivos a extraer (evita duplicados)´
        # Itera sobre cada patron
        for pat in pats:
            # Itera sobre cada miembro del ZIP
            for m in members:
                # Comprueba si el miembro coincide con el patron
                if fnmatch.fnmatch(m, pat):
                    to_get.add(m) # Si coincide, lo añade a la lista de extraccion

        # Si no se encontro ningun archivo que coincida
        if not to_get:
            print(f"⚠️  No se encontraron archivos que coincidan con {mode} (bandas={bands}) dentro del ZIP.")
            return [] # Devuelve una lista vacia

        # Define la carpeta de salida
        base_out = os.path.join(os.path.dirname(zip_path), os.path.splitext(os.path.basename(zip_path))[0], "extracted")
        os.makedirs(base_out, exist_ok=True) # Crea la carpeta si no existe

        # ITERA SOBRE LOS ARCHIVOS ENCONTRADOS
        for m in sorted(to_get):
            # Remplaza "/" por "_" para un nombre de archivo seguro
            safe_name = m.replace("/", "_")
            # Define la ruta final del archivo extraido
            out_file = os.path.join(base_out, safe_name)



            # --- Bloque de extracción y conversión ---
            # Abre el miembro "m" dentro del ZIP y abre el archivo de salida "out_file"
            with zf.open(m) as src, open(out_file, "wb") as dst:
                # Lee los bytes del ZIP y los escribe en el disco
                dst.write(src.read())
            
            # Comprueba si el archivo es un .jp2 y el modo es "tci" para convertir a PNG
            if out_file.lower().endswith(".jp2") and mode == "tci":
                try:
                    # Define la ruta del .png (reemplazando la extension)
                    png_path = out_file.replace(".jp2", ".png")
                    # Abre el jp2 que acabamos de extraer
                    with Image.open(out_file) as img:
                        print(f"   ↳ Convirtiendo a PNG...")
                        # Guarda la imagen en formato PNG
                        img.save(png_path, "PNG")
                    # Añade la ruta del png a la lista de "extraidos"
                    extracted.append(png_path)
                    print(f"✔ Generado: {png_path}")
                except Exception as e:
                    # Si falla la conversion informa del error
                    print(f"⚠️ Error convirtiendo imagen: {e}")
                    # y añade el jp2 original a la lista
                    extracted.append(out_file) # Si falla, nos quedamos con el jp2
            else:
                # Si no es TCI (es SCL o una banda B04/B08), nunca lo convierte
                # Añade el jp2 cientifico a la lista
                extracted.append(out_file)
            # --- FIN CAMBIO ---
    # Devuelve la lista de rutas de los archivos extraidos
    return extracted

# --- Configuracion de la interfaz de Linea de Comandos (CLI) ---

# Diccionario de Áreas de Interes (AOI) predefinidas
AOIS = {
    "tiny": "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
    "madrid": "POLYGON((-3.9 40.2, -3.9 40.6, -3.3 40.6, -3.3 40.2, -3.9 40.2))",
}

# Define la funcion que parsea los argumentos de la terminal
def parse_args():
    # Inicia el parser de argumentos
    ap = argparse.ArgumentParser(description="Sentinel-2 — descarga ZIP completo y extracción selectiva (TCI/Bxx/SCL)")
    # Define todos los argumentos que el script acepta
    ap.add_argument("--days-back", type=int, default=7)
    ap.add_argument("--collection", type=str, default="SENTINEL-2")
    ap.add_argument("--aoi", type=str, choices=["tiny", "madrid", "custom"], default="madrid")
    ap.add_argument("--wkt", type=str, default=None) # Para usar con --aoi custom
    ap.add_argument("--top", type=int, default=50) # Resultados por pagina
    ap.add_argument("--max-pages", type=int, default=2)
    ap.add_argument("--orderby", type=str, default="ContentDate/Start desc") # Mas nuevos primero
    ap.add_argument("--select", type=str, default="Id,Name,ContentDate,ContentType,ContentLength,OriginDate,GeoFootprint")
    ap.add_argument("--only-l2a", dest="only_l2a", action="store_true") # (flag)
    ap.add_argument("--no-only-l2a", dest="only_l2a", action="store_false") # (flag)
    ap.set_defaults(only_l2a=True) # L2A por defecto
    ap.add_argument("--tile", type=str, default=None)
    ap.add_argument("--csv", type=str, default="copernicus_schema_sample.csv") # Donde guardar metadatos
    ap.add_argument("--download", action="store_true", help="Descargar ZIP .SAFE y extraer") # (flag)
    ap.add_argument("--asset", type=str, choices=["tci", "scl"], default=None, help="tci | scl")
    ap.add_argument("--bands", type=str, default=None, help="B04,B08,...")
    ap.add_argument("--max-downloads", type=int, default=1)
    ap.add_argument("--out-dir", type=str, default="data/s2")
    # Parsea los argumentos y los devuelve
    return ap.parse_args()

# ======================================================
# BLOQUE 3: Función principal
# ======================================================

def main():
    # Carga las variables del archivo .env (COPERNICUS_USER/PASSWORD)
    load_dotenv()
    # Lee los argumentos de laterminal (ej. --aoi madrid --asset tci ...)
    args = parse_args()

    # --- Configuracion de rutas ---
    # Define DATA_DIR basado en el Directorio de Trabajo Actual (donde ejecutas el comando)
    DATA_DIR = os.path.join(os.getcwd(), "data", "s2")
    os.makedirs(DATA_DIR, exist_ok=True)
    # Sobrescrive el argumenot "out_dir" con esta ruta
    args.out_dir = DATA_DIR
    # Sobrescrive el argumento "csv" con esta ruta
    args.csv = os.path.join(DATA_DIR, os.path.basename(args.csv))

    # --- Lógica de AOI (Area de Interes) ---
    if args.aoi == "custom": # Si el usuario quiere un poligono personalizado
        if not args.wkt: # Comprueba que haya pasado el WKT
            print("ERROR: --aoi custom requiere --wkt 'POLYGON((...))'", file=sys.stderr)
            sys.exit(2) # Cierra el script con codigo de error
        wkt = args.wkt
    else: # Si el usuario eligio "madrid" o "tiny"
        wkt = AOIS[args.aoi] # Lo busca en el diccionario AOIS

    # --- Búsqueda de Metadatos ---
    # Obtiene las fechas de inicio y fin
    today_iso, start_iso = today_and_start(args.days_back)
    # Imprime un resumen de la consulta
    print("→ Consulta OData…")
    print("Colección:", args.collection)
    print("Ventana:  ", f"{start_iso} → {today_iso}")
    print("AOI:      ", args.aoi)
    print("Solo L2A: ", args.only_l2a)
    if args.tile:
        print("Tile:     ", args.tile)
    print()

    # Llama a la función que descarga TODAS las paginas de metadatos
    js = fetch_all(
        collection=args.collection,
        start_iso=start_iso,
        end_iso=today_iso,
        wkt=wkt,
        top=args.top,
        max_pages=args.max_pages,
        orderby=args.orderby,
        include_count=True,
        only_l2a=args.only_l2a,
        tile=args.tile,
        select=args.select
    )
    # Imprime el Nº total de productos encontradso
    if "@odata.count" in js:
        print("Total (count) filtro:", js["@odata.count"])
    # Convierte el JSON de metadatos en un DataFrame de Pandas
    df = to_flat_df(js)
    # Si no se encontro nada, termina
    if df.empty:
        print("No hay productos para este filtro.")
        return

    # Imprime una muestra de los 10 primeros productos
    print("\n— Primeros productos —")
    cols = [c for c in ["Id", "Name", "ContentDate.Start"] if c in df.columns]
    print(df[cols].head(10))

    # Intenta guardar el DataFrame de metadatos en un CSV
    try:
        df.to_csv(args.csv, index=False)
        print(f"\nCSV guardado: {args.csv} (filas: {len(df)})")
    except Exception as e:
        print(f"Advertencia al escribir CSV: {e}")

    # Si no se paso el flag --download, termina aqui
    if not args.download:
        print("\nDescarga desactivada (usa --download).")
        return

    # --- Lógica de descarga (requiere--download) ---
    # Intenta cargar las credenciales del .env
    try:
        user = ensure_env("COPERNICUS_USER")
        pwd = ensure_env("COPERNICUS_PASSWORD")
    except RuntimeError as e: # Si fallan 
        print(f"Descarga deshabilitada: {e}")
        return # Termina

    # Intenta obtener el token de autenticación
    try:
        token = get_keycloak(user, pwd) # si falla el login
    except Exception as e:
        print(f"Error de autenticación: {e}")
        return # Termina

    # Crea una "sesion" de requests (para reusar la conexion y el token)
    session = requests.Session()
    # Añade el token a las cabeceras de todas las peticiones futuras de esta sesion
    session.headers.update({"Authorization": f"Bearer {token}"})

    # --- Determina el modo de extraccion (TCI, SCL, o bandas) ---
    bands = None
    mode = "tci" # TCI es el modo por defecto si no se especifica
    if args.bands: # Si el usuario pasó --bandas B04,B08
        bands = [s.strip() for s in args.bands.split(",") if s.strip()]
        mode = "bands"
    elif args.asset: # Si el usuario pasó --aset scl
        mode = args.asset.lower()

    # --- Bucle de descarga y extracción ---
    downloaded = 0 # Contador de descargas
    # Itera sobre la lista de productos (metadatos)
    for item in js.get("value", []):
        # Si ya hemos alcanzado el limite de --max-downloads, paramos
        if downloaded >= args.max_downloads:
            break
        # Obtiene el ID (GUID) y el Nombre del producto
        pid = item.get("Id")
        name = item.get("Name", "")
        # Crea un identificador
        identifier = name.split(".")[0] if isinstance(name, str) and name else pid
        if not pid: # Si no hay id salta al siguiente
            continue

        # Inicia un bloque "try" para capturar errores de este producto
        try:
            # 1. Descarga el archivo ZIP (o lo salta si ya existe)
            print(f"\n[{identifier}] Descargando ZIP completo…")
            zip_path = download_product_zip(session, pid, identifier, args.out_dir)
            print(f"[{identifier}] ZIP guardado en: {zip_path}")

            # 2. Extrae los archios especificos (TCI,SCL o Bandas)
            print(f"[{identifier}] Extrayendo '{mode}' (bandas={bands})…")
            extracted = extract_selected_from_zip(zip_path, mode, bands, args.out_dir)

            # Informa del resultado de la extraccion
            if not extracted:
                print(f"[{identifier}] ⚠️  No se extrajo nada que coincida con el modo.")
            else:
                print(f"[{identifier}] ✅ Extraídos {len(extracted)} archivos.")

            # Incrementa el contador de descargas
            downloaded += 1
            time.sleep(0.3) # Pausa breve

        # Captura de errores específicos de esta descarga
        except requests.HTTPError as he:
            print(f"[{identifier}] Error HTTP al descargar/extraer: {he}")
        except Exception as e:
            print(f"[{identifier}] Error: {e}")

# --- Resumen Final ---

    if downloaded == 0:
        print("\nNo se descargaron ZIPs (revisa token, filtros o estado online/offline).")
    else:
        print(f"\n✅ Listo. Productos procesados: {downloaded}")

# --- Punto de Entrada del Script ---
# Si el script se ejecuta directamente (python Sentinel-2_extract.py)
if __name__ == "__main__":
    try:
        main() # LLama a la funcion principal
    # Captura de errores globales
    except requests.HTTPError as e:
        print(f"HTTPError: {e} – contenido: {getattr(e.response, 'text', '')[:200]}...")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
