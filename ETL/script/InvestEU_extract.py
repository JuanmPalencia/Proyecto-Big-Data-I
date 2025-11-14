# -*- coding: utf-8 -*-
"""
InvestEU ETL — Green Turning Point (GTP)
----------------------------------------
Descarga RAW desde:
  A) Listado de operaciones (HTML paginado)  -> data/raw/investeu/operations_list.csv
  B) Beneficiarios/Final recipients (PDF EIB) -> data/raw/investeu/final_recipients_YYYY.csv

Uso local:
  py ETL/script/investeu_extract.py --what all
  py ETL/script/investeu_extract.py --what list
  py ETL/script/investeu_extract.py --what recipients --years 2024

Variables de entorno (Docker/CI):
# Explica las variables .env que se pueden usar para configurar el script
  INVESTEU_BASE_SLEEP=300          # pausa base (seg) entre llamadas
  INVESTEU_429_PENALTIES=300,600,1200 # Pausas (en seg) si la API nos bloquea (Error 429)
  INVESTEU_TIMEOUT=90
  INVESTEU_MAX_RETRIES=3
  INVESTEU_RECIPIENT_PDFS=URL1,URL2   # opcional: lista explícita de PDFs a parsear
"""

# ---------------------------------------------------------------------------
# IMPORTACIONES DE LIBRERÍAS
# ---------------------------------------------------------------------------


from __future__ import annotations # Habilita "type hints" modernos (PEP 563)
import os # Para interactuar con el sistema Operativo
import re # Para expresiones regulares (Regex), usadas para buscar texto
import time # Para poner pausas (time.sleep)
import argparse # Para crear la interfaz de línea de comandos
import csv # Para manejar archivos CSV
import math # Para operaciones matemáticas
import sys # Para interactuar con el sistema
import requests #Librería clave: para realizar peticiones HTTP (descargas web)
from datetime import datetime, UTC # Para manejar fechas y horas, usando el estándar
from urllib.parse import urljoin # Para construir URLs absolutas desde enlaces relativos (ej. /pagina -> http://.../pagina)
from pathlib import Path # Librería moderna para manejar rutas de archivos (mejor que os.path)
from typing import List, Dict, Optional # "type hints" para que el código sea más legible 
from bs4 import BeautifulSoup # Librería clave para "parsear" (leer y navegar) HTML (Fuente A)
import pdfplumber # Librería clave: Para extraer texto y tablas de archivo PDF
import pandas as pd # Librería clave: PAra manipular los datos en tablas


# ---------------------------------------------------------------------------
# CONFIGURACIÓN DE RUTAS Y LOGS
# ---------------------------------------------------------------------------

# Define la ruta raíz del proyecto (sube 2 niveles: script->ETL->Proyecto)
BASE_DIR = Path(__file__).resolve().parents[2]

# Construye la ruta de destino para los datos RAW (Crudos) de InvestEU
DATA_DIR = BASE_DIR / "data" / "raw" / "investeu"

# Define la ruta para el archivo de log
LOG_DIR = BASE_DIR / "logs"

#Crea la carpeta de datos si no existe (parents=True crea carpetas intermedias)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Crea la carpeta de logs si no existe
LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    """
    Función de loggin personalizada
    Escribe un mensaje en la consola Y en un archivo de log
    """
    ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]") # Obtiene el imestamp actual
    line = f"{ts} {msg}" # Formatea el mensaje
    print(line) # Imprime en la consola

    # Abre el archivo de log en modo "append" (a) para añadir al final
    with open(LOG_DIR / "etl_investeu.log", "a", encoding="utf-8") as f:
        f.write(line + "\n") # Escrobe en el archivo

def _drop_duplicates_safe(df: pd.DataFrame, note: str = "") -> pd.DataFrame:
    """
    Función auxiliar para eliminar filas duplicadas de un DataFrame
    """
    if df is None or df.empty: # Comprobación: si el DataFrame esta vacio
        return df # Lo devuelve tal cual
    before = len(df) # Guarda el número de filas antes de limpiar
    df = df.drop_duplicates(ignore_index=True) # Elimina filas idénticas y resetea el indice
    removed = before - len(df) # calcula cuántas filas se borran
    if removed > 0: # Si se borro alguna
        log(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})") # Informa
    return df # Devuelve el DataFrame limpio


# ---------------------------------------------------------------------------
# CONSTANTES DE CONFIGURACIÓN
# ---------------------------------------------------------------------------

# URL de la Fuente A: la página HTML que lista las operaciones
LIST_URL = "https://investeu.europa.eu/investeu-operations/investeu-operations-list_en"

# URL de la fuente B: la lista por defecto de PDFs a descargar
DEFAULT_RECIPIENT_PDFS = [
    "https://www.eib.org/attachments/general/lists/investeu-final-recipients-beneficiaries-en.pdf"
]

# Álias para el directorio de salida (es el mismo que DATA_DIR)
OUT_DIR = DATA_DIR

# --- Carga de configuración desde variables de entorno (o usa valores por defecto) ---
TIMEOUT = int(os.getenv("INVESTEU_TIMEOUT", "90"))

# Carga en N.º de reintentos desde .env (defecto: 90s)
MAX_RETRIES = int(os.getenv("INVESTEU_MAX_RETRIES", "3"))

# Carga la pausa base (en seg) entre peticiones (defecto: 300s = 5min)
BASE_SLEEP = int(os.getenv("INVESTEU_BASE_SLEEP", "300"))

# Carga la lista de pausas de penalización (ej:"300,600,1200")
PENALTIES = [int(x) for x in os.getenv("INVESTEU_429_PENALTIES", "300,600,1200").split(",") if x.strip()]

# Cabeceras HTTP para simular ser un navegador y evitar bloqueos (404)
HEADERS = {
    "User-Agent": "GTP/1.0 (academic, non-commercial; contact: data@gtp.local)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# FUNCIÓN DE DESCARGA ROBUSTA (CON REINTENTOS)
# ---------------------------------------------------------------------------


def get_with_backoff(url: str, params=None, headers=None) -> requests.Response:
    """
    Función de descarga MUY robusta
    Maneja reintentos, errores 429 (rate limit) y otros errores de red
    """
    last_err = None # Variable para guardar el último error
    headers = headers or {} # Asegura que "headers" sea un dict
    penalty_idx = 0 # Índice para la lista de penalizaciones

    # Bucle de reintentos (ej: de 1 a 3)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # 1. Intenta la descarga
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)

            # 2. Manejo especial del error 429 (Too Many Requests)
            if r.status_code == 429:

                # Selecciona el tiempo de espera (ej: 300,600,1200)
                wait_s = PENALTIES[min(penalty_idx, len(PENALTIES) - 1)]
                penalty_idx += 1 # Aumenta el índice para la próxima espera
                print(
                    f"   ⏳ 429 rate limit — esperando {wait_s}s (intento {attempt}/{MAX_RETRIES})…"
                )
                time.sleep(wait_s) # Pausa el script
                continue #Salta al siguiente intento del bucle

            # 3. Si no es 429, comprueba otros errores (404,500,etc)
            r.raise_for_status()

            # 4. Exito, devuelve la erspuesta
            return r
        
        except requests.HTTPError as e: #Captura errores HTTP (4xx,5xx)
            last_err = e

            # Si es el último intento lanza el error y falla
            if attempt == MAX_RETRIES:
                raise
            print(
                f"   ⚠️  HTTP {e.response.status_code} — reintento {attempt}/{MAX_RETRIES} en 30s…"
            )
            time.sleep(30) # Pausa genérica de 30s

        # Captura otros errores (conexion,timeout)
        except Exception as e:
            last_err = e

            # Si es el último intento lanza error
            if attempt == MAX_RETRIES:
                raise
            print(
                f"   ⚠️  Error conexión — reintento {attempt}/{MAX_RETRIES} en 15s…"
            )
            time.sleep(15) #Pausa genérica de 15s
    raise last_err # Si el bucle termina sin return lanza el último error


# ---------------------------------------------------------------------------
# PARSEO DE HTML (Fuente A: Lista de Operaciones)
# ---------------------------------------------------------------------------

def parse_list_page(html: str) -> Dict[str, List[Dict]]:
    """
    Parsea el contenido HTML de una página de resultados
    para extraer los proyectos y el enlace a la página siguiente
    """
    # Carga el texto HTML en BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Listta para guardar los proyectos de esta pagina
    items = []

    # Busca todas las etiquetas <a>(enlaces) cuyo "href" contenga "/investeu-operations/"
    for a in soup.select('a[href*="/investeu-operations/"]'):

        # Obtiene el enlace
        href = a.get("href") or ""

        # Filtro: Solo queremos enlaces de operaciones que terminen en "_en"
        if "/investeu-operations/" in href and href.strip().endswith("_en"):

            # Extrae el título del enlace
            title = a.get_text(strip=True)

            # Convierte el enlace relativo en absoluto
            url = urljoin(LIST_URL, href)

            # Intenta encontrar el "contenedor" (article,div,li)
            li = a.find_parent(["article", "div", "li"]) or a

            # Extrae todo el texto del contenedor y lo limpia
            meta_text = " ".join(li.get_text(" ", strip=True).split())

            # Usa Regex para buscar una fecha
            m_date = re.search(r"(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})", meta_text)

            # Guarda la fecha si la encuentra
            date = m_date.group(1) if m_date else None

            # Lista para guardar "tags"
            tags = []

            # Lista de selectores CSS oara buscar tags (prueba varios formatos)
            for tag_sel in [
                "span.field--name-field-tags a",
                ".tags a",
                "a[href*='/country/']",
            ]:
                for t in li.select(tag_sel): # Busca usando el selector
                    txt = t.get_text(strip=True) # Extrae el texto del tag
                    if txt and txt not in tags: # Si es válido y no duplicado
                        tags.append(txt) # Lo añade

            # Añade un diccionario con todos los datos de este proyecto
            items.append(
                {
                    "title": title,
                    "url": url,
                    "date_text": date,
                    "tags": ";".join(tags) if tags else None, # Une los tags con ";""
                }
            )


    # --- Búsqueda de Paginación ---
    # Variable para el enlace "Pág. siguiente"
    next_url = None

    #Lista de selectores CSS para encontrar el botón next
    for sel in [
        "a[rel='next']",
        "a.pager__link--next",
        "a:contains('Next')",
        "a:contains('next')",
    ]:
        nxt = soup.select_one(sel) # Intenta encontrarlo
        if nxt and nxt.get("href"): # Si no lo encuentra y tiene enlace
            next_url = urljoin(LIST_URL, nxt.get("href")) # guarda la url absoluta
            break # y detiene la búsqueda

    # Devuelve los proyectos de esta página y el enlace a la siguiente
    return {"items": items, "next_url": next_url}


def scrape_operations_list(base_url: str = LIST_URL) -> pd.DataFrame:
    """
    Orquesta el scraping de la lista de operaciones, manejando la paginación
    """
    print("🔄 Listado de operaciones InvestEU …")
    # "Set" para guardar "urls" visitadas (evita bucles)
    seen_urls = set()
    # Lista para guardar TODAS las filas de TODAS las páginas
    all_rows = []
    # Empezamos por la página 1
    url = base_url

    #Bucle de paginación: mientras haya una "next_url" Y no la hayamos visitado ya while url and url not in seen_urls
    while url and url not in seen_urls:
        # Marca esta URL como visitada
        seen_urls.add(url)
        # Descarga el HTML con reintentos
        r = get_with_backoff(url, headers=HEADERS)
        # Parsea el HTML
        data = parse_list_page(r.text)
        # Obtiene los proyectos de esta página
        rows = data["items"]
        # Log de progreso
        print(f"   • Página {len(seen_urls)}: {len(rows)} ítems")
        # Añade los proyectos a la lista total
        all_rows.extend(rows)
        # Coge la URL de la pág. siguiente
        url = data["next_url"]
        # Pausa larga (5 min) para ser amable
        time.sleep(BASE_SLEEP)

    # Si no encontramos nada
    if not all_rows:
        print("⚠️  No se extrajeron operaciones (revisa estructura/paginación).")
        # Devuelve un DF vacio
        return pd.DataFrame()

    # Convierte la lista total a DataFrame y elimina duplicados por URL
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"])
    # Añade metadato: URL de origen
    df["source"] = base_url
    # Añade metadato: fecha
    df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")
    cols = ["title", "url", "date_text", "tags", "source", "extraction_date"]
    # Devuelve un DataFrame ordenado
    return df[cols]


# ---------------------------------------------------------------------------
# PARSEO DE PDF (Fuente B: Beneficiarios Finales)
# ---------------------------------------------------------------------------

def normalise_pdf_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia los nombres de las columnas extraídas del PDF,
    que suelen ser sucios 
    """
    # Comprobación de seguridad
    if df is None or df.empty:
        return pd.DataFrame()
    # Elimina columnas 100% vacias
    df = df.dropna(how="all", axis=1)
    # Limpia espacios en nombres de columnas 
    df = df.rename(columns=lambda c: str(c).strip())

    # Lista de nombres de columna "ideales" que buscamos
    expected = [
        "Financial Product",
        "Operation Name",
        "Borrower Name",
        "Borrower Address",
        "Borrower Country",
        "Financing Form",
        "Policy Area supported",
        "Amount of financial support received in EUR",
    ]

    # Diccionario para mapear {sucio:limpio}
    rename_map = {}

    # Lógica de "fuzzy matching" (coincidencia aproximada)
    # Itera sobre los nombres sucios
    for col in df.columns:
        # Pasa a minúsculas
        c = col.lower()
        if "financial" in c and "product" in c:
            rename_map[col] = "Financial Product"
        elif "operation" in c and "name" in c:
            rename_map[col] = "Operation Name"
        elif "borrower" in c and "name" in c:
            rename_map[col] = "Borrower Name"
        elif "borrower" in c and "address" in c:
            rename_map[col] = "Borrower Address"
        elif "country" in c:
            rename_map[col] = "Borrower Country"
        elif "financing" in c and "form" in c:
            rename_map[col] = "Financing Form"
        elif "policy" in c and "area" in c:
            rename_map[col] = "Policy Area supported"
        elif "amount" in c and "eur" in c:
            rename_map[col] = "Amount of financial support received in EUR"
    df = df.rename(columns=rename_map)
    return df


def extract_final_recipients_from_pdf(pdf_url: str) -> pd.DataFrame:
    """
    Orquesta la descarga y extracción de tabla de un archivo PDF
    """
    print(f"🔄 PDF EIB recipients: {pdf_url}")

    # 1. Descarga el archivo PDF con intentos
    r = get_with_backoff(
        pdf_url,
        headers={
            "User-Agent": HEADERS["User-Agent"],
            # Le decimos al servidor que queremos un PDF
            "Accept": "application/pdf",
        },
    )

    # 2. Guarda el PDF temporalmente en el disco
    # "wb" es obligatorio para archivos binarios
    with open(OUT_DIR / "_tmp_investeu.pdf", "wb") as f:
        f.write(r.content)
    # Lista para guardar las tablas (DataFrames)
    tables = []

    # 3. Abre el PDF temporal con pdfplumber
    with pdfplumber.open(OUT_DIR / "_tmp_investeu.pdf") as pdf:
        # 4. Itera sobre CADA página del PFD
        for i, page in enumerate(pdf.pages, start=1):
            try:
                # 5. El comando mágico: Intesta extraer una tabla de la página
                tbl = page.extract_table()

                # 6. Validación de la tabla
                # Si no hay tabla, o solo tiene cabeza
                if not tbl or len(tbl) < 2:
                    # Salta a la siguiente página
                    continue

                # 7. Procesa la tabla
                # Limpia la cabecera
                header = [
                    str(x).strip() if x is not None else "" for x in tbl[0]
                ]
                # Coge el resto de filas
                rows = tbl[1:]
                # Convierte en DataFrame
                df = pd.DataFrame(rows, columns=header)
                # Añade metadato: Nº de página
                df["page"] = i
                # Añade el DataFrame de esta página a la lista
                tables.append(df)
            except Exception:
                # Si "extract_table" falla, ignora la pag
                continue

    # Si no encontramos ninguna tabla en el pdf
    if not tables:
        print("⚠️  No se extrajeron tablas del PDF (estructura no tabular).")
        return pd.DataFrame()

    # 8. Une todas las tablas de todas las páginas en un solo DataFrame
    df = pd.concat(tables, ignore_index=True)
    # 9. Limpia los nombres de las columnas
    df = normalise_pdf_table(df)
    # Añade metadato: URL del PDF
    df["pdf_url"] = pdf_url
    # Añade metadato fecha
    df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")
    # 10. Elimina filas duplicadas
    df = _drop_duplicates_safe(df, note="final_recipients(pdf)")
    return df

# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL (ORQUESTADOR DE CLI)
# ---------------------------------------------------------------------------

def main():
    """
    Función principal que se ejecuta la llamar al script
    """
    # 1. Configuración del Parseador de Argumentos
    ap = argparse.ArgumentParser(
        description="InvestEU ETL (GTP) — operaciones y beneficiarios"
    )
    ap.add_argument(
        "--what",
        default="all",
        choices=["all", "list", "recipients"],# Opciones: todo, solo HTML, solo PDF
        help="Qué extraer: list (operaciones), recipients (beneficiarios PDF) o all",
    )
    ap.add_argument(
        "--years",
        default="",
        help="Años objetivo p/ recipients (informativo si pasas PDFs explícitos)",
    )
    ap.add_argument(
        "--out-dir", default=str(OUT_DIR), help="Carpeta salida"
    )
    # 2. Lee los argumentos pasados por la terminal
    args = ap.parse_args()
    #Convierte la ruta de salida a objeto path
    out = Path(args.out_dir)
    # Crea la carpeta de salida
    out.mkdir(parents=True, exist_ok=True)

    # 3. Log de inicio
    print("🚀 InvestEU ETL — inicio")
    print(f"   Carpeta out: {out}")
    print(f"   Timeout:     {TIMEOUT}s | Retries: {MAX_RETRIES}")
    print(
        f"   Pausa base:  {BASE_SLEEP}s  | Castigos 429 (s): {PENALTIES}\n"
    )

    # 4. EJECUCIÓN TAREA 1: Scraping de lista (HTML)
    # Si el usuario pidio "all" o "list"
    if args.what in ("all", "list"):
        try:
            # Llama a la funcion de scraping
            df_list = scrape_operations_list(LIST_URL)
            # Si tiene exito aplica una limpieza final de duplicados
            if not df_list.empty:
                df_list = _drop_duplicates_safe(
                    df_list, note="operations_list.csv"
                )
                # Define la ruta del CSV de salida
                p = out / "operations_list.csv"
                # Guarda el CSV
                df_list.to_csv(p, index=False, encoding="utf-8")
                print(
                    f"✅ operations_list.csv -> {p} ({len(df_list):,} filas)"
                )
        except requests.HTTPError as e:
            print(f"❌ HTTP list: {e}")# Captura de errores
        except Exception as e:
            print(f"❌ Error list: {e}")

        # Pausa larga (5min) antes de tarea 2
        time.sleep(BASE_SLEEP)

    # 5. EJECUCIÓN TAREA 2: Extracción de recipientes (PDF)
    # Si el usuario pidió "all" o "recipients"
    if args.what in ("all", "recipients"):
        # Coprueuba si se pasó una lista de PDFs en el .env
        pdfs_env = os.getenv("INVESTEU_RECIPIENT_PDFS")
        # Lógica: usa la lista del .env si existe, si no, usa por defecto
        pdf_urls = (
            [u.strip() for u in pdfs_env.split(",")]
            if pdfs_env
            else DEFAULT_RECIPIENT_PDFS
        )

        # Lista para guardar DataFrames (si hay varios PDFs)
        all_pdf_rows = []
        # Itera sobre cada URL de PDF
        for u in pdf_urls:
            try:
                # Llama a la función de extraccón
                df_pdf = extract_final_recipients_from_pdf(u)
                if not df_pdf.empty:
                    # Añade el DataFrame a la lista
                    all_pdf_rows.append(df_pdf)
            except requests.HTTPError as e:
                print(f"❌ HTTP recipients: {e}") # Captura de errores por PDF
            except Exception as e:
                print(f"❌ Error recipients: {e}")
            time.sleep(BASE_SLEEP) #Pausa larga (5 min) entre PDFs

        # Si se extrajo al menos una tabla
        if all_pdf_rows:
            # Une todos los DataFrames (de todos los PDFs) en uno solo
            df_all = pd.concat(all_pdf_rows, ignore_index=True)
            # Aplica una limpieza final de duplicados (por si los PDFs se solapan)
            df_all = _drop_duplicates_safe(
                df_all, note="final_recipients(all)"
            )

            # Intenta "adivinar" el año desde las URLs de los PDF
            year_hint = ""
            m = re.search(r"(20\d{2})", " ".join(pdf_urls))
            if m:
                year_hint = f"_{m.group(1)}" 
            # Define el nombre del archivo de salida
            p = out / f"final_recipients{year_hint}.csv"
            df_all.to_csv(p, index=False, encoding="utf-8")#Guarda el csv final
            print(
                f"✅ final_recipients{year_hint}.csv -> {p} ({len(df_all):,} filas)"
            )
    # 6. Log de fin
    print(
        f"\n🕒 Fin: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )


if __name__ == "__main__":
    main()
