# Importamos la librería 'requests' para poder hacer llamadas a páginas web (APIs).
import requests
# Importamos 'pandas', la herramienta estándar para manipular tablas de datos.
import pandas as pd
# Importamos 're' (expresiones regulares) para limpiar textos complejos.
import re
# Importamos 'os' para manejar rutas de carpetas y archivos del sistema operativo.
import os
# Importamos 'datetime' para saber qué día y hora es hoy.
from datetime import datetime

# ========================================================
# CONFIGURACIÓN DE RUTAS (Sistema de Archivos)
# ========================================================

# Calculamos la ruta base del proyecto.
# __file__ es este archivo. os.path.dirname sube una carpeta.
# Hacemos esto dos veces ("..", "..") para subir de 'ETL/script' a la raíz del proyecto.
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Definimos la carpeta destino uniendo la base con "data" e "ine".
DATA_DIR = os.path.join(BASE_DIR, "data", "ine")

# Creamos la carpeta física en el disco si no existe (evita errores).
os.makedirs(DATA_DIR, exist_ok=True)

# Definimos una función auxiliar para eliminar duplicados de forma segura.
def _drop_duplicates_safe(df: pd.DataFrame, subset=None, note: str = "") -> pd.DataFrame:
    # Si el DataFrame está vacío, no hacemos nada y lo devolvemos tal cual.
    if df is None or df.empty:
        return df
    
    # Contamos cuántas filas hay antes de limpiar.
    before = len(df)
    
    # Eliminamos las filas repetidas basándonos en las columnas clave (subset).
    # ignore_index=True reinicia los números de fila (0, 1, 2...).
    df = df.drop_duplicates(subset=subset, ignore_index=True)
    
    # Calculamos cuántas filas hemos borrado.
    removed = before - len(df)
    
    # Si hemos borrado algo, imprimimos un aviso en la consola.
    if removed > 0:
        print(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})")
    
    # Devolvemos el DataFrame limpio.
    return df

# ========================================================
# FASE 1: EXTRACCIÓN (Extract)
# ========================================================

def obtener_datos_pib():
    """
    Conecta a la API del INE.
    CORREGIDO: Soporta el formato nuevo con campos 'Anyo' y 'FK_Periodo'.
    """
    # Definimos la URL de la tabla 30678 (PIB Oferta).
    # ?nult=100 pide los últimos 100 datos disponibles.
    url = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/30678?nult=100" 
    
    # Avisamos al usuario de que empieza la conexión.
    print(f"📡 Conectando con: {url}")

    # Definimos una cabecera para identificarnos (evita bloqueos por seguridad).
    headers = {"User-Agent": "PIBDownloader/1.0 (contacto@example.com)"}
    
    # Hacemos la petición GET a internet.
    response = requests.get(url, headers=headers)
    
    # Si la web da error (404, 500), detenemos el programa aquí.
    response.raise_for_status()
    
    # Convertimos la respuesta de texto a formato JSON (diccionario de Python).
    data = response.json()

    # Si el JSON está vacío, avisamos y devolvemos una tabla vacía.
    if not data:
        print("⚠️ La API devolvió un JSON vacío.")
        return pd.DataFrame()

    # Preparamos una lista vacía para ir guardando las filas procesadas.
    filas = []
    
    # --- LÓGICA NUEVA DE TRADUCCIÓN ---
    # El INE usa códigos internos: 19=Trimestre1, 20=Trimestre2, etc.
    # Creamos un diccionario para traducir esos números a texto legible.
    mapa_periodos = {19: "T1", 20: "T2", 21: "T3", 22: "T4"}

    # Recorremos cada serie de datos que ha devuelto el INE.
    for serie in data:
        # Extraemos el nombre de la serie (ej: "PIB a precios de mercado").
        nombre = serie.get("Nombre", "Sin nombre")

        # Recorremos la lista de datos temporales dentro de esa serie.
        for dato in serie.get("Data", []):
            # --- AQUÍ ESTÁ EL CAMBIO CRÍTICO ---
            # Extraemos el año (ej: 2024) y el código de periodo (ej: 19).
            anyo = dato.get("Anyo")
            fk = dato.get("FK_Periodo")
            
            # Inicializamos la variable periodo.
            periodo = None
            
            # Si tenemos año y el código FK está en nuestro mapa traductor:
            if anyo and fk in mapa_periodos:
                # Construimos el string manualmente: "2024" + "T1" = "2024T1".
                periodo = f"{anyo}{mapa_periodos[fk]}"
            else:
                # Si falla lo anterior, intentamos buscar los campos antiguos por si acaso.
                periodo = dato.get("T") or dato.get("NombrePeriodo")

            # Extraemos el valor numérico (el dinero del PIB).
            valor = dato.get("Valor")

            # Añadimos un diccionario con estos 3 datos a nuestra lista de filas.
            filas.append({
                "Indicador": nombre,
                "Periodo": periodo,
                "Valor": valor
            })

    # Convertimos la lista de diccionarios en un DataFrame de Pandas.
    df = pd.DataFrame(filas)
    
    # Eliminamos duplicados si el INE mandó el mismo dato dos veces.
    df = _drop_duplicates_safe(df, subset=["Indicador", "Periodo"], note="pib_ine(raw)")

    # Si la tabla está vacía, avisamos.
    if df.empty:
        print("⚠️ DataFrame vacío.")
    else:
        # Si tiene datos, decimos cuántos hemos bajado.
        print(f"✅ Datos descargados: {len(df)} registros.")
    
    # Devolvemos la tabla cruda.
    return df

# ========================================================
# FASE 2: TRANSFORMACIÓN (Transform)
# ========================================================

def limpiar_datos_pib(df):
    # Si la tabla viene vacía, la devolvemos tal cual.
    if df.empty: return df

    # Forzamos que la columna 'Valor' sea numérica.
    # 'coerce' convierte errores en NaN (Not a Number).
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    
    # Eliminamos las filas que no tengan valor numérico.
    df = df.dropna(subset=["Valor"])

    # Limpiamos la columna 'Periodo' para asegurar que es texto limpio.
    # La expresión regular elimina cualquier cosa entre paréntesis (ej: "(p)").
    df["Periodo"] = df["Periodo"].astype(str).apply(
        lambda x: re.sub(r"\(.*?\)", "", x).strip()
    )
    
    # 🔹 FILTRO AUTOMÁTICO
    # Buscamos filas que contengan "Producto interior bruto" (ignorando mayúsculas).
    filtro = df['Indicador'].str.contains("Producto interior bruto", case=False, na=False)
    
    # Si encontramos alguna fila que cumpla el filtro:
    if filtro.any():
        # Nos quedamos SOLO con esas filas.
        df = df[filtro].copy()
        print(f"   ℹ️ Filtrado automático: mantenemos solo filas de 'Producto interior bruto'")

    # Eliminamos duplicados de nuevo por seguridad.
    df = _drop_duplicates_safe(df, subset=["Indicador", "Periodo"], note="pib_ine(clean)")
    return df

def calcular_variacion_interanual(df):
    # Si está vacía, salimos.
    if df.empty: return df
    
    # Trabajamos sobre una copia para no romper el original.
    df = df.copy()
    
    # Ordenamos los datos por fecha (Periodo) para que el cálculo matemático tenga sentido.
    df.sort_values("Periodo", inplace=True)
    
    # Agrupamos por 'Indicador' (por si hubiera más de una serie).
    # Calculamos el cambio porcentual respecto a hace 4 filas (1 año).
    df["Variacion_interanual_%"] = df.groupby("Indicador")["Valor"].pct_change(4) * 100
    return df

# ========================================================
# FASE 3: CARGA (Load)
# ========================================================

def main():
    # Imprimimos aviso de inicio (sin emojis para evitar errores en Windows).
    print("Iniciando extraccion PIB (INE)...")
    
    # PASO 1: Llamamos a la función de extracción.
    df = obtener_datos_pib()

    # PASO 2: Transformamos y limpiamos.
    print("Transformando datos...")
    df_pib = limpiar_datos_pib(df)
    df_pib = calcular_variacion_interanual(df_pib)
    
    # Dedup final.
    df_pib = _drop_duplicates_safe(df_pib, subset=["Indicador", "Periodo"], note="pib_ine(final)")

    # Mostramos una muestra de los últimos datos en pantalla.
    print("\nUltimos datos del PIB:")
    print(df_pib.tail(8))

    # Generamos una marca de tiempo para el nombre del archivo.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # PASO 3: Guardado.
    # Definimos la ruta y guardamos el histórico en CSV.
    csv_path_hist = os.path.join(DATA_DIR, f"pib_ine_{timestamp}.csv")
    df_pib.to_csv(csv_path_hist, index=False, encoding="utf-8")

    # Guardamos también una versión en Excel.
    excel_path_hist = os.path.join(DATA_DIR, f"pib_ine_{timestamp}.xlsx")
    df_pib.to_excel(excel_path_hist, index=False)

    # Guardamos la versión 'latest' (la que usarán los otros scripts).
    csv_path_latest = os.path.join(DATA_DIR, "pib_ine_latest.csv")
    df_pib.to_csv(csv_path_latest, index=False, encoding="utf-8")

    # Confirmamos que todo ha ido bien.
    print(f"Guardado: {csv_path_latest}")

# Punto de entrada: Si ejecutamos el archivo directamente, arranca main().
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Si algo explota, imprimimos el error.
        print(f"Error inesperado: {e}")