import requests  # Cliente HTTP para conectar con la API del INE
import pandas as pd  # Estándar industrial para manipulación de datos tabulares
import re  # Expresiones Regulares (Regex) para limpieza avanzada de texto
import os  # Gestión de rutas del sistema operativo
from datetime import datetime  # Para generar marcas de tiempo (timestamps) en los archivos


# ========================================================
# CONFIGURACIÓN DE RUTAS (Sistema de Archivos)
# ========================================================

# Calculamos la ruta base del proyecto de forma dinámica
# Esto asegura que el script funcione igual en tu PC y en Docker
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Definimos dónde se guardarán los datos raw (crudos) descargados
DATA_DIR = os.path.join(BASE_DIR, "data", "ine")

# Crea la carpeta si no existe (evita errores de "File Not Found")
os.makedirs(DATA_DIR, exist_ok=True)


# 🔹 NUEVO: helper dedup seguro
def _drop_duplicates_safe(df: pd.DataFrame, subset=None, note: str = "") -> pd.DataFrame:
    """
    Elimina filas duplicadas sin romper nada.
    - subset: columnas clave para considerar un registro como duplicado.
    - note: texto de contexto para el log.
    """
    if df is None or df.empty:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=subset, ignore_index=True)
    removed = before - len(df)
    if removed > 0:
        print(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})")
    return df


# ========================================================
# FASE 1: EXTRACCIÓN (Extract)
# ========================================================

def obtener_datos_pib():
    """
    Conecta a la API JSON del INE (Tempus 3) y descarga 
    la tabla 33387 (Contabilidad Nacional Trimestral).
    Devuelve DataFrame con columnas: Indicador, Periodo, Valor.
    """

    # URL del endpoint específico para el PIB trimestral
    # nult=40 solicita los últimos 40 periodos (10 años = 40 trimestres)
    url = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/33387?nult=40"
    print(f"📡 Conectando con: {url}")

    # User-Agent: nos identificamos ante el servidor para evitar bloqueos
    headers = {"User-Agent": "PIBDownloader/1.0 (contacto@example.com)"}

    # Realizamos la solicitud GET a la API
    response = requests.get(url, headers=headers)

    # Si la respuesta no es 200 (OK), lanza una excepción y detiene el script
    response.raise_for_status()
    data = response.json()

    # Validación de seguridad: ¿Viene vacía la respuesta?
    if not data:
        print("⚠️ La API devolvió un JSON vacío.")
        return pd.DataFrame()

    # Aplanado de datos (Flattening)
    filas = []
    for serie in data:
        nombre = serie.get("Nombre", "Sin nombre")

        # Iteramos sobre la lista de valores temporales dentro de cada serie
        for dato in serie.get("Data", []):
            # Extraemos periodo (ej: "2023T1") y valor (ej: "12345.67")
            periodo = dato.get("T") or dato.get("NombrePeriodo")
            valor = dato.get("V") or dato.get("Valor")

            filas.append({
                "Indicador": nombre,
                "Periodo": periodo,
                "Valor": valor
            })

    # Convertimos la lista a DataFrame de pandas
    df = pd.DataFrame(filas)

    # 🔹 NUEVO: dedup básico por Indicador + Periodo a nivel raw
    df = _drop_duplicates_safe(df, subset=["Indicador", "Periodo"], note="pib_ine(raw)")

    if df.empty:
        print("⚠️ DataFrame vacío después de procesar los datos.")
    else:
        print(f"✅ Datos descargados: {len(df)} registros.")
    return df


# ========================================================
# FASE 2: TRANSFORMACIÓN (Transform)
# ========================================================

def limpiar_datos_pib(df):
    """
    Limpieza de datos y formateo de cadenas.
    - Convierte 'Valor' a numérico.
    - Elimina filas sin valor válido.
    - Limpia el 'Periodo' de anotaciones entre paréntesis.
    """
    if df.empty:
        print("⚠️ DataFrame vacío. Revisa la conexión o el ID de la tabla.")
        return df

    # Conversión de tipos: Aseguramos que el PIB sea numérico (float)
    # "coerce" convierte errores (textos) en NaN
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")

    # Eliminamos filas sin datos válidos
    df = df.dropna(subset=["Valor"])

    # Limpieza de texto con Regex (ej: "2023T1 (p)" -> "2023T1")
    df["Periodo"] = df["Periodo"].astype(str).apply(
        lambda x: re.sub(r"\(.*?\)", "", x).strip()
    )

    # 🔹 NUEVO: dedup tras limpieza
    df = _drop_duplicates_safe(df, subset=["Indicador", "Periodo"], note="pib_ine(clean)")
    return df


def calcular_variacion_interanual(df):
    """
    Ingeniería de características (Feature Engineering):
    Calcula el % de crecimiento respecto al año anterior (t-4 trimestres).
    """
    if df.empty:
        return df

    # Trabajamos sobre una copia para no alterar el original
    df = df.copy()

    # Ordenamos cronológicamente para que el cálculo sea correcto
    df.sort_values("Periodo", inplace=True)

    """
    Cálculo financiero:
    pct_change(4) calcula la variación respecto a 4 filas atrás (1 año)
    ¿Por qué 4? Porque los datos son trimestrales (4 trimestres = 1 año)
    Comparamos Trimestre 1 de este año con Trimestre 1 del año pasado.
    """
    df["Variacion_interanual_%"] = df["Valor"].pct_change(4) * 100
    return df


# ========================================================
# FASE 3: CARGA (Load)
# ========================================================

def main():
    print("📡 Descargando datos del PIB desde el INE...")
    df = obtener_datos_pib()

    print("🧹 Limpiando y procesando datos...")
    df_pib = limpiar_datos_pib(df)

    # Aplicamos lógica de negocio (KPIs económicos)
    df_pib = calcular_variacion_interanual(df_pib)

    # 🔹 NUEVO: dedup final de seguridad
    df_pib = _drop_duplicates_safe(df_pib, subset=["Indicador", "Periodo"], note="pib_ine(final)")

    print("\n📊 Últimos datos del PIB:")
    print(df_pib.tail(8))

    # Generamos nombre de archivo único con fecha/hora
    # Vital para mantener histórico y no sobrescribir datos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    # Archivo con histórico (backup) CSV
    csv_path_hist = os.path.join(DATA_DIR, f"pib_ine_{timestamp}.csv")
    df_pib.to_csv(csv_path_hist, index=False, encoding="utf-8")

    # 🔹 NUEVO: histórico también en Excel
    excel_path_hist = os.path.join(DATA_DIR, f"pib_ine_{timestamp}.xlsx")
    df_pib.to_excel(excel_path_hist, index=False)

    # Archivo 'latest' (para que lo lea el siguiente script sin buscar la fecha)
    csv_path_latest = os.path.join(DATA_DIR, "pib_ine_latest.csv")
    df_pib.to_csv(csv_path_latest, index=False, encoding="utf-8")

    print(f"✅ Guardado histórico CSV:   {csv_path_hist}")
    print(f"✅ Guardado histórico Excel: {excel_path_hist}")
    print(f"✅ Actualizado 'latest':     {csv_path_latest}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Captura cualquier error inesperado y lo muestra en consola
        print(f"❌ Error inesperado: {e}")
