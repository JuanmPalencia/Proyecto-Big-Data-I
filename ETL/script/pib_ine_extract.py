# En este fichero se descargan los datos del PIB anual de españa desde la API del INE,
# se limpian y se calculan las variaciones interanuales.

import requests
import pandas as pd
import re
import os
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(BASE_DIR, "data", "ine")
os.makedirs(DATA_DIR, exist_ok=True)

# 🔹 NUEVO: helper dedup
def _drop_duplicates_safe(df: pd.DataFrame, subset=None, note: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=subset, ignore_index=True)
    removed = before - len(df)
    if removed > 0:
        print(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})")
    return df

def obtener_datos_pib():
    """
    Descarga los datos del PIB trimestral desde la API del INE (ID 33387)
    y devuelve un DataFrame con columnas: Indicador, Periodo, Valor.
    """
    url = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/33387?nult=40"
    print(f"📡 Conectando con: {url}")

    headers = {"User-Agent": "PIBDownloader/1.0 (contacto@example.com)"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    if not data:
        print("⚠️ La API devolvió un JSON vacío.")
        return pd.DataFrame()

    filas = []
    for serie in data:
        nombre = serie.get("Nombre", "Sin nombre")
        for dato in serie.get("Data", []):
            periodo = dato.get("T") or dato.get("NombrePeriodo")
            valor = dato.get("V") or dato.get("Valor")
            filas.append({"Indicador": nombre, "Periodo": periodo, "Valor": valor})

    df = pd.DataFrame(filas)
    # 🔹 NUEVO: dedup básico por Indicador+Periodo
    df = _drop_duplicates_safe(df, subset=["Indicador", "Periodo"], note="pib_ine(raw)")
    if df.empty:
        print("⚠️ DataFrame vacío después de procesar los datos.")
    else:
        print(f"✅ Datos descargados: {len(df)} registros.")
    return df

def limpiar_datos_pib(df):
    if df.empty:
        print("⚠️ DataFrame vacío. Revisa la conexión o el ID de la tabla.")
        return df
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    df = df.dropna(subset=["Valor"])
    df["Periodo"] = df["Periodo"].astype(str).apply(lambda x: re.sub(r"\(.*?\)", "", x).strip())
    # 🔹 NUEVO: dedup tras limpieza
    return _drop_duplicates_safe(df, subset=["Indicador", "Periodo"], note="pib_ine(clean)")

def calcular_variacion_interanual(df):
    if df.empty:
        return df
    df = df.copy()
    df.sort_values("Periodo", inplace=True)
    df["Variacion_interanual_%"] = df["Valor"].pct_change(4) * 100
    return df

def main():
    print("📡 Descargando datos del PIB desde el INE...")
    df = obtener_datos_pib()

    print("🧹 Limpiando y procesando datos...")
    df_pib = limpiar_datos_pib(df)
    df_pib = calcular_variacion_interanual(df_pib)

    # 🔹 NUEVO: dedup final de seguridad
    df_pib = _drop_duplicates_safe(df_pib, subset=["Indicador", "Periodo"], note="pib_ine(final)")

    print("\n📊 Últimos datos del PIB:")
    print(df_pib.tail(8))

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    csv_path = os.path.join(DATA_DIR, f"pib_ine_{timestamp}.csv")
    excel_path = os.path.join(DATA_DIR, f"pib_ine_{timestamp}.xlsx")

    df_pib.to_csv(csv_path, index=False, encoding="utf-8")
    df_pib.to_excel(excel_path, index=False)

    print(f"✅ Datos guardados en:\n  {csv_path}\n  {excel_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
