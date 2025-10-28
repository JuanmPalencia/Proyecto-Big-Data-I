# En este fichero se descargan los datos del PIB anual de españa desde la API del INE,
# se limpian y se calculan las variaciones interanuales.  

import requests
import pandas as pd
import re

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
            # Claves correctas según la estructura actual del INE
            periodo = dato.get("T") or dato.get("NombrePeriodo")
            valor = dato.get("V") or dato.get("Valor")

            filas.append({
                "Indicador": nombre,
                "Periodo": periodo,
                "Valor": valor
            })

    df = pd.DataFrame(filas)

    if df.empty:
        print("⚠️ DataFrame vacío después de procesar los datos.")
    else:
        print(f"✅ Datos descargados: {len(df)} registros.")

    return df



def limpiar_datos_pib(df):
    """
    Limpia y prepara los datos del PIB:
    - Convierte los valores a numéricos
    - Elimina valores nulos
    - Limpia el texto del periodo (elimina 'avance', 'definitivo', etc.)
    """
    if df.empty:
        print("⚠️ DataFrame vacío. Revisa la conexión o el ID de la tabla.")
        return df

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    df = df.dropna(subset=["Valor"])

    # 🔹 Limpia el campo Periodo: elimina todo lo que esté entre paréntesis
    df["Periodo"] = df["Periodo"].astype(str).apply(
        lambda x: re.sub(r"\(.*?\)", "", x).strip()
    )

    return df



def calcular_variacion_interanual(df):
    """
    Calcula la variación interanual (respecto al mismo trimestre del año anterior).
    """
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

    print("\n📊 Últimos datos del PIB:")
    print(df_pib.tail(8))

    # Guardar en CSV con codificación UTF-8
    df_pib.to_csv("pib_ine.csv", index=False, encoding="utf-8")
    print("\n✅ Datos guardados en 'pib_ine.csv'")



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
