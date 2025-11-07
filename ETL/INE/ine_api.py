import requests
import pandas as pd

def obtener_datos_pib():
    """
    Descarga los datos del PIB trimestral desde la API del INE (ID 33387)
    y devuelve un DataFrame con columnas: Indicador, Periodo, Valor
    """

    url = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/33387?nult=40"
    print(f"📡 Conectando con: {url}")

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    if not data:
        print("⚠️ La API devolvió un JSON vacío.")
        return pd.DataFrame()

    filas = []
    for serie in data:
        nombre = serie.get("Nombre", "Sin nombre")
        data_items = serie.get("Data", [])
        if not data_items:
            print(f"⚠️ Serie '{nombre}' sin datos.")
            continue
        for dato in data_items:
            periodo = dato.get("Fecha") or dato.get("fecha") or dato.get("FechaDato") or dato.get("NombrePeriodo")
            valor = dato.get("Valor")

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
