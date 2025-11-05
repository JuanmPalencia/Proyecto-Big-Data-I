import pandas as pd 
def limpiar_datos_pib(df):
    # Filtra el pib total y convierte los valores a numéricos
    if df.empty:
        print("⚠️ DataFrame vacío. Revisa la conexión o el ID de la tabla.")
        return df
    

    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce")
    df = df.dropna(subset=["Valor"])
    return df 


def calcular_variacion_interanual(df):
    # Calcula la variación interanual del PIB (respecto al mismo trimestre del año anterior)

    if df.empty:
        return df
    df = df.copy()
    df.sort_values("Periodo", inplace=True)
    df["Variacion_interanual_%"] = df["Valor"].pct_change(4) * 100
    return df

