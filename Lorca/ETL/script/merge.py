import pandas as pd
import numpy as np
from scipy.stats import linregress
from pathlib import Path
import config  # Importamos nuestro nuevo archivo de configuración

# --- SECTORES OBJETIVO ---
# Mantenemos esto aquí porque es lógica específica de negocio, no solo configuración estática.
TARGET_SECTORS = [
    "Energy", 
    "Utilities", 
    "Utilities - Renewable", 
    "Utilities - Diversified"
]

def calculate_ndvi_trend(df):
    """
    Calcula la pendiente (slope) de la regresión lineal del NDVI vs Año para cada ciudad.
    
    ¿Qué buscamos aquí?
    Si la pendiente es cercana a 0 o empieza a subir después de años de caída,
    podríamos estar ante un 'Turning Point' o punto de inflexión ecológica.
    Es una señal temprana muy valiosa para inversores.
    """
    def get_slope(group):
        # Ordenamos cronológicamente para que la regresión tenga sentido temporal
        group = group.sort_values("Year")
        
        # Necesitamos al menos 3 años de datos para trazar una línea mínimamente fiable.
        # Menos que eso sería especular demasiado.
        if len(group) < 3:
            return 0.0
        
        # linregress es genial porque nos da la pendiente directa (slope)
        # y descartamos el resto de estadísticos que no necesitamos ahora.
        slope, _, _, _, _ = linregress(group["Year"], group["NDVI_Mean"])
        return slope

    # Aplicamos esta lógica ciudad por ciudad.
    # Pasamos solo las columnas necesarias para compatibilidad con pandas >= 2.0
    return df.groupby("City")[["Year", "NDVI_Mean"]].apply(get_slope)

def load_environmental_data():
    """
    Carga y fusiona todas las fuentes ambientales:
    1. Sentinel-2 (Vegetación/NDVI)
    2. Sentinel-5P (Contaminación/NO2)
    3. HRL (Impermeabilización del suelo)
    4. ERA5-Land (Temperatura + Precipitación)
    5. S5P Aerosol (UVAI — proxy PM2.5/humo)
    6. Urban Atlas — ESA WorldCover (Uso de suelo)
    """

    # --- 1. Sentinel-2 (NDVI) — dataset base ---
    s2_path = config.INPUT_DIR_PROCESSED / "sentinel2.csv"
    if s2_path.exists():
        df_s2 = pd.read_csv(s2_path)
        df_env = df_s2.groupby(["City", "Year"])["NDVI_Mean"].mean().reset_index()
    else:
        print(f"[WARN] No encontré {s2_path.name}. Iniciando con DataFrame vacío.")
        df_env = pd.DataFrame(columns=["City", "Year", "NDVI_Mean"])

    # --- 2. Sentinel-5P (NO2) ---
    s5p_path = config.INPUT_DIR_PROCESSED / "s5p.csv"
    if s5p_path.exists():
        df_s5p = pd.read_csv(s5p_path)
        s5p_yearly = df_s5p.groupby(["City", "Year"])["NO2_Mean"].mean().reset_index()
        df_env = pd.merge(df_env, s5p_yearly, on=["City", "Year"], how="left")

    # --- 3. HRL (Impermeabilización) ---
    hrl_path = config.INPUT_DIR_PROCESSED / "hrl.csv"
    if hrl_path.exists():
        df_hrl = pd.read_csv(hrl_path)
        df_env = pd.merge(df_env, df_hrl, on=["City", "Year"], how="left")

    # --- 4. ERA5-Land (Temperatura + Precipitación) ---
    era5_path = config.INPUT_DIR_PROCESSED / "era5.csv"
    if era5_path.exists():
        df_era5 = pd.read_csv(era5_path)
        era5_yearly = (
            df_era5
            .groupby(["City", "Year"])
            .agg(Temp_Mean_C=("Temp_Mean_C", "mean"),
                 Precip_Total_m=("Precip_Total_m", "sum"))
            .reset_index()
        )
        df_env = pd.merge(df_env, era5_yearly, on=["City", "Year"], how="left")

    # --- 5. S5P Aerosol (UVAI) ---
    aerosol_path = config.INPUT_DIR_PROCESSED / "s5p_aerosol.csv"
    if aerosol_path.exists():
        df_aer = pd.read_csv(aerosol_path)
        aer_yearly = df_aer.groupby(["City", "Year"])["UVAI_Mean"].mean().reset_index()
        df_env = pd.merge(df_env, aer_yearly, on=["City", "Year"], how="left")

    # --- 6. Urban Atlas — ESA WorldCover (Uso de suelo) ---
    ua_path = config.INPUT_DIR_PROCESSED / "urban_atlas.csv"
    if ua_path.exists():
        df_ua = pd.read_csv(ua_path)
        df_env = pd.merge(df_env, df_ua, on=["City", "Year"], how="left")

    return df_env


def load_co2_data():
    """
    Carga las emisiones CO2 nacionales de EDGAR.
    Granularidad: city_code × año (valor de país aplicado a todas las ciudades).
    """
    co2_path = config.INPUT_DIR_PROCESSED / "edgar_co2.csv"
    if not co2_path.exists():
        return pd.DataFrame()

    df_co2 = pd.read_csv(co2_path)
    df_co2 = (
        df_co2
        .groupby(["City", "Year"], as_index=False)["CO2_kt"]
        .sum()
    )
    return df_co2


def load_oecd_data():
    """
    Carga indicadores OECD de política ambiental.
    Granularidad: city_code × año (valor de país aplicado a todas las ciudades).
    """
    path = config.INPUT_DIR_PROCESSED / "oecd_indicators.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def load_investeu_data():
    """
    Carga resumen InvestEU/EIB de inversión verde.
    Granularidad: city_code × año (valor de país aplicado a todas las ciudades).
    """
    path = config.INPUT_DIR_PROCESSED / "investeu_summary.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)

def load_financial_data():
    """
    Prepara la capa financiera.
    Carga el histórico de cotizaciones, filtra por sectores 'verdes' y
    agrega la información por país para poder cruzarla con las ciudades.
    """
    fin_path = config.INPUT_FILE_FINANCE
    
    if not fin_path.exists():
        print(f"[ERROR] Crítico: No encuentro el archivo financiero en {fin_path}")
        return pd.DataFrame()

    df_fin = pd.read_csv(fin_path)

    # Filtramos: Solo nos interesan empresas energéticas o de utilities.
    # El resto es ruido para nuestra tesis de inversión verde.
    df_green = df_fin[df_fin["Industry"].isin(TARGET_SECTORS)].copy()

    # Agregación mensual → anual por País:
    # El CSV financiero es mensual (una fila por Ticker × Año × Mes).
    # Agregamos primero a anual: promedio de precios y volatilidades mensuales.
    # Volatilidad anualizada = avg(monthly_volatility) * sqrt(12).
    fin_agg = df_green.groupby(["FUA_Country_Code", "Year"]).agg({
        "Ticker": lambda x: list(x.unique()),         # Lista de empresas disponibles
        "Company_Name": lambda x: list(x.unique()),   # Nombres legibles
        "Close_Price": "mean",                        # Precio promedio anual
        "Monthly_Volatility": "mean"                  # Volatilidad mensual media
    }).reset_index()
    # Anualizar volatilidad: avg_mensual × sqrt(12)
    fin_agg["Annual_Volatility"] = fin_agg["Monthly_Volatility"] * (12 ** 0.5)
    fin_agg.drop(columns=["Monthly_Volatility"], inplace=True)

    return fin_agg

def main():
    print("--- INICIANDO GENERACIÓN DEL DATASET MAESTRO (GTP) ---")

    # 1. Cargar la 'Verdad Física' (Datos Ambientales)
    df_env = load_environmental_data()
    
    if df_env.empty:
        print("[ERROR] No se han podido cargar datos ambientales. Abortando.")
        return

    # Calculamos la métrica clave: ¿La ciudad se está volviendo más verde o más gris?
    print(" -> Calculando pendientes de tendencia ecológica (NDVI Slope)...")
    trends = calculate_ndvi_trend(df_env)
    # Unimos esa métrica calculada al dataset principal
    df_env = df_env.join(trends.rename("NDVI_Slope"), on="City")

    # 2. Cargar la 'Verdad de Mercado' (Datos Financieros)
    print(" -> Procesando histórico financiero...")
    df_finance = load_financial_data()

    # 3. Cargar emisiones CO2 (EDGAR)
    print(" -> Cargando emisiones CO2 (EDGAR)...")
    df_co2 = load_co2_data()

    # 4. Cargar indicadores OECD
    print(" -> Cargando indicadores OECD...")
    df_oecd = load_oecd_data()

    # 5. Cargar datos InvestEU
    print(" -> Cargando datos InvestEU/EIB...")
    df_investeu = load_investeu_data()

    # 7. Fusión de Mundos (Merge)
    # Primero necesitamos saber a qué país pertenece cada ciudad.
    # Truco: El formato es 'NombreCiudad_XX', así que extraemos 'XX'.
    df_env["Country_Code"] = df_env["City"].apply(lambda x: x.split("_")[-1] if isinstance(x, str) and "_" in x else None)

    print(" -> Cruzando realidad física con realidad financiera...")
    df_master = pd.merge(
        df_env,
        df_finance,
        left_on=["Country_Code", "Year"],
        right_on=["FUA_Country_Code", "Year"],
        how="left"  # Priorizamos tener datos de ciudad, aunque falten finanzas algún año
    )

    # Limpieza final: borramos columnas redundantes
    df_master.drop(columns=["FUA_Country_Code"], inplace=True, errors="ignore")

    # Unir CO2 (por ciudad, ya expandido a nivel ciudad en edgar_co2_extract.py)
    if not df_co2.empty:
        print(" -> Añadiendo emisiones CO2 nacionales (EDGAR)...")
        df_master = pd.merge(df_master, df_co2[["City", "Year", "CO2_kt"]],
                             on=["City", "Year"], how="left")

    # Unir indicadores OECD
    if not df_oecd.empty:
        print(" -> Añadiendo indicadores OECD...")
        oecd_cols = ["City", "Year"] + [c for c in df_oecd.columns
                                        if c not in ("City", "Year")]
        df_master = pd.merge(df_master, df_oecd[oecd_cols],
                             on=["City", "Year"], how="left")

    # Unir InvestEU
    if not df_investeu.empty:
        print(" -> Añadiendo datos InvestEU/EIB...")
        investeu_cols = ["City", "Year"] + [c for c in df_investeu.columns
                                            if c not in ("City", "Year")]
        df_master = pd.merge(df_master, df_investeu[investeu_cols],
                             on=["City", "Year"], how="left")

    # 9. Guardado
    # Nos aseguramos de que la carpeta exista antes de guardar
    config.OUTPUT_FILE_MASTER.parent.mkdir(parents=True, exist_ok=True)
    df_master.to_csv(config.OUTPUT_FILE_MASTER, index=False)
    
    print(f"\n[ÉXITO] Dataset Maestro generado correctamente en:")
    print(f" -> {config.OUTPUT_FILE_MASTER}")
    print(f" -> Dimensiones finales: {df_master.shape[0]} filas x {df_master.shape[1]} columnas")
    
    # Un pequeño vistazo a los datos
    print("\nPrimeras filas del resultado:")
    print(df_master.head())

    # Bonus: Detección rápida de oportunidades
    # Buscamos ciudades estables ecológicamente (pendiente plana) en países con mercado activo.
    opportunities = df_master[
        (df_master["NDVI_Slope"].abs() < 0.01) & 
        (df_master["Ticker"].notnull())
    ]
    
    if not opportunities.empty:
        print(f"\n[INSIGHT] El algoritmo ha detectado {len(opportunities)} potenciales puntos de entrada (Turning Points).")
        print("Ejemplo de oportunidad detectada:")
        print(opportunities.iloc[0][["City", "Year", "NDVI_Slope", "Ticker"]])

if __name__ == "__main__":
    main()