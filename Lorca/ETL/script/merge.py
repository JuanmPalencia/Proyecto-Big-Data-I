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
    return df.groupby("City").apply(get_slope)

def load_environmental_data():
    """
    Carga y fusiona las tres fuentes de 'verdad física':
    1. Sentinel-2 (Vegetación/NDVI)
    2. Sentinel-5P (Contaminación/NO2)
    3. HRL (Capas de alta resolución de suelo impermeabilizado)
    """
    
    # --- 1. Sentinel-2 (NDVI) ---
    # Este es nuestro dataset base. Si no tenemos vegetación, no tenemos análisis.
    s2_path = config.INPUT_DIR_PROCESSED / "sentinel2.csv"
    if s2_path.exists():
        df_s2 = pd.read_csv(s2_path)
        # Agregamos por año para simplificar la serie temporal.
        # Nos interesa la tendencia macro, no la variación mensual.
        df_env = df_s2.groupby(["City", "Year"])["NDVI_Mean"].mean().reset_index()
    else:
        print(f"[WARN] Ojo: No encontré {s2_path.name}. Iniciando con DataFrame vacío.")
        df_env = pd.DataFrame(columns=["City", "Year", "NDVI_Mean"])

    # --- 2. Sentinel-5P (NO2) ---
    # Cruzamos los datos de calidad del aire. Es un 'Left Join' porque queremos
    # mantener todas las ciudades con datos de vegetación, tengan o no datos de NO2.
    s5p_path = config.INPUT_DIR_PROCESSED / "s5p.csv"
    if s5p_path.exists():
        df_s5p = pd.read_csv(s5p_path)
        s5p_yearly = df_s5p.groupby(["City", "Year"])["NO2_Mean"].mean().reset_index()
        df_env = pd.merge(df_env, s5p_yearly, on=["City", "Year"], how="left")

    # --- 3. HRL (Impermeabilización) ---
    # Datos estructurales sobre cuánto cemento hay. También 'Left Join'.
    hrl_path = config.INPUT_DIR_PROCESSED / "hrl.csv"
    if hrl_path.exists():
        df_hrl = pd.read_csv(hrl_path)
        df_env = pd.merge(df_env, df_hrl, on=["City", "Year"], how="left")

    return df_env

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

    # Agregación por País y Año:
    # Esto es clave: como no tenemos datos financieros por ciudad (aún),
    # usamos el promedio del país como proxy del mercado local.
    # Guardamos también la lista de Tickers (empresas) disponibles para que el usuario sepa dónde invertir.
    fin_agg = df_green.groupby(["FUA_Country_Code", "Year"]).agg({
        "Ticker": lambda x: list(x.unique()),       # Lista de empresas disponibles
        "Company_Name": lambda x: list(x.unique()), # Nombres legibles
        "Close_Price": "mean",                      # Precio promedio del sector
        "Annual_Volatility": "mean"                 # Riesgo promedio del sector
    }).reset_index()

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

    # 3. Fusión de Mundos (Merge)
    # Primero necesitamos saber a qué país pertenece cada ciudad.
    # Truco: El formato es 'NombreCiudad_XX', así que extraemos 'XX'.
    df_env["Country_Code"] = df_env["City"].apply(lambda x: x.split("_")[-1] if isinstance(x, str) and "_" in x else None)

    print(" -> Cruzando realidad física con realidad financiera...")
    df_master = pd.merge(
        df_env,
        df_finance,
        left_on=["Country_Code", "Year"],
        right_on=["FUA_Country_Code", "Year"],
        how="left" # Priorizamos tener datos de ciudad, aunque falten finanzas algún año
    )

    # Limpieza final: borramos columnas redundantes
    df_master.drop(columns=["FUA_Country_Code"], inplace=True)

    # 4. Guardado
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