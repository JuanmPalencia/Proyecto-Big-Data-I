import os
import io
import gzip
import requests
import pandas as pd
import numpy as np
import config  # Tu configuración centralizada

# --- CONFIGURACIÓN DE ENDPOINTS ---
# Usamos las APIs de datos abiertos de Eurostat (SDMX/TSV)
# met_10r_3gdp: PIB a nivel metropolitano (Ideal)
# nama_10r_3gdp: PIB a nivel NUTS 3 (Respaldo regional)
URL_METRO = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/met_10r_3gdp?format=TSV&compressed=true"
URL_NUTS3 = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10r_3gdp?format=TSV&compressed=true"

RAW_DIR = config.DATA_DIR / "raw_eurostat"
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "eurostat_PIB.csv"

# --- DATOS DE RESPALDO (UK / POST-BREXIT) ---
# Eurostat dejó de actualizar datos de UK tras el Brexit.
# Para evitar huecos en la serie temporal de Londres, Manchester, etc.,
# usamos esta serie aproximada basada en ONS/Banco Mundial (en Euros).
UK_GDP_FALLBACK = pd.Series({
    2000: 26500, 2001: 27100, 2002: 28200, 2003: 29500, 2004: 31000,
    2005: 32500, 2006: 34200, 2007: 35800, 2008: 32500, 2009: 29800,
    2010: 31500, 2011: 32200, 2012: 33500, 2013: 34100, 2014: 36500,
    2015: 39800, 2016: 37500, 2017: 36800, 2018: 37200, 2019: 38500,
    2020: 34000, 2021: 39500, 2022: 41200, 2023: 42500, 2024: 43100
}, name="Value")
UK_GDP_FALLBACK.index.name = "Year"

def download_eurostat_tsv(url, filename):
    """
    Descarga segura de archivos comprimidos desde Eurostat.
    Implementa caché local para no machacar el servidor de la UE.
    """
    local_path = RAW_DIR / filename
    
    if local_path.exists():
        print(f"[CACHE] Cargando datos locales: {filename}")
        # Eurostat usa tabuladores (\t) como separador
        return pd.read_csv(local_path, sep='\t')
    
    print(f"[WEB] Descargando desde Eurostat: {filename}...")
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        
        # Guardamos el comprimido tal cual para trazabilidad
        with open(local_path, 'wb') as f:
            f.write(r.content)
            
        # Descomprimimos en memoria para devolver el DataFrame
        with gzip.open(io.BytesIO(r.content), 'rt') as f:
            return pd.read_csv(f, sep='\t')
    except Exception as e:
        print(f"[ERROR] Fallo en descarga {filename}: {e}")
        return None

def process_eurostat_data(df, geo_column_name):
    """
    Limpia el formato "sucio" de Eurostat.
    El TSV original viene con una columna combinada tipo "freq,unit,metroreg\time".
    Esta función la separa y limpia los flags de los valores (ej: '34000 p' -> 34000).
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 1. Separar dimensiones (Split de la primera columna)
    first_col = df.columns[0]
    dim_names = first_col.split('\\')[0].split(',') # ['freq', 'unit', 'metroreg']
    
    # Expandimos esa columna en varias
    dims = df[first_col].str.split(',', expand=True)
    dims.columns = dim_names
    
    # Unimos las nuevas columnas con los datos anuales
    df_clean = pd.concat([dims, df.iloc[:, 1:]], axis=1)
    
    # 2. Filtrar unidad correcta
    # Solo nos interesa "EUR_HAB" (Euros por habitante), no paridad de poder adquisitivo (PPS)
    if 'unit' in df_clean.columns:
        df_clean = df_clean[df_clean['unit'].str.contains("EUR_HAB", na=False)]

    # 3. Normalizar nombre de columna de región
    # Buscamos la columna que tenga el código geográfico (geo, metroreg, etc.)
    region_col = next((c for c in df_clean.columns if c in [geo_column_name, 'geo', 'metroreg']), None)
    
    if not region_col:
        return pd.DataFrame()

    # 4. Pivotar (Unpivot/Melt)
    # Convertimos los años (que están en columnas) a filas para tener una serie temporal larga
    id_vars = dim_names
    year_cols = [c for c in df_clean.columns if c not in id_vars]
    
    df_melt = df_clean.melt(id_vars=id_vars, value_vars=year_cols, var_name='Year', value_name='Value')
    
    # 5. Limpieza numérica (Regex)
    # Eliminamos letras de flags ('b'reak, 'e'stimated, 'p'rovisional) y espacios
    df_melt['Value'] = pd.to_numeric(
        df_melt['Value'].astype(str).str.replace(r'[^\d\.-]', '', regex=True), 
        errors='coerce'
    )
    # Limpiamos el año (a veces vienen con espacios)
    df_melt['Year'] = pd.to_numeric(df_melt['Year'].str.strip(), errors='coerce')
    
    # Renombrar para estandarizar
    df_melt.rename(columns={region_col: 'Region'}, inplace=True)
    
    return df_melt.dropna(subset=['Value', 'Year'])

def resolve_city_gdp(city_name, codes, df_metro, df_nuts3):
    """
    Algoritmo de cascada para encontrar el mejor dato posible.
    Jerarquía de calidad:
    1. Dato Metropolitano (Lo más preciso para una ciudad).
    2. Dato Regional NUTS 3 (Provincia/Departamento).
    3. Promedio Nacional (Si no hay dato regional, ej: países pequeños o fallos).
    4. Manual (Solo para UK post-Brexit).
    """
    metro_code, nuts3_code = codes
    country_code = metro_code[:2] # Ej: 'ES' de 'ES001'
    
    # --- NIVEL 1: METRO ---
    matches = df_metro[df_metro['Region'].str.startswith(metro_code, na=False)]
    if not matches.empty:
        return matches[['Year', 'Value']], "METRO"

    # --- NIVEL 2: NUTS 3 ---
    matches = df_nuts3[df_nuts3['Region'] == nuts3_code]
    if not matches.empty:
        return matches[['Year', 'Value']], "NUTS3"

    # --- NIVEL 3: MEDIA NACIONAL (Fallback para EU) ---
    if country_code != "UK":
        country_data = df_nuts3[df_nuts3['Region'].str.startswith(country_code, na=False)]
        if not country_data.empty:
            national_mean = country_data.groupby('Year')['Value'].mean().reset_index()
            return national_mean, "NATIONAL_AVG"

    # --- NIVEL 4: MANUAL (UK) ---
    if country_code == "UK":
        return UK_GDP_FALLBACK.reset_index(), "MANUAL_UK"

    return pd.DataFrame(), "NONE"

def main():
    print("--- INICIANDO PROCESAMIENTO MACROECONÓMICO (EUROSTAT) ---")
    
    # Crear directorios
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # 1. Cargar y Limpiar Datos Raw
    raw_metro = download_eurostat_tsv(URL_METRO, "met_10r_3gdp.tsv.gz")
    raw_nuts3 = download_eurostat_tsv(URL_NUTS3, "nama_10r_3gdp.tsv.gz")
    
    df_metro = process_eurostat_data(raw_metro, "metroreg")
    df_nuts3 = process_eurostat_data(raw_nuts3, "geo")

    print(f"Buscando datos para {len(config.EUROSTAT_CODES)} ciudades...")
    
    all_records = []

    # 2. Resolución por Ciudad
    for city, codes in config.EUROSTAT_CODES.items():
        data, source = resolve_city_gdp(city, codes, df_metro, df_nuts3)
        
        if not data.empty:
            data = data.copy()
            data['City'] = city
            data['Source'] = source
            all_records.append(data)
        else:
            print(f"[WARN] Sin datos económicos para {city}")

    if not all_records:
        print("[ERROR] No se pudo generar ningún registro.")
        return

    # 3. Consolidación e Interpolación
    # Los datos económicos suelen tener huecos. Usamos interpolación lineal para
    # tener una serie continua anual desde 2000 hasta 2024.
    df_final = pd.concat(all_records, ignore_index=True)

    # Guardar mapa de fuente ANTES de deduplicar (groupby eliminaría la columna Source)
    source_map = df_final[['City', 'Source']].drop_duplicates(subset=['City']).set_index('City')['Source']

    # Deduplicar: si hay varios registros para el mismo (City, Year) tomamos la media
    df_final = (
        df_final.groupby(['City', 'Year'], as_index=False)['Value'].mean()
    )

    # Pivotamos para interpolar por columnas (series temporales)
    df_pivot = df_final.pivot(index='City', columns='Year', values='Value')
    
    # Rellenamos el rango de años completo
    full_years = range(2000, 2025)
    df_pivot = df_pivot.reindex(columns=full_years)
    
    # Interpolación
    df_interpolated = df_pivot.interpolate(method='linear', axis=1, limit_direction='both')
    
    # Volvemos a formato tabla (Long format)
    df_out = df_interpolated.reset_index().melt(id_vars='City', var_name='Year', value_name='GDP_Per_Capita')
    
    # Recuperamos la fuente original (Metadata) — source_map ya fue extraído antes del groupby
    df_out['Source'] = df_out['City'].map(source_map)
    
    # Limpieza final
    df_out.dropna(subset=['GDP_Per_Capita'], inplace=True)
    df_out.to_csv(OUTPUT_CSV, index=False)
    
    print(f"\n[ÉXITO] Datos Macro procesados.")
    print(f" -> Guardado en: {OUTPUT_CSV}")
    print(f" -> Muestra aleatoria:")
    print(df_out.sample(5))

if __name__ == "__main__":
    main()