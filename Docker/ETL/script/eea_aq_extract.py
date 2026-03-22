"""
eea_aq_extract.py — GTP (Green Turning Point)
Extrae concentraciones anuales de PM2.5 y PM10 por país europeo, 2000-2024,
y las expande a granularidad mensual y de ciudad.

Metodología:
  1. PM2.5 (primario): WHO Global Health Observatory API (SDGPM25).
     Indicador SDG 11.6.2 — concentración urbana de PM2.5 (μg/m³) por país.
     Cubre 35 países europeos, años 2010-2019. Fill forward/backward para
     años fuera del rango (2000-2009, 2020-2024).
  2. PM2.5 (fallback): Eurostat sdg_11_50 (si WHO falla).
  3. PM10: Eurostat env_air_emis (emisiones) o sdg_11_51 (concentración).
  4. Datos anuales → replicados 12 meses, expandidos de país a ciudad.

Fuentes:
  - WHO GHO: https://ghoapi.azureedge.net/api/SDGPM25  (PM2.5, sin auth)
  - Eurostat sdg_11_50: fallback PM2.5
  - Eurostat env_air_emis / sdg_11_51: PM10

Output: DatosProcesados/eea_aq.csv
Columnas: City, Country, Year, Month, PM25_Mean, PM10_Mean, AQ_Source

Ejecución:
  python eea_aq_extract.py
"""

import os
import gzip
import io
import requests
import pandas as pd
import numpy as np
import config

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
URL_WHO_PM25  = "https://ghoapi.azureedge.net/api/SDGPM25"   # sin auth, libre
URL_PM25 = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "sdg_11_50?format=TSV&compressed=true"
)
URL_PM10_EMIS = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "env_air_emis?format=TSV&compressed=true"
)
# Dataset alternativo con concentraciones de PM10 (si env_air_emis no tiene lo esperado)
URL_PM10_ALT = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "sdg_11_51?format=TSV&compressed=true"
)

OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "eea_aq.csv"
RAW_DIR    = config.DATA_DIR / "raw" / "eea_aq"

YEARS  = list(range(2000, 2025))
MONTHS = list(range(1, 13))

DOWNLOAD_TIMEOUT = 120

# Códigos de contaminante PM en env_air_emis
PM10_CODES = {"PM10", "PM2_5", "PM10_PM2_5", "PM_10", "PM"}

# Mapeo ISO3 → ISO2 para países europeos (WHO usa ISO3, nuestro proyecto usa ISO2)
ISO3_TO_ISO2: dict[str, str] = {
    "ALB": "AL", "AUT": "AT", "BEL": "BE", "BGR": "BG", "BIH": "BA",
    "BLR": "BY", "CHE": "CH", "CYP": "CY", "CZE": "CZ", "DEU": "DE",
    "DNK": "DK", "ESP": "ES", "EST": "EE", "FIN": "FI", "FRA": "FR",
    "GBR": "GB", "GRC": "GR", "HRV": "HR", "HUN": "HU", "IRL": "IE",
    "ISL": "IS", "ITA": "IT", "LIE": "LI", "LTU": "LT", "LUX": "LU",
    "LVA": "LV", "MDA": "MD", "MKD": "MK", "MLT": "MT", "MNE": "ME",
    "NLD": "NL", "NOR": "NO", "POL": "PL", "PRT": "PT", "ROU": "RO",
    "SRB": "RS", "SVK": "SK", "SVN": "SI", "SWE": "SE", "TUR": "TR",
    "UKR": "UA", "XKX": "XK",
}

EEA_API_BASE   = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
EEA_META_URL   = "https://discomap.eea.europa.eu/map/fme/metadata/PanEuropean_metadata.csv"
PM25_URI       = "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/6001"


# ==============================================================================
# UTILIDADES
# ==============================================================================


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia en km entre dos puntos (lat/lon en grados decimales)."""
    R = 6371.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
def _download_tsv_gz(url: str, cache_name: str) -> pd.DataFrame | None:
    """Descarga un TSV comprimido de Eurostat con caché en disco."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_DIR / cache_name

    if cache_path.exists():
        print(f"  [CACHE] {cache_name}")
        try:
            with gzip.open(cache_path, "rt", encoding="utf-8") as f:
                return pd.read_csv(f, sep="\t", low_memory=False)
        except Exception:
            # Si el caché está corrupto, re-descargar
            cache_path.unlink(missing_ok=True)

    print(f"  [DOWNLOAD] {url}")
    try:
        r = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"  [ERROR] Descarga fallida ({cache_name}): {e}")
        return None

    with open(cache_path, "wb") as f:
        f.write(r.content)

    try:
        with gzip.open(io.BytesIO(r.content), "rt", encoding="utf-8") as f:
            df = pd.read_csv(f, sep="\t", low_memory=False)
    except Exception as e:
        print(f"  [ERROR] No se pudo leer el TSV descargado ({cache_name}): {e}")
        return None

    print(f"  [OK] {cache_name} descargado ({len(df):,} filas, {len(df.columns)} cols)")
    return df


def _split_first_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expande la columna combinada Eurostat ('dim1,dim2,geo\\time') en columnas
    individuales. La primera columna es el único elemento que necesita este split.
    """
    first_col = df.columns[0]
    dim_str = first_col.split("\\")[0]
    dim_names = [d.strip() for d in dim_str.split(",")]
    try:
        dims = df[first_col].str.split(",", expand=True)
        # Si el split produce más columnas de las esperadas, tomar solo las que hay
        dims = dims.iloc[:, :len(dim_names)]
        dims.columns = dim_names[:dims.shape[1]]
    except Exception:
        return df
    rest = df.iloc[:, 1:].copy()
    return pd.concat([dims, rest], axis=1)


def _find_geo_col(df: pd.DataFrame) -> str | None:
    """Detecta la columna con códigos ISO2 de 2 letras."""
    for candidate in ("geo", "geo\\time", "ref_area", "country"):
        if candidate in df.columns:
            return candidate
    for col in df.columns:
        sample = df[col].dropna().head(30).astype(str).str.strip()
        if sample.str.match(r"^[A-Z]{2}$").mean() > 0.4:
            return col
    return None


def _clean_value(val) -> float:
    """Elimina flags Eurostat y convierte a float. Retorna NaN si no parseable."""
    s = str(val).strip()
    if s in (":", "", "nan", "None"):
        return np.nan
    cleaned = "".join(c for c in s if c.isdigit() or c in (".", "-"))
    try:
        return float(cleaned) if cleaned else np.nan
    except ValueError:
        return np.nan


def _melt_to_annual(df: pd.DataFrame, id_cols: list[str],
                    value_name: str, target_countries: set[str],
                    geo_col: str) -> pd.DataFrame:
    """
    Convierte el DataFrame ancho (años como columnas) a formato largo,
    filtrando por países objetivo y años en YEARS.
    """
    year_cols = []
    for col in df.columns:
        c = str(col).strip()
        if c not in id_cols and len(c) == 4 and c.isdigit():
            yr = int(c)
            if 1995 <= yr <= 2030:
                year_cols.append(col)

    if not year_cols:
        return pd.DataFrame()

    df_filt = df[df[geo_col].isin(target_countries)].copy()
    if df_filt.empty:
        return pd.DataFrame()

    df_melt = df_filt.melt(
        id_vars=id_cols, value_vars=year_cols,
        var_name="year_str", value_name="value_raw"
    )
    df_melt["Year"]  = pd.to_numeric(df_melt["year_str"].str.strip(), errors="coerce").astype("Int64")
    df_melt[value_name] = df_melt["value_raw"].apply(_clean_value)
    df_melt = df_melt.dropna(subset=["Year"])
    df_melt = df_melt[df_melt["Year"].isin(YEARS)].copy()

    df_agg = (
        df_melt.groupby([geo_col, "Year"], as_index=False)[value_name]
        .mean()
        .rename(columns={geo_col: "Country"})
    )
    df_agg["Year"] = df_agg["Year"].astype(int)
    return df_agg


# ==============================================================================
# EXTRACCIÓN PM2.5 — EEA AirBase (fuente primaria, ciudad × mes)
# ==============================================================================
def _extract_pm25_eea_station(
    iso2_to_cities: dict,
    radius_km: float = 50.0,
) -> pd.DataFrame:
    """
    Descarga PM2.5 de estaciones EEA AirBase y lo agrega a ciudad × mes.

    Flujo:
      1. Descarga PanEuropean_metadata.csv (coordenadas de estaciones).
      2. Asigna cada estación a la ciudad EURO_FUA más cercana <= radius_km.
      3. Por cada país con estaciones asignadas: obtiene URLs de Parquet (EEA API).
      4. Descarga Parquet, filtra Validity > 0, agrega horario → mensual.
      5. Agrega a nivel ciudad (media de todas las estaciones asignadas).

    Retorna DataFrame: City, Country, Year, Month, PM25_Mean.
    Los años sin datos EEA son rellenados en main() con WHO como fallback.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("  [WARN] pyarrow no disponible — saltando EEA AirBase")
        return pd.DataFrame()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    parquet_cache = RAW_DIR / "parquet_pm25"
    parquet_cache.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Metadata de estaciones
    # ------------------------------------------------------------------
    meta_cache = RAW_DIR / "PanEuropean_metadata.csv"
    if not meta_cache.exists():
        print(f"  [DOWNLOAD] {EEA_META_URL}")
        try:
            r = requests.get(EEA_META_URL, timeout=60,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            with open(meta_cache, "wb") as f:
                f.write(r.content)
        except Exception as e:
            print(f"  [WARN] Metadata EEA fallida: {e}")
            return pd.DataFrame()
    else:
        print("  [CACHE] PanEuropean_metadata.csv")

    try:
        meta = pd.read_csv(meta_cache, sep="\t", low_memory=False,
                           encoding="utf-8", on_bad_lines="skip")
    except Exception as e:
        print(f"  [WARN] Error leyendo metadata: {e}")
        return pd.DataFrame()

    pm25_meta = meta[
        meta["AirPollutantCode"].astype(str).str.contains("6001", na=False) &
        meta["SamplingPoint"].notna()
    ].copy()
    pm25_meta["Longitude"] = pd.to_numeric(pm25_meta["Longitude"], errors="coerce")
    pm25_meta["Latitude"]  = pd.to_numeric(pm25_meta["Latitude"],  errors="coerce")
    pm25_meta = pm25_meta.dropna(subset=["Longitude", "Latitude",
                                          "AirQualityStationEoICode", "SamplingPoint"])
    print(f"  [META] {len(pm25_meta)} filas PM2.5 "
          f"({pm25_meta['AirQualityStationEoICode'].nunique()} estaciones, "
          f"{pm25_meta['Countrycode'].nunique()} países)")

    # ------------------------------------------------------------------
    # 2. Matching estación → ciudad (Haversine, deduplicado por EoI)
    # ------------------------------------------------------------------
    city_coords = config.EURO_FUAS
    city_list   = list(city_coords.keys())
    city_arr    = np.array([(lat, lon) for lat, lon in city_coords.values()])
    city_iso2_  = [c.rsplit("_", 1)[-1] for c in city_list]

    sta_unique = (pm25_meta.drop_duplicates(subset=["AirQualityStationEoICode"])
                  [["AirQualityStationEoICode", "Countrycode",
                    "Latitude", "Longitude"]].copy())

    eoi_to_city: dict[str, str] = {}
    for _, sta in sta_unique.iterrows():
        iso2 = str(sta["Countrycode"]).strip()
        mask = [j for j, ciso in enumerate(city_iso2_) if ciso == iso2]
        if not mask:
            continue
        slat, slon = float(sta["Latitude"]), float(sta["Longitude"])
        best_d, best_j = radius_km + 1, -1
        for j in mask:
            d = _haversine_km(slat, slon, city_arr[j][0], city_arr[j][1])
            if d < best_d:
                best_d, best_j = d, j
        if best_j >= 0:
            eoi_to_city[str(sta["AirQualityStationEoICode"])] = city_list[best_j]

    # Expandir EoI → SamplingPoint  (clave directa del nombre del Parquet)
    sp_to_city: dict[str, str] = {}
    for _, row in pm25_meta.iterrows():
        eoi = str(row["AirQualityStationEoICode"])
        sp  = str(row["SamplingPoint"]).strip()
        if eoi in eoi_to_city and sp:
            sp_to_city[sp] = eoi_to_city[eoi]

    n_cities = len(set(sp_to_city.values()))
    print(f"  [MATCH] {len(sp_to_city)} sampling points → {n_cities} ciudades "
          f"(radio {radius_km} km)")
    if not sp_to_city:
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # 3. Obtener URLs de Parquet por país y descargar
    # ------------------------------------------------------------------
    target_iso2s = sorted({c.rsplit("_", 1)[-1] for c in sp_to_city.values()})
    all_monthly: list[pd.DataFrame] = []

    for iso2 in target_iso2s:
        relevant_sps = {sp for sp, city in sp_to_city.items()
                        if city.rsplit("_", 1)[-1] == iso2}
        # Solicitar dataset=1 (E1a validado) Y dataset=2 (E2a reciente) para
        # maximizar cobertura histórica (E1a) + datos recientes (E2a).
        parquet_urls_set: set[str] = set()
        for dataset_id in (1, 2):
            try:
                r = requests.post(
                    f"{EEA_API_BASE}/ParquetFile/urls",
                    json={"countries": [iso2], "cities": [],
                          "pollutants": [PM25_URI], "dataset": dataset_id},
                    timeout=30,
                    headers={"Content-Type": "application/json"},
                )
                r.raise_for_status()
                for line in r.text.splitlines():
                    if line.strip().lower().endswith(".parquet"):
                        parquet_urls_set.add(line.strip())
            except Exception as e:
                print(f"  [WARN] URLs EEA dataset={dataset_id} fallidas para {iso2}: {e}")
        parquet_urls = sorted(parquet_urls_set)

        # SamplingPoint == nombre del archivo sin .parquet → matching trivial
        matched_urls: list[tuple[str, str]] = []
        for url in parquet_urls:
            sp_key = url.split("/")[-1].removesuffix(".parquet")
            if sp_key in relevant_sps:
                matched_urls.append((url, sp_key))

        print(f"  [{iso2}] {len(parquet_urls)} URLs → {len(matched_urls)} relevantes")

        # Descargar y procesar cada Parquet
        for url, sp_key in matched_urls:
            city_code = sp_to_city.get(sp_key)
            if not city_code:
                continue

            fname     = url.split("/")[-1]
            cache_f   = parquet_cache / fname

            if not cache_f.exists():
                try:
                    pr = requests.get(url, timeout=90)
                    pr.raise_for_status()
                    with open(cache_f, "wb") as f:
                        f.write(pr.content)
                except Exception as e:
                    print(f"  [WARN] Descarga fallida {fname}: {e}")
                    continue

            try:
                df_sta = pq.read_table(str(cache_f)).to_pandas()
            except Exception as e:
                print(f"  [WARN] Parquet corrupto {fname}: {e}")
                cache_f.unlink(missing_ok=True)
                continue

            # Filtrar válidos y agregar horario → mensual
            df_sta = df_sta[df_sta["Validity"] > 0].copy()
            df_sta["Value"] = pd.to_numeric(df_sta["Value"], errors="coerce")
            df_sta["Start"] = pd.to_datetime(df_sta["Start"], errors="coerce")
            df_sta = df_sta.dropna(subset=["Start", "Value"])
            df_sta["Year"]  = df_sta["Start"].dt.year.astype(int)
            df_sta["Month"] = df_sta["Start"].dt.month.astype(int)

            monthly = (
                df_sta[df_sta["Year"].isin(YEARS)]
                .groupby(["Year", "Month"], as_index=False)["Value"]
                .mean()
                .rename(columns={"Value": "PM25_Mean"})
            )
            if not monthly.empty:
                monthly["City"] = city_code
                all_monthly.append(monthly)

    if not all_monthly:
        print("  [WARN] EEA AirBase: sin datos procesados")
        return pd.DataFrame()

    combined = pd.concat(all_monthly, ignore_index=True)

    # Agregar a nivel ciudad (media de todas las estaciones asignadas)
    city_monthly = (
        combined.groupby(["City", "Year", "Month"], as_index=False)["PM25_Mean"]
        .mean()
    )
    city_monthly["PM25_Mean"] = city_monthly["PM25_Mean"].round(4)
    city_monthly["Country"]   = city_monthly["City"].str.rsplit("_", n=1).str[-1]

    print(f"  [OK] EEA AirBase PM2.5: {len(city_monthly):,} registros "
          f"({city_monthly['City'].nunique()} ciudades, "
          f"{city_monthly['Year'].nunique()} años)")

    return city_monthly[["City", "Country", "Year", "Month", "PM25_Mean"]]


# ==============================================================================
# EXTRACCIÓN PM2.5 — WHO Global Health Observatory (fallback nivel país)
# ==============================================================================
def _extract_pm25_who(target_countries_iso2: set[str]) -> pd.DataFrame:
    """
    Extrae PM2.5 anual por país desde la API WHO GHO (indicador SDGPM25).
    Usa concentración urbana (RESIDENCEAREATYPE_CITY), luego TOTL como fallback.
    Cubre 2010-2019; forward/backward fill para años fuera del rango.
    Retorna DataFrame con columnas: Country (ISO2), Year, PM25_Mean.
    """
    print(f"  [DOWNLOAD] WHO GHO API: {URL_WHO_PM25}")
    try:
        r = requests.get(URL_WHO_PM25, timeout=60)
        r.raise_for_status()
        vals = r.json().get("value", [])
    except Exception as e:
        print(f"  [WARN] WHO GHO API fallida: {e}")
        return pd.DataFrame()

    if not vals:
        return pd.DataFrame()

    # Filtrar países EU con ISO3 conocido
    eu_iso3 = {iso3 for iso3, iso2 in ISO3_TO_ISO2.items()
               if iso2 in target_countries_iso2}

    # Preferir concentración en ciudad (FUAs = zonas urbanas), luego URB, luego TOTL
    subset = []
    for dim1_type in ("RESIDENCEAREATYPE_CITY", "RESIDENCEAREATYPE_URB",
                      "RESIDENCEAREATYPE_TOTL"):
        subset = [v for v in vals
                  if v.get("SpatialDim") in eu_iso3
                  and v.get("Dim1") == dim1_type
                  and v.get("NumericValue") is not None]
        if subset:
            print(f"  [OK] WHO GHO usando dimensión: {dim1_type}")
            break

    if not subset:
        print("  [WARN] WHO GHO: sin datos PM2.5 para Europa")
        return pd.DataFrame()

    df = pd.DataFrame([{
        "iso3":      v["SpatialDim"],
        "Year":      int(v["TimeDim"]),
        "PM25_Mean": float(v["NumericValue"]),
    } for v in subset])

    # ISO3 → ISO2
    df["Country"] = df["iso3"].map(ISO3_TO_ISO2)
    df = df.dropna(subset=["Country"])

    # Agregar por país × año (media, por si hay duplicados)
    df = df.groupby(["Country", "Year"], as_index=False)["PM25_Mean"].mean()
    df["PM25_Mean"] = df["PM25_Mean"].round(4)

    # Extender cobertura a todos los años del pipeline (2000-2024)
    # con forward fill y backward fill por país
    from itertools import product as iproduct
    all_combos = pd.DataFrame(
        [(c, y) for c, y in iproduct(df["Country"].unique(), YEARS)],
        columns=["Country", "Year"]
    )
    df = all_combos.merge(df, on=["Country", "Year"], how="left")
    df = df.sort_values(["Country", "Year"])
    df["PM25_Mean"] = (df.groupby("Country")["PM25_Mean"]
                         .transform(lambda s: s.ffill().bfill()))
    df = df[df["PM25_Mean"].notna()].copy()

    print(f"  [OK] WHO GHO PM2.5: {len(df):,} registros "
          f"({df['Country'].nunique()} países, {df['Year'].nunique()} años)")
    return df


# ==============================================================================
# EXTRACCIÓN PM2.5 — sdg_11_50 (fallback Eurostat)
# ==============================================================================
def _extract_pm25(target_countries: set[str]) -> pd.DataFrame:
    """
    Extrae PM2.5 anual por país desde sdg_11_50.
    Retorna DataFrame con columnas: Country, Year, PM25_Mean
    """
    df_raw = _download_tsv_gz(URL_PM25, "sdg_11_50.tsv.gz")
    if df_raw is None:
        return pd.DataFrame()

    df = _split_first_col(df_raw)
    geo_col = _find_geo_col(df)
    if geo_col is None:
        print("  [WARN] sdg_11_50: columna geográfica no encontrada")
        return pd.DataFrame()

    # sdg_11_50 puede tener columna de unidad; filtrar por μg/m³ si existe
    unit_col = next((c for c in df.columns if c.lower() == "unit"), None)
    if unit_col:
        # Preferir UG_M3 (microgramos por metro cúbico)
        df_ug = df[df[unit_col].str.upper().str.contains("UG", na=False)].copy()
        if not df_ug.empty:
            df = df_ug
        # Si no hay filtro claro, usar todo

    id_cols = [c for c in df.columns
               if not (len(str(c).strip()) == 4 and str(c).strip().isdigit())]

    result = _melt_to_annual(df, id_cols, "PM25_Mean", target_countries, geo_col)
    if not result.empty:
        result["PM25_Mean"] = result["PM25_Mean"].round(4)
        print(f"  [OK] sdg_11_50 PM2.5: {len(result):,} registros país×año")
    else:
        print("  [WARN] sdg_11_50: sin datos válidos")
    return result


# ==============================================================================
# EXTRACCIÓN PM10 — env_air_emis (primario) / sdg_11_51 (alternativo)
# ==============================================================================
def _extract_pm10(target_countries: set[str]) -> pd.DataFrame:
    """
    Extrae PM10 anual por país.
    Intenta env_air_emis (emisiones); si no, sdg_11_51 (exposición).
    Retorna DataFrame con columnas: Country, Year, PM10_Mean
    """
    # Intento 1: env_air_emis filtrado por pollutant = PM10
    df_raw = _download_tsv_gz(URL_PM10_EMIS, "env_air_emis.tsv.gz")
    if df_raw is not None:
        df = _split_first_col(df_raw)
        geo_col = _find_geo_col(df)

        if geo_col is not None:
            # Buscar columna de contaminante
            airpol_col = next(
                (c for c in df.columns if c.lower() in ("airpol", "pollutant", "siec", "nrg_bal")),
                None
            )
            if airpol_col:
                df_pm10 = df[
                    df[airpol_col].str.upper().isin(PM10_CODES) |
                    df[airpol_col].str.upper().str.contains("PM10", na=False)
                ].copy()
            else:
                df_pm10 = df.copy()

            id_cols = [c for c in df_pm10.columns
                       if not (len(str(c).strip()) == 4 and str(c).strip().isdigit())]

            result = _melt_to_annual(df_pm10, id_cols, "PM10_Mean", target_countries, geo_col)
            if not result.empty:
                result["PM10_Mean"] = result["PM10_Mean"].round(4)
                print(f"  [OK] env_air_emis PM10: {len(result):,} registros país×año")
                return result

    # Intento 2: sdg_11_51 (exposición a PM10, si existe)
    print("  [WARN] env_air_emis sin datos PM10 útiles. Intentando sdg_11_51...")
    df_raw2 = _download_tsv_gz(URL_PM10_ALT, "sdg_11_51.tsv.gz")
    if df_raw2 is None:
        return pd.DataFrame()

    df2 = _split_first_col(df_raw2)
    geo_col2 = _find_geo_col(df2)
    if geo_col2 is None:
        print("  [WARN] sdg_11_51: columna geográfica no encontrada")
        return pd.DataFrame()

    id_cols2 = [c for c in df2.columns
                if not (len(str(c).strip()) == 4 and str(c).strip().isdigit())]

    result2 = _melt_to_annual(df2, id_cols2, "PM10_Mean", target_countries, geo_col2)
    if not result2.empty:
        result2["PM10_Mean"] = result2["PM10_Mean"].round(4)
        print(f"  [OK] sdg_11_51 PM10: {len(result2):,} registros país×año")
    else:
        print("  [WARN] sdg_11_51: sin datos válidos")
    return result2


# ==============================================================================
# EXPANSIÓN ANUAL → MENSUAL → CIUDADES
# ==============================================================================
def _expand_to_city_monthly(
    df_pm25: pd.DataFrame,
    df_pm10: pd.DataFrame,
    iso2_to_cities: dict[str, list[str]],
    target_countries: set[str],
) -> pd.DataFrame:
    """
    1. Combina PM25 y PM10 (outer join por Country × Year).
    2. Expande a 12 meses por año (mismo valor en todos los meses).
    3. Expande de país a todas las ciudades de ese país.
    """
    # Merge PM25 + PM10
    if not df_pm25.empty and not df_pm10.empty:
        df_aq = df_pm25.merge(df_pm10, on=["Country", "Year"], how="outer")
    elif not df_pm25.empty:
        df_aq = df_pm25.copy()
        df_aq["PM10_Mean"] = np.nan
    elif not df_pm10.empty:
        df_aq = df_pm10.copy()
        df_aq["PM25_Mean"] = np.nan
    else:
        return pd.DataFrame()

    # Asegurar cobertura de todos los países × años (incluso sin dato = NaN)
    all_combos = pd.MultiIndex.from_product(
        [sorted(target_countries), YEARS], names=["Country", "Year"]
    )
    df_all = pd.DataFrame(index=all_combos).reset_index()
    df_aq = df_all.merge(df_aq, on=["Country", "Year"], how="left")

    # Expandir a ciudades y meses
    rows = []
    for _, row in df_aq.iterrows():
        country = row["Country"]
        cities  = iso2_to_cities.get(country, [])
        if not cities:
            continue
        for month in MONTHS:
            for city_code in cities:
                rows.append({
                    "City":       city_code,
                    "Country":    country,
                    "Year":       int(row["Year"]),
                    "Month":      month,
                    "PM25_Mean":  row.get("PM25_Mean"),
                    "PM10_Mean":  row.get("PM10_Mean"),
                    "AQ_Source":  "EUROSTAT_ANNUAL_REPEAT",
                })

    return pd.DataFrame(rows)


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    print("=" * 60)
    print("  GTP — EXTRACCIÓN CALIDAD DEL AIRE (EEA/Eurostat PM2.5 + PM10)")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)

    # Construir mapa ISO2 → lista de ciudades
    iso2_to_cities: dict[str, list[str]] = {}
    for city_code in config.EURO_FUAS:
        iso2 = city_code.rsplit("_", 1)[-1]
        iso2_to_cities.setdefault(iso2, []).append(city_code)

    target_countries = set(iso2_to_cities.keys())
    print(f"  Países objetivo:  {len(target_countries)}")
    print(f"  Ciudades totales: {len(config.EURO_FUAS)}")
    print()

    # ------------------------------------------------------------------
    # PM2.5: EEA AirBase (ciudad×mes) → WHO GHO (país×año) → Eurostat
    # ------------------------------------------------------------------
    print("[1/3] Extrayendo PM2.5 (EEA AirBase — estaciones por ciudad)...")
    df_pm25_city = _extract_pm25_eea_station(iso2_to_cities)
    eea_ok = not df_pm25_city.empty

    # Fallback a nivel país (WHO / Eurostat) para rellenar años sin datos EEA
    print("\n  [FALLBACK] Obteniendo datos WHO GHO para complementar años sin EEA...")
    df_pm25_country = _extract_pm25_who(target_countries)
    if df_pm25_country.empty:
        print("  [FALLBACK 2] Intentando Eurostat sdg_11_50...")
        df_pm25_country = _extract_pm25(target_countries)

    # Construir df_pm25 a nivel ciudad: EEA donde hay datos, WHO donde no
    if eea_ok:
        # Identificar ciudad×año×mes ya cubiertos por EEA
        eea_keys = set(zip(df_pm25_city["City"], df_pm25_city["Year"],
                           df_pm25_city["Month"]))
        # Expandir WHO a ciudad×mes para los huecos
        if not df_pm25_country.empty:
            df_who_expanded = _expand_to_city_monthly(
                df_pm25_country, pd.DataFrame(),
                iso2_to_cities, target_countries
            ).rename(columns={"PM25_Mean": "PM25_Mean_who"})
            # Sólo filas no cubiertas por EEA
            df_who_expanded = df_who_expanded[
                ~df_who_expanded.apply(
                    lambda r: (r["City"], r["Year"], r["Month"]) in eea_keys, axis=1
                )
            ].copy()
            if not df_who_expanded.empty and "PM25_Mean_who" in df_who_expanded.columns:
                df_who_expanded = df_who_expanded.rename(
                    columns={"PM25_Mean_who": "PM25_Mean"}
                )
                df_who_expanded["AQ_Source"] = "WHO_GHO_COUNTRY_FILL"
                df_pm25_city["AQ_Source"] = "EEA_STATION"
                fill_cols = ["City", "Country", "Year", "Month",
                             "PM25_Mean", "AQ_Source"]
                df_pm25 = pd.concat(
                    [df_pm25_city[fill_cols],
                     df_who_expanded[[c for c in fill_cols
                                      if c in df_who_expanded.columns]]],
                    ignore_index=True
                )
            else:
                df_pm25_city["AQ_Source"] = "EEA_STATION"
                df_pm25 = df_pm25_city
        else:
            df_pm25_city["AQ_Source"] = "EEA_STATION"
            df_pm25 = df_pm25_city
    else:
        # EEA falló completamente — usar país×año expandido
        df_pm25 = df_pm25_country  # se expande más abajo junto con PM10

    # --- Extracción PM10 ---
    print("\n[2/3] Extrayendo PM10 (env_air_emis / sdg_11_51)...")
    df_pm10 = _extract_pm10(target_countries)

    if (df_pm25.empty or len(df_pm25.columns) < 4) and df_pm10.empty:
        print("[WARN] Sin datos de calidad del aire. Generando CSV vacío.")
        df_out = pd.DataFrame(columns=["City", "Country", "Year", "Month",
                                       "PM25_Mean", "PM10_Mean", "AQ_Source"])
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        return

    # --- Construir df_out ---
    if eea_ok:
        # PM2.5 ya está a nivel ciudad — fusionar con PM10 expandido
        print("\n[3/3] Combinando PM2.5 (ciudad×mes) con PM10 (país×año)...")
        if not df_pm10.empty:
            df_pm10_city = _expand_to_city_monthly(
                pd.DataFrame(), df_pm10, iso2_to_cities, target_countries
            )[["City", "Country", "Year", "Month", "PM10_Mean"]]
            df_out = df_pm25.merge(df_pm10_city,
                                   on=["City", "Country", "Year", "Month"],
                                   how="left")
        else:
            df_out = df_pm25.copy()
            df_out["PM10_Mean"] = np.nan
        if "AQ_Source" not in df_out.columns:
            df_out["AQ_Source"] = "EEA_STATION"
    else:
        # Expansión clásica país×año → ciudad×mes
        print("\n[3/3] Expandiendo datos de país × año a ciudad × mes...")
        df_out = _expand_to_city_monthly(df_pm25, df_pm10, iso2_to_cities,
                                         target_countries)

    if df_out.empty:
        print("[WARN] Sin datos expandidos para exportar.")
        return

    df_out = df_out.sort_values(["City", "Year", "Month"]).reset_index(drop=True)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    pm25_valid = df_out["PM25_Mean"].notna().sum()
    pm10_valid = df_out["PM10_Mean"].notna().sum()

    print()
    print("=" * 60)
    print("  FIN — Calidad del aire extraída")
    print(f"  Total filas:         {len(df_out):,}")
    print(f"  Filas con PM2.5:     {pm25_valid:,}")
    print(f"  Filas con PM10:      {pm10_valid:,}")
    print(f"  Ciudades:            {df_out['City'].nunique()}")
    print(f"  Países cubiertos:    {df_out['Country'].nunique()}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
