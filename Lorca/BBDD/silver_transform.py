"""
silver_transform.py — GTP (Green Turning Point)
Capa Silver: transforma Bronze (HDFS) en Star Schema Kimball escrito directamente en MariaDB.

Flujo:
  Bronze HDFS (Parquet)
    → Spark transforma
    → toPandas()
    → MariaDB Silver (dims + facts)

Tablas generadas en MariaDB bd_rvm_gtp:
  Dimensiones (SCD-1 / SCD-2):
    dim_city      (SCD-1 — INSERT IGNORE)
    dim_date      (estática 2004-2024 mensual, INSERT IGNORE)
    dim_source    (SCD-1 — INSERT IGNORE)
    dim_company   (SCD-2 — versioning con valid_from/valid_to)
  Facts (upsert ON DUPLICATE KEY UPDATE):
    fact_environmental  (city_sk, year, month)
    fact_economic       (city_sk, year, month) — anual interpolado mensualmente
    fact_financial      (company_sk, year, month)
    fact_policy         (country_code, year, month) — OECD + EDGAR + InvestEU

Ejecucion:
  spark-submit --master local[4] silver_transform.py
"""

import sys
import argparse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
import numpy as np

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, BooleanType
)

# ==============================================================================
# RUTAS
# ==============================================================================
# Bronze sigue en HDFS
HDFS_BRONZE = "hdfs:///user/gtp/bronze"

# Para leer archivos locales via Spark en Docker
LORCA_BASE    = Path(__file__).resolve().parent.parent
ETL_DATA      = LORCA_BASE / "data"
PROCESSED_DIR = ETL_DATA / "DatosProcesados"


def _local(p) -> str:
    return f"file://{p}"


SILVER_LOAD_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Rango temporal del proyecto
YEAR_START = 2004
YEAR_END   = 2024

# Config de MariaDB (relativo a este script: ../config.ini)
CONFIG_FILE = str(Path(__file__).resolve().parent.parent / "config.ini")

# Catalogo de fuentes para dim_source (estatico)
SOURCE_CATALOG = [
    (1,  "GEE_S2",         "Sentinel-2 NDVI",            "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S2_SR_HARMONIZED",
     "2018-present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (2,  "GEE_S5P",        "Sentinel-5P NO2",             "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S5P_OFFL_L3_NO2",
     "2018-present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (3,  "Copernicus_HRL", "HRL Imperviousness",          "API_REST",        "EEA / Copernicus",
     "https://land.copernicus.eu/pan-european/high-resolution-layers",
     "2018",          "Europe",        "Triennial", "CC BY 4.0"),
    (4,  "YFinance",       "Yahoo Finance Stocks",        "API_REST",        "Yahoo Finance",
     "https://pypi.org/project/yfinance/",
     "2006-present", "Global",        "Monthly",   "Non-commercial"),
    (5,  "Eurostat_GDP",   "Eurostat GDP per capita",     "API_REST",        "Eurostat / EU",
     "https://ec.europa.eu/eurostat/api/dissemination",
     "2000-present", "EU27+EEA",      "Annual",    "CC BY 4.0"),
    (6,  "Eurostat_POP",   "Eurostat Population FUAs",    "API_REST",        "Eurostat / EU",
     "https://ec.europa.eu/eurostat/api/dissemination",
     "2000-present", "EU27+EEA",      "Annual",    "CC BY 4.0"),
    (7,  "GEE_ERA5",       "ERA5-Land Temp/Precip",       "GEE_COLLECTION",  "ECMWF / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/ECMWF_ERA5_LAND_MONTHLY_AGGR",
     "1950-present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (8,  "EDGAR_CO2",      "EDGAR CO2 Emisiones Pais",    "HTTP_DOWNLOAD",   "JRC / EU Commission",
     "https://edgar.jrc.ec.europa.eu",
     "1970-present", "EU27+World",    "Annual",    "CC BY 4.0"),
    (9,  "GEE_S5P_AER",    "Sentinel-5P Aerosol UVAI",    "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S5P_OFFL_L3_AER_AI",
     "2018-present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (10, "GEE_WORLDCOVER", "ESA WorldCover Uso Suelo",    "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/ESA_WorldCover_v200",
     "2020-2021",    "European FUAs", "Biennial",  "CC BY 4.0"),
    (11, "OECD_API",       "OECD Env Policy Indicators",  "API_REST",        "OECD",
     "https://sdmx.oecd.org/public/rest/data",
     "2010-present", "OECD members",  "Annual",    "CC BY 4.0"),
    (12, "InvestEU_EIB",   "InvestEU Final Recipients",   "PDF_SCRAPE",      "EIB / EU Commission",
     "https://www.eib.org/en/projects/beneficiaries/index.htm",
     "2021-present", "EU27",          "Annual",    "CC BY 4.0"),
]

COUNTRY_NAMES = {
    "ES": "Spain",        "PT": "Portugal",    "FR": "France",
    "IT": "Italy",        "DE": "Germany",     "UK": "United Kingdom",
    "NL": "Netherlands",  "BE": "Belgium",     "PL": "Poland",
    "RO": "Romania",      "CZ": "Czech Republic", "HU": "Hungary",
    "AT": "Austria",      "SE": "Sweden",      "DK": "Denmark",
    "FI": "Finland",      "IE": "Ireland",     "GR": "Greece",
    "CH": "Switzerland",  "NO": "Norway",      "SK": "Slovakia",
    "BG": "Bulgaria",     "HR": "Croatia",     "LT": "Lithuania",
    "LV": "Latvia",       "EE": "Estonia",     "SI": "Slovenia",
    "LU": "Luxembourg",   "CY": "Cyprus",      "MT": "Malta",
}

# ==============================================================================
# SPARK SESSION — solo para leer Bronze desde HDFS
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_Silver_Transform")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


# ==============================================================================
# DB POOL — usa db_pool.py del mismo directorio
# ==============================================================================
def get_pool():
    bbdd_dir = str(Path(__file__).resolve().parent)
    if bbdd_dir not in sys.path:
        sys.path.insert(0, bbdd_dir)
    from db_pool import MariaDBPool
    return MariaDBPool(config_file=CONFIG_FILE)


# ==============================================================================
# UTILIDADES
# ==============================================================================
def read_bronze(spark, table_name: str):
    """Lee una tabla Bronze desde HDFS Parquet."""
    path = f"{HDFS_BRONZE}/{table_name}"
    try:
        return spark.read.parquet(path)
    except Exception as e:
        print(f"  [WARN] No se pudo leer bronze/{table_name}: {e}")
        return None


def batch_upsert(pool, sql: str, rows: list, batch_size: int = 1000):
    """Inserta filas en lotes usando execute_many."""
    total = len(rows)
    if total == 0:
        return
    for i in range(0, total, batch_size):
        chunk = rows[i: i + batch_size]
        pool.execute_many(sql, chunk)
    print(f"    {total:,} filas procesadas")


def safe_float(v):
    """Convierte a float; devuelve None si es NaN o None."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (f != f) else f   # NaN check
    except Exception:
        return None


def safe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


# ==============================================================================
# DIMENSION 1: DIM_CITY — SCD-1 (INSERT IGNORE)
# ==============================================================================
def build_dim_city(pool):
    print("\n[DIM 1/4] Construyendo dim_city ...")

    etl_script_path = str(Path(__file__).resolve().parent.parent / "ETL" / "script")
    if etl_script_path not in sys.path:
        sys.path.insert(0, etl_script_path)

    try:
        import config as etl_config
        euro_fuas      = etl_config.EURO_FUAS
        eurostat_codes = getattr(etl_config, "EUROSTAT_CODES", {})
    except ImportError:
        print("  [ERROR] No se pudo importar config.py del ETL.")
        euro_fuas      = {}
        eurostat_codes = {}

    sql = """
        INSERT IGNORE INTO dim_city
            (city_sk, city_code, city_name, country_code, country_name,
             nuts_code, eurostat_city_code, lat, lon, fua_area_km2, _last_updated)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    rows = []
    for sk, (city_code, (lat, lon)) in enumerate(euro_fuas.items(), start=1):
        parts        = city_code.rsplit("_", 1)
        city_name    = parts[0] if len(parts) == 2 else city_code
        country_code = parts[1] if len(parts) == 2 else "XX"
        codes        = eurostat_codes.get(city_code, [""])
        eurostat_cc  = codes[0] if codes else ""
        rows.append((
            sk, city_code, city_name, country_code,
            COUNTRY_NAMES.get(country_code, "Unknown"),
            None, eurostat_cc, lat, lon, None, SILVER_LOAD_DATE
        ))

    batch_upsert(pool, sql, rows)
    print(f"  [DIM] dim_city: {len(rows)} ciudades insertadas (INSERT IGNORE)")
    return {r[1]: r[0] for r in rows}   # city_code -> city_sk


# ==============================================================================
# DIMENSION 2: DIM_DATE — mensual 2004-2024 (INSERT IGNORE)
# date_sk = YYYYMM (integer)
# ==============================================================================
def build_dim_date(pool):
    print("\n[DIM 2/4] Construyendo dim_date (mensual 2004-2024) ...")

    sql = """
        INSERT IGNORE INTO dim_date
            (date_sk, full_date, year, quarter, month, month_name,
             week_of_year, day_of_month, day_of_week, day_name,
             is_weekend, is_leap_year, fiscal_year, semester, is_satellite_era)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    month_names = ["January","February","March","April","May","June",
                   "July","August","September","October","November","December"]
    day_names   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    rows = []
    for year in range(YEAR_START, YEAR_END + 1):
        for month in range(1, 13):
            d           = date(year, month, 1)
            date_sk     = year * 100 + month          # YYYYMM
            iso_weekday = d.isoweekday()
            rows.append((
                date_sk,
                d.strftime("%Y-%m-01"),
                year,
                (month - 1) // 3 + 1,
                month,
                month_names[month - 1],
                int(d.strftime("%V")),
                1,                                    # day_of_month = 1 (primer dia del mes)
                iso_weekday,
                day_names[iso_weekday - 1],
                iso_weekday >= 6,
                (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)),
                year,
                1 if month <= 6 else 2,
                year >= 2018,
            ))

    batch_upsert(pool, sql, rows)
    print(f"  [DIM] dim_date: {len(rows)} entradas mensuales (2004-2024)")


# ==============================================================================
# DIMENSION 3: DIM_SOURCE — SCD-1 (INSERT IGNORE)
# ==============================================================================
def build_dim_source(pool):
    print("\n[DIM 3/4] Construyendo dim_source ...")

    sql = """
        INSERT IGNORE INTO dim_source
            (source_sk, source_code, source_name, source_type, provider,
             url_reference, temporal_coverage, spatial_coverage,
             update_frequency, license, _last_updated)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    rows = [(sk, code, name, stype, prov, url, temp, spat, freq, lic, SILVER_LOAD_DATE)
            for (sk, code, name, stype, prov, url, temp, spat, freq, lic) in SOURCE_CATALOG]

    batch_upsert(pool, sql, rows)
    print(f"  [DIM] dim_source: {len(rows)} fuentes")


# ==============================================================================
# DIMENSION 4: DIM_COMPANY — SCD-2
# ==============================================================================
def build_dim_company(spark, pool):
    print("\n[DIM 4/4] Construyendo dim_company (SCD-2) ...")

    bronze_fin = read_bronze(spark, "finance_raw")
    if bronze_fin is None:
        print("  [SKIP] Sin datos de finance en Bronze.")
        return

    today_str = SILVER_LOAD_DATE
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    # Snapshot mas reciente por ticker
    w_latest = Window.partitionBy("ticker").orderBy(F.desc("year"))
    current_snapshot = (
        bronze_fin
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select("ticker", "company_name", "sector", "industry",
                "fua_country_code", "country")
        .dropDuplicates(["ticker"])
    )
    pdf_new = current_snapshot.toPandas()

    # Leer registros vigentes de MariaDB
    existing_rows = pool.execute_query(
        "SELECT company_sk, ticker, sector, industry FROM dim_company WHERE is_current = 1"
    )
    existing = {r["ticker"]: r for r in existing_rows}
    max_sk_row = pool.execute_query("SELECT COALESCE(MAX(company_sk),0) AS max_sk FROM dim_company")
    max_sk = max_sk_row[0]["max_sk"] if max_sk_row else 0

    close_sql = """
        UPDATE dim_company SET valid_to=%s, is_current=0
        WHERE ticker=%s AND is_current=1
    """
    insert_sql = """
        INSERT INTO dim_company
            (company_sk, ticker, company_name, sector, industry, country_code, country_name,
             stock_exchange, esg_classification, valid_from, valid_to, is_current)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    new_rows = []
    for _, row in pdf_new.iterrows():
        ticker  = row.get("ticker") or ""
        sector  = row.get("sector") or ""
        industry = row.get("industry") or ""

        if ticker in existing:
            old = existing[ticker]
            if old["sector"] != sector or old["industry"] != industry:
                # Cerrar version antigua
                pool.execute_write(close_sql, (yesterday, ticker))
                # Nueva version
                max_sk += 1
                new_rows.append((
                    max_sk, ticker, row.get("company_name"), sector, industry,
                    row.get("fua_country_code"), row.get("country"),
                    None, "GREEN", today_str, "9999-12-31", 1
                ))
        else:
            # Nuevo ticker
            max_sk += 1
            new_rows.append((
                max_sk, ticker, row.get("company_name"), sector, industry,
                row.get("fua_country_code"), row.get("country"),
                None, "GREEN", "2006-01-01", "9999-12-31", 1
            ))

    if new_rows:
        batch_upsert(pool, insert_sql, new_rows)

    print(f"  [DIM] dim_company: {len(new_rows)} filas insertadas, "
          f"{len(existing)} tickers existentes evaluados")


# ==============================================================================
# FACT 1: FACT_ENVIRONMENTAL — (city_sk, year, month)
# Fuentes: Sentinel-2 (mensual), S5P NO2 (mensual), ERA5 (mensual),
#          HRL (anual → repetir en los 12 meses), WorldCover (bienal → idem),
#          S5P Aerosol (mensual)
# ==============================================================================
def build_fact_environmental(spark, pool, city_sk_map: dict):
    print("\n[FACT 1/4] Construyendo fact_environmental ...")

    # --- Sentinel-2: datos mensuales base ---
    s2 = read_bronze(spark, "sentinel2_raw")
    if s2 is None:
        print("  [SKIP] Sin datos Sentinel-2 en Bronze.")
        return

    # Asegurar columnas month y year
    if "month" not in s2.columns:
        print("  [SKIP] sentinel2_raw no tiene columna month.")
        return

    s2 = s2.filter(F.col("ndvi_mean").isNotNull() | F.col("ndvi_std").isNotNull())
    s2_pdf = s2.select("city", "year", "month", "ndvi_mean", "ndvi_std").toPandas()

    # --- S5P NO2: mensual ---
    s5p = read_bronze(spark, "sentinel5p_raw")
    if s5p is not None and "month" in s5p.columns:
        s5p_pdf = s5p.select("city", "year", "month", "no2_mean",
                              F.col("no2_std") if "no2_std" in s5p.columns else F.lit(None).cast(DoubleType()).alias("no2_std")
                              ).toPandas()
    else:
        s5p_pdf = None

    # --- ERA5: mensual ---
    era5 = read_bronze(spark, "era5_raw")
    if era5 is not None and "month" in era5.columns:
        era5_cols = ["city", "year", "month"]
        if "temp_mean_c"      in era5.columns: era5_cols.append("temp_mean_c")
        if "temp_annual_mean_c" in era5.columns: era5_cols.append("temp_annual_mean_c")
        if "precip_total_m"   in era5.columns: era5_cols.append("precip_total_m")
        if "precip_sum_m"     in era5.columns: era5_cols.append("precip_sum_m")
        era5_pdf = era5.select(*era5_cols).toPandas()
    else:
        era5_pdf = None

    # --- S5P Aerosol: mensual ---
    aer = read_bronze(spark, "s5p_aerosol_raw")
    if aer is not None and "month" in aer.columns:
        aer_pdf = aer.select("city", "year", "month", "uvai_mean").toPandas()
    else:
        aer_pdf = None

    # --- HRL: anual (repetir en los 12 meses del anio) ---
    hrl = read_bronze(spark, "hrl_raw")
    if hrl is not None:
        hrl_cols = ["city", "year"]
        if "imperviousness_mean" in hrl.columns: hrl_cols.append("imperviousness_mean")
        if "tree_cover_pct"      in hrl.columns: hrl_cols.append("tree_cover_pct")
        hrl_pdf = hrl.select(*hrl_cols).toPandas()
    else:
        hrl_pdf = None

    # --- WorldCover: bienal (repetir en los 12 meses del anio) ---
    wc = read_bronze(spark, "urban_atlas_raw")
    if wc is not None:
        wc_cols = ["city", "year"]
        for c in ["wc_tree_pct", "wc_built_pct", "wc_crop_pct", "wc_shrub_pct",
                  "wc_grass_pct", "wc_bare_pct", "wc_water_pct"]:
            if c in wc.columns:
                wc_cols.append(c)
        wc_pdf = wc.select(*wc_cols).toPandas()
    else:
        wc_pdf = None

    import pandas as pd

    # Merge mensual partiendo de S2
    df = s2_pdf.rename(columns={"city": "city_code"})

    # Merge S5P NO2
    if s5p_pdf is not None:
        s5p_pdf = s5p_pdf.rename(columns={"city": "city_code"})
        df = df.merge(s5p_pdf, on=["city_code", "year", "month"], how="left")

    # Merge ERA5
    if era5_pdf is not None:
        era5_pdf = era5_pdf.rename(columns={"city": "city_code"})
        if "temp_annual_mean_c" in era5_pdf.columns and "temp_mean_c" not in era5_pdf.columns:
            era5_pdf = era5_pdf.rename(columns={"temp_annual_mean_c": "temp_mean_c"})
        if "precip_sum_m" in era5_pdf.columns and "precip_total_m" not in era5_pdf.columns:
            era5_pdf = era5_pdf.rename(columns={"precip_sum_m": "precip_total_m"})
        df = df.merge(era5_pdf, on=["city_code", "year", "month"], how="left")

    # Merge S5P Aerosol
    if aer_pdf is not None:
        aer_pdf = aer_pdf.rename(columns={"city": "city_code"})
        df = df.merge(aer_pdf, on=["city_code", "year", "month"], how="left")

    # Merge HRL (anual → repetir para cada mes via merge solo en city+year)
    if hrl_pdf is not None:
        hrl_pdf = hrl_pdf.rename(columns={"city": "city_code"})
        df = df.merge(hrl_pdf, on=["city_code", "year"], how="left")

    # Merge WorldCover
    if wc_pdf is not None:
        wc_pdf = wc_pdf.rename(columns={"city": "city_code"})
        df = df.merge(wc_pdf, on=["city_code", "year"], how="left")

    # --- Columnas derivadas ---
    # ndvi_yoy_change (respecto al mismo mes del anio anterior)
    df = df.sort_values(["city_code", "year", "month"])
    df["ndvi_yoy_change"] = df.groupby(["city_code", "month"])["ndvi_mean"].diff()

    # green_index: combinacion normalizada NDVI, NO2, imperviousness
    ndvi_min = df["ndvi_mean"].min()
    ndvi_max = df["ndvi_mean"].max()
    ndvi_rng = (ndvi_max - ndvi_min) if ndvi_max != ndvi_min else 1.0

    no2_col = "no2_mean" if "no2_mean" in df.columns else None
    imp_col = "imperviousness_mean" if "imperviousness_mean" in df.columns else None

    if no2_col:
        no2_min = df[no2_col].min()
        no2_max = df[no2_col].max()
        no2_rng = (no2_max - no2_min) if no2_max != no2_min else 1.0
        no2_norm = (df[no2_col].fillna(no2_min) - no2_min) / no2_rng
    else:
        no2_norm = 0.0

    if imp_col:
        imp_min = df[imp_col].min()
        imp_max = df[imp_col].max()
        imp_rng = (imp_max - imp_min) if imp_max != imp_min else 1.0
        imp_norm = (df[imp_col].fillna(imp_min) - imp_min) / imp_rng
    else:
        imp_norm = 0.0

    df["green_index"] = (
        0.5 * ((df["ndvi_mean"] - ndvi_min) / ndvi_rng)
        + 0.3 * (1.0 - no2_norm)
        + 0.2 * (1.0 - imp_norm)
    ).round(6)

    # wc_natural_pct
    if "wc_tree_pct" in df.columns:
        df["wc_natural_pct"] = (
            df.get("wc_tree_pct", 0.0).fillna(0)
            + df.get("wc_shrub_pct", pd.Series(0, index=df.index)).fillna(0)
            + df.get("wc_grass_pct", pd.Series(0, index=df.index)).fillna(0)
            + df.get("wc_water_pct", pd.Series(0, index=df.index)).fillna(0)
        )
    else:
        df["wc_natural_pct"] = None

    # built_up_area_pct
    if "wc_built_pct" in df.columns:
        df["built_up_area_pct"] = df["wc_built_pct"]
    else:
        df["built_up_area_pct"] = None

    # date_sk = YYYYMM
    df["date_sk"] = df["year"] * 100 + df["month"]

    # city_sk
    df["city_sk"] = df["city_code"].map(city_sk_map)

    # Filtrar sin city_sk
    before = len(df)
    df = df[df["city_sk"].notna()]
    if before - len(df) > 0:
        print(f"  [WARN] {before - len(df)} filas sin city_sk descartadas")

    # source FKs
    df["source_sk_ndvi"] = 1
    df["source_sk_no2"]  = 2
    df["source_sk_era5"] = 7
    df["_silver_load_date"] = SILVER_LOAD_DATE

    # --- Upsert a MariaDB ---
    sql = """
        INSERT INTO fact_environmental
            (city_sk, year, month, date_sk,
             ndvi_mean, ndvi_std, no2_mean, no2_std,
             uvai_mean, temp_mean_c, precip_sum_m,
             imperviousness_mean, tree_cover_pct, built_up_area_pct,
             wc_tree_pct, wc_built_pct, wc_crop_pct, wc_natural_pct,
             green_index, ndvi_yoy_change,
             source_sk_ndvi, source_sk_no2, source_sk_era5,
             _silver_load_date)
        VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s, %s,%s,%s, %s)
        ON DUPLICATE KEY UPDATE
            ndvi_mean=VALUES(ndvi_mean), ndvi_std=VALUES(ndvi_std),
            no2_mean=VALUES(no2_mean), no2_std=VALUES(no2_std),
            uvai_mean=VALUES(uvai_mean), temp_mean_c=VALUES(temp_mean_c),
            precip_sum_m=VALUES(precip_sum_m),
            imperviousness_mean=VALUES(imperviousness_mean),
            tree_cover_pct=VALUES(tree_cover_pct),
            built_up_area_pct=VALUES(built_up_area_pct),
            wc_tree_pct=VALUES(wc_tree_pct), wc_built_pct=VALUES(wc_built_pct),
            wc_crop_pct=VALUES(wc_crop_pct), wc_natural_pct=VALUES(wc_natural_pct),
            green_index=VALUES(green_index), ndvi_yoy_change=VALUES(ndvi_yoy_change),
            _silver_load_date=VALUES(_silver_load_date)
    """

    def get_col(row, col):
        return safe_float(row.get(col)) if col in df.columns else None

    rows = []
    for _, row in df.iterrows():
        rows.append((
            int(row["city_sk"]), int(row["year"]), int(row["month"]), int(row["date_sk"]),
            safe_float(row.get("ndvi_mean")), safe_float(row.get("ndvi_std")),
            get_col(row, "no2_mean"), get_col(row, "no2_std"),
            get_col(row, "uvai_mean"), get_col(row, "temp_mean_c"),
            get_col(row, "precip_total_m"),
            get_col(row, "imperviousness_mean"), get_col(row, "tree_cover_pct"),
            safe_float(row.get("built_up_area_pct")),
            get_col(row, "wc_tree_pct"), get_col(row, "wc_built_pct"),
            get_col(row, "wc_crop_pct"), safe_float(row.get("wc_natural_pct")),
            safe_float(row.get("green_index")), safe_float(row.get("ndvi_yoy_change")),
            1, 2, 7,
            SILVER_LOAD_DATE,
        ))

    batch_upsert(pool, sql, rows)
    print(f"  [FACT] fact_environmental: {len(rows)} filas")


# ==============================================================================
# FACT 2: FACT_ECONOMIC — (city_sk, year, month)
# GDP Eurostat es anual → interpolar linealmente a mensual
# ==============================================================================
def build_fact_economic(spark, pool, city_sk_map: dict):
    print("\n[FACT 2/4] Construyendo fact_economic (interpolacion mensual GDP) ...")

    gdp = read_bronze(spark, "eurostat_gdp_raw")
    if gdp is None:
        print("  [SKIP] Sin datos Eurostat GDP en Bronze.")
        return

    import pandas as pd

    # Normalizar nombres de columna
    rename_map = {}
    for old_name in gdp.columns:
        if "geo" in old_name.lower() and "code" in old_name.lower():
            rename_map[old_name] = "city_code"
        if "gdp" in old_name.lower() and "value" in old_name.lower():
            rename_map[old_name] = "gdp_value"
    for old_name, new_name in rename_map.items():
        gdp = gdp.withColumnRenamed(old_name, new_name)

    if "city_code" not in gdp.columns:
        if "city" in gdp.columns:
            gdp = gdp.withColumnRenamed("city", "city_code")

    gdp_pdf = gdp.select(
        "city_code", "year",
        F.col("gdp_pps_per_capita" if "gdp_pps_per_capita" in gdp.columns else "gdp_value").alias("gdp_pps_per_capita"),
    ).toPandas()

    if "gdp_pps_per_capita" not in gdp_pdf.columns:
        gdp_pdf["gdp_pps_per_capita"] = None

    # Eurostat population (si existe)
    pop = read_bronze(spark, "eurostat_pop_raw")
    pop_pdf = None
    if pop is not None:
        pop_cols = ["city_code" if "city_code" in pop.columns else "city", "year",
                    "fua_population" if "fua_population" in pop.columns else pop.columns[-1]]
        pop_pdf = pop.select(*pop_cols[:3]).toPandas()
        pop_pdf.columns = ["city_code", "year", "fua_population"]

    # EDGAR CO2 por pais
    edgar = read_bronze(spark, "edgar_co2_raw")
    edgar_pdf = None
    if edgar is not None:
        edgar_cols = [c for c in edgar.columns if c in
                      ["city", "city_code", "country_code", "year", "co2_kt", "co2_country_kt"]]
        edgar_pdf = edgar.select(*edgar_cols).toPandas()
        if "city" in edgar_pdf.columns:
            edgar_pdf["country_code"] = edgar_pdf["city"].str.rsplit("_", n=1).str[-1]
        elif "city_code" in edgar_pdf.columns:
            edgar_pdf["country_code"] = edgar_pdf["city_code"].str.rsplit("_", n=1).str[-1]
        co2_col = "co2_kt" if "co2_kt" in edgar_pdf.columns else "co2_country_kt"
        edgar_pdf = edgar_pdf[["country_code", "year", co2_col]].drop_duplicates(["country_code", "year"])
        edgar_pdf = edgar_pdf.rename(columns={co2_col: "co2_country_kt"})

    # Generar meses 1-12 por ciudad-anio via interpolacion lineal
    gdp_pdf = gdp_pdf.sort_values(["city_code", "year"])
    monthly_rows = []

    for city_code, grp in gdp_pdf.groupby("city_code"):
        city_sk = city_sk_map.get(city_code)
        if city_sk is None:
            continue
        country_code = city_code.rsplit("_", 1)[-1] if "_" in city_code else "XX"

        grp = grp.sort_values("year").reset_index(drop=True)
        years  = grp["year"].values
        values = grp["gdp_pps_per_capita"].values

        for idx, year in enumerate(years):
            val_start = values[idx]
            val_end   = values[idx + 1] if idx + 1 < len(years) else val_start

            for month in range(1, 13):
                # Interpolacion lineal: fraccion del anio
                frac = (month - 1) / 12.0
                gdp_monthly = (
                    float(val_start) + (float(val_end) - float(val_start)) * frac
                    if val_start is not None and val_end is not None and not (
                        isinstance(val_start, float) and val_start != val_start
                    )
                    else safe_float(val_start)
                )

                if gdp_monthly and gdp_monthly > 0:
                    ln_gdp     = float(np.log(gdp_monthly))
                    ln_gdp_sq  = ln_gdp ** 2
                else:
                    ln_gdp = ln_gdp_sq = None

                # co2 anual por pais
                co2 = None
                if edgar_pdf is not None:
                    mask = (edgar_pdf["country_code"] == country_code) & (edgar_pdf["year"] == year)
                    co2_rows = edgar_pdf[mask]
                    if not co2_rows.empty:
                        co2 = safe_float(co2_rows.iloc[0]["co2_country_kt"])

                monthly_rows.append((
                    int(city_sk), int(year), int(month),
                    int(year * 100 + month),    # date_sk
                    safe_float(gdp_monthly),    # gdp_pps_per_capita
                    safe_float(gdp_monthly),    # gdp_eur_per_capita (proxy)
                    None,                       # gdp_pps_index
                    ln_gdp, ln_gdp_sq,
                    None,                       # gdp_growth_rate (se calcula post-hoc)
                    None,                       # fua_population
                    None,                       # population_density
                    None,                       # population_yoy_growth
                    5,                          # source_sk_gdp
                    6,                          # source_sk_pop
                    co2,
                    SILVER_LOAD_DATE,
                ))

    # Calcular gdp_growth_rate mensual como variacion respecto al mismo mes del anio anterior
    # Simplificacion: calculamos solo en el dataframe pandas antes de insertar
    import pandas as pd
    mdf = pd.DataFrame(monthly_rows, columns=[
        "city_sk", "year", "month", "date_sk",
        "gdp_pps_per_capita", "gdp_eur_per_capita", "gdp_pps_index",
        "ln_gdp_pps", "ln_gdp_pps_sq", "gdp_growth_rate",
        "fua_population", "population_density", "population_yoy_growth",
        "source_sk_gdp", "source_sk_pop", "co2_country_kt",
        "_silver_load_date"
    ])
    mdf = mdf.sort_values(["city_sk", "year", "month"])
    mdf["gdp_growth_rate"] = (
        mdf.groupby(["city_sk", "month"])["gdp_pps_per_capita"]
        .pct_change() * 100
    ).round(4)

    sql = """
        INSERT INTO fact_economic
            (city_sk, year, month, date_sk,
             gdp_pps_per_capita, gdp_eur_per_capita, gdp_pps_index,
             ln_gdp_pps, ln_gdp_pps_sq, gdp_growth_rate,
             fua_population, population_density, population_yoy_growth,
             source_sk_gdp, source_sk_pop, co2_country_kt,
             _silver_load_date)
        VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s)
        ON DUPLICATE KEY UPDATE
            gdp_pps_per_capita=VALUES(gdp_pps_per_capita),
            gdp_eur_per_capita=VALUES(gdp_eur_per_capita),
            ln_gdp_pps=VALUES(ln_gdp_pps),
            ln_gdp_pps_sq=VALUES(ln_gdp_pps_sq),
            gdp_growth_rate=VALUES(gdp_growth_rate),
            co2_country_kt=VALUES(co2_country_kt),
            _silver_load_date=VALUES(_silver_load_date)
    """
    rows_out = []
    for _, r in mdf.iterrows():
        rows_out.append((
            int(r["city_sk"]), int(r["year"]), int(r["month"]), int(r["date_sk"]),
            safe_float(r["gdp_pps_per_capita"]), safe_float(r["gdp_eur_per_capita"]),
            safe_float(r["gdp_pps_index"]),
            safe_float(r["ln_gdp_pps"]), safe_float(r["ln_gdp_pps_sq"]),
            safe_float(r["gdp_growth_rate"]),
            safe_int(r["fua_population"]), safe_float(r["population_density"]),
            safe_float(r["population_yoy_growth"]),
            int(r["source_sk_gdp"]), int(r["source_sk_pop"]),
            safe_float(r["co2_country_kt"]),
            SILVER_LOAD_DATE,
        ))

    batch_upsert(pool, sql, rows_out)
    print(f"  [FACT] fact_economic: {len(rows_out)} filas")


# ==============================================================================
# FACT 3: FACT_FINANCIAL — (company_sk, year, month)
# ==============================================================================
def build_fact_financial(spark, pool):
    print("\n[FACT 3/4] Construyendo fact_financial ...")

    fin = read_bronze(spark, "finance_raw")
    if fin is None:
        print("  [SKIP] Sin datos YFinance en Bronze.")
        return

    # Obtener company_sk actuales
    company_rows = pool.execute_query(
        "SELECT company_sk, ticker FROM dim_company WHERE is_current = 1"
    )
    ticker_to_sk = {r["ticker"]: r["company_sk"] for r in company_rows}

    fin_cols = ["ticker", "year", "month"]
    for c in ["close_price", "monthly_return", "monthly_volatility",
              "volume_avg", "current_pe", "current_beta"]:
        if c in fin.columns:
            fin_cols.append(c)

    # month puede no existir en finance
    if "month" not in fin.columns:
        fin = fin.withColumn("month", F.lit(1))

    fin_pdf = fin.select(*[c for c in fin_cols if c in fin.columns]).toPandas()

    sql = """
        INSERT INTO fact_financial
            (company_sk, year, month, date_sk,
             close_price, monthly_return, monthly_volatility,
             volume_avg, current_pe, current_beta,
             source_sk, _silver_load_date)
        VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s)
        ON DUPLICATE KEY UPDATE
            close_price=VALUES(close_price),
            monthly_return=VALUES(monthly_return),
            monthly_volatility=VALUES(monthly_volatility),
            volume_avg=VALUES(volume_avg),
            current_pe=VALUES(current_pe),
            current_beta=VALUES(current_beta),
            _silver_load_date=VALUES(_silver_load_date)
    """
    rows = []
    for _, row in fin_pdf.iterrows():
        ticker     = row.get("ticker")
        company_sk = ticker_to_sk.get(ticker)
        if company_sk is None:
            continue
        year  = safe_int(row.get("year"))
        month = safe_int(row.get("month")) or 1
        if not year:
            continue
        rows.append((
            int(company_sk), year, month, year * 100 + month,
            safe_float(row.get("close_price")),
            safe_float(row.get("monthly_return")),
            safe_float(row.get("monthly_volatility")),
            safe_float(row.get("volume_avg")),
            safe_float(row.get("current_pe")),
            safe_float(row.get("current_beta")),
            4,
            SILVER_LOAD_DATE,
        ))

    batch_upsert(pool, sql, rows)
    print(f"  [FACT] fact_financial: {len(rows)} filas")


# ==============================================================================
# FACT 4: FACT_POLICY — (country_code, year, month)
# OECD + EDGAR + InvestEU — todos anuales → repetir en los 12 meses del anio
# EDGAR CO2 se interpola linealmente; OECD e InvestEU se repiten constantes
# ==============================================================================
def build_fact_policy(spark, pool):
    print("\n[FACT 4/4] Construyendo fact_policy ...")

    import pandas as pd

    # --- OECD ---
    oecd = read_bronze(spark, "oecd_raw")
    oecd_pdf = None
    if oecd is not None:
        oecd_cols = ["city", "year"]
        for c in ["eps_index", "env_tax_usd", "env_expenditure", "ghg_total_kt"]:
            if c in oecd.columns:
                oecd_cols.append(c)
        oecd_pdf = oecd.select(*oecd_cols).toPandas()
        oecd_pdf["country_code"] = oecd_pdf["city"].str.rsplit("_", n=1).str[-1]
        oecd_pdf = oecd_pdf.drop_duplicates(["country_code", "year"])

    # --- EDGAR CO2 ---
    edgar = read_bronze(spark, "edgar_co2_raw")
    edgar_pdf = None
    if edgar is not None:
        edgar_cols = [c for c in edgar.columns if c in
                      ["city", "city_code", "country_code", "year", "co2_kt", "co2_country_kt"]]
        edgar_pdf = edgar.select(*edgar_cols).toPandas()
        if "city" in edgar_pdf.columns:
            edgar_pdf["country_code"] = edgar_pdf["city"].str.rsplit("_", n=1).str[-1]
        co2_col = "co2_kt" if "co2_kt" in edgar_pdf.columns else "co2_country_kt"
        edgar_pdf = edgar_pdf[["country_code", "year", co2_col]].drop_duplicates(["country_code", "year"])
        edgar_pdf = edgar_pdf.rename(columns={co2_col: "co2_country_kt"})

    # --- InvestEU ---
    investeu = read_bronze(spark, "investeu_raw")
    investeu_pdf = None
    if investeu is not None:
        inv_cols = [c for c in investeu.columns if c in
                    ["city", "city_code", "country_code", "year",
                     "investeu_ops_count", "investeu_total_eur"]]
        investeu_pdf = investeu.select(*inv_cols).toPandas()
        if "city" in investeu_pdf.columns:
            investeu_pdf["country_code"] = investeu_pdf["city"].str.rsplit("_", n=1).str[-1]
        elif "city_code" in investeu_pdf.columns:
            investeu_pdf["country_code"] = investeu_pdf["city_code"].str.rsplit("_", n=1).str[-1]
        investeu_pdf = investeu_pdf.drop_duplicates(["country_code", "year"])

    # Construir grid pais × anio a partir de los paises disponibles
    countries = set()
    years_set = set()
    for df_ref in [oecd_pdf, edgar_pdf, investeu_pdf]:
        if df_ref is not None:
            if "country_code" in df_ref.columns:
                countries.update(df_ref["country_code"].dropna().unique())
            if "year" in df_ref.columns:
                years_set.update(df_ref["year"].dropna().astype(int).unique())

    if not countries:
        print("  [SKIP] Sin datos OECD/EDGAR/InvestEU en Bronze.")
        return

    # Generar filas mensuales
    sql = """
        INSERT INTO fact_policy
            (country_code, year, month, date_sk,
             eps_index, env_tax_usd, env_expenditure, ghg_total_kt,
             co2_country_kt, investeu_ops_count, investeu_total_eur,
             source_sk_oecd, source_sk_edgar, source_sk_investeu,
             _silver_load_date)
        VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s)
        ON DUPLICATE KEY UPDATE
            eps_index=VALUES(eps_index), env_tax_usd=VALUES(env_tax_usd),
            env_expenditure=VALUES(env_expenditure), ghg_total_kt=VALUES(ghg_total_kt),
            co2_country_kt=VALUES(co2_country_kt),
            investeu_ops_count=VALUES(investeu_ops_count),
            investeu_total_eur=VALUES(investeu_total_eur),
            _silver_load_date=VALUES(_silver_load_date)
    """

    rows = []
    for country_code in sorted(countries):
        for year in sorted(years_set):
            # OECD: valor constante para los 12 meses
            eps = env_tax = env_exp = ghg = None
            if oecd_pdf is not None:
                mask = (oecd_pdf["country_code"] == country_code) & (oecd_pdf["year"] == year)
                oecd_row = oecd_pdf[mask]
                if not oecd_row.empty:
                    r = oecd_row.iloc[0]
                    eps    = safe_float(r.get("eps_index"))
                    env_tax = safe_float(r.get("env_tax_usd"))
                    env_exp = safe_float(r.get("env_expenditure"))
                    ghg    = safe_float(r.get("ghg_total_kt"))

            # EDGAR CO2: interpolado linealmente entre anios
            co2 = None
            if edgar_pdf is not None:
                edgar_country = edgar_pdf[edgar_pdf["country_code"] == country_code].sort_values("year")
                if not edgar_country.empty:
                    yr_list  = edgar_country["year"].values
                    co2_list = edgar_country["co2_country_kt"].values
                    idx_arr  = np.searchsorted(yr_list, year)
                    if idx_arr < len(yr_list) and yr_list[idx_arr] == year:
                        co2 = safe_float(co2_list[idx_arr])
                    elif 0 < idx_arr < len(yr_list):
                        y0, y1 = yr_list[idx_arr - 1], yr_list[idx_arr]
                        v0, v1 = safe_float(co2_list[idx_arr - 1]), safe_float(co2_list[idx_arr])
                        if v0 is not None and v1 is not None:
                            co2 = v0 + (v1 - v0) * (year - y0) / (y1 - y0)

            # InvestEU: valor constante
            inv_ops = inv_eur = None
            if investeu_pdf is not None:
                mask = (investeu_pdf["country_code"] == country_code) & (investeu_pdf["year"] == year)
                inv_row = investeu_pdf[mask]
                if not inv_row.empty:
                    r = inv_row.iloc[0]
                    inv_ops = safe_float(r.get("investeu_ops_count"))
                    inv_eur = safe_float(r.get("investeu_total_eur"))

            for month in range(1, 13):
                rows.append((
                    country_code, int(year), month, int(year * 100 + month),
                    eps, env_tax, env_exp, ghg,
                    co2, inv_ops, inv_eur,
                    11, 8, 12,
                    SILVER_LOAD_DATE,
                ))

    batch_upsert(pool, sql, rows)
    print(f"  [FACT] fact_policy: {len(rows)} filas")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP Silver Transform → MariaDB")
    parser.add_argument("--skip-dims",  action="store_true", help="Saltar dimensiones")
    parser.add_argument("--skip-facts", action="store_true", help="Saltar fact tables")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — SILVER TRANSFORM (Star Schema → MariaDB)")
    print(f"  Inicio: {SILVER_LOAD_DATE}")
    print(f"  Bronze: {HDFS_BRONZE}")
    print(f"  Destino: MariaDB bd_rvm_gtp (via db_pool)")
    print("=" * 65)

    spark = get_spark()
    pool  = get_pool()

    city_sk_map = {}

    if not args.skip_dims:
        city_sk_map = build_dim_city(pool)
        build_dim_date(pool)
        build_dim_source(pool)
        build_dim_company(spark, pool)
    else:
        # Cargar mapa city_code->city_sk desde MariaDB
        rows = pool.execute_query("SELECT city_sk, city_code FROM dim_city")
        city_sk_map = {r["city_code"]: r["city_sk"] for r in rows}

    if not args.skip_facts:
        build_fact_environmental(spark, pool, city_sk_map)
        build_fact_economic(spark, pool, city_sk_map)
        build_fact_financial(spark, pool)
        build_fact_policy(spark, pool)

    print("\n" + "=" * 65)
    print("  SILVER TRANSFORM COMPLETADO → MariaDB")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    main()
