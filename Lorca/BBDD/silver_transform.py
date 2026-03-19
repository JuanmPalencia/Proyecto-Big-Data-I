"""
silver_transform.py — GTP (Green Turning Point)
Capa Silver: transforma Bronze en Star Schema Kimball (Hive sobre HDFS).

Tablas generadas:
  Dimensiones:
    dim_city      (SCD-1 — coordenadas estables, overwrite)
    dim_company   (SCD-2 — sectores cambian, versioning con valid_from/valid_to)
    dim_date      (estática, pre-generada 2000–2030)
    dim_source    (SCD-1 — catálogo de fuentes)
  Facts:
    fact_environmental  (NDVI + NO2 + HRL, ciudad × año)
    fact_economic       (GDP, ciudad × año)
    fact_financial      (métricas empresa × año)

Ejecución en Lorca:
  spark-submit --master yarn silver_transform.py
"""

import sys
import argparse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, BooleanType, DateType
)

# ==============================================================================
# RUTAS HDFS
# ==============================================================================
HDFS_BRONZE  = "hdfs:///user/gtp/bronze"
HDFS_SILVER  = "hdfs:///user/gtp/silver"

SILVER_LOAD_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Catálogo de fuentes para dim_source (estático, gestionado manualmente)
SOURCE_CATALOG = [
    (1,  "GEE_S2",          "Sentinel-2 NDVI",           "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S2_SR_HARMONIZED",
     "2018–present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (2,  "GEE_S5P",         "Sentinel-5P NO2",           "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S5P_OFFL_L3_NO2",
     "2018–present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (3,  "Copernicus_HRL",  "HRL Imperviousness",        "API_REST",        "EEA / Copernicus",
     "https://land.copernicus.eu/pan-european/high-resolution-layers",
     "2018",         "Europe",        "Triennial", "CC BY 4.0"),
    (4,  "YFinance",        "Yahoo Finance — Stocks",    "API_REST",        "Yahoo Finance",
     "https://pypi.org/project/yfinance/",
     "2006–present", "Global",        "Monthly",   "Non-commercial"),
    (5,  "Eurostat_GDP",    "Eurostat GDP per capita",   "API_REST",        "Eurostat / EU",
     "https://ec.europa.eu/eurostat/api/dissemination",
     "2000–present", "EU27+EEA",      "Annual",    "CC BY 4.0"),
    (6,  "Eurostat_POP",    "Eurostat Population FUAs",  "API_REST",        "Eurostat / EU",
     "https://ec.europa.eu/eurostat/api/dissemination",
     "2000–present", "EU27+EEA",      "Annual",    "CC BY 4.0"),
    (7,  "GEE_ERA5",        "ERA5-Land Temp/Precip",     "GEE_COLLECTION",  "ECMWF / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/ECMWF_ERA5_LAND_MONTHLY_AGGR",
     "1950–present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (8,  "EDGAR_CO2",       "EDGAR CO2 Emisiones País",  "HTTP_DOWNLOAD",   "JRC / EU Commission",
     "https://edgar.jrc.ec.europa.eu",
     "1970–present", "EU27+World",    "Annual",    "CC BY 4.0"),
    (9,  "GEE_S5P_AER",     "Sentinel-5P Aerosol UVAI",  "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S5P_OFFL_L3_AER_AI",
     "2018–present", "European FUAs", "Monthly",   "CC BY 4.0"),
    (10, "GEE_WORLDCOVER",  "ESA WorldCover Uso Suelo",  "GEE_COLLECTION",  "ESA / Google",
     "https://developers.google.com/earth-engine/datasets/catalog/ESA_WorldCover_v200",
     "2020–2021",    "European FUAs", "Biennial",  "CC BY 4.0"),
    (11, "OECD_API",        "OECD Env Policy Indicators","API_REST",        "OECD",
     "https://sdmx.oecd.org/public/rest/data",
     "2010–present", "OECD members",  "Annual",    "CC BY 4.0"),
    (12, "InvestEU_EIB",    "InvestEU Final Recipients", "PDF_SCRAPE",      "EIB / EU Commission",
     "https://www.eib.org/en/projects/beneficiaries/index.htm",
     "2021–present", "EU27",          "Annual",    "CC BY 4.0"),
]

# ==============================================================================
# SPARK SESSION
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_Silver_Transform")
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )

# ==============================================================================
# UTILIDADES
# ==============================================================================

def read_bronze(spark, table_path: str):
    """Lee una tabla Bronze desde HDFS Parquet."""
    try:
        return spark.read.parquet(table_path)
    except Exception as e:
        print(f"  [WARN] No se pudo leer {table_path}: {e}")
        return None


def write_silver_dim(df, table_name: str, partition_cols: list = None):
    """Escribe una dimensión Silver a HDFS (overwrite completo)."""
    path = f"{HDFS_SILVER}/{table_name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    print(f"  [SILVER] {table_name}: {df.count():,} filas → {path}")


def write_silver_fact(df, table_name: str, partition_cols: list):
    """Escribe una fact table Silver a HDFS, particionada."""
    path = f"{HDFS_SILVER}/{table_name}"
    (
        df.write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(path)
    )
    print(f"  [SILVER] {table_name}: {df.count():,} filas → {path}")


def repair_table(spark, db: str, table: str):
    try:
        spark.sql(f"MSCK REPAIR TABLE {db}.{table}")
    except Exception as e:
        print(f"  [WARN] MSCK REPAIR {db}.{table}: {e}")

# ==============================================================================
# DIMENSIÓN 1: DIM_CITY — SCD Tipo 1
# Construida desde el diccionario EURO_FUAS de config.py (fuente de verdad).
# Se importa dinámicamente para no duplicar el diccionario.
# ==============================================================================
def build_dim_city(spark):
    print("\n[DIM 1/4] Construyendo dim_city (SCD-1) ...")

    # Importar EURO_FUAS desde config.py del ETL
    # Derivar ruta relativa desde este archivo (Lorca/BBDD/) → portátil en cualquier entorno
    etl_script_path = str(Path(__file__).resolve().parent.parent / "ETL" / "script")
    if etl_script_path not in sys.path:
        sys.path.insert(0, etl_script_path)

    try:
        import config as etl_config
        euro_fuas = etl_config.EURO_FUAS
        eurostat_codes = etl_config.EUROSTAT_CODES
    except ImportError:
        print("  [ERROR] No se pudo importar config.py del ETL. Usando EURO_FUAS vacío.")
        euro_fuas = {}
        eurostat_codes = {}

    records = []
    for sk, (city_code, (lat, lon)) in enumerate(euro_fuas.items(), start=1):
        parts = city_code.rsplit("_", 1)
        city_name    = parts[0] if len(parts) == 2 else city_code
        country_code = parts[1] if len(parts) == 2 else "XX"

        codes = eurostat_codes.get(city_code, ["", ""])
        eurostat_city_code = codes[0] if codes else ""

        # Mapeo country_code → country_name
        country_names = {
            "ES": "Spain",        "PT": "Portugal",    "FR": "France",
            "IT": "Italy",        "DE": "Germany",     "UK": "United Kingdom",
            "NL": "Netherlands",  "BE": "Belgium",     "PL": "Poland",
            "RO": "Romania",      "CZ": "Czech Republic", "HU": "Hungary",
            "AT": "Austria",      "SE": "Sweden",      "DK": "Denmark",
            "FI": "Finland",      "IE": "Ireland",     "GR": "Greece",
            "CH": "Switzerland",  "NO": "Norway",
        }

        records.append((
            sk, city_code, city_name, country_code,
            country_names.get(country_code, "Unknown"),
            None,                 # nuts_code
            eurostat_city_code,
            lat, lon,
            None,                 # fua_area_km2
            SILVER_LOAD_DATE,
        ))

    schema = StructType([
        StructField("city_sk",            LongType(),   False),
        StructField("city_code",          StringType(), True),
        StructField("city_name",          StringType(), True),
        StructField("country_code",       StringType(), True),
        StructField("country_name",       StringType(), True),
        StructField("nuts_code",          StringType(), True),
        StructField("eurostat_city_code", StringType(), True),
        StructField("lat",                DoubleType(), True),
        StructField("lon",                DoubleType(), True),
        StructField("fua_area_km2",       DoubleType(), True),
        StructField("_last_updated",      StringType(), True),
    ])

    df = spark.createDataFrame(records, schema)
    write_silver_dim(df, "dim_city", partition_cols=["country_code"])
    repair_table(spark, "gtp_silver", "dim_city")
    return df


# ==============================================================================
# DIMENSIÓN 2: DIM_DATE — Estática 2000–2030
# ==============================================================================
def build_dim_date(spark):
    print("\n[DIM 2/4] Construyendo dim_date (estática 2000–2030) ...")

    records = []
    start = date(2000, 1, 1)
    end   = date(2030, 12, 31)
    delta = timedelta(days=1)
    current = start

    month_names = ["January","February","March","April","May","June",
                   "July","August","September","October","November","December"]
    day_names   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    while current <= end:
        date_sk     = int(current.strftime("%Y%m%d"))
        iso_weekday = current.isoweekday()   # 1=Mon, 7=Sun
        records.append((
            date_sk,
            current.strftime("%Y-%m-%d"),
            current.year,
            (current.month - 1) // 3 + 1,   # quarter
            current.month,
            month_names[current.month - 1],
            int(current.strftime("%V")),      # ISO week
            current.day,
            iso_weekday,
            day_names[iso_weekday - 1],
            iso_weekday >= 6,                 # is_weekend
            (current.year % 4 == 0 and (current.year % 100 != 0 or current.year % 400 == 0)),
            current.year,                     # fiscal_year
            1 if current.month <= 6 else 2,  # semester
            current.year >= 2018,             # is_satellite_era
        ))
        current += delta

    schema = StructType([
        StructField("date_sk",          IntegerType(),  False),
        StructField("full_date",        StringType(),   True),
        StructField("year",             IntegerType(),  True),
        StructField("quarter",          IntegerType(),  True),
        StructField("month",            IntegerType(),  True),
        StructField("month_name",       StringType(),   True),
        StructField("week_of_year",     IntegerType(),  True),
        StructField("day_of_month",     IntegerType(),  True),
        StructField("day_of_week",      IntegerType(),  True),
        StructField("day_name",         StringType(),   True),
        StructField("is_weekend",       BooleanType(),  True),
        StructField("is_leap_year",     BooleanType(),  True),
        StructField("fiscal_year",      IntegerType(),  True),
        StructField("semester",         IntegerType(),  True),
        StructField("is_satellite_era", BooleanType(),  True),
    ])

    df = spark.createDataFrame(records, schema)
    # dim_date no se particiona — es pequeña y siempre se lee completa
    df.write.mode("overwrite").parquet(f"{HDFS_SILVER}/dim_date")
    print(f"  [SILVER] dim_date: {df.count():,} filas (2000–2030)")
    repair_table(spark, "gtp_silver", "dim_date")
    return df


# ==============================================================================
# DIMENSIÓN 3: DIM_SOURCE — SCD Tipo 1
# ==============================================================================
def build_dim_source(spark):
    print("\n[DIM 3/4] Construyendo dim_source (SCD-1) ...")

    schema = StructType([
        StructField("source_sk",         IntegerType(), False),
        StructField("source_code",       StringType(),  True),
        StructField("source_name",       StringType(),  True),
        StructField("source_type",       StringType(),  True),
        StructField("provider",          StringType(),  True),
        StructField("url_reference",     StringType(),  True),
        StructField("temporal_coverage", StringType(),  True),
        StructField("spatial_coverage",  StringType(),  True),
        StructField("update_frequency",  StringType(),  True),
        StructField("license",           StringType(),  True),
        StructField("_last_updated",     StringType(),  True),
    ])

    records = [(
        sk, code, name, stype, provider, url, temporal, spatial, freq, lic, SILVER_LOAD_DATE
    ) for (sk, code, name, stype, provider, url, temporal, spatial, freq, lic) in SOURCE_CATALOG]

    df = spark.createDataFrame(records, schema)
    df.write.mode("overwrite").parquet(f"{HDFS_SILVER}/dim_source")
    print(f"  [SILVER] dim_source: {df.count():,} filas")
    repair_table(spark, "gtp_silver", "dim_source")
    return df


# ==============================================================================
# DIMENSIÓN 4: DIM_COMPANY — SCD Tipo 2
# Lógica SCD-2:
#   - Si el ticker+sector no ha cambiado respecto a la versión activa → no hacer nada
#   - Si cambió → cerrar registro anterior (valid_to=ayer, is_current=False)
#                  insertar nuevo registro (valid_from=hoy, valid_to=9999-12-31, is_current=True)
#   - Si es nuevo ticker → insertar con valid_from=fecha_más_antigua_del_CSV
# ==============================================================================
def build_dim_company(spark):
    print("\n[DIM 4/4] Construyendo dim_company (SCD-2) ...")

    bronze_fin = read_bronze(spark, f"{HDFS_BRONZE}/finance_raw")
    if bronze_fin is None:
        print("  [SKIP] Sin datos de finance en Bronze.")
        return None

    # Tomar el snapshot más reciente por ticker (para detectar cambios de sector)
    today_str = SILVER_LOAD_DATE
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    # Extraer snapshot de empresa: un registro por ticker con atributos actuales
    w_latest = Window.partitionBy("ticker").orderBy(F.desc("year"))
    current_snapshot = (
        bronze_fin
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "ticker", "company_name", "sector", "industry",
            "fua_country_code", "country"
        )
    )

    # Intentar leer dim_company existente para SCD-2
    dim_path = f"{HDFS_SILVER}/dim_company"
    try:
        existing = spark.read.parquet(dim_path)
        has_existing = True
    except Exception:
        has_existing = False
        existing = None

    if not has_existing:
        # Primera carga: todos los tickers son nuevos
        df_new = (
            current_snapshot
            .withColumn("company_sk",       F.monotonically_increasing_id() + 1)
            .withColumn("stock_exchange",    F.lit(None).cast(StringType()))
            .withColumn("esg_classification", F.lit("GREEN"))
            .withColumn("valid_from",        F.lit("2006-01-01"))
            .withColumn("valid_to",          F.lit("9999-12-31"))
            .withColumn("is_current",        F.lit(True))
            .withColumnRenamed("fua_country_code", "country_code")
            .withColumnRenamed("country",          "country_name")
        )
    else:
        # SCD-2: detectar cambios de sector/industria
        active = existing.filter(F.col("is_current") == True)

        # Tickers con cambio en sector o industria
        joined = current_snapshot.alias("new").join(
            active.select("ticker", "sector", "industry").alias("old"),
            on="ticker",
            how="left"
        )

        changed = joined.filter(
            (F.col("new.sector") != F.col("old.sector")) |
            (F.col("new.industry") != F.col("old.industry")) |
            F.col("old.ticker").isNull()
        )

        # Cerrar registros vigentes de los cambiados
        tickers_changed = [r.ticker for r in changed.select("ticker").collect()]
        if tickers_changed:
            existing = existing.withColumn(
                "valid_to",
                F.when(
                    F.col("ticker").isin(tickers_changed) & F.col("is_current"),
                    F.lit(yesterday)
                ).otherwise(F.col("valid_to"))
            ).withColumn(
                "is_current",
                F.when(
                    F.col("ticker").isin(tickers_changed) & F.col("is_current"),
                    F.lit(False)
                ).otherwise(F.col("is_current"))
            )

        # Construir nuevas filas para los cambiados/nuevos
        max_sk = existing.agg(F.max("company_sk")).collect()[0][0] or 0
        new_rows = (
            changed
            .select(
                "ticker", "company_name",
                F.col("new.sector").alias("sector"),
                F.col("new.industry").alias("industry"),
                "fua_country_code", "country"
            )
            .withColumn("company_sk",       F.monotonically_increasing_id() + max_sk + 1)
            .withColumn("stock_exchange",   F.lit(None).cast(StringType()))
            .withColumn("esg_classification", F.lit("GREEN"))
            .withColumn("valid_from",       F.lit(today_str))
            .withColumn("valid_to",         F.lit("9999-12-31"))
            .withColumn("is_current",       F.lit(True))
            .withColumnRenamed("fua_country_code", "country_code")
            .withColumnRenamed("country",          "country_name")
        )

        df_new = existing.union(new_rows.select(existing.columns))

    write_silver_dim(df_new, "dim_company", partition_cols=["country_code"])
    repair_table(spark, "gtp_silver", "dim_company")
    return df_new


# ==============================================================================
# FACT 1: FACT_ENVIRONMENTAL — NDVI + NO2 + HRL, ciudad × año
# ==============================================================================
def build_fact_environmental(spark, dim_city):
    print("\n[FACT 1/3] Construyendo fact_environmental ...")

    # --- Cargar Bronze ---
    s2  = read_bronze(spark, f"{HDFS_BRONZE}/sentinel2_raw")
    s5p = read_bronze(spark, f"{HDFS_BRONZE}/sentinel5p_raw")
    hrl = read_bronze(spark, f"{HDFS_BRONZE}/hrl_raw")

    if s2 is None:
        print("  [SKIP] Sin datos Sentinel-2 en Bronze.")
        return

    # --- S2: agregar mensual → anual ---
    s2_annual = (
        s2
        .groupBy("city", "year")
        .agg(
            F.avg("ndvi_mean").alias("ndvi_annual_mean"),
            F.stddev("ndvi_mean").alias("ndvi_annual_std"),
            F.avg(F.when(F.col("month").isin(3,4,5), F.col("ndvi_mean"))).alias("ndvi_spring_mean"),
            F.avg(F.when(F.col("month").isin(6,7,8), F.col("ndvi_mean"))).alias("ndvi_summer_mean"),
            F.avg(F.when(F.col("month").isin(9,10,11), F.col("ndvi_mean"))).alias("ndvi_autumn_mean"),
            F.avg(F.when(F.col("month").isin(12,1,2), F.col("ndvi_mean"))).alias("ndvi_winter_mean"),
            F.count(F.when(F.col("ndvi_mean").isNotNull(), 1)).alias("ndvi_valid_months"),
        )
    )

    # --- S5P: agregar mensual → anual ---
    if s5p is not None:
        s5p_annual = (
            s5p
            .groupBy("city", "year")
            .agg(
                F.avg("no2_mean").alias("no2_annual_mean"),
                F.stddev("no2_mean").alias("no2_annual_std"),
                F.count(F.when(F.col("no2_mean").isNotNull(), 1)).alias("no2_valid_months"),
            )
        )
        s2_annual = s2_annual.join(s5p_annual, on=["city", "year"], how="left")
    else:
        s2_annual = (
            s2_annual
            .withColumn("no2_annual_mean",  F.lit(None).cast(DoubleType()))
            .withColumn("no2_annual_std",   F.lit(None).cast(DoubleType()))
            .withColumn("no2_valid_months", F.lit(None).cast(IntegerType()))
        )

    # --- HRL: ya es anual ---
    # Columnas reales producidas por hrl_to_csv.py: imperviousness_mean, tree_cover_pct, small_woody_mean
    if hrl is not None:
        s2_annual = s2_annual.join(
            hrl.select("city", "year", "imperviousness_mean",
                       "tree_cover_pct", "small_woody_mean"),
            on=["city", "year"], how="left"
        )
    else:
        s2_annual = (
            s2_annual
            .withColumn("imperviousness_mean", F.lit(None).cast(DoubleType()))
            .withColumn("tree_cover_pct",      F.lit(None).cast(DoubleType()))
            .withColumn("small_woody_mean",    F.lit(None).cast(DoubleType()))
        )

    # --- ERA5-Land: temperatura + precipitación mensual → anual ---
    era5 = read_bronze(spark, f"{HDFS_BRONZE}/era5_raw")
    if era5 is not None:
        era5_annual = (
            era5
            .groupBy("city", "year")
            .agg(
                F.avg("temp_mean_c").alias("temp_annual_mean_c"),
                F.sum("precip_total_m").alias("precip_annual_sum_m"),
            )
        )
        s2_annual = s2_annual.join(era5_annual, on=["city", "year"], how="left")
    else:
        s2_annual = (
            s2_annual
            .withColumn("temp_annual_mean_c",  F.lit(None).cast(DoubleType()))
            .withColumn("precip_annual_sum_m", F.lit(None).cast(DoubleType()))
        )

    # --- S5P Aerosol: UVAI mensual → anual ---
    aer = read_bronze(spark, f"{HDFS_BRONZE}/s5p_aerosol_raw")
    if aer is not None:
        aer_annual = (
            aer
            .groupBy("city", "year")
            .agg(F.avg("uvai_mean").alias("uvai_annual_mean"))
        )
        s2_annual = s2_annual.join(aer_annual, on=["city", "year"], how="left")
    else:
        s2_annual = s2_annual.withColumn("uvai_annual_mean", F.lit(None).cast(DoubleType()))

    # --- Urban Atlas (ESA WorldCover): uso de suelo — datos anuales estáticos ---
    # Solo disponibles para 2020 y 2021. Para otros años el join devuelve NULL.
    ua = read_bronze(spark, f"{HDFS_BRONZE}/urban_atlas_raw")
    if ua is not None:
        s2_annual = s2_annual.join(
            ua.select("city", "year",
                      "wc_tree_pct", "wc_shrub_pct", "wc_grass_pct",
                      "wc_crop_pct", "wc_built_pct", "wc_bare_pct", "wc_water_pct"),
            on=["city", "year"], how="left"
        )
    else:
        for col in ["wc_tree_pct", "wc_shrub_pct", "wc_grass_pct",
                    "wc_crop_pct", "wc_built_pct", "wc_bare_pct", "wc_water_pct"]:
            s2_annual = s2_annual.withColumn(col, F.lit(None).cast(DoubleType()))

    # --- Indicadores derivados ---
    # NDVI YoY change
    w_city = Window.partitionBy("city").orderBy("year")
    s2_annual = s2_annual.withColumn(
        "ndvi_yoy_change",
        F.col("ndvi_annual_mean") - F.lag("ndvi_annual_mean").over(w_city)
    )

    # NDVI trend slope por ciudad (regresión lineal simple usando covarianza/varianza)
    # slope = Cov(year, ndvi) / Var(year) → calculado con Spark window functions
    s2_annual = (
        s2_annual
        .withColumn("year_mean",  F.avg("year").over(Window.partitionBy("city")))
        .withColumn("ndvi_mean_", F.avg("ndvi_annual_mean").over(Window.partitionBy("city")))
        .withColumn("cov_yn",     (F.col("year") - F.col("year_mean")) * (F.col("ndvi_annual_mean") - F.col("ndvi_mean_")))
        .withColumn("var_y",      F.pow(F.col("year") - F.col("year_mean"), 2))
    )

    slopes = (
        s2_annual
        .groupBy("city")
        .agg(
            (F.sum("cov_yn") / F.sum("var_y")).alias("ndvi_trend_slope")
        )
    )
    s2_annual = s2_annual.join(slopes, on="city", how="left").drop("year_mean", "ndvi_mean_", "cov_yn", "var_y")

    # Green Index compuesto GTP ∈ [0,1]
    # Fórmula: 0.5*ndvi_norm + 0.3*(1-no2_norm) + 0.2*(1-imperv_norm)
    # Normalización MinMax sobre el dataset completo
    stats = s2_annual.agg(
        F.min("ndvi_annual_mean").alias("ndvi_min"), F.max("ndvi_annual_mean").alias("ndvi_max"),
        F.min("no2_annual_mean").alias("no2_min"),   F.max("no2_annual_mean").alias("no2_max"),
        F.min("imperviousness_mean").alias("imp_min"), F.max("imperviousness_mean").alias("imp_max"),
    ).collect()[0]

    ndvi_range = (stats["ndvi_max"] - stats["ndvi_min"]) or 1.0
    no2_range  = (stats["no2_max"]  - stats["no2_min"])  or 1.0
    imp_range  = (stats["imp_max"]  - stats["imp_min"])  or 1.0

    s2_annual = s2_annual.withColumn(
        "green_index",
        F.round(
            0.5 * ((F.col("ndvi_annual_mean") - stats["ndvi_min"]) / ndvi_range)
            + 0.3 * (1.0 - (F.coalesce(F.col("no2_annual_mean"),  F.lit(stats["no2_min"]))  - stats["no2_min"])  / no2_range)
            + 0.2 * (1.0 - (F.coalesce(F.col("imperviousness_mean"), F.lit(stats["imp_min"])) - stats["imp_min"]) / imp_range),
            6
        )
    )

    # --- Unir con dim_city para obtener city_sk ---
    city_keys = dim_city.select("city_sk", "city_code")
    s2_annual = s2_annual.join(
        city_keys.withColumnRenamed("city_code", "city"),
        on="city", how="left"
    )

    # date_sk = primer día del año (YYYYMMDD)
    s2_annual = s2_annual.withColumn(
        "date_sk", (F.col("year") * 10000 + 101).cast(IntegerType())
    )

    # Surrogate key fact
    s2_annual = s2_annual.withColumn(
        "env_fact_sk", F.monotonically_increasing_id() + 1
    )

    # FKs de source (valores fijos del catálogo dim_source)
    s2_annual = (
        s2_annual
        .withColumn("source_sk_s2",      F.lit(1))
        .withColumn("source_sk_s5p",     F.lit(2))
        .withColumn("source_sk_hrl",     F.lit(3))
        .withColumn("source_sk_era5",    F.lit(7))
        .withColumn("source_sk_aerosol", F.lit(9))
        .withColumn("source_sk_wcover",  F.lit(10))
        .withColumn("_silver_load_date", F.lit(SILVER_LOAD_DATE))
    )

    # Extraer country para partición
    s2_annual = s2_annual.withColumn(
        "country", F.element_at(F.split(F.col("city"), "_"), -1)
    )

    final_cols = [
        "env_fact_sk", "city_sk", "date_sk",
        "source_sk_s2", "source_sk_s5p", "source_sk_hrl",
        "source_sk_era5", "source_sk_aerosol", "source_sk_wcover",
        "ndvi_annual_mean", "ndvi_annual_std",
        "ndvi_spring_mean", "ndvi_summer_mean", "ndvi_autumn_mean", "ndvi_winter_mean",
        "ndvi_valid_months", "no2_annual_mean", "no2_annual_std", "no2_valid_months",
        "imperviousness_mean", "tree_cover_pct", "small_woody_mean",
        "temp_annual_mean_c", "precip_annual_sum_m",
        "uvai_annual_mean",
        "wc_tree_pct", "wc_shrub_pct", "wc_grass_pct",
        "wc_crop_pct", "wc_built_pct", "wc_bare_pct", "wc_water_pct",
        "ndvi_yoy_change", "ndvi_trend_slope", "green_index",
        "_silver_load_date", "year", "country"
    ]
    # Solo incluir columnas que existan
    existing_cols = [c for c in final_cols if c in s2_annual.columns]
    s2_annual = s2_annual.select(existing_cols)

    write_silver_fact(s2_annual, "fact_environmental", ["year", "country"])
    repair_table(spark, "gtp_silver", "fact_environmental")


# ==============================================================================
# FACT 2: FACT_ECONOMIC — GDP ciudad × año
# ==============================================================================
def build_fact_economic(spark, dim_city):
    print("\n[FACT 2/3] Construyendo fact_economic ...")

    gdp = read_bronze(spark, f"{HDFS_BRONZE}/eurostat_gdp_raw")
    if gdp is None:
        print("  [SKIP] Sin datos Eurostat GDP en Bronze.")
        return

    gdp = gdp.withColumnRenamed("geo_code", "city").withColumnRenamed("gdp_value", "gdp_eur_per_capita")

    # Calcular variables EKC
    gdp = (
        gdp
        .withColumn("ln_gdp_pps",    F.log(F.col("gdp_eur_per_capita")))
        .withColumn("ln_gdp_pps_sq", F.pow(F.col("ln_gdp_pps"), 2))
    )

    # Growth rate YoY
    w_city = Window.partitionBy("city").orderBy("year")
    gdp = gdp.withColumn(
        "gdp_growth_rate",
        (F.col("gdp_eur_per_capita") / F.lag("gdp_eur_per_capita").over(w_city) - 1) * 100
    )

    # Unir con dim_city
    city_keys = dim_city.select("city_sk", "city_code")
    gdp = gdp.join(
        city_keys.withColumnRenamed("city_code", "city"),
        on="city", how="left"
    )

    # --- EDGAR CO2: emisiones nacionales por país × año ---
    edgar = read_bronze(spark, f"{HDFS_BRONZE}/edgar_co2_raw")
    if edgar is not None:
        # edgar_co2_raw tiene una fila por (city, year) — extraer ISO2 y deduplicar
        edgar_country = (
            edgar
            .withColumn("iso2", F.element_at(F.split(F.col("city"), "_"), -1))
            .groupBy("iso2", "year")
            .agg(F.first("co2_kt").alias("co2_country_kt"))
        )
        edgar_country = edgar_country.withColumnRenamed("year", "_edgar_year")
        gdp = gdp.withColumn("_iso2", F.element_at(F.split(F.col("city"), "_"), -1))
        gdp = gdp.join(
            edgar_country,
            on=[gdp["_iso2"] == edgar_country["iso2"], gdp["year"] == edgar_country["_edgar_year"]],
            how="left"
        ).drop("_iso2", "iso2", "_edgar_year")
    else:
        gdp = gdp.withColumn("co2_country_kt", F.lit(None).cast(DoubleType()))

    # --- OECD: indicadores de política ambiental por ciudad × año ---
    oecd = read_bronze(spark, f"{HDFS_BRONZE}/oecd_raw")
    if oecd is not None:
        gdp = gdp.join(
            oecd.select("city", "year", "eps_index", "env_tax_usd",
                        "env_expenditure", "ghg_total_kt"),
            on=["city", "year"], how="left"
        )
    else:
        for col in ["eps_index", "env_tax_usd", "env_expenditure", "ghg_total_kt"]:
            gdp = gdp.withColumn(col, F.lit(None).cast(DoubleType()))

    # --- InvestEU: inversión verde EIB por ciudad × año ---
    investeu = read_bronze(spark, f"{HDFS_BRONZE}/investeu_raw")
    if investeu is not None:
        gdp = gdp.join(
            investeu.select("city", "year", "investeu_ops_count", "investeu_total_eur"),
            on=["city", "year"], how="left"
        )
    else:
        gdp = (
            gdp
            .withColumn("investeu_ops_count", F.lit(None).cast(IntegerType()))
            .withColumn("investeu_total_eur",  F.lit(None).cast(DoubleType()))
        )

    gdp = (
        gdp
        .withColumn("econ_fact_sk",        F.monotonically_increasing_id() + 1)
        .withColumn("date_sk",             (F.col("year") * 10000 + 101).cast(IntegerType()))
        .withColumn("source_sk_gdp",       F.lit(5))
        .withColumn("source_sk_pop",       F.lit(6))
        .withColumn("source_sk_co2",       F.lit(8))
        .withColumn("source_sk_oecd",      F.lit(11))
        .withColumn("source_sk_investeu",  F.lit(12))
        # Población: no disponible en este CSV — se añade cuando exista el extractor
        .withColumn("fua_population",       F.lit(None).cast(LongType()))
        .withColumn("population_density",   F.lit(None).cast(DoubleType()))
        .withColumn("population_yoy_growth",F.lit(None).cast(DoubleType()))
        # GDP PPS: usamos EUR_HAB como proxy (idealmente sería PPS)
        .withColumn("gdp_pps_per_capita",   F.col("gdp_eur_per_capita"))
        .withColumn("gdp_pps_index",        F.lit(None).cast(DoubleType()))
        .withColumn("_silver_load_date",    F.lit(SILVER_LOAD_DATE))
        .withColumn("country",              F.element_at(F.split(F.col("city"), "_"), -1))
    )

    final_cols = [
        "econ_fact_sk", "city_sk", "date_sk",
        "source_sk_gdp", "source_sk_pop", "source_sk_co2",
        "source_sk_oecd", "source_sk_investeu",
        "gdp_pps_per_capita", "gdp_eur_per_capita", "gdp_pps_index",
        "ln_gdp_pps", "ln_gdp_pps_sq", "gdp_growth_rate",
        "fua_population", "population_density", "population_yoy_growth",
        "co2_country_kt",
        "eps_index", "env_tax_usd", "env_expenditure", "ghg_total_kt",
        "investeu_ops_count", "investeu_total_eur",
        "_silver_load_date", "year", "country"
    ]
    existing_cols = [c for c in final_cols if c in gdp.columns]
    gdp = gdp.select(existing_cols)

    write_silver_fact(gdp, "fact_economic", ["year", "country"])
    repair_table(spark, "gtp_silver", "fact_economic")


# ==============================================================================
# FACT 3: FACT_FINANCIAL — empresa × año
# ==============================================================================
def build_fact_financial(spark):
    print("\n[FACT 3/3] Construyendo fact_financial ...")

    fin = read_bronze(spark, f"{HDFS_BRONZE}/finance_raw")
    if fin is None:
        print("  [SKIP] Sin datos YFinance en Bronze.")
        return

    # Unir company_sk desde dim_company (solo versión vigente)
    try:
        dim_co = spark.read.parquet(f"{HDFS_SILVER}/dim_company")
        dim_co_active = dim_co.filter(F.col("is_current") == True).select("company_sk", "ticker")
        fin = fin.join(dim_co_active, on="ticker", how="left")
    except Exception:
        fin = fin.withColumn("company_sk", F.lit(None).cast(LongType()))

    fin = (
        fin
        .withColumn("fin_fact_sk",       F.monotonically_increasing_id() + 1)
        .withColumn("date_sk",           (F.col("year") * 10000 + F.col("month") * 100 + 1).cast(IntegerType()))
        .withColumn("source_sk",         F.lit(4))
        .withColumn("close_price_avg",   F.col("close_price"))
        .withColumn("_silver_load_date", F.lit(SILVER_LOAD_DATE))
    )

    final_cols = [
        "fin_fact_sk", "company_sk", "date_sk", "source_sk",
        "close_price_avg", "monthly_return", "monthly_volatility",
        "current_beta", "volume_avg", "current_pe",
        "_silver_load_date", "year", "month", "country_code"
    ]
    existing_cols = [c for c in final_cols if c in fin.columns]
    fin = fin.select(existing_cols)

    write_silver_fact(fin, "fact_financial", ["year", "country_code"])
    repair_table(spark, "gtp_silver", "fact_financial")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP Silver Transform")
    parser.add_argument("--skip-dims",  action="store_true", help="Saltar construcción de dimensiones")
    parser.add_argument("--skip-facts", action="store_true", help="Saltar construcción de facts")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — SILVER TRANSFORM (Star Schema Kimball)")
    print(f"  Inicio: {SILVER_LOAD_DATE}")
    print(f"  Bronze: {HDFS_BRONZE}")
    print(f"  Silver: {HDFS_SILVER}")
    print("=" * 65)

    spark = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS gtp_silver LOCATION 'hdfs:///user/gtp/silver'")

    dim_city = None

    if not args.skip_dims:
        dim_city    = build_dim_city(spark)
        build_dim_date(spark)
        build_dim_source(spark)
        build_dim_company(spark)

    if not args.skip_facts:
        # Si se saltaron dims, cargamos dim_city desde HDFS
        if dim_city is None:
            try:
                dim_city = spark.read.parquet(f"{HDFS_SILVER}/dim_city")
            except Exception:
                print("[ERROR] dim_city no disponible. Ejecuta sin --skip-dims primero.")
                spark.stop()
                sys.exit(1)

        build_fact_environmental(spark, dim_city)
        build_fact_economic(spark, dim_city)
        build_fact_financial(spark)

    print("\n" + "=" * 65)
    print("  SILVER TRANSFORM COMPLETADO")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    main()
