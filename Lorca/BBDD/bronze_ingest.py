"""
bronze_ingest.py — GTP (Green Turning Point)
Capa Bronze: ingesta de todos los CSVs del ETL hacia HDFS en formato Parquet.

Fuentes ingestadas:
  1.  sentinel2.csv           → bronze_sentinel2_raw
  2.  s5p.csv                 → bronze_sentinel5p_raw
  3.  hrl.csv                 → bronze_hrl_raw
  4.  finance_2006_2025.csv   → bronze_finance_raw
  5.  eurostat_PIB.csv        → bronze_eurostat_gdp_raw
  6.  era5.csv                → bronze_era5_raw
  7.  s5p_aerosol.csv         → bronze_s5p_aerosol_raw
  8.  urban_atlas.csv         → bronze_urban_atlas_raw
  9.  edgar_co2.csv           → bronze_edgar_co2_raw
  10. oecd_indicators.csv     → bronze_oecd_raw
  11. investeu_summary.csv    → bronze_investeu_raw
  12. (sin CSV propio — bronze_eurostat_population_raw se llena via source_sk=20)
  13. modis_ndvi.csv          → bronze_modis_ndvi_raw
  14. modis_lst.csv           → bronze_modis_lst_raw
  15. ntl.csv                 → bronze_ntl_raw
  16. renewables.csv          → bronze_renewables_raw
  17. tourism.csv             → bronze_tourism_raw
  18. ets_price.csv           → bronze_ets_raw
  19. eea_aq.csv              → bronze_eea_aq_raw
  20. population.csv          → bronze_eurostat_population_raw

Ejecución en Lorca:
  spark-submit --master yarn bronze_ingest.py [--mode overwrite|append] [--source all|s2|s5p|hrl|finance|eurostat|era5|s5p_aerosol|urban_atlas|edgar_co2|oecd|investeu|modis_ndvi|modis_lst|ntl|renewables|tourism|ets|eea_aq|population]
"""

import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType
)

# ==============================================================================
# CONFIGURACIÓN DE RUTAS
# Ajusta HDFS_BASE y ETL_DATA_DIR según el entorno de ejecución.
# ==============================================================================
HDFS_BASE       = "hdfs:///user/gtp/bronze"
# Lorca/BBDD/ → subir un nivel → Lorca/ → data/
LORCA_BASE      = Path(__file__).resolve().parent.parent
ETL_DATA        = LORCA_BASE / "data"
PROCESSED_DIR   = ETL_DATA / "DatosProcesados"
FINANCE_CSV     = ETL_DATA / "finance_monthly_2006_2025.csv"


def _local(p) -> str:
    """
    Convierte una ruta local (Path o str) a URI file:// para que Spark no la
    interprete como una ruta HDFS cuando fs.defaultFS apunta al namenode.
    """
    return f"file://{p}"


# Metadata fija de ingesta
INGESTION_DATE  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
INGESTION_TS    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ==============================================================================
# SPARK SESSION
# enableHiveSupport() permite registrar tablas en el metastore de Hive y
# usar MSCK REPAIR TABLE para detectar particiones Parquet nuevas.
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_Bronze_Ingest")
        .config("spark.sql.shuffle.partitions", "100")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Necesario para que Hive detecte particiones escritas directamente como Parquet
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )

# ==============================================================================
# UTILIDADES
# ==============================================================================

def extract_country(city_code_col):
    """
    Extrae el código ISO-2 del país desde el city_code.
    Formato esperado: 'NombreCiudad_XX'  →  'XX'
    """
    return F.element_at(F.split(city_code_col, "_"), -1)


def add_metadata(df, source_system: str, file_name: str):
    """
    Añade las tres columnas de auditoría Bronze a cualquier DataFrame.
    Estas columnas nunca se modifican en capas superiores.
    """
    return (
        df
        .withColumn("_ingestion_date",  F.lit(INGESTION_DATE))
        .withColumn("_source_system",   F.lit(source_system))
        .withColumn("_file_name",       F.lit(file_name))
    )


def write_bronze(df, hdfs_path: str, partition_cols: list, mode: str = "overwrite"):
    """
    Escribe un DataFrame Spark a HDFS en Parquet particionado.
    mode='overwrite' reemplaza las particiones afectadas (seguro en HDFS).
    mode='append'    añade filas a las particiones existentes.
    """
    (
        df.write
        .mode(mode)
        .partitionBy(*partition_cols)
        .parquet(hdfs_path)
    )
    print(f"  [HDFS] Escrito en: {hdfs_path}  (particionado por {partition_cols})")


def repair_table(spark, db: str, table: str):
    """
    MSCK REPAIR TABLE detecta nuevas particiones Parquet en HDFS y las
    registra en el metastore de Hive. Necesario tras escritura directa como Parquet.
    """
    try:
        spark.sql(f"MSCK REPAIR TABLE {db}.{table}")
        print(f"  [HIVE] Particiones registradas: {db}.{table}")
    except Exception as e:
        print(f"  [WARN] MSCK REPAIR falló en {db}.{table}: {e}")
        print(f"         Ejecuta manualmente: MSCK REPAIR TABLE {db}.{table};")


def log_stats(df, label: str):
    """Imprime conteo de filas y partición mínima/máxima para trazabilidad."""
    n = df.count()
    if "year" in [c.lower() for c in df.columns]:
        year_col = next(c for c in df.columns if c.lower() == "year")
        stats = df.agg(F.min(year_col), F.max(year_col)).collect()[0]
        print(f"  [{label}] {n:,} filas | años: {stats[0]}–{stats[1]}")
    else:
        print(f"  [{label}] {n:,} filas")


# ==============================================================================
# INGESTA 1 — Sentinel-2 (NDVI)
# CSV fuente: DatosProcesados/sentinel2.csv
# Columnas esperadas: City, Year, Month, NDVI_Mean (mínimo requerido)
# ==============================================================================
def ingest_sentinel2(spark, mode: str):
    print("\n[1/5] Ingesta Sentinel-2 (NDVI) ...")
    path = _local(PROCESSED_DIR / "sentinel2.csv")

    # Columnas reales que produce Sentinel-2_extract.py (vía GEE reduceRegion)
    # NOTA: pixel_count no está en el CSV (no lo genera el script GEE).
    # Se usa PERMISSIVE para que Spark rellene null en columnas faltantes en lugar
    # de descartar filas (DROPMALFORMED descartaría TODAS si el CSV tiene menos
    # columnas que el schema).
    schema = StructType([
        StructField("City",        StringType(),  True),
        StructField("Year",        IntegerType(), True),
        StructField("Month",       IntegerType(), True),
        StructField("NDVI_Mean",   DoubleType(),  True),
        StructField("NDVI_Std",    DoubleType(),  True),
        StructField("pixel_count", IntegerType(), True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",        "city")
        .withColumnRenamed("Year",        "year")
        .withColumnRenamed("Month",       "month")
        .withColumnRenamed("NDVI_Mean",   "ndvi_mean")
        .withColumnRenamed("NDVI_Std",    "ndvi_std")
        .withColumnRenamed("pixel_count", "ndvi_valid_pixels")
        # Coordenadas no están en el CSV — se unen desde config en Silver
        .withColumn("lat", F.lit(None).cast(DoubleType()))
        .withColumn("lon", F.lit(None).cast(DoubleType()))
    )

    # Extraer país para la partición
    df = df.withColumn("country", extract_country(F.col("city")))

    # Filtrar filas sin datos clave
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())

    df = add_metadata(df, "GEE_S2", "sentinel2.csv")
    log_stats(df, "S2")

    write_bronze(df, f"{HDFS_BASE}/sentinel2_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_sentinel2_raw")


# ==============================================================================
# INGESTA 2 — Sentinel-5P (NO2)
# CSV fuente: DatosProcesados/s5p.csv
# Columnas esperadas: City, Year, Month, NO2_Mean (mínimo requerido)
# ==============================================================================
def ingest_sentinel5p(spark, mode: str):
    print("\n[2/5] Ingesta Sentinel-5P (NO2) ...")
    path = _local(PROCESSED_DIR / "s5p.csv")

    # Columnas reales que produce Sentinel-5p_extract.py (vía GEE reduceRegion)
    # NOTA: s5p.csv solo tiene City,Year,Month,NO2_Mean (sin NO2_Std ni pixel_count).
    # PERMISSIVE rellena null para columnas faltantes en lugar de descartar filas.
    schema = StructType([
        StructField("City",        StringType(),  True),
        StructField("Year",        IntegerType(), True),
        StructField("Month",       IntegerType(), True),
        StructField("NO2_Mean",    DoubleType(),  True),
        StructField("NO2_Std",     DoubleType(),  True),
        StructField("pixel_count", IntegerType(), True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",        "city")
        .withColumnRenamed("Year",        "year")
        .withColumnRenamed("Month",       "month")
        .withColumnRenamed("NO2_Mean",    "no2_mean")
        .withColumnRenamed("NO2_Std",     "no2_std")
        .withColumnRenamed("pixel_count", "no2_valid_pixels")
        .withColumn("lat", F.lit(None).cast(DoubleType()))
        .withColumn("lon", F.lit(None).cast(DoubleType()))
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "GEE_S5P", "s5p.csv")
    log_stats(df, "S5P")

    write_bronze(df, f"{HDFS_BASE}/sentinel5p_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_sentinel5p_raw")


# ==============================================================================
# INGESTA 3 — HRL (Impermeabilización del suelo)
# CSV fuente: DatosProcesados/hrl.csv
# Columnas esperadas: City, Year, Imperviousness_Mean, Tree_Cover_Pct, etc.
# ==============================================================================
def ingest_hrl(spark, mode: str):
    print("\n[3/5] Ingesta HRL (Impermeabilización) ...")
    path = _local(PROCESSED_DIR / "hrl.csv")

    # Columnas reales que produce hrl_to_csv.py
    schema = StructType([
        StructField("City",                StringType(),  True),
        StructField("Year",                IntegerType(), True),
        StructField("Imperviousness_Mean", DoubleType(),  True),
        StructField("Tree_Cover_Pct",      DoubleType(),  True),
        StructField("Small_Woody_Mean",    DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",                "city")
        .withColumnRenamed("Year",                "year")
        .withColumnRenamed("Imperviousness_Mean", "imperviousness_mean")
        .withColumnRenamed("Tree_Cover_Pct",      "tree_cover_pct")
        .withColumnRenamed("Small_Woody_Mean",    "small_woody_mean")
        .withColumn("lat", F.lit(None).cast(DoubleType()))
        .withColumn("lon", F.lit(None).cast(DoubleType()))
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "Copernicus_HRL", "hrl.csv")
    log_stats(df, "HRL")

    write_bronze(df, f"{HDFS_BASE}/hrl_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_hrl_raw")


# ==============================================================================
# INGESTA 4 — YFinance (Datos financieros mensuales)
# CSV fuente: finance_monthly_2006_2025.csv (raíz del ETL)
# Columnas: Ticker, Company_Name, Sector, Industry, Country, FUA_Country_Code,
#           Year, Month, Close_Price, Monthly_Return, Monthly_Volatility,
#           Volume_Avg, Current_PE, Current_Beta, Extraction_Date
# ==============================================================================
def ingest_finance(spark, mode: str):
    print("\n[4/5] Ingesta YFinance (Datos Financieros Mensuales) ...")
    path = _local(FINANCE_CSV)

    schema = StructType([
        StructField("Ticker",              StringType(),  True),
        StructField("Company_Name",        StringType(),  True),
        StructField("Sector",              StringType(),  True),
        StructField("Industry",            StringType(),  True),
        StructField("Country",             StringType(),  True),
        StructField("FUA_Country_Code",    StringType(),  True),
        StructField("Year",                IntegerType(), True),
        StructField("Month",               IntegerType(), True),
        StructField("Close_Price",         DoubleType(),  True),
        StructField("Monthly_Return",      DoubleType(),  True),
        StructField("Monthly_Volatility",  DoubleType(),  True),
        StructField("Volume_Avg",          DoubleType(),  True),
        StructField("Current_PE",          DoubleType(),  True),
        StructField("Current_Beta",        DoubleType(),  True),
        StructField("Extraction_Date",     StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("Ticker",             "ticker")
        .withColumnRenamed("Company_Name",       "company_name")
        .withColumnRenamed("Sector",             "sector")
        .withColumnRenamed("Industry",           "industry")
        .withColumnRenamed("Country",            "country")
        .withColumnRenamed("FUA_Country_Code",   "fua_country_code")
        .withColumnRenamed("Year",               "year")
        .withColumnRenamed("Month",              "month")
        .withColumnRenamed("Close_Price",        "close_price")
        .withColumnRenamed("Monthly_Return",     "monthly_return")
        .withColumnRenamed("Monthly_Volatility", "monthly_volatility")
        .withColumnRenamed("Volume_Avg",         "volume_avg")
        .withColumnRenamed("Current_PE",         "current_pe")
        .withColumnRenamed("Current_Beta",       "current_beta")
        .withColumnRenamed("Extraction_Date",    "extraction_date")
    )

    # Para finanzas la partición de país va por fua_country_code
    df = df.withColumn("country_code", F.col("fua_country_code"))
    df = df.filter(F.col("ticker").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "YFinance", "finance_monthly_2006_2025.csv")
    log_stats(df, "Finance")

    write_bronze(df, f"{HDFS_BASE}/finance_raw", ["year", "country_code"], mode)
    repair_table(spark, "gtp_bronze", "bronze_finance_raw")


# ==============================================================================
# INGESTA 5 — Eurostat GDP
# CSV fuente: DatosProcesados/eurostat_PIB.csv
# Columnas: City, Year, GDP_Per_Capita, Source
# ==============================================================================
def ingest_eurostat_gdp(spark, mode: str):
    print("\n[5/5] Ingesta Eurostat (GDP per cápita) ...")
    path = _local(PROCESSED_DIR / "eurostat_PIB.csv")

    schema = StructType([
        StructField("City",            StringType(),  True),
        StructField("Year",            IntegerType(), True),
        StructField("GDP_Per_Capita",  DoubleType(),  True),
        StructField("Source",          StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",           "geo_code")
        .withColumnRenamed("Year",           "year")
        .withColumnRenamed("GDP_Per_Capita", "gdp_value")
        .withColumnRenamed("Source",         "obs_flag")
        # Campos adicionales del DDL (no disponibles en el CSV actual)
        .withColumn("geo_label",    F.lit(None).cast(StringType()))
        .withColumn("unit",         F.lit("EUR_HAB"))
        .withColumn("_dataset_code", F.lit("met_10r_3gdp/nama_10r_3gdp"))
    )

    # Extraer país del city_code para la partición
    df = df.withColumn("country", extract_country(F.col("geo_code")))
    df = df.filter(F.col("geo_code").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "Eurostat_API", "eurostat_PIB.csv")
    log_stats(df, "Eurostat_GDP")

    write_bronze(df, f"{HDFS_BASE}/eurostat_gdp_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_eurostat_gdp_raw")


# ==============================================================================
# INGESTA 6 — ERA5-Land (Temperatura + Precipitación)
# CSV fuente: DatosProcesados/era5.csv
# Columnas: City, Year, Month, Temp_Mean_C, Precip_Total_m
# ==============================================================================
def ingest_era5(spark, mode: str):
    print("\n[6/9] Ingesta ERA5-Land (Temperatura + Precipitación) ...")
    path = _local(PROCESSED_DIR / "era5.csv")

    schema = StructType([
        StructField("City",           StringType(),  True),
        StructField("Year",           IntegerType(), True),
        StructField("Month",          IntegerType(), True),
        StructField("Temp_Mean_C",    DoubleType(),  True),
        StructField("Precip_Total_m", DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",           "city")
        .withColumnRenamed("Year",           "year")
        .withColumnRenamed("Month",          "month")
        .withColumnRenamed("Temp_Mean_C",    "temp_mean_c")
        .withColumnRenamed("Precip_Total_m", "precip_total_m")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "GEE_ERA5", "era5.csv")
    log_stats(df, "ERA5")

    write_bronze(df, f"{HDFS_BASE}/era5_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_era5_raw")


# ==============================================================================
# INGESTA 7 — S5P Aerosol (UVAI)
# CSV fuente: DatosProcesados/s5p_aerosol.csv
# Columnas: City, Year, Month, UVAI_Mean, UVAI_Std, pixel_count
# ==============================================================================
def ingest_s5p_aerosol(spark, mode: str):
    print("\n[7/9] Ingesta S5P Aerosol (UVAI) ...")
    path = _local(PROCESSED_DIR / "s5p_aerosol.csv")

    schema = StructType([
        StructField("City",        StringType(),  True),
        StructField("Year",        IntegerType(), True),
        StructField("Month",       IntegerType(), True),
        StructField("UVAI_Mean",   DoubleType(),  True),
        StructField("UVAI_Std",    DoubleType(),  True),
        StructField("pixel_count", IntegerType(), True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",        "city")
        .withColumnRenamed("Year",        "year")
        .withColumnRenamed("Month",       "month")
        .withColumnRenamed("UVAI_Mean",   "uvai_mean")
        .withColumnRenamed("UVAI_Std",    "uvai_std")
        .withColumnRenamed("pixel_count", "uvai_valid_pixels")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "GEE_S5P_AER", "s5p_aerosol.csv")
    log_stats(df, "S5P_Aerosol")

    write_bronze(df, f"{HDFS_BASE}/s5p_aerosol_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_s5p_aerosol_raw")


# ==============================================================================
# INGESTA 8 — Urban Atlas / ESA WorldCover (Uso de suelo)
# CSV fuente: DatosProcesados/urban_atlas.csv
# Columnas: City, Year, Tree_Pct, Shrub_Pct, Grass_Pct, Crop_Pct,
#           Built_Pct, Bare_Pct, Water_Pct
# ==============================================================================
def ingest_urban_atlas(spark, mode: str):
    print("\n[8/9] Ingesta Urban Atlas (ESA WorldCover — uso de suelo) ...")
    path = _local(PROCESSED_DIR / "urban_atlas.csv")

    schema = StructType([
        StructField("City",      StringType(),  True),
        StructField("Year",      IntegerType(), True),
        StructField("Tree_Pct",  DoubleType(),  True),
        StructField("Shrub_Pct", DoubleType(),  True),
        StructField("Grass_Pct", DoubleType(),  True),
        StructField("Crop_Pct",  DoubleType(),  True),
        StructField("Built_Pct", DoubleType(),  True),
        StructField("Bare_Pct",  DoubleType(),  True),
        StructField("Water_Pct", DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    # Renombrar con prefijo wc_ para distinguir de HRL tree_cover_pct
    df = (
        df
        .withColumnRenamed("City",      "city")
        .withColumnRenamed("Year",      "year")
        .withColumnRenamed("Tree_Pct",  "wc_tree_pct")
        .withColumnRenamed("Shrub_Pct", "wc_shrub_pct")
        .withColumnRenamed("Grass_Pct", "wc_grass_pct")
        .withColumnRenamed("Crop_Pct",  "wc_crop_pct")
        .withColumnRenamed("Built_Pct", "wc_built_pct")
        .withColumnRenamed("Bare_Pct",  "wc_bare_pct")
        .withColumnRenamed("Water_Pct", "wc_water_pct")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "GEE_WorldCover", "urban_atlas.csv")
    log_stats(df, "UrbanAtlas")

    write_bronze(df, f"{HDFS_BASE}/urban_atlas_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_urban_atlas_raw")


# ==============================================================================
# INGESTA 9 — EDGAR CO2 (Emisiones nacionales CO2 fósil)
# CSV fuente: DatosProcesados/edgar_co2.csv
# Columnas: City, Year, CO2_kt, Country_Code
# ==============================================================================
def ingest_edgar_co2(spark, mode: str):
    print("\n[9/9] Ingesta EDGAR CO2 (emisiones nacionales) ...")
    path = _local(PROCESSED_DIR / "edgar_co2.csv")

    schema = StructType([
        StructField("City",         StringType(),  True),
        StructField("Year",         IntegerType(), True),
        StructField("CO2_kt",       DoubleType(),  True),
        StructField("Country_Code", StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",         "city")
        .withColumnRenamed("Year",         "year")
        .withColumnRenamed("CO2_kt",       "co2_kt")
        .withColumnRenamed("Country_Code", "country_code")
    )

    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "EDGAR_CO2", "edgar_co2.csv")
    log_stats(df, "EDGAR_CO2")

    write_bronze(df, f"{HDFS_BASE}/edgar_co2_raw", ["year", "country_code"], mode)
    repair_table(spark, "gtp_bronze", "bronze_edgar_co2_raw")


# ==============================================================================
# INGESTA 10 — OECD Indicadores (EPS, impuestos, gasto, GHG)
# CSV fuente: DatosProcesados/oecd_indicators.csv
# Columnas: City, Year, EPS_Index, Env_Tax_USD, Env_Expenditure, GHG_Total_kt
# ==============================================================================
def ingest_oecd(spark, mode: str):
    print("\n[10/11] Ingesta OECD (indicadores política ambiental) ...")
    path = _local(PROCESSED_DIR / "oecd_indicators.csv")

    schema = StructType([
        StructField("City",            StringType(),  True),
        StructField("Year",            IntegerType(), True),
        StructField("EPS_Index",       DoubleType(),  True),
        StructField("Env_Tax_USD",     DoubleType(),  True),
        StructField("Env_Expenditure", DoubleType(),  True),
        StructField("GHG_Total_kt",    DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",            "city")
        .withColumnRenamed("Year",            "year")
        .withColumnRenamed("EPS_Index",       "eps_index")
        .withColumnRenamed("Env_Tax_USD",     "env_tax_usd")
        .withColumnRenamed("Env_Expenditure", "env_expenditure")
        .withColumnRenamed("GHG_Total_kt",    "ghg_total_kt")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "OECD_API", "oecd_indicators.csv")
    log_stats(df, "OECD")

    write_bronze(df, f"{HDFS_BASE}/oecd_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_oecd_raw")


# ==============================================================================
# INGESTA 11 — InvestEU (inversión verde EIB)
# CSV fuente: DatosProcesados/investeu_summary.csv
# Columnas: City, Year, InvestEU_Ops_Count, InvestEU_Total_EUR
# ==============================================================================
def ingest_investeu(spark, mode: str):
    print("\n[11/11] Ingesta InvestEU (inversión verde EIB) ...")
    path = _local(PROCESSED_DIR / "investeu_summary.csv")

    schema = StructType([
        StructField("City",                StringType(),  True),
        StructField("Year",                IntegerType(), True),
        StructField("InvestEU_Ops_Count",  IntegerType(), True),
        StructField("InvestEU_Total_EUR",  DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",               "city")
        .withColumnRenamed("Year",               "year")
        .withColumnRenamed("InvestEU_Ops_Count", "investeu_ops_count")
        .withColumnRenamed("InvestEU_Total_EUR", "investeu_total_eur")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "InvestEU_EIB", "investeu_summary.csv")
    log_stats(df, "InvestEU")

    write_bronze(df, f"{HDFS_BASE}/investeu_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_investeu_raw")


# ==============================================================================
# INGESTA 13 — MODIS NDVI (gap-fill pre-Landsat 2000-2003)
# CSV fuente: DatosProcesados/modis_ndvi.csv
# Columnas: City, Year, Month, NDVI_Mean, NDVI_Std
# ==============================================================================
def ingest_modis_ndvi(spark, mode: str):
    print("\n[13/20] Ingesta MODIS NDVI (gap-fill 2000-2003) ...")
    path = _local(PROCESSED_DIR / "modis_ndvi.csv")

    schema = StructType([
        StructField("City",      StringType(),  True),
        StructField("Year",      IntegerType(), True),
        StructField("Month",     IntegerType(), True),
        StructField("NDVI_Mean", DoubleType(),  True),
        StructField("NDVI_Std",  DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",      "city")
        .withColumnRenamed("Year",      "year")
        .withColumnRenamed("Month",     "month")
        .withColumnRenamed("NDVI_Mean", "ndvi_mean")
        .withColumnRenamed("NDVI_Std",  "ndvi_std")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "GEE_MODIS_NDVI", "modis_ndvi.csv")
    log_stats(df, "MODIS_NDVI")

    write_bronze(df, f"{HDFS_BASE}/modis_ndvi_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_modis_ndvi_raw")


# ==============================================================================
# INGESTA 14 — MODIS LST (Land Surface Temperature — Urban Heat Island)
# CSV fuente: DatosProcesados/modis_lst.csv
# Columnas: City, Year, Month, LST_Day_C, LST_Night_C
# ==============================================================================
def ingest_modis_lst(spark, mode: str):
    print("\n[14/20] Ingesta MODIS LST (Urban Heat Island) ...")
    path = _local(PROCESSED_DIR / "modis_lst.csv")

    schema = StructType([
        StructField("City",        StringType(),  True),
        StructField("Year",        IntegerType(), True),
        StructField("Month",       IntegerType(), True),
        StructField("LST_Day_C",   DoubleType(),  True),
        StructField("LST_Night_C", DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",        "city")
        .withColumnRenamed("Year",        "year")
        .withColumnRenamed("Month",       "month")
        .withColumnRenamed("LST_Day_C",   "lst_day_c")
        .withColumnRenamed("LST_Night_C", "lst_night_c")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "GEE_MODIS_LST", "modis_lst.csv")
    log_stats(df, "MODIS_LST")

    write_bronze(df, f"{HDFS_BASE}/modis_lst_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_modis_lst_raw")


# ==============================================================================
# INGESTA 15 — Nighttime Lights (DMSP anual 2000-2011 + VIIRS mensual 2012+)
# CSV fuente: DatosProcesados/ntl.csv
# Columnas: City, Year, Month, NTL_Mean, NTL_Std, NTL_Source
# Nota: DMSP rows tienen Month=0 (dato anual); VIIRS rows tienen Month=1-12.
# ==============================================================================
def ingest_ntl(spark, mode: str):
    print("\n[15/20] Ingesta Nighttime Lights (DMSP+VIIRS) ...")
    path = _local(PROCESSED_DIR / "ntl.csv")

    schema = StructType([
        StructField("City",       StringType(),  True),
        StructField("Year",       IntegerType(), True),
        StructField("Month",      IntegerType(), True),
        StructField("NTL_Mean",   DoubleType(),  True),
        StructField("NTL_Std",    DoubleType(),  True),
        StructField("NTL_Source", StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",       "city")
        .withColumnRenamed("Year",       "year")
        .withColumnRenamed("Month",      "month")
        .withColumnRenamed("NTL_Mean",   "ntl_mean")
        .withColumnRenamed("NTL_Std",    "ntl_std")
        .withColumnRenamed("NTL_Source", "ntl_source")
    )

    df = df.withColumn("country", extract_country(F.col("city")))
    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "GEE_NTL", "ntl.csv")
    log_stats(df, "NTL")

    write_bronze(df, f"{HDFS_BASE}/ntl_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_ntl_raw")


# ==============================================================================
# INGESTA 16 — Renovables (% electricidad renovable mensual por país)
# CSV fuente: DatosProcesados/renewables.csv
# Columnas: Country, Year, Month, Renewables_Pct, Source
# Nota: Particionado solo por year (country es columna de datos, no tiene city).
# ==============================================================================
def ingest_renewables(spark, mode: str):
    print("\n[16/20] Ingesta Renovables (Eurostat nrg_cb_pem) ...")
    path = _local(PROCESSED_DIR / "renewables.csv")

    schema = StructType([
        StructField("Country",          StringType(),  True),
        StructField("Year",             IntegerType(), True),
        StructField("Month",            IntegerType(), True),
        StructField("Renewables_Pct",   DoubleType(),  True),
        StructField("Source",           StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("Country",        "country")
        .withColumnRenamed("Year",           "year")
        .withColumnRenamed("Month",          "month")
        .withColumnRenamed("Renewables_Pct", "renewables_pct")
        .withColumnRenamed("Source",         "renewables_source")
    )

    df = df.filter(F.col("country").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "Eurostat_nrg", "renewables.csv")
    log_stats(df, "Renewables")

    # Particionado solo por year — country es columna de datos
    write_bronze(df, f"{HDFS_BASE}/renewables_raw", ["year"], mode)
    repair_table(spark, "gtp_bronze", "bronze_renewables_raw")


# ==============================================================================
# INGESTA 17 — Turismo (pernoctaciones mensuales por ciudad/país)
# CSV fuente: DatosProcesados/tourism.csv
# Columnas: City, Country, Year, Month, Tourism_Nights, Tourism_Source
# ==============================================================================
def ingest_tourism(spark, mode: str):
    print("\n[17/20] Ingesta Turismo (Eurostat tour_occ_nim) ...")
    path = _local(PROCESSED_DIR / "tourism.csv")

    schema = StructType([
        StructField("City",           StringType(),  True),
        StructField("Country",        StringType(),  True),
        StructField("Year",           IntegerType(), True),
        StructField("Month",          IntegerType(), True),
        StructField("Tourism_Nights", DoubleType(),  True),
        StructField("Tourism_Source", StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",           "city")
        .withColumnRenamed("Country",        "country")
        .withColumnRenamed("Year",           "year")
        .withColumnRenamed("Month",          "month")
        .withColumnRenamed("Tourism_Nights", "tourism_nights")
        .withColumnRenamed("Tourism_Source", "tourism_source")
    )

    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "Eurostat_tourism", "tourism.csv")
    log_stats(df, "Tourism")

    write_bronze(df, f"{HDFS_BASE}/tourism_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_tourism_raw")


# ==============================================================================
# INGESTA 18 — EU ETS (precio del carbono mensual)
# CSV fuente: DatosProcesados/ets_price.csv
# Columnas: Year, Month, ETS_Price_EUR, ETS_Source
# Nota: Serie temporal global — sin city ni country. Particionado solo por year.
# ==============================================================================
def ingest_ets(spark, mode: str):
    print("\n[18/20] Ingesta EU ETS (precio carbono) ...")
    path = _local(PROCESSED_DIR / "ets_price.csv")

    schema = StructType([
        StructField("Year",          IntegerType(), True),
        StructField("Month",         IntegerType(), True),
        StructField("ETS_Price_EUR", DoubleType(),  True),
        StructField("ETS_Source",    StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("Year",          "year")
        .withColumnRenamed("Month",         "month")
        .withColumnRenamed("ETS_Price_EUR", "ets_price_eur")
        .withColumnRenamed("ETS_Source",    "ets_source")
    )

    df = df.filter(F.col("year").isNotNull())
    df = add_metadata(df, "ETS_EUA", "ets_price.csv")
    log_stats(df, "ETS")

    # Particionado solo por year — no tiene ciudad ni país
    write_bronze(df, f"{HDFS_BASE}/ets_raw", ["year"], mode)
    repair_table(spark, "gtp_bronze", "bronze_ets_raw")


# ==============================================================================
# INGESTA 19 — EEA Air Quality (PM2.5 y PM10 anuales expandidos mensualmente)
# CSV fuente: DatosProcesados/eea_aq.csv
# Columnas: City, Country, Year, Month, PM25_Mean, PM10_Mean, AQ_Source
# ==============================================================================
def ingest_eea_aq(spark, mode: str):
    print("\n[19/20] Ingesta EEA Air Quality (PM2.5/PM10) ...")
    path = _local(PROCESSED_DIR / "eea_aq.csv")

    schema = StructType([
        StructField("City",      StringType(),  True),
        StructField("Country",   StringType(),  True),
        StructField("Year",      IntegerType(), True),
        StructField("Month",     IntegerType(), True),
        StructField("PM25_Mean", DoubleType(),  True),
        StructField("PM10_Mean", DoubleType(),  True),
        StructField("AQ_Source", StringType(),  True),
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",      "city")
        .withColumnRenamed("Country",   "country")
        .withColumnRenamed("Year",      "year")
        .withColumnRenamed("Month",     "month")
        .withColumnRenamed("PM25_Mean", "pm25_mean")
        .withColumnRenamed("PM10_Mean", "pm10_mean")
        .withColumnRenamed("AQ_Source", "aq_source")
    )

    df = df.filter(F.col("city").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "EEA_AQ", "eea_aq.csv")
    log_stats(df, "EEA_AQ")

    write_bronze(df, f"{HDFS_BASE}/eea_aq_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_eea_aq_raw")


# ==============================================================================
# INGESTA 20 — Población FUA (rellena bronze_eurostat_population_raw vacío)
# CSV fuente: DatosProcesados/population.csv
# Columnas: City, Country, Year, Population
# ==============================================================================
def ingest_population(spark, mode: str):
    print("\n[20/20] Ingesta Población FUA (Eurostat population) ...")
    path = _local(PROCESSED_DIR / "population.csv")

    schema = StructType([
        StructField("City",       StringType(),  True),
        StructField("Country",    StringType(),  True),
        StructField("Year",       IntegerType(), True),
        StructField("Population", LongType(),    True),
        StructField("Source",     StringType(),  True),  # columna extra en CSV
    ])

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    df = (
        df
        .withColumnRenamed("City",       "city_code")
        .withColumnRenamed("Country",    "geo_code")
        .withColumnRenamed("Year",       "year")
        .withColumnRenamed("Population", "population")
        # Campos del DDL no disponibles en este CSV simplificado
        .withColumn("city_name",    F.lit(None).cast(StringType()))
        .withColumn("indic_ur",     F.lit("POP"))
        .withColumn("obs_flag",     F.lit(None).cast(StringType()))
        .withColumn("_dataset_code", F.lit("population_extract"))
    )

    # Extraer país para la partición desde geo_code
    df = df.withColumn("country", F.col("geo_code"))
    df = df.filter(F.col("city_code").isNotNull() & F.col("year").isNotNull())
    df = add_metadata(df, "Eurostat_API", "population.csv")
    log_stats(df, "Population")

    write_bronze(df, f"{HDFS_BASE}/eurostat_population_raw", ["year", "country"], mode)
    repair_table(spark, "gtp_bronze", "bronze_eurostat_population_raw")


# ==============================================================================
# ORQUESTADOR
# ==============================================================================
INGESTORS = {
    "s2":           ingest_sentinel2,
    "s5p":          ingest_sentinel5p,
    "hrl":          ingest_hrl,
    "finance":      ingest_finance,
    "eurostat":     ingest_eurostat_gdp,
    "era5":         ingest_era5,
    "s5p_aerosol":  ingest_s5p_aerosol,
    "urban_atlas":  ingest_urban_atlas,
    "edgar_co2":    ingest_edgar_co2,
    "oecd":         ingest_oecd,
    "investeu":     ingest_investeu,
    "modis_ndvi":   ingest_modis_ndvi,
    "modis_lst":    ingest_modis_lst,
    "ntl":          ingest_ntl,
    "renewables":   ingest_renewables,
    "tourism":      ingest_tourism,
    "ets":          ingest_ets,
    "eea_aq":       ingest_eea_aq,
    "population":   ingest_population,
}


def main():
    parser = argparse.ArgumentParser(description="GTP Bronze Layer Ingestion")
    parser.add_argument(
        "--source", default="all",
        choices=["all"] + list(INGESTORS.keys()),
        help="Fuente a ingestar (default: all)"
    )
    parser.add_argument(
        "--mode", default="overwrite",
        choices=["overwrite", "append"],
        help="Modo de escritura Spark (default: overwrite)"
    )
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — BRONZE LAYER INGESTION")
    print(f"  Inicio:  {INGESTION_TS}")
    print(f"  Modo:    {args.mode}")
    print(f"  Fuente:  {args.source}")
    print(f"  HDFS:    {HDFS_BASE}")
    print("=" * 65)

    spark = get_spark()
    # Crear base de datos si no existe
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gtp_bronze LOCATION '{HDFS_BASE}'")

    sources = list(INGESTORS.keys()) if args.source == "all" else [args.source]
    errors = []

    for src in sources:
        try:
            INGESTORS[src](spark, args.mode)
        except Exception as e:
            print(f"\n[ERROR] Falló ingesta de '{src}': {e}")
            errors.append((src, str(e)))

    print("\n" + "=" * 65)
    if errors:
        print(f"  COMPLETADO CON {len(errors)} ERROR(ES):")
        for src, err in errors:
            print(f"    - {src}: {err}")
    else:
        print(f"  COMPLETADO SIN ERRORES — {len(sources)} fuente(s) ingestadas")
    print(f"  Fin: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print("=" * 65)

    spark.stop()


if __name__ == "__main__":
    main()
