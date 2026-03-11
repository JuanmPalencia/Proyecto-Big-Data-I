"""
bronze_ingest.py — GTP (Green Turning Point)
Capa Bronze: ingesta de todos los CSVs del ETL hacia HDFS en formato Parquet.

Fuentes ingestadas:
  1. sentinel2.csv      → bronze_sentinel2_raw
  2. s5p.csv            → bronze_sentinel5p_raw
  3. hrl.csv            → bronze_hrl_raw
  4. finance_2006_2025.csv → bronze_finance_raw
  5. eurostat_PIB.csv   → bronze_eurostat_gdp_raw

Ejecución en Lorca:
  spark-submit --master yarn bronze_ingest.py [--mode overwrite|append] [--source all|s2|s5p|hrl|finance|eurostat]
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
    path = str(PROCESSED_DIR / "sentinel2.csv")

    # Columnas reales que produce Sentinel-2_extract.py (vía GEE reduceRegion)
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
        .option("mode", "DROPMALFORMED")
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
    path = str(PROCESSED_DIR / "s5p.csv")

    # Columnas reales que produce Sentinel-5p_extract.py (vía GEE reduceRegion)
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
        .option("mode", "DROPMALFORMED")
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
    path = str(PROCESSED_DIR / "hrl.csv")

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
    path = str(FINANCE_CSV)

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
    path = str(PROCESSED_DIR / "eurostat_PIB.csv")

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
# ORQUESTADOR
# ==============================================================================
INGESTORS = {
    "s2":       ingest_sentinel2,
    "s5p":      ingest_sentinel5p,
    "hrl":      ingest_hrl,
    "finance":  ingest_finance,
    "eurostat": ingest_eurostat_gdp,
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
    spark.sql("CREATE DATABASE IF NOT EXISTS gtp_bronze LOCATION 'hdfs:///user/gtp/bronze'")

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
