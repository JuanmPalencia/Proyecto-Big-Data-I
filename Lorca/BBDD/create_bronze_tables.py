"""
create_bronze_tables.py — GTP
Crea las tablas Hive Bronze que faltan (tablas 13-19).
Ejecutar una sola vez cuando se añaden nuevas fuentes al pipeline.

Uso:
  spark-submit create_bronze_tables.py
"""
from pyspark.sql import SparkSession

HDFS_BRONZE = "hdfs:///user/gtp/bronze"

spark = (
    SparkSession.builder
    .appName("GTP_CreateBronzeTables")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql(f"CREATE DATABASE IF NOT EXISTS gtp_bronze LOCATION '{HDFS_BRONZE}'")

TABLES = [
    """CREATE TABLE IF NOT EXISTS gtp_bronze.bronze_modis_ndvi_raw (
        city STRING, month INT,
        ndvi_mean DOUBLE, ndvi_std DOUBLE,
        _ingestion_date STRING, _source_system STRING, _file_name STRING
    ) PARTITIONED BY (year INT, country STRING)
    STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/modis_ndvi_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')""",

    """CREATE TABLE IF NOT EXISTS gtp_bronze.bronze_modis_lst_raw (
        city STRING, month INT,
        lst_day_c DOUBLE, lst_night_c DOUBLE,
        _ingestion_date STRING, _source_system STRING, _file_name STRING
    ) PARTITIONED BY (year INT, country STRING)
    STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/modis_lst_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')""",

    """CREATE TABLE IF NOT EXISTS gtp_bronze.bronze_ntl_raw (
        city STRING, month INT,
        ntl_mean DOUBLE, ntl_std DOUBLE, ntl_source STRING,
        _ingestion_date STRING, _source_system STRING, _file_name STRING
    ) PARTITIONED BY (year INT, country STRING)
    STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/ntl_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')""",

    """CREATE TABLE IF NOT EXISTS gtp_bronze.bronze_renewables_raw (
        country STRING, month INT,
        renewables_pct DOUBLE, renewables_source STRING,
        _ingestion_date STRING, _source_system STRING, _file_name STRING
    ) PARTITIONED BY (year INT)
    STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/renewables_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')""",

    """CREATE TABLE IF NOT EXISTS gtp_bronze.bronze_tourism_raw (
        city STRING, month INT,
        tourism_nights DOUBLE, tourism_source STRING,
        _ingestion_date STRING, _source_system STRING, _file_name STRING
    ) PARTITIONED BY (year INT, country STRING)
    STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/tourism_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')""",

    """CREATE TABLE IF NOT EXISTS gtp_bronze.bronze_ets_raw (
        month INT,
        ets_price_eur DOUBLE, ets_source STRING,
        _ingestion_date STRING, _source_system STRING, _file_name STRING
    ) PARTITIONED BY (year INT)
    STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/ets_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')""",

    """CREATE TABLE IF NOT EXISTS gtp_bronze.bronze_eea_aq_raw (
        city STRING, month INT,
        pm25_mean DOUBLE, pm10_mean DOUBLE, aq_source STRING,
        _ingestion_date STRING, _source_system STRING, _file_name STRING
    ) PARTITIONED BY (year INT, country STRING)
    STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/eea_aq_raw'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')""",
]

REPAIR = [
    "gtp_bronze.bronze_modis_ndvi_raw",
    "gtp_bronze.bronze_modis_lst_raw",
    "gtp_bronze.bronze_ntl_raw",
    "gtp_bronze.bronze_renewables_raw",
    "gtp_bronze.bronze_tourism_raw",
    "gtp_bronze.bronze_ets_raw",
    "gtp_bronze.bronze_eea_aq_raw",
]

for ddl in TABLES:
    table = ddl.split("gtp_bronze.")[1].split(" ")[0]
    try:
        spark.sql(ddl)
        print(f"  [OK] Tabla creada/verificada: {table}")
    except Exception as e:
        print(f"  [ERROR] {table}: {e}")

for tbl in REPAIR:
    try:
        spark.sql(f"MSCK REPAIR TABLE {tbl}")
        print(f"  [OK] Particiones registradas: {tbl}")
    except Exception as e:
        print(f"  [WARN] MSCK REPAIR {tbl}: {e}")

print("\n[DONE] Tablas Bronze nuevas listas.")
spark.stop()
