"""
gold_build.py — GTP (Green Turning Point)
Capa Gold: construye tablas analíticas finales desde Silver.

Tablas generadas:
  fact_kuznets    — ciudad × año con todos los indicadores + turning point
  city_ranking    — ranking europeo por investment_score, snapshot anual
  ekc_parameters  — parámetros EKC por cluster (placeholder hasta que corran los modelos)
  model_results   — outputs ML unificados (placeholder inicial)

Nota: ekc_parameters y model_results se rellenan con valores reales
      DESPUÉS de ejecutar los scripts de modelos ML.

Ejecución en Lorca:
  spark-submit --master yarn gold_build.py
"""

import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, BooleanType
)

# ==============================================================================
# RUTAS HDFS
# ==============================================================================
HDFS_SILVER = "hdfs:///user/gtp/silver"
HDFS_GOLD   = "hdfs:///user/gtp/gold"

GOLD_LOAD_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ==============================================================================
# SPARK SESSION
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_Gold_Build")
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

def read_silver(spark, table_name: str):
    path = f"{HDFS_SILVER}/{table_name}"
    try:
        return spark.read.parquet(path)
    except Exception as e:
        print(f"  [WARN] No se pudo leer silver/{table_name}: {e}")
        return None


def write_gold(df, table_name: str, partition_cols: list, mode: str = "overwrite"):
    path = f"{HDFS_GOLD}/{table_name}"
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    print(f"  [GOLD] {table_name}: {df.count():,} filas → {path}")


def repair_table(spark, table: str):
    try:
        spark.sql(f"MSCK REPAIR TABLE gtp_gold.{table}")
    except Exception as e:
        print(f"  [WARN] MSCK REPAIR gtp_gold.{table}: {e}")

# ==============================================================================
# TABLA 1: FACT_KUZNETS — Tabla central EKC
# Join: fact_environmental + fact_economic + fact_financial (por país) + dim_city
# ==============================================================================
def build_fact_kuznets(spark):
    print("\n[GOLD 1/4] Construyendo fact_kuznets ...")

    env  = read_silver(spark, "fact_environmental")
    eco  = read_silver(spark, "fact_economic")
    fin  = read_silver(spark, "fact_financial")
    city = read_silver(spark, "dim_city")

    if env is None or city is None:
        print("  [ERROR] fact_environmental o dim_city no disponibles. Abortando fact_kuznets.")
        return None

    # --- Base: ambiental + city metadata ---
    fk = env.join(
        city.select("city_sk", "city_code", "city_name", "country_code", "country_name"),
        on="city_sk", how="left"
    )

    # --- Unir económico ---
    if eco is not None:
        eco_sel = eco.select(
            "city_sk", "year",
            "gdp_pps_per_capita", "gdp_eur_per_capita",
            "ln_gdp_pps", "ln_gdp_pps_sq", "gdp_growth_rate",
            "fua_population"
        )
        fk = fk.join(eco_sel, on=["city_sk", "year"], how="left")
    else:
        fk = (fk
              .withColumn("gdp_pps_per_capita", F.lit(None).cast(DoubleType()))
              .withColumn("gdp_eur_per_capita", F.lit(None).cast(DoubleType()))
              .withColumn("ln_gdp_pps",         F.lit(None).cast(DoubleType()))
              .withColumn("ln_gdp_pps_sq",      F.lit(None).cast(DoubleType()))
              .withColumn("gdp_growth_rate",    F.lit(None).cast(DoubleType()))
              .withColumn("fua_population",     F.lit(None).cast(LongType())))

    # --- Unir financiero: agregar mensual → anual por país × año ---
    # monthly_volatility = std_diaria * sqrt(21); anualizar: avg_mensual * sqrt(12)
    if fin is not None:
        fin_agg = (
            fin
            .groupBy("country_code", "year")
            .agg(
                F.collect_list("close_price_avg").alias("fin_prices_raw"),
                F.avg("close_price_avg").alias("fin_close_price_avg"),
                # Anualización estándar: volatilidad mensual × sqrt(12)
                (F.avg("monthly_volatility") * F.lit(3.4641)).alias("fin_annual_volatility"),
            )
        )
        # Unir por country_code y year
        fk = fk.join(
            fin_agg,
            on=[fk["country_code"] == fin_agg["country_code"], fk["year"] == fin_agg["year"]],
            how="left"
        ).drop(fin_agg["country_code"]).drop(fin_agg["year"]).drop("fin_prices_raw")
    else:
        fk = (fk
              .withColumn("fin_close_price_avg",   F.lit(None).cast(DoubleType()))
              .withColumn("fin_annual_volatility", F.lit(None).cast(DoubleType())))

    # Unir tickers por país-año desde Bronze finance (JSON array)
    try:
        bronze_fin = spark.read.parquet("hdfs:///user/gtp/bronze/finance_raw")
        tickers_agg = (
            bronze_fin
            .groupBy("fua_country_code", "year")
            .agg(
                F.to_json(F.collect_set("ticker")).alias("fin_tickers"),
                F.to_json(F.collect_set("company_name")).alias("fin_companies"),
            )
        )
        fk = fk.join(
            tickers_agg,
            on=[fk["country_code"] == tickers_agg["fua_country_code"], fk["year"] == tickers_agg["year"]],
            how="left"
        ).drop(tickers_agg["fua_country_code"]).drop(tickers_agg["year"])
    except Exception:
        fk = (fk
              .withColumn("fin_tickers",   F.lit(None).cast(StringType()))
              .withColumn("fin_companies", F.lit(None).cast(StringType())))

    # --- Columnas de modelos ML (placeholders hasta que corran los modelos) ---
    fk = (fk
          .withColumn("cluster_id",               F.lit(None).cast(IntegerType()))
          .withColumn("cluster_label",             F.lit(None).cast(StringType()))
          .withColumn("cluster_silhouette",        F.lit(None).cast(DoubleType()))
          .withColumn("distance_to_centroid",      F.lit(None).cast(DoubleType()))
          .withColumn("ekc_beta1",                 F.lit(None).cast(DoubleType()))
          .withColumn("ekc_beta2",                 F.lit(None).cast(DoubleType()))
          .withColumn("ekc_alpha",                 F.lit(None).cast(DoubleType()))
          .withColumn("ekc_turning_point_y",       F.lit(None).cast(DoubleType()))
          .withColumn("ekc_r_squared",             F.lit(None).cast(DoubleType()))
          .withColumn("ekc_p_value_beta1",         F.lit(None).cast(DoubleType()))
          .withColumn("ekc_p_value_beta2",         F.lit(None).cast(DoubleType()))
          .withColumn("ekc_shape",                 F.lit("PENDING").cast(StringType()))
          .withColumn("turning_point_phase",       F.lit("PENDING").cast(StringType()))
          .withColumn("phase_confidence",          F.lit(None).cast(DoubleType()))
          .withColumn("gdp_gap_to_turning",        F.lit(None).cast(DoubleType()))
          .withColumn("prophet_ndvi_forecast_1y",  F.lit(None).cast(DoubleType()))
          .withColumn("prophet_ndvi_forecast_3y",  F.lit(None).cast(DoubleType()))
          .withColumn("prophet_ndvi_forecast_5y",  F.lit(None).cast(DoubleType()))
          .withColumn("prophet_turning_year",      F.lit(None).cast(IntegerType()))
          .withColumn("prophet_forecast_lower_95", F.lit(None).cast(DoubleType()))
          .withColumn("prophet_forecast_upper_95", F.lit(None).cast(DoubleType()))
          )

    # --- Investment Score provisional (basado solo en green_index hasta tener modelos) ---
    # Formula provisional: score = green_index * 100
    # Se refinará en gold_build.py tras ejecutar los modelos ML.
    fk = fk.withColumn(
        "investment_score",
        F.round(F.coalesce(F.col("green_index"), F.lit(0.0)) * 100.0, 2)
    )
    fk = fk.withColumn(
        "investment_recommendation",
        F.when(F.col("investment_score") >= 70, "BUY")
         .when(F.col("investment_score") >= 50, "HOLD")
         .otherwise("AVOID")
    )

    fk = fk.withColumn("_gold_load_date", F.lit(GOLD_LOAD_DATE))

    # Renombrar columnas ambientales para alinear con el DDL de Gold
    rename_map = {
        "ndvi_annual_mean":    "ndvi_mean",
        "ndvi_trend_slope":    "ndvi_trend_slope",
        "no2_annual_mean":     "no2_mean",
    }
    for old, new in rename_map.items():
        if old in fk.columns and old != new:
            fk = fk.withColumnRenamed(old, new)

    # Asegurar columna country para partición
    if "country" not in fk.columns:
        fk = fk.withColumn("country", F.col("country_code"))

    write_gold(fk, "fact_kuznets", ["year", "country"])
    repair_table(spark, "fact_kuznets")
    return fk


# ==============================================================================
# TABLA 2: CITY_RANKING — Ranking europeo por año
# ==============================================================================
def build_city_ranking(spark, fact_kuznets=None):
    print("\n[GOLD 2/4] Construyendo city_ranking ...")

    if fact_kuznets is None:
        fact_kuznets = spark.read.parquet(f"{HDFS_GOLD}/fact_kuznets")

    # Ranking dentro del año (global y por país)
    w_global  = Window.partitionBy("year").orderBy(F.desc("investment_score"))
    w_country = Window.partitionBy("year", "country_code").orderBy(F.desc("investment_score"))

    ranking = (
        fact_kuznets
        .withColumn("rank_position",         F.rank().over(w_global))
        .withColumn("rank_within_country",   F.rank().over(w_country))
    )

    # Cambio de ranking YoY
    w_city_time = Window.partitionBy("city_code").orderBy("year")
    ranking = (
        ranking
        .withColumn("rank_change",  F.col("rank_position")   - F.lag("rank_position").over(w_city_time))
        .withColumn("score_change", F.col("investment_score") - F.lag("investment_score").over(w_city_time))
    )

    # Top ticker del país (primer ticker de la lista JSON — simplificación)
    ranking = (
        ranking
        .withColumn("top_ticker",  F.get_json_object(F.col("fin_tickers"),  "$[0]"))
        .withColumn("top_company", F.get_json_object(F.col("fin_companies"), "$[0]"))
    )

    final = ranking.select(
        "rank_position", "rank_within_country",
        "city_code", "city_name", "country_code", "country_name",
        "investment_score", "investment_recommendation",
        "green_index", "turning_point_phase",
        "prophet_turning_year",
        "rank_change", "score_change",
        "top_ticker", "top_company",
        F.lit(GOLD_LOAD_DATE).alias("_gold_load_date"),
        "year"
    )

    write_gold(final, "city_ranking", ["year"])
    repair_table(spark, "city_ranking")


# ==============================================================================
# TABLA 3: EKC_PARAMETERS — Placeholder (se rellena con ekc_regression.py)
# ==============================================================================
def build_ekc_parameters_placeholder(spark):
    print("\n[GOLD 3/4] Creando ekc_parameters (placeholder — se rellenará tras modelos ML) ...")

    schema = StructType([
        StructField("cluster_id",               IntegerType(), True),
        StructField("cluster_label",            StringType(),  True),
        StructField("estimation_year",          IntegerType(), True),
        StructField("n_cities",                 IntegerType(), True),
        StructField("n_observations",           IntegerType(), True),
        StructField("city_codes",               StringType(),  True),
        StructField("countries_represented",    StringType(),  True),
        StructField("gdp_pps_mean",             DoubleType(),  True),
        StructField("gdp_pps_std",              DoubleType(),  True),
        StructField("ndvi_mean",                DoubleType(),  True),
        StructField("alpha",                    DoubleType(),  True),
        StructField("beta1",                    DoubleType(),  True),
        StructField("beta2",                    DoubleType(),  True),
        StructField("beta1_se",                 DoubleType(),  True),
        StructField("beta2_se",                 DoubleType(),  True),
        StructField("beta1_pvalue",             DoubleType(),  True),
        StructField("beta2_pvalue",             DoubleType(),  True),
        StructField("turning_point_y_star",     DoubleType(),  True),
        StructField("turning_point_ln_y",       DoubleType(),  True),
        StructField("r_squared",                DoubleType(),  True),
        StructField("r_squared_adj",            DoubleType(),  True),
        StructField("f_statistic",              DoubleType(),  True),
        StructField("f_pvalue",                 DoubleType(),  True),
        StructField("aic",                      DoubleType(),  True),
        StructField("bic",                      DoubleType(),  True),
        StructField("ekc_hypothesis_supported", BooleanType(), True),
        StructField("ekc_shape",                StringType(),  True),
        StructField("_estimation_timestamp",    StringType(),  True),
        StructField("_model_version",           StringType(),  True),
    ])

    # DataFrame vacío — se rellenará tras ejecutar ekc_regression.py
    df = spark.createDataFrame([], schema)
    df.write.mode("overwrite").parquet(f"{HDFS_GOLD}/ekc_parameters")
    print(f"  [GOLD] ekc_parameters: placeholder vacío creado (pendiente modelos ML)")
    repair_table(spark, "ekc_parameters")


# ==============================================================================
# TABLA 4: MODEL_RESULTS — Placeholder (se rellena con los scripts de modelos)
# ==============================================================================
def build_model_results_placeholder(spark):
    print("\n[GOLD 4/4] Creando model_results (placeholder — se rellenará tras modelos ML) ...")

    schema = StructType([
        StructField("city_code",                StringType(),  True),
        StructField("city_name",                StringType(),  True),
        StructField("country_code",             StringType(),  True),
        StructField("kmeans_cluster_id",        IntegerType(), True),
        StructField("kmeans_cluster_label",     StringType(),  True),
        StructField("kmeans_silhouette",        DoubleType(),  True),
        StructField("kmeans_distance_centroid", DoubleType(),  True),
        StructField("kmeans_k_selected",        IntegerType(), True),
        StructField("ekc_cluster_id",           IntegerType(), True),
        StructField("ekc_fitted_ndvi",          DoubleType(),  True),
        StructField("ekc_residual",             DoubleType(),  True),
        StructField("ekc_turning_point_y",      DoubleType(),  True),
        StructField("ekc_distance_to_turning",  DoubleType(),  True),
        StructField("xgb_predicted_phase",      StringType(),  True),
        StructField("xgb_prob_degradando",      DoubleType(),  True),
        StructField("xgb_prob_turning",         DoubleType(),  True),
        StructField("xgb_prob_recuperando",     DoubleType(),  True),
        StructField("xgb_confidence",           DoubleType(),  True),
        StructField("xgb_feature_importance",   StringType(),  True),
        StructField("prophet_ndvi_actual",      DoubleType(),  True),
        StructField("prophet_ndvi_fitted",      DoubleType(),  True),
        StructField("prophet_trend",            DoubleType(),  True),
        StructField("prophet_seasonality",      DoubleType(),  True),
        StructField("prophet_forecast_1y",      DoubleType(),  True),
        StructField("prophet_forecast_3y",      DoubleType(),  True),
        StructField("prophet_forecast_5y",      DoubleType(),  True),
        StructField("prophet_lower_95_5y",      DoubleType(),  True),
        StructField("prophet_upper_95_5y",      DoubleType(),  True),
        StructField("prophet_turning_year",     IntegerType(), True),
        StructField("prophet_mape",             DoubleType(),  True),
        StructField("_model_run_date",          StringType(),  True),
        StructField("_pipeline_version",        StringType(),  True),
        StructField("year",                     IntegerType(), True),
        StructField("country",                  StringType(),  True),
    ])

    df = spark.createDataFrame([], schema)
    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "country")
        .parquet(f"{HDFS_GOLD}/model_results")
    )
    print(f"  [GOLD] model_results: placeholder vacío creado (pendiente modelos ML)")
    repair_table(spark, "model_results")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP Gold Build")
    parser.add_argument("--table", default="all",
                        choices=["all", "fact_kuznets", "city_ranking", "ekc_parameters", "model_results"],
                        help="Tabla específica a construir (default: all)")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — GOLD BUILD")
    print(f"  Inicio: {GOLD_LOAD_DATE}")
    print(f"  Silver: {HDFS_SILVER}")
    print(f"  Gold:   {HDFS_GOLD}")
    print("=" * 65)

    spark = get_spark()
    spark.sql("CREATE DATABASE IF NOT EXISTS gtp_gold LOCATION 'hdfs:///user/gtp/gold'")

    fk = None

    if args.table in ("all", "fact_kuznets"):
        fk = build_fact_kuznets(spark)

    if args.table in ("all", "city_ranking"):
        build_city_ranking(spark, fk)

    if args.table in ("all", "ekc_parameters"):
        build_ekc_parameters_placeholder(spark)

    if args.table in ("all", "model_results"):
        build_model_results_placeholder(spark)

    print("\n" + "=" * 65)
    print("  GOLD BUILD COMPLETADO")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    main()
