"""
ekc_regression.py — GTP (Green Turning Point) | Modelo 2: EKC Panel Regression
Estima los parámetros de la Curva de Kuznets Ambiental (EKC) por cluster.

Ecuación del modelo de panel con efectos fijos:
  ln(NDVI_it) = α + β₁·ln(GDP_it) + β₂·[ln(GDP_it)]² + μᵢ + λₜ + εᵢₜ

Donde:
  μᵢ = efectos fijos de ciudad (heterogeneidad no observada)
  λₜ = efectos fijos de tiempo (shocks comunes)
  β₁ > 0, β₂ < 0  →  curva EKC en U invertida (hipótesis confirmada)

Turning Point (renta en la que la degradación ambiental se revierte):
  Y* = exp(−β₁ / 2β₂)

Por qué pooled dentro del cluster:
  Cada ciudad individual tiene ~8 observaciones (2018–2026), insuficientes
  para estimar un modelo cuadrático con efectos fijos. Haciendo pool dentro
  del cluster (ej. 50 ciudades × 8 años = 400 obs), tenemos suficiente
  potencia estadística para identificar la forma de la curva.

Librerías:
  - linearmodels (PanelOLS) para efectos fijos de dos vías
  - pandas / numpy para la preparación de datos
  - Spark solo para leer/escribir (los datos caben en RAM por cluster)

Output:
  - Actualiza Gold ekc_parameters con β₁, β₂, Y*, R², AIC, etc.
  - Actualiza fact_kuznets con turning_point, ekc_shape, gdp_gap_to_turning

Ejecución:
  spark-submit --master yarn models/ekc_regression.py [--year YYYY]
"""

import sys
import json
import argparse
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, LongType
)

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HDFS_SILVER = "hdfs:///user/gtp/silver"
HDFS_GOLD   = "hdfs:///user/gtp/gold"

MODEL_VERSION   = "1.0"
MODEL_RUN_DATE  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
MODEL_TS        = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Mínimo de observaciones por cluster para estimar la regresión
MIN_OBS_PER_CLUSTER = 30

# ==============================================================================
# SPARK
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_EKC_Regression")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )

# ==============================================================================
# CARGA DE DATOS
# Lee fact_kuznets desde Gold (ya tiene cluster_id del paso anterior)
# ==============================================================================
def load_panel_data(spark):
    print("  Cargando datos de panel desde Gold fact_kuznets ...")

    fk = spark.read.parquet(f"{HDFS_GOLD}/fact_kuznets")

    # Columnas necesarias para EKC
    needed = ["city_code", "city_name", "country_code", "year",
              "ndvi_mean", "ln_gdp_pps", "ln_gdp_pps_sq",
              "gdp_pps_per_capita", "cluster_id"]

    existing = [c for c in needed if c in fk.columns]
    df = fk.select(existing).filter(
        F.col("ndvi_mean").isNotNull() &
        F.col("ln_gdp_pps").isNotNull() &
        F.col("cluster_id").isNotNull()
    )

    # Convertir a pandas (los datos por cluster caben en RAM: ~2000 filas máx)
    pdf = df.toPandas()
    print(f"  Panel: {len(pdf):,} observaciones ciudad×año")
    print(f"  Clusters disponibles: {sorted(pdf['cluster_id'].unique())}")
    return pdf


# ==============================================================================
# ESTIMACIÓN EKC POR CLUSTER
# Usa linearmodels.PanelOLS (efectos fijos de dos vías: ciudad + año)
# ==============================================================================
def estimate_ekc_for_cluster(pdf_cluster, cluster_id):
    """
    Estima el modelo de panel EKC para un cluster específico.
    Devuelve un dict con todos los parámetros estimados.
    """
    try:
        # linearmodels requiere MultiIndex (entity, time)
        pdf_cluster = pdf_cluster.copy()
        pdf_cluster["ln_ndvi"] = np.log(pdf_cluster["ndvi_mean"].clip(lower=1e-6))
        pdf_cluster = pdf_cluster.set_index(["city_code", "year"])

        # Verificar que tenemos suficientes observaciones
        n_obs     = len(pdf_cluster)
        n_cities  = pdf_cluster.index.get_level_values("city_code").nunique()
        n_years   = pdf_cluster.index.get_level_values("year").nunique()

        if n_obs < MIN_OBS_PER_CLUSTER:
            print(f"    [SKIP] Cluster {cluster_id}: solo {n_obs} obs (mínimo {MIN_OBS_PER_CLUSTER})")
            return None

        from linearmodels import PanelOLS
        import statsmodels.api as sm

        # Modelo de panel con efectos fijos de ciudad y tiempo
        # entity_effects=True → μᵢ (within transformation)
        # time_effects=True   → λₜ
        model = PanelOLS(
            dependent=pdf_cluster["ln_ndvi"],
            exog=sm.add_constant(pdf_cluster[["ln_gdp_pps", "ln_gdp_pps_sq"]]),
            entity_effects=True,
            time_effects=True,
            drop_absorbed=True,
        )
        result = model.fit(cov_type="clustered", cluster_entity=True)

        # Extraer parámetros
        params   = result.params
        stderr   = result.std_errors
        pvalues  = result.pvalues

        alpha = params.get("const",        None)
        b1    = params.get("ln_gdp_pps",   None)
        b2    = params.get("ln_gdp_pps_sq", None)

        b1_se = stderr.get("ln_gdp_pps",    None)
        b2_se = stderr.get("ln_gdp_pps_sq", None)
        b1_p  = pvalues.get("ln_gdp_pps",   None)
        b2_p  = pvalues.get("ln_gdp_pps_sq", None)

        # Calcular Turning Point: Y* = exp(−β₁ / 2β₂)
        turning_point_y = None
        turning_point_ln_y = None
        if b1 is not None and b2 is not None and b2 != 0:
            ln_y_star = -b1 / (2 * b2)
            turning_point_ln_y = ln_y_star
            turning_point_y    = np.exp(ln_y_star)

        # Bondad de ajuste
        r2     = result.rsquared
        r2_adj = result.rsquared_adj if hasattr(result, "rsquared_adj") else None
        f_stat = result.f_statistic.stat if hasattr(result, "f_statistic") else None
        f_pval = result.f_statistic.pval if hasattr(result, "f_statistic") else None

        # AIC / BIC de statsmodels (aproximación)
        aic = result.loglik * (-2) + 2 * len(params) if hasattr(result, "loglik") else None
        bic = result.loglik * (-2) + np.log(n_obs) * len(params) if hasattr(result, "loglik") else None

        # Clasificar forma de la curva
        sig5 = lambda p: p is not None and p < 0.05
        ekc_confirmed = (
            b1 is not None and b1 > 0 and sig5(b1_p) and
            b2 is not None and b2 < 0 and sig5(b2_p)
        )
        if ekc_confirmed:
            ekc_shape = "INVERTED_U"
        elif b1 is not None and b2 is not None:
            if b1 > 0 and b2 > 0:
                ekc_shape = "MONOTONIC_UP"
            elif b1 < 0:
                ekc_shape = "MONOTONIC_DOWN"
            else:
                ekc_shape = "UNCLEAR"
        else:
            ekc_shape = "UNCLEAR"

        city_codes = list(pdf_cluster.index.get_level_values("city_code").unique())
        countries  = list(pdf_cluster.reset_index()["country_code"].unique()) if "country_code" in pdf_cluster.columns else []

        return {
            "cluster_id":             int(cluster_id),
            "cluster_label":          None,   # se unirá desde fact_kuznets
            "estimation_year":        int(datetime.now().year),
            "n_cities":               int(n_cities),
            "n_observations":         int(n_obs),
            "city_codes":             json.dumps(city_codes),
            "countries_represented":  json.dumps(countries),
            "gdp_pps_mean":           float(pdf_cluster.reset_index()["gdp_pps_per_capita"].mean()) if "gdp_pps_per_capita" in pdf_cluster.columns else None,
            "gdp_pps_std":            float(pdf_cluster.reset_index()["gdp_pps_per_capita"].std())  if "gdp_pps_per_capita" in pdf_cluster.columns else None,
            "ndvi_mean":              float(pdf_cluster.reset_index()["ndvi_mean"].mean()),
            "alpha":                  float(alpha) if alpha is not None else None,
            "beta1":                  float(b1)    if b1    is not None else None,
            "beta2":                  float(b2)    if b2    is not None else None,
            "beta1_se":               float(b1_se) if b1_se is not None else None,
            "beta2_se":               float(b2_se) if b2_se is not None else None,
            "beta1_pvalue":           float(b1_p)  if b1_p  is not None else None,
            "beta2_pvalue":           float(b2_p)  if b2_p  is not None else None,
            "turning_point_y_star":   float(turning_point_y)    if turning_point_y    is not None else None,
            "turning_point_ln_y":     float(turning_point_ln_y) if turning_point_ln_y is not None else None,
            "r_squared":              float(r2)     if r2     is not None else None,
            "r_squared_adj":          float(r2_adj) if r2_adj is not None else None,
            "f_statistic":            float(f_stat) if f_stat is not None else None,
            "f_pvalue":               float(f_pval) if f_pval is not None else None,
            "aic":                    float(aic)    if aic    is not None else None,
            "bic":                    float(bic)    if bic    is not None else None,
            "ekc_hypothesis_supported": bool(ekc_confirmed),
            "ekc_shape":              ekc_shape,
            "_estimation_timestamp":  MODEL_TS,
            "_model_version":         MODEL_VERSION,
        }

    except ImportError:
        print("  [ERROR] linearmodels no instalado. Instala: pip install linearmodels")
        return None
    except Exception as e:
        print(f"    [ERROR] Cluster {cluster_id}: {e}")
        return None


# ==============================================================================
# ACTUALIZAR GOLD CON LOS PARÁMETROS EKC
# ==============================================================================
def update_gold(spark, ekc_results: list):
    print("\n  Escribiendo resultados EKC en Gold ...")

    # --- Escribir ekc_parameters ---
    if ekc_results:
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

        rows = [tuple(r[f.name] for f in schema.fields) for r in ekc_results]
        df_params = spark.createDataFrame(rows, schema)
        df_params.write.mode("overwrite").parquet(f"{HDFS_GOLD}/ekc_parameters")
        print(f"  [GOLD] ekc_parameters: {len(ekc_results)} clusters escritos")

    # --- Actualizar fact_kuznets con parámetros EKC por cluster ---
    fk = spark.read.parquet(f"{HDFS_GOLD}/fact_kuznets")

    ekc_lookup = spark.createDataFrame(
        [(r["cluster_id"], r.get("beta1"), r.get("beta2"), r.get("alpha"),
          r.get("turning_point_y_star"), r.get("r_squared"),
          r.get("beta1_pvalue"), r.get("beta2_pvalue"), r.get("ekc_shape"))
         for r in ekc_results],
        StructType([
            StructField("cl_id",           IntegerType(), True),
            StructField("ekc_beta1",       DoubleType(),  True),
            StructField("ekc_beta2",       DoubleType(),  True),
            StructField("ekc_alpha",       DoubleType(),  True),
            StructField("ekc_tp",          DoubleType(),  True),
            StructField("ekc_r2",          DoubleType(),  True),
            StructField("ekc_p_beta1",     DoubleType(),  True),
            StructField("ekc_p_beta2",     DoubleType(),  True),
            StructField("ekc_shape_val",   StringType(),  True),
        ])
    )

    # Drop columnas EKC anteriores
    drop_cols = ["ekc_beta1","ekc_beta2","ekc_alpha","ekc_turning_point_y",
                 "ekc_r_squared","ekc_p_value_beta1","ekc_p_value_beta2","ekc_shape"]
    fk_clean = fk.drop(*[c for c in drop_cols if c in fk.columns])

    fk_updated = fk_clean.join(
        ekc_lookup,
        fk_clean["cluster_id"] == ekc_lookup["cl_id"],
        how="left"
    ).drop("cl_id")

    # Renombrar a nombres del DDL
    rename = {
        "ekc_tp":       "ekc_turning_point_y",
        "ekc_r2":       "ekc_r_squared",
        "ekc_p_beta1":  "ekc_p_value_beta1",
        "ekc_p_beta2":  "ekc_p_value_beta2",
        "ekc_shape_val":"ekc_shape",
    }
    for old, new in rename.items():
        if old in fk_updated.columns:
            fk_updated = fk_updated.withColumnRenamed(old, new)

    # GDP gap al turning point: GDP_actual − Y*
    if "gdp_pps_per_capita" in fk_updated.columns and "ekc_turning_point_y" in fk_updated.columns:
        fk_updated = fk_updated.withColumn(
            "gdp_gap_to_turning",
            F.col("gdp_pps_per_capita") - F.col("ekc_turning_point_y")
        )

    (
        fk_updated.write
        .mode("overwrite")
        .partitionBy("year", "country")
        .parquet(f"{HDFS_GOLD}/fact_kuznets")
    )
    print(f"  [GOLD] fact_kuznets actualizado con parámetros EKC")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP EKC Panel Regression")
    parser.add_argument("--clusters", nargs="+", type=int, default=None,
                        help="IDs de clusters a procesar (default: todos)")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — MODELO 2: EKC PANEL REGRESSION")
    print(f"  Modelo:  ln(NDVI) = α + β₁·ln(GDP) + β₂·[ln(GDP)]² + FE")
    print(f"  Turning: Y* = exp(−β₁ / 2β₂)")
    print("=" * 65)

    spark = get_spark()

    # 1. Cargar datos de panel
    pdf = load_panel_data(spark)

    # 2. Estimar por cluster
    clusters_to_run = args.clusters or sorted(pdf["cluster_id"].dropna().unique().astype(int))
    ekc_results = []

    print(f"\n  Estimando EKC para {len(clusters_to_run)} cluster(s) ...")
    for cluster_id in clusters_to_run:
        pdf_cluster = pdf[pdf["cluster_id"] == cluster_id].copy()
        print(f"\n  --- Cluster {cluster_id}: {len(pdf_cluster)} obs, {pdf_cluster['city_code'].nunique()} ciudades ---")
        result = estimate_ekc_for_cluster(pdf_cluster, cluster_id)
        if result:
            ekc_results.append(result)
            tp = result.get("turning_point_y_star")
            b1 = result.get("beta1")
            b2 = result.get("beta2")
            r2 = result.get("r_squared")
            print(f"    β₁={b1:.4f}  β₂={b2:.4f}  Y*={tp:.0f} PPS  R²={r2:.3f}  Forma={result['ekc_shape']}")

    # 3. Actualizar Gold
    if ekc_results:
        update_gold(spark, ekc_results)
    else:
        print("\n[WARN] No se generaron resultados EKC.")

    print("\n" + "=" * 65)
    print(f"  EKC REGRESSION COMPLETADA | {len(ekc_results)}/{len(clusters_to_run)} clusters estimados")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    main()
