"""
clustering.py — GTP (Green Turning Point) | Modelo 1: K-Means Clustering
Segmenta las ciudades europeas en grupos homogéneos antes de la regresión EKC.

Por qué clustering primero:
  Las ciudades de Europa tienen perfiles muy distintos (Noruega vs Rumanía).
  Hacer una sola regresión EKC para todas mezclaría dinámicas incompatibles.
  Agrupando primero, cada cluster tiene ciudades con patrones similares y
  la regresión posterior tiene más sentido estadístico.

Algoritmo:
  - K-Means PySpark MLlib (escalable a 237+ ciudades × 8+ años)
  - Selección automática de K por índice Silhouette (rango K=2..8)
  - Features: ndvi_annual_mean, ndvi_trend_slope, no2_annual_mean,
              imperviousness_mean, ln_gdp_pps (del año más reciente disponible)
  - Normalización: StandardScaler antes de K-Means

Output:
  - Actualiza fact_kuznets en Gold con cluster_id, cluster_label, silhouette
  - Guarda modelo serializado en HDFS para reutilización

Ejecución:
  spark-submit --master yarn models/clustering.py [--k-min 2] [--k-max 8]
"""

import sys
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HDFS_SILVER  = "hdfs:///user/gtp/silver"
HDFS_GOLD    = "hdfs:///user/gtp/gold"
HDFS_MODELS  = "hdfs:///user/gtp/models/clustering"

MODEL_VERSION   = "1.0"
MODEL_RUN_DATE  = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Features para clustering (todas numéricas, escaladas)
CLUSTER_FEATURES = [
    "ndvi_mean_city",          # NDVI medio histórico de la ciudad
    "ndvi_slope_city",         # Tendencia NDVI (positivo = ciudad reverdeciendo)
    "no2_mean_city",           # NO2 medio histórico
    "imperviousness_city",     # Impermeabilización media
    "ln_gdp_mean_city",        # ln(GDP per cápita) medio
    "gdp_growth_mean_city",    # Crecimiento económico medio
]

# Etiquetas interpretables por cluster (asignadas post-hoc según los centroides)
# Serán sobreescritas tras inspección manual de los resultados.
CLUSTER_LABEL_FALLBACK = "Cluster_{id}"

# ==============================================================================
# SPARK
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_Clustering")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )

# ==============================================================================
# PREPARACIÓN DE FEATURES
# Agregamos a nivel ciudad (promedio histórico) para capturar el perfil
# estructural de cada ciudad, no la variación anual.
# ==============================================================================
def build_feature_matrix(spark):
    print("  Cargando Silver fact_environmental y fact_economic ...")

    try:
        env = spark.read.parquet(f"{HDFS_SILVER}/fact_environmental")
        eco = spark.read.parquet(f"{HDFS_SILVER}/fact_economic")
        city = spark.read.parquet(f"{HDFS_SILVER}/dim_city")
    except Exception as e:
        print(f"[SKIP] Silver no disponible para clustering: {e}")
        return None

    # Agregar ambiental por ciudad (promedio histórico)
    env_agg = (
        env
        .groupBy("city_sk")
        .agg(
            F.avg("ndvi_annual_mean").alias("ndvi_mean_city"),
            F.avg("ndvi_trend_slope").alias("ndvi_slope_city"),
            F.avg("no2_annual_mean").alias("no2_mean_city"),
            F.avg("imperviousness_mean").alias("imperviousness_city"),
            F.count("*").alias("n_years_env"),
        )
    )

    # Agregar económico por ciudad
    eco_agg = (
        eco
        .groupBy("city_sk")
        .agg(
            F.avg("ln_gdp_pps").alias("ln_gdp_mean_city"),
            F.avg("gdp_growth_rate").alias("gdp_growth_mean_city"),
        )
    )

    # Join ciudad + ambiental + económico
    features = (
        city.select("city_sk", "city_code", "city_name", "country_code")
        .join(env_agg, on="city_sk", how="left")
        .join(eco_agg, on="city_sk", how="left")
    )

    # Rellenar nulos con mediana (KMeans no tolera NaN)
    for col_name in CLUSTER_FEATURES:
        if col_name in features.columns:
            median_val = features.approxQuantile(col_name, [0.5], 0.01)[0]
            features = features.fillna({col_name: median_val})
        else:
            features = features.withColumn(col_name, F.lit(0.0).cast(DoubleType()))

    n = features.count()
    print(f"  Feature matrix: {n} ciudades × {len(CLUSTER_FEATURES)} features")
    return features


# ==============================================================================
# SELECCIÓN DE K POR SILHOUETTE
# ==============================================================================
def select_best_k(df_scaled, k_min: int, k_max: int, seed: int = 42):
    print(f"\n  Evaluando K-Means para K={k_min}..{k_max} (criterio: Silhouette) ...")

    evaluator = ClusteringEvaluator(
        featuresCol="scaled_features",
        predictionCol="cluster_id",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )

    results = []
    for k in range(k_min, k_max + 1):
        km = KMeans(
            featuresCol="scaled_features",
            predictionCol="cluster_id",
            k=k,
            seed=seed,
            maxIter=50,
            tol=1e-4
        )
        model = km.fit(df_scaled)
        preds = model.transform(df_scaled)
        sil   = evaluator.evaluate(preds)
        results.append((k, sil, model))
        print(f"    K={k:2d}  Silhouette={sil:.4f}")

    # El mejor K es el que maximiza el Silhouette
    best = max(results, key=lambda x: x[1])
    print(f"\n  → K óptimo: K={best[0]}  (Silhouette={best[1]:.4f})")
    return best  # (k, silhouette, model)


# ==============================================================================
# ASIGNACIÓN DE ETIQUETAS POR CLUSTER
# Analiza los centroides para asignar nombres interpretables.
# Criterios:
#   - High GDP + High NDVI slope → "High-Income-Greening"
#   - High GDP + Low NDVI slope  → "High-Income-Stable"
#   - Low GDP + High NO2         → "Industrializing"
#   - Low GDP + Low NO2          → "Developing-Green"
# ==============================================================================
def label_clusters(model, assembler_cols):
    centers = model.clusterCenters()
    labels = []

    # Índices de las features en el vector
    idx = {name: i for i, name in enumerate(assembler_cols)}

    for cluster_id, center in enumerate(centers):
        ndvi_slope = center[idx.get("ndvi_slope_city", 0)]
        ln_gdp     = center[idx.get("ln_gdp_mean_city", 0)]
        no2        = center[idx.get("no2_mean_city", 0)]
        imperv     = center[idx.get("imperviousness_city", 0)]

        # Heurística simple de etiquetado
        gdp_high  = ln_gdp  > 0      # Después de escalar, > 0 = sobre la media
        green_trend = ndvi_slope > 0
        polluted   = no2 > 0
        built_up   = imperv > 0

        if gdp_high and green_trend:
            label = "High-Income-Greening"
        elif gdp_high and not green_trend and polluted:
            label = "High-Income-Stagnant"
        elif gdp_high and not green_trend and not polluted:
            label = "High-Income-Stable"
        elif not gdp_high and polluted:
            label = "Industrializing-Polluted"
        elif not gdp_high and green_trend:
            label = "Emerging-Improving"
        else:
            label = "Developing-Mixed"

        labels.append((cluster_id, label))

    return labels  # lista de (cluster_id, label)


# ==============================================================================
# ACTUALIZAR GOLD
# Escribe los resultados de clustering en fact_kuznets y model_results
# ==============================================================================
def update_gold_with_clusters(spark, predictions_df, labels: list, best_k: int, best_sil: float):
    print("\n  Actualizando Gold con asignaciones de cluster ...")

    # Crear lookup label
    spark_labels = spark.createDataFrame(
        [(c_id, label) for c_id, label in labels],
        ["cluster_id_ref", "cluster_label"]
    )

    preds_with_label = predictions_df.join(
        spark_labels,
        predictions_df["cluster_id"] == spark_labels["cluster_id_ref"],
        how="left"
    ).drop("cluster_id_ref")

    # --- Actualizar fact_kuznets ---
    try:
        fk = spark.read.parquet(f"{HDFS_GOLD}/fact_kuznets")
    except Exception as e:
        print(f"[SKIP] fact_kuznets no disponible en Gold (clustering): {e}")
        return

    # Agregar silhouette y distancia al centroide por ciudad
    evaluator = ClusteringEvaluator(
        featuresCol="scaled_features",
        predictionCol="cluster_id",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )

    cluster_info = preds_with_label.select(
        "city_code", "cluster_id", "cluster_label",
        F.lit(best_sil).cast(DoubleType()).alias("cluster_silhouette"),
        F.lit(best_k).cast(IntegerType()).alias("kmeans_k_selected"),
    )

    # Drop columnas de cluster anteriores y reemplazar
    cols_to_drop = ["cluster_id", "cluster_label", "cluster_silhouette"]
    fk_clean = fk.drop(*[c for c in cols_to_drop if c in fk.columns])

    fk_updated = fk_clean.join(
        cluster_info.select("city_code", "cluster_id", "cluster_label", "cluster_silhouette"),
        on="city_code",
        how="left"
    )

    # Materializar antes del overwrite para evitar FileNotFoundException por self-overwrite
    fk_updated = fk_updated.cache()
    fk_updated.count()

    # Re-escribir fact_kuznets con clusters
    (
        fk_updated.write
        .mode("overwrite")
        .partitionBy("year", "country")
        .parquet(f"{HDFS_GOLD}/fact_kuznets")
    )
    fk_updated.unpersist()
    print(f"  [GOLD] fact_kuznets actualizado con cluster_id y cluster_label")

    # --- Actualizar / crear model_results ---
    try:
        mr = spark.read.parquet(f"{HDFS_GOLD}/model_results")
    except Exception:
        mr = None

    new_mr_rows = (
        preds_with_label
        .select("city_code", "cluster_id", "cluster_label", "cluster_silhouette", "kmeans_k_selected")
        .withColumnRenamed("cluster_id",        "kmeans_cluster_id")
        .withColumnRenamed("cluster_label",     "kmeans_cluster_label")
        .withColumnRenamed("cluster_silhouette","kmeans_silhouette")
    )

    if mr is not None and mr.count() > 0:
        # Merge: actualizar columnas de kmeans en model_results existente
        mr_clean = mr.drop("kmeans_cluster_id", "kmeans_cluster_label", "kmeans_silhouette", "kmeans_k_selected")
        mr_updated = mr_clean.join(new_mr_rows, on="city_code", how="left")
    else:
        mr_updated = new_mr_rows

    writer = mr_updated.write.mode("overwrite")
    if "year" in mr_updated.columns and "country" in mr_updated.columns:
        writer = writer.partitionBy("year", "country")
    writer.parquet(f"{HDFS_GOLD}/model_results")
    print(f"  [GOLD] model_results actualizado con resultados K-Means")


# ==============================================================================
# GUARDAR MODELO
# ==============================================================================
def save_model(model, k: int, silhouette: float):
    model_path = f"{HDFS_MODELS}/kmeans_k{k}_{MODEL_RUN_DATE}"
    try:
        model.write().overwrite().save(model_path)
        print(f"  [MODEL] Guardado en: {model_path}")
    except Exception as e:
        print(f"  [WARN] No se pudo guardar el modelo: {e}")

    # Guardar metadata JSON en HDFS
    meta = {
        "k": k,
        "silhouette": silhouette,
        "features": CLUSTER_FEATURES,
        "model_version": MODEL_VERSION,
        "run_date": MODEL_RUN_DATE,
        "model_path": model_path,
    }
    print(f"  [META] {json.dumps(meta, indent=2)}")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP K-Means Clustering")
    parser.add_argument("--k-min",  type=int, default=2, help="K mínimo a evaluar (default: 2)")
    parser.add_argument("--k-max",  type=int, default=8, help="K máximo a evaluar (default: 8)")
    parser.add_argument("--seed",   type=int, default=42, help="Semilla aleatoria (default: 42)")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — MODELO 1: K-MEANS CLUSTERING")
    print(f"  K range: {args.k_min}–{args.k_max}  |  Seed: {args.seed}")
    print(f"  Features: {CLUSTER_FEATURES}")
    print("=" * 65)

    spark = get_spark()

    # 1. Construir matriz de features
    features_df = build_feature_matrix(spark)
    if features_df is None:
        print("[SKIP] Clustering omitido: Silver no disponible.")
        spark.stop()
        return

    # 2. Ensamblar vector de features
    assembler = VectorAssembler(
        inputCols=[c for c in CLUSTER_FEATURES if c in features_df.columns],
        outputCol="raw_features",
        handleInvalid="skip"
    )
    df_vec = assembler.transform(features_df)

    # 3. Escalar (StandardScaler: media 0, varianza 1)
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withMean=True,
        withStd=True
    )
    scaler_model = scaler.fit(df_vec)
    df_scaled    = scaler_model.transform(df_vec)

    # 4. Seleccionar K óptimo
    best_k, best_sil, best_model = select_best_k(df_scaled, args.k_min, args.k_max, args.seed)

    # 5. Predicciones finales con el mejor modelo
    predictions = best_model.transform(df_scaled)

    # 6. Calcular silhouette individual por punto
    evaluator = ClusteringEvaluator(
        featuresCol="scaled_features",
        predictionCol="cluster_id",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )

    # 7. Asignar etiquetas interpretables a cada cluster
    used_features = [c for c in CLUSTER_FEATURES if c in features_df.columns]
    labels = label_clusters(best_model, used_features)

    print("\n  Asignaciones de cluster:")
    for c_id, label in labels:
        count = predictions.filter(F.col("cluster_id") == c_id).count()
        print(f"    Cluster {c_id} [{label}]: {count} ciudades")

    # 8. Agregar silhouette global al DataFrame
    predictions = (
        predictions
        .withColumn("cluster_silhouette", F.lit(best_sil).cast(DoubleType()))
        .withColumn("kmeans_k_selected",  F.lit(best_k).cast(IntegerType()))
    )

    # 9. Actualizar Gold
    update_gold_with_clusters(spark, predictions, labels, best_k, best_sil)

    # 10. Guardar modelo
    save_model(best_model, best_k, best_sil)

    print("\n" + "=" * 65)
    print(f"  CLUSTERING COMPLETADO | K={best_k} | Silhouette={best_sil:.4f}")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    main()
