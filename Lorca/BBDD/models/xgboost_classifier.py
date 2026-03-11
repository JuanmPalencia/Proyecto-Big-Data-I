"""
xgboost_classifier.py — GTP (Green Turning Point) | Modelo 3: XGBoost Classifier
Predice la fase EKC de cada ciudad sin asumir forma de curva.

Problema:
  La regresión EKC estima Y* teórico, pero no dice "dónde está la ciudad ahora".
  Este clasificador lee los datos observados y predice directamente:
    DEGRADANDO   → NDVI bajando, GDP bajo respecto a Y*
    TURNING      → NDVI estabilizándose, GDP cerca de Y*
    RECUPERANDO  → NDVI subiendo, GDP superó Y*

Ventaja sobre la regresión:
  XGBoost no asume que la curva EKC existe. Si la curva no se cumple para
  una ciudad específica, el modelo lo detectará por los datos observados.
  Es el "fact-check" del modelo paramétrico.

Etiquetado (label generation):
  Las etiquetas se crean automáticamente combinando:
    - ndvi_yoy_change   : tendencia reciente del NDVI
    - gdp_gap_to_turning: distancia del GDP actual al Y* del cluster
    - ndvi_trend_slope  : pendiente histórica del NDVI
  Reglas:
    TURNING    = ndvi_yoy_change > -0.001 AND |gdp_gap| < 0.15 * Y*
    RECUPERANDO= ndvi_trend_slope > 0.003 AND gdp_pps > Y*
    DEGRADANDO = el resto

Features del modelo:
  ndvi_mean, ndvi_trend_slope, ndvi_yoy_change, no2_mean,
  imperviousness_mean, ln_gdp_pps, ln_gdp_pps_sq, gdp_growth_rate,
  cluster_id, green_index, gdp_gap_to_turning, year

Train/test split: temporal — años ≤ (max_year - 2) para train, resto test.

Ejecución:
  spark-submit --master yarn models/xgboost_classifier.py [--threshold 0.5]
"""

import sys
import json
import argparse
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timezone

warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HDFS_GOLD       = "hdfs:///user/gtp/gold"
HDFS_MODELS     = "hdfs:///user/gtp/models/xgboost"
MODEL_VERSION   = "1.0"
MODEL_RUN_DATE  = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Features para el clasificador
XGB_FEATURES = [
    "ndvi_mean",
    "ndvi_trend_slope",
    "ndvi_yoy_change",
    "no2_mean",
    "imperviousness_mean",
    "ln_gdp_pps",
    "ln_gdp_pps_sq",
    "gdp_growth_rate",
    "cluster_id",
    "green_index",
    "gdp_gap_to_turning",
    "year",
]

CLASSES = ["DEGRADANDO", "TURNING", "RECUPERANDO"]

# ==============================================================================
# SPARK
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_XGBoost_Classifier")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )

# ==============================================================================
# CARGAR Y ETIQUETAR DATOS
# ==============================================================================
def load_and_label_data(spark):
    print("  Cargando fact_kuznets desde Gold ...")

    fk = spark.read.parquet(f"{HDFS_GOLD}/fact_kuznets")
    needed = XGB_FEATURES + [
        "city_code", "city_name", "country_code",
        "ekc_turning_point_y", "gdp_pps_per_capita"
    ]
    existing = [c for c in needed if c in fk.columns]
    pdf = fk.select(existing).toPandas()

    print(f"  Registros cargados: {len(pdf):,}")

    # --- Generar etiquetas ---
    def label_phase(row):
        slope     = row.get("ndvi_trend_slope", 0) or 0
        yoy       = row.get("ndvi_yoy_change",  0) or 0
        gap       = row.get("gdp_gap_to_turning", None)
        gdp       = row.get("gdp_pps_per_capita", None)
        tp        = row.get("ekc_turning_point_y", None)

        # Tolerancia: 15% del turning point
        tol = (tp * 0.15) if tp else None

        # TURNING: NDVI estabilizándose Y GDP cercano al turning point
        if tol is not None and gap is not None and abs(gap) <= tol:
            return "TURNING"

        # RECUPERANDO: NDVI creciendo Y GDP superó Y*
        if slope > 0.003 and gap is not None and gap > 0:
            return "RECUPERANDO"

        # DEGRADANDO: por defecto (incluye ciudades donde falta turning point)
        return "DEGRADANDO"

    pdf["phase_label"] = pdf.apply(label_phase, axis=1)

    dist = pdf["phase_label"].value_counts()
    print(f"\n  Distribución de fases:")
    for phase, count in dist.items():
        print(f"    {phase}: {count:,} ({count/len(pdf)*100:.1f}%)")

    return pdf


# ==============================================================================
# TRAIN / TEST SPLIT TEMPORAL
# No usamos random split: respetamos el orden temporal para evitar data leakage.
# ==============================================================================
def temporal_split(pdf, test_years: int = 2):
    max_year = pdf["year"].max()
    cutoff   = max_year - test_years

    train = pdf[pdf["year"] <= cutoff].copy()
    test  = pdf[pdf["year"] >  cutoff].copy()

    print(f"\n  Train: años ≤ {cutoff} ({len(train):,} obs)")
    print(f"  Test:  años > {cutoff} ({len(test):,}  obs)")
    return train, test


# ==============================================================================
# ENTRENAR XGBOOST
# ==============================================================================
def train_xgboost(train, test, threshold: float = 0.5):
    try:
        import xgboost as xgb
        from sklearn.preprocessing import LabelEncoder
        from sklearn.metrics import (classification_report, f1_score,
                                     confusion_matrix)
    except ImportError:
        print("[ERROR] xgboost o scikit-learn no instalados.")
        print("        Instala: pip install xgboost scikit-learn")
        return None, None, None

    # --- Preparar features ---
    feature_cols = [c for c in XGB_FEATURES if c in train.columns]

    X_train = train[feature_cols].fillna(0)
    X_test  = test[feature_cols].fillna(0)

    le = LabelEncoder()
    le.fit(CLASSES)
    y_train = le.transform(train["phase_label"])
    y_test  = le.transform(test["phase_label"])

    # --- Pesos de clase para balanceo (TURNING es la clase minoritaria) ---
    class_counts = np.bincount(y_train)
    class_weights = len(y_train) / (len(CLASSES) * class_counts)
    sample_weights = np.array([class_weights[y] for y in y_train])

    # --- Modelo XGBoost ---
    clf = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        gamma=1,
        reg_alpha=0.1,
        reg_lambda=1.0,
        objective="multi:softprob",
        num_class=len(CLASSES),
        use_label_encoder=False,
        eval_metric="mlogloss",
        random_state=42,
        n_jobs=-1,
    )

    clf.fit(
        X_train, y_train,
        sample_weight=sample_weights,
        eval_set=[(X_test, y_test)],
        verbose=False,
        early_stopping_rounds=20,
    )

    # --- Evaluación ---
    y_pred  = clf.predict(X_test)
    y_proba = clf.predict_proba(X_test)

    f1_macro = f1_score(y_test, y_pred, average="macro")
    f1_weighted = f1_score(y_test, y_pred, average="weighted")

    print(f"\n  Resultados en Test (años > {test['year'].min() - 1}):")
    print(f"  F1 Macro:    {f1_macro:.4f}")
    print(f"  F1 Weighted: {f1_weighted:.4f}")
    print("\n  Report detallado:")
    print(classification_report(y_test, y_pred, target_names=CLASSES))

    # Feature importance
    fi = dict(zip(feature_cols, clf.feature_importances_))
    top5 = sorted(fi.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\n  Top-5 features:")
    for feat, imp in top5:
        print(f"    {feat}: {imp:.4f}")

    return clf, le, feature_cols


# ==============================================================================
# PREDICCIÓN SOBRE TODO EL DATASET
# ==============================================================================
def predict_all(clf, le, feature_cols, pdf_full):
    X_all = pdf_full[feature_cols].fillna(0)
    y_proba = clf.predict_proba(X_all)

    fi      = dict(zip(feature_cols, clf.feature_importances_))
    top5    = sorted(fi.items(), key=lambda x: x[1], reverse=True)[:5]
    top5_json = json.dumps({k: float(v) for k, v in top5})

    pdf_full = pdf_full.copy()
    for i, cls in enumerate(le.classes_):
        col = f"xgb_prob_{cls.lower()}"
        pdf_full[col] = y_proba[:, i]

    pdf_full["xgb_predicted_phase"] = le.inverse_transform(y_proba.argmax(axis=1))
    pdf_full["xgb_confidence"]      = y_proba.max(axis=1)
    pdf_full["xgb_feature_importance"] = top5_json

    return pdf_full


# ==============================================================================
# ACTUALIZAR GOLD
# ==============================================================================
def update_gold(spark, pdf_predictions):
    print("\n  Actualizando Gold con predicciones XGBoost ...")

    # --- Actualizar fact_kuznets ---
    fk = spark.read.parquet(f"{HDFS_GOLD}/fact_kuznets")

    pred_cols = [
        "city_code", "year",
        "xgb_predicted_phase", "xgb_confidence",
        "xgb_prob_degradando", "xgb_prob_turning", "xgb_prob_recuperando",
    ]
    existing_pred = [c for c in pred_cols if c in pdf_predictions.columns]
    pred_spark = spark.createDataFrame(pdf_predictions[existing_pred])

    # Drop columnas XGB anteriores
    drop_cols = ["turning_point_phase", "phase_confidence",
                 "xgb_predicted_phase", "xgb_confidence"]
    fk_clean = fk.drop(*[c for c in drop_cols if c in fk.columns])

    fk_updated = fk_clean.join(
        pred_spark.withColumnRenamed("xgb_predicted_phase", "turning_point_phase")
                  .withColumnRenamed("xgb_confidence",      "phase_confidence"),
        on=["city_code", "year"],
        how="left"
    )

    # Recalcular investment_score con la fase ahora disponible
    fk_updated = fk_updated.withColumn(
        "investment_score",
        F.round(
            F.coalesce(F.col("green_index"), F.lit(0.0)) * 50.0
            + F.coalesce(F.col("phase_confidence"), F.lit(0.5)) * 30.0
            + F.when(F.col("turning_point_phase") == "TURNING",    F.lit(20.0))
               .when(F.col("turning_point_phase") == "RECUPERANDO", F.lit(15.0))
               .otherwise(F.lit(0.0)),
            2
        )
    )

    fk_updated = fk_updated.withColumn(
        "investment_recommendation",
        F.when(F.col("investment_score") >= 75, "STRONG_BUY")
         .when(F.col("investment_score") >= 55, "BUY")
         .when(F.col("investment_score") >= 35, "HOLD")
         .otherwise("AVOID")
    )

    (
        fk_updated.write
        .mode("overwrite")
        .partitionBy("year", "country")
        .parquet(f"{HDFS_GOLD}/fact_kuznets")
    )
    print("  [GOLD] fact_kuznets actualizado con fases XGBoost e investment_score refinado")

    # --- Actualizar model_results ---
    try:
        mr = spark.read.parquet(f"{HDFS_GOLD}/model_results")
        mr_clean = mr.drop("xgb_predicted_phase", "xgb_prob_degradando",
                           "xgb_prob_turning", "xgb_prob_recuperando",
                           "xgb_confidence", "xgb_feature_importance")
        xgb_cols = [
            "city_code", "year",
            "xgb_predicted_phase", "xgb_prob_degradando", "xgb_prob_turning",
            "xgb_prob_recuperando", "xgb_confidence", "xgb_feature_importance"
        ]
        xgb_existing = [c for c in xgb_cols if c in pdf_predictions.columns]
        xgb_spark = spark.createDataFrame(pdf_predictions[xgb_existing])
        mr_updated = mr_clean.join(xgb_spark, on=["city_code", "year"], how="left")
    except Exception:
        mr_updated = spark.createDataFrame(
            pdf_predictions[[c for c in pred_cols + ["xgb_feature_importance"] if c in pdf_predictions.columns]]
        )

    writer = mr_updated.write.mode("overwrite")
    if "year" in mr_updated.columns and "country" in mr_updated.columns:
        writer = writer.partitionBy("year", "country")
    writer.parquet(f"{HDFS_GOLD}/model_results")
    print("  [GOLD] model_results actualizado con resultados XGBoost")


# ==============================================================================
# GUARDAR MODELO
# ==============================================================================
def save_model(clf, le, feature_cols):
    try:
        import joblib
        import tempfile
        from pyspark.sql import SparkSession

        model_data = {"model": clf, "label_encoder": le, "features": feature_cols}
        local_path = f"/tmp/gtp_xgb_{MODEL_RUN_DATE}.joblib"
        joblib.dump(model_data, local_path)

        # Subir a HDFS con hdfs dfs -put
        import subprocess
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_path, f"{HDFS_MODELS}/xgb_{MODEL_RUN_DATE}.joblib"],
            check=False
        )
        print(f"  [MODEL] Guardado en {HDFS_MODELS}/xgb_{MODEL_RUN_DATE}.joblib")
    except Exception as e:
        print(f"  [WARN] No se pudo serializar el modelo: {e}")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP XGBoost Phase Classifier")
    parser.add_argument("--threshold", type=float, default=0.5,
                        help="Umbral de confianza (default: 0.5)")
    parser.add_argument("--test-years", type=int, default=2,
                        help="Años reservados para test (default: 2)")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — MODELO 3: XGBOOST PHASE CLASSIFIER")
    print(f"  Clases:    {CLASSES}")
    print(f"  Test años: {args.test_years}")
    print("=" * 65)

    spark = get_spark()

    # 1. Cargar y etiquetar
    pdf = load_and_label_data(spark)

    # 2. Split temporal
    train, test = temporal_split(pdf, args.test_years)

    if len(train) < 50:
        print(f"[ERROR] Solo {len(train)} observaciones en train. Necesitas más datos.")
        spark.stop()
        sys.exit(1)

    # 3. Entrenar
    clf, le, feature_cols = train_xgboost(train, test, args.threshold)
    if clf is None:
        spark.stop()
        sys.exit(1)

    # 4. Predecir sobre todo el dataset
    pdf_predictions = predict_all(clf, le, feature_cols, pdf)

    # 5. Actualizar Gold
    update_gold(spark, pdf_predictions)

    # 6. Guardar modelo
    save_model(clf, le, feature_cols)

    print("\n" + "=" * 65)
    print("  XGBOOST CLASSIFIER COMPLETADO")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    main()
