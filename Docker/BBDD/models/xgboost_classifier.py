"""
xgboost_classifier.py — GTP (Green Turning Point) | Modelo 3: XGBoost Classifier
Predice la fase EKC de cada ciudad sin asumir forma de curva.

Fases:
  DEGRADANDO   → NDVI bajando, GDP bajo respecto a Y*
  TURNING      → NDVI estabilizandose, GDP cerca de Y*
  RECUPERANDO  → NDVI subiendo, GDP supero Y*

Lee fact_kuznets desde MariaDB (ya tiene cluster_id + EKC params).
Actualiza MariaDB:
  - fact_kuznets: turning_point_phase, phase_confidence, investment_score, investment_recommendation
  - model_results: xgb_predicted_phase, xgb_prob_*, xgb_confidence

Ejecucion:
  python models/xgboost_classifier.py [--threshold 0.5] [--test-years 2]
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

# ==============================================================================
# CONFIGURACION
# ==============================================================================
CONFIG_FILE   = str(Path(__file__).resolve().parent.parent.parent / "config.ini")
MODEL_VERSION = "1.0"
MODEL_RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

XGB_FEATURES = [
    "ndvi_mean",
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
    "ntl_mean",
    "lst_day_c",
    "pm25_mean",
    "renewables_pct",
    "ets_price_eur",
]

CLASSES = ["DEGRADANDO", "TURNING", "RECUPERANDO"]


# ==============================================================================
# DB POOL
# ==============================================================================
def get_pool():
    bbdd_dir = str(Path(__file__).resolve().parent.parent)
    if bbdd_dir not in sys.path:
        sys.path.insert(0, bbdd_dir)
    from db_pool import MariaDBPool
    return MariaDBPool(config_file=CONFIG_FILE)


def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f
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
# CARGAR Y ETIQUETAR DATOS DESDE MARIADB
# Agrega a nivel ciudad x anio (promedio de los 12 meses) para el clasificador
# ==============================================================================
def load_and_label_data(pool):
    print("  Cargando fact_kuznets desde MariaDB ...")

    # Agregar mensual → anual para el clasificador
    rows = pool.execute_query("""
        SELECT fk.city_sk, dc.city_code, dc.country_code,
               fk.year,
               AVG(fk.ndvi_mean)            AS ndvi_mean,
               AVG(fk.ndvi_yoy_change)      AS ndvi_yoy_change,
               AVG(fk.no2_mean)             AS no2_mean,
               AVG(fk.imperviousness_mean)  AS imperviousness_mean,
               AVG(fk.ln_gdp_pps)           AS ln_gdp_pps,
               AVG(fk.ln_gdp_pps_sq)        AS ln_gdp_pps_sq,
               AVG(fk.gdp_growth_rate)      AS gdp_growth_rate,
               MAX(fk.cluster_id)           AS cluster_id,
               AVG(fk.green_index)          AS green_index,
               AVG(fk.gdp_gap_to_turning)   AS gdp_gap_to_turning,
               AVG(fk.ekc_turning_point_y)  AS ekc_turning_point_y,
               AVG(fk.gdp_pps_per_capita)   AS gdp_pps_per_capita
        FROM fact_kuznets fk
        JOIN dim_city dc ON fk.city_sk = dc.city_sk
        GROUP BY fk.city_sk, dc.city_code, dc.country_code, fk.year
    """)

    if not rows:
        print("  [SKIP] fact_kuznets vacia.")
        return None

    pdf = pd.DataFrame(rows)
    print(f"  Registros: {len(pdf):,} ciudad x anio")

    # Generar etiquetas
    def label_phase(row):
        slope = float(row.get("ndvi_yoy_change") or 0)
        gap   = safe_float(row.get("gdp_gap_to_turning"))
        gdp   = safe_float(row.get("gdp_pps_per_capita"))
        tp    = safe_float(row.get("ekc_turning_point_y"))
        tol   = (tp * 0.15) if tp and tp > 0 else None

        if tol is not None and gap is not None and abs(gap) <= tol:
            return "TURNING"
        if slope > 0.003 and gap is not None and gap > 0:
            return "RECUPERANDO"
        return "DEGRADANDO"

    pdf["phase_label"] = pdf.apply(label_phase, axis=1)

    # Separar filas con GDP completo para train/test.
    # Años sin Eurostat GDP (ej. 2025-2026) no se pueden etiquetar correctamente
    # y sesgan el test set hacia DEGRADANDO. Se excluyen del entrenamiento
    # pero se mantienen en pdf_full para que el modelo prediga sobre ellos.
    pdf_labeled = pdf[pdf["gdp_pps_per_capita"].notna()].copy()
    n_sin_gdp = len(pdf) - len(pdf_labeled)
    if n_sin_gdp > 0:
        print(f"  [INFO] {n_sin_gdp} ciudad x año sin GDP excluidos de train/test "
              f"(prediccion aplicada igualmente)")

    dist = pdf_labeled["phase_label"].value_counts()
    print("\n  Distribucion de fases (datos con GDP completo):")
    for phase, count in dist.items():
        print(f"    {phase}: {count:,} ({count/len(pdf_labeled)*100:.1f}%)")

    return pdf, pdf_labeled


# ==============================================================================
# TRAIN / TEST SPLIT TEMPORAL
# ==============================================================================
def temporal_split(pdf, test_years: int = 2):
    max_year = int(pdf["year"].max())
    cutoff   = max_year - test_years
    train    = pdf[pdf["year"] <= cutoff].copy()
    test     = pdf[pdf["year"] >  cutoff].copy()
    print(f"\n  Train: anos <= {cutoff} ({len(train):,} obs)")
    print(f"  Test:  anos > {cutoff}  ({len(test):,} obs)")
    return train, test


# ==============================================================================
# ENTRENAR XGBOOST
# ==============================================================================
def train_xgboost(train, test):
    try:
        import xgboost as xgb
        from sklearn.preprocessing import LabelEncoder
        from sklearn.metrics import f1_score, classification_report
    except ImportError:
        print("[ERROR] xgboost o scikit-learn no instalados.")
        print("        Instala: pip install xgboost scikit-learn")
        return None, None, None

    feature_cols = [c for c in XGB_FEATURES if c in train.columns]

    X_train = train[feature_cols].fillna(0)
    X_test  = test[feature_cols].fillna(0)

    le = LabelEncoder()
    le.fit(CLASSES)
    y_train = le.transform(train["phase_label"])
    y_test  = le.transform(test["phase_label"])

    # Pesos para balanceo
    class_counts  = np.bincount(y_train)
    class_weights = len(y_train) / (len(CLASSES) * class_counts.clip(1))
    sample_weights = np.array([class_weights[y] for y in y_train])

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
        eval_metric="mlogloss",
        early_stopping_rounds=20,
        random_state=42,
        n_jobs=-1,
    )

    clf.fit(
        X_train, y_train,
        sample_weight=sample_weights,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    y_pred = clf.predict(X_test)
    f1_macro    = f1_score(y_test, y_pred, average="macro")
    f1_weighted = f1_score(y_test, y_pred, average="weighted")

    print(f"\n  F1 Macro:    {f1_macro:.4f}")
    print(f"  F1 Weighted: {f1_weighted:.4f}")
    print("\n  Report:")
    print(classification_report(y_test, y_pred, target_names=CLASSES,
                                labels=list(range(len(CLASSES))), zero_division=0))

    fi = dict(zip(feature_cols, clf.feature_importances_))
    top5 = sorted(fi.items(), key=lambda x: x[1], reverse=True)[:5]
    print("  Top-5 features:")
    for feat, imp in top5:
        print(f"    {feat}: {imp:.4f}")

    return clf, le, feature_cols


# ==============================================================================
# PREDICCION SOBRE TODO EL DATASET
# ==============================================================================
def predict_all(clf, le, feature_cols, pdf_full):
    pdf_full = pdf_full.copy().sort_values(["city_sk", "year"])

    # Para años sin GDP (2025+), forward-fill gdp_gap_to_turning por ciudad
    # usando el último valor conocido. Se reemplaza automáticamente en el
    # siguiente run del pipeline cuando Eurostat publique el GDP real.
    if "gdp_gap_to_turning" in pdf_full.columns:
        pdf_full["gdp_gap_to_turning"] = (
            pdf_full.groupby("city_sk")["gdp_gap_to_turning"].ffill()
        )

    X_all    = pdf_full[feature_cols].fillna(0)
    y_proba  = clf.predict_proba(X_all)
    pdf_full = pdf_full.copy()

    for i, cls in enumerate(le.classes_):
        pdf_full[f"xgb_prob_{cls.lower()}"] = y_proba[:, i]

    pdf_full["xgb_predicted_phase"] = le.inverse_transform(y_proba.argmax(axis=1))
    pdf_full["xgb_confidence"]      = y_proba.max(axis=1)

    fi     = dict(zip(feature_cols, clf.feature_importances_))
    top5   = sorted(fi.items(), key=lambda x: x[1], reverse=True)[:5]
    fi_json = json.dumps({k: float(v) for k, v in top5})
    pdf_full["xgb_feature_importance"] = fi_json

    return pdf_full


# ==============================================================================
# ACTUALIZAR MARIADB
# ==============================================================================
def update_mariadb(pool, pdf_predictions):
    print("\n  Actualizando MariaDB con predicciones XGBoost ...")

    # Actualizar fact_kuznets (para todos los meses del anio)
    for _, row in pdf_predictions.iterrows():
        city_sk = safe_int(row["city_sk"])
        year    = safe_int(row["year"])
        phase   = str(row.get("xgb_predicted_phase", "DEGRADANDO"))
        conf    = safe_float(row.get("xgb_confidence"))
        gi      = safe_float(row.get("green_index"))
        tp      = safe_float(row.get("ekc_turning_point_y"))
        tp_score = 20.0 if phase == "TURNING" else (15.0 if phase == "RECUPERANDO" else 0.0)
        inv_score = round(
            (gi or 0.0) * 50.0 + (conf or 0.5) * 30.0 + tp_score, 2
        )
        inv_rec = (
            "STRONG_BUY" if inv_score >= 75 else
            "BUY"        if inv_score >= 55 else
            "HOLD"       if inv_score >= 35 else
            "AVOID"
        )

        pool.execute_write(
            """UPDATE fact_kuznets
               SET turning_point_phase=%s, phase_confidence=%s,
                   investment_score=%s, investment_recommendation=%s
               WHERE city_sk=%s AND year=%s""",
            (phase, conf, inv_score, inv_rec, city_sk, year)
        )

    print(f"  [FACT_KUZNETS] Fases XGBoost: {len(pdf_predictions)} ciudades x anio actualizadas")

    # Actualizar model_results
    sql = """
        INSERT INTO model_results
            (city_sk, year, month,
             xgb_predicted_phase, xgb_prob_degradando, xgb_prob_turning,
             xgb_prob_recuperando, xgb_confidence, xgb_feature_importance,
             _model_run_date, _pipeline_version)
        VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s)
        ON DUPLICATE KEY UPDATE
            xgb_predicted_phase=VALUES(xgb_predicted_phase),
            xgb_prob_degradando=VALUES(xgb_prob_degradando),
            xgb_prob_turning=VALUES(xgb_prob_turning),
            xgb_prob_recuperando=VALUES(xgb_prob_recuperando),
            xgb_confidence=VALUES(xgb_confidence),
            xgb_feature_importance=VALUES(xgb_feature_importance),
            _model_run_date=VALUES(_model_run_date)
    """
    # Obtener meses disponibles desde fact_kuznets
    fk_months = pool.execute_query(
        "SELECT DISTINCT city_sk, year, month FROM fact_kuznets"
    )
    month_map = {}
    for r in fk_months:
        key = (r["city_sk"], r["year"])
        if key not in month_map:
            month_map[key] = []
        month_map[key].append(r["month"])

    rows = []
    for _, row in pdf_predictions.iterrows():
        city_sk = safe_int(row["city_sk"])
        year    = safe_int(row["year"])
        phase   = str(row.get("xgb_predicted_phase", "DEGRADANDO"))
        conf    = safe_float(row.get("xgb_confidence"))
        prob_d  = safe_float(row.get("xgb_prob_degradando"))
        prob_t  = safe_float(row.get("xgb_prob_turning"))
        prob_r  = safe_float(row.get("xgb_prob_recuperando"))
        fi_json = str(row.get("xgb_feature_importance", "{}"))

        for month in month_map.get((city_sk, year), [1]):
            rows.append((
                city_sk, year, month,
                phase, prob_d, prob_t, prob_r, conf, fi_json,
                MODEL_RUN_DATE, MODEL_VERSION,
            ))

    if rows:
        pool.execute_many(sql, rows)
    print(f"  [MODEL_RESULTS] XGBoost: {len(rows)} filas actualizadas")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP XGBoost Phase Classifier")
    parser.add_argument("--threshold",  type=float, default=0.5)
    parser.add_argument("--test-years", type=int,   default=2)
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — MODELO 3: XGBOOST PHASE CLASSIFIER")
    print(f"  Clases: {CLASSES}")
    print(f"  Test anos: {args.test_years}")
    print(f"  Fuente: MariaDB Gold (fact_kuznets)")
    print("=" * 65)

    pool = get_pool()

    # 1. Cargar y etiquetar
    pdf_full, pdf_labeled = load_and_label_data(pool)
    if pdf_labeled is None or len(pdf_labeled) == 0:
        print("[SKIP] XGBoost omitido.")
        return

    # 2. Split temporal (solo sobre datos con GDP completo)
    train, test = temporal_split(pdf_labeled, args.test_years)
    if len(train) < 50:
        print(f"[ERROR] Solo {len(train)} obs en train. Necesitas mas datos.")
        sys.exit(1)

    # 3. Entrenar
    clf, le, feature_cols = train_xgboost(train, test)
    if clf is None:
        sys.exit(1)

    # 4. Prediccion sobre todo el dataset (incluye años sin GDP como 2025-2026)
    pdf_predictions = predict_all(clf, le, feature_cols, pdf_full)

    # 5. Actualizar MariaDB
    update_mariadb(pool, pdf_predictions)

    print("\n" + "=" * 65)
    print("  XGBOOST CLASSIFIER COMPLETADO")
    print("=" * 65)


if __name__ == "__main__":
    main()
