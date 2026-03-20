"""
ekc_regression.py — GTP (Green Turning Point) | Modelo 2: EKC Panel Regression
Estima los parametros de la Curva de Kuznets Ambiental (EKC) por cluster.

Ecuacion del modelo de panel con efectos fijos:
  ln(NDVI_it) = alpha + beta1*ln(GDP_it) + beta2*[ln(GDP_it)]^2 + mu_i + lambda_t + eps_it

Turning Point:
  Y* = exp(-beta1 / 2*beta2)

Lee Silver desde MariaDB (fact_environmental + fact_economic + dim_cluster).
Actualiza MariaDB:
  - dim_cluster: SCD-2 con nuevos parametros EKC
  - fact_kuznets: ekc_beta1, ekc_beta2, ekc_turning_point_y, ekc_shape, gdp_gap_to_turning

Ejecucion:
  python models/ekc_regression.py [--clusters 0 1 2]
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
MODEL_TS       = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

MIN_OBS_PER_CLUSTER = 30


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


# ==============================================================================
# CARGA DE DATOS DESDE MARIADB
# Lee fact_kuznets (ya tiene cluster_id asignado por clustering.py)
# Usa datos mensuales: agrega a anual por ciudad para la regresion de panel
# ==============================================================================
def load_panel_data(pool):
    print("  Cargando datos de panel desde MariaDB ...")

    rows = pool.execute_query("""
        SELECT fk.city_sk, dc.city_code, dc.country_code,
               fk.year,
               AVG(fk.ndvi_mean)         AS ndvi_mean,
               AVG(fk.ln_gdp_pps)        AS ln_gdp_pps,
               AVG(fk.ln_gdp_pps_sq)     AS ln_gdp_pps_sq,
               AVG(fk.gdp_pps_per_capita) AS gdp_pps_per_capita,
               MAX(fk.cluster_id)        AS cluster_id
        FROM fact_kuznets fk
        JOIN dim_city dc ON fk.city_sk = dc.city_sk
        WHERE fk.ndvi_mean IS NOT NULL
          AND fk.ln_gdp_pps IS NOT NULL
          AND fk.cluster_id IS NOT NULL
        GROUP BY fk.city_sk, dc.city_code, dc.country_code, fk.year
    """)

    if not rows:
        print("  [SKIP] Sin datos en fact_kuznets con cluster_id.")
        return None

    pdf = pd.DataFrame(rows)
    pdf["cluster_id"] = pd.to_numeric(pdf["cluster_id"], errors="coerce").astype("Int64")
    print(f"  Panel: {len(pdf):,} obs ciudad x anio")
    print(f"  Clusters: {sorted(pdf['cluster_id'].dropna().unique())}")
    return pdf


# ==============================================================================
# ESTIMACION EKC POR CLUSTER
# Usa linearmodels.PanelOLS con efectos fijos de dos vias
# ==============================================================================
def estimate_ekc_for_cluster(pdf_cluster, cluster_id):
    try:
        pdf_cluster = pdf_cluster.copy()
        pdf_cluster["ln_ndvi"] = np.log(pdf_cluster["ndvi_mean"].clip(lower=1e-6))
        pdf_cluster = pdf_cluster.set_index(["city_code", "year"])

        n_obs    = len(pdf_cluster)
        n_cities = pdf_cluster.index.get_level_values("city_code").nunique()

        if n_obs < MIN_OBS_PER_CLUSTER:
            print(f"    [SKIP] Cluster {cluster_id}: {n_obs} obs (minimo {MIN_OBS_PER_CLUSTER})")
            return None

        from linearmodels import PanelOLS
        import statsmodels.api as sm

        model = PanelOLS(
            dependent=pdf_cluster["ln_ndvi"],
            exog=sm.add_constant(pdf_cluster[["ln_gdp_pps", "ln_gdp_pps_sq"]]),
            entity_effects=True,
            time_effects=True,
            drop_absorbed=True,
        )
        result = model.fit(cov_type="clustered", cluster_entity=True)

        params  = result.params
        stderr  = result.std_errors
        pvalues = result.pvalues

        alpha = params.get("const",          None)
        b1    = params.get("ln_gdp_pps",     None)
        b2    = params.get("ln_gdp_pps_sq",  None)

        b1_se = stderr.get("ln_gdp_pps",    None)
        b2_se = stderr.get("ln_gdp_pps_sq", None)
        b1_p  = pvalues.get("ln_gdp_pps",   None)
        b2_p  = pvalues.get("ln_gdp_pps_sq", None)

        turning_point_y    = None
        turning_point_ln_y = None
        if b1 is not None and b2 is not None and b2 != 0:
            ln_y_star          = -b1 / (2 * b2)
            turning_point_ln_y = ln_y_star
            turning_point_y    = np.exp(ln_y_star)

        r2     = result.rsquared
        r2_adj = result.rsquared_adj if hasattr(result, "rsquared_adj") else None
        f_stat = result.f_statistic.stat if hasattr(result, "f_statistic") else None
        f_pval = result.f_statistic.pval if hasattr(result, "f_statistic") else None
        aic    = result.loglik * (-2) + 2 * len(params) if hasattr(result, "loglik") else None
        bic    = result.loglik * (-2) + np.log(n_obs) * len(params) if hasattr(result, "loglik") else None

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
            "cluster_id":              int(cluster_id),
            "n_cities":                int(n_cities),
            "n_observations":          int(n_obs),
            "city_codes":              json.dumps(city_codes),
            "countries_represented":   json.dumps(countries),
            "gdp_pps_mean":            float(pdf_cluster.reset_index()["gdp_pps_per_capita"].mean()) if "gdp_pps_per_capita" in pdf_cluster.columns else None,
            "alpha":                   float(alpha)    if alpha    is not None else None,
            "beta1":                   float(b1)       if b1       is not None else None,
            "beta2":                   float(b2)       if b2       is not None else None,
            "beta1_se":                float(b1_se)    if b1_se    is not None else None,
            "beta2_se":                float(b2_se)    if b2_se    is not None else None,
            "beta1_pvalue":            float(b1_p)     if b1_p     is not None else None,
            "beta2_pvalue":            float(b2_p)     if b2_p     is not None else None,
            "turning_point_y_star":    float(turning_point_y)    if turning_point_y    is not None else None,
            "turning_point_ln_y":      float(turning_point_ln_y) if turning_point_ln_y is not None else None,
            "r_squared":               float(r2)       if r2       is not None else None,
            "r_squared_adj":           float(r2_adj)   if r2_adj   is not None else None,
            "f_statistic":             float(f_stat)   if f_stat   is not None else None,
            "f_pvalue":                float(f_pval)   if f_pval   is not None else None,
            "aic":                     float(aic)      if aic      is not None else None,
            "bic":                     float(bic)      if bic      is not None else None,
            "ekc_hypothesis_supported": bool(ekc_confirmed),
            "ekc_shape":               ekc_shape,
        }

    except ImportError:
        print("  [ERROR] linearmodels no instalado: pip install linearmodels")
        return None
    except Exception as e:
        print(f"    [ERROR] Cluster {cluster_id}: {e}")
        return None


# ==============================================================================
# ACTUALIZAR DIM_CLUSTER (SCD-2) CON PARAMETROS EKC
# ==============================================================================
def update_dim_cluster_ekc(pool, ekc_results: list):
    """
    Para cada cluster con resultado EKC, actualiza la version activa de dim_cluster
    cerrando la existente e insertando una nueva con los parametros reales.
    """
    today_str = MODEL_RUN_DATE
    yesterday_str = datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() - 86400
    ).strftime("%Y-%m-%d")

    for r in ekc_results:
        cluster_id = r["cluster_id"]

        # Leer version activa para obtener label y n_cities
        existing = pool.execute_query(
            "SELECT cluster_sk, cluster_label, n_cities FROM dim_cluster WHERE cluster_id=%s AND is_current=1",
            (cluster_id,)
        )
        cluster_label = existing[0]["cluster_label"] if existing else f"Cluster_{cluster_id}"
        n_cities      = r.get("n_cities") or (existing[0]["n_cities"] if existing else 0)

        # Cerrar version activa
        pool.execute_write(
            "UPDATE dim_cluster SET valid_to=%s, is_current=0 WHERE cluster_id=%s AND is_current=1",
            (yesterday_str, cluster_id)
        )

        # Insertar nueva version
        max_sk_row = pool.execute_query("SELECT COALESCE(MAX(cluster_sk),0) AS m FROM dim_cluster")
        max_sk = (max_sk_row[0]["m"] or 0) + 1

        pool.execute_write(
            """INSERT INTO dim_cluster
               (cluster_sk, cluster_id, cluster_label,
                beta1, beta2, alpha,
                turning_point_y_star, r_squared_adj,
                ekc_shape, ekc_hypothesis_supported,
                n_cities,
                valid_from, valid_to, is_current)
               VALUES (%s,%s,%s, %s,%s,%s, %s,%s, %s,%s, %s, %s,%s,%s)""",
            (max_sk, cluster_id, cluster_label,
             safe_float(r.get("beta1")), safe_float(r.get("beta2")),
             safe_float(r.get("alpha")),
             safe_float(r.get("turning_point_y_star")),
             safe_float(r.get("r_squared_adj")),
             r.get("ekc_shape", "UNCLEAR"),
             1 if r.get("ekc_hypothesis_supported") else 0,
             int(n_cities),
             today_str, "9999-12-31", 1)
        )

    print(f"  [DIM_CLUSTER] {len(ekc_results)} versiones EKC actualizadas (SCD-2)")


# ==============================================================================
# ACTUALIZAR FACT_KUZNETS CON PARAMETROS EKC
# ==============================================================================
def update_fact_kuznets_ekc(pool, ekc_results: list):
    """
    Para cada cluster, actualiza fact_kuznets con los parametros EKC y calcula
    gdp_gap_to_turning = gdp_pps_per_capita - turning_point_y_star
    """
    print("  Actualizando fact_kuznets con parametros EKC ...")
    for r in ekc_results:
        cluster_id = r["cluster_id"]
        b1         = safe_float(r.get("beta1"))
        b2         = safe_float(r.get("beta2"))
        alpha      = safe_float(r.get("alpha"))
        tp         = safe_float(r.get("turning_point_y_star"))
        r2         = safe_float(r.get("r_squared"))
        ekc_shape  = r.get("ekc_shape", "UNCLEAR")

        pool.execute_write(
            """UPDATE fact_kuznets
               SET ekc_beta1=%s, ekc_beta2=%s, ekc_alpha=%s,
                   ekc_turning_point_y=%s, ekc_r_squared=%s,
                   ekc_shape=%s,
                   gdp_gap_to_turning = CASE
                       WHEN %s IS NOT NULL AND gdp_pps_per_capita IS NOT NULL
                       THEN gdp_pps_per_capita - %s
                       ELSE NULL
                   END
               WHERE cluster_id=%s""",
            (b1, b2, alpha, tp, r2, ekc_shape,
             tp, tp,
             cluster_id)
        )

    print(f"  [FACT_KUZNETS] EKC actualizado para {len(ekc_results)} clusters")


# ==============================================================================
# ACTUALIZAR MODEL_RESULTS CON EKC
# ==============================================================================
def update_model_results_ekc(pool, ekc_results: list):
    """Inserta/actualiza model_results con parametros EKC por cluster."""
    # Obtener city_sk de cada cluster desde fact_kuznets
    fk_rows = pool.execute_query(
        "SELECT DISTINCT city_sk, year, month, cluster_id FROM fact_kuznets WHERE cluster_id IS NOT NULL"
    )

    cluster_to_result = {r["cluster_id"]: r for r in ekc_results}

    sql = """
        INSERT INTO model_results
            (city_sk, year, month, ekc_cluster_id,
             ekc_turning_point_y, _model_run_date, _pipeline_version)
        VALUES (%s,%s,%s,%s, %s,%s,%s)
        ON DUPLICATE KEY UPDATE
            ekc_cluster_id=VALUES(ekc_cluster_id),
            ekc_turning_point_y=VALUES(ekc_turning_point_y),
            _model_run_date=VALUES(_model_run_date)
    """
    rows = []
    for fk in fk_rows:
        r = cluster_to_result.get(fk["cluster_id"])
        if r:
            rows.append((
                fk["city_sk"], fk["year"], fk["month"],
                fk["cluster_id"],
                safe_float(r.get("turning_point_y_star")),
                MODEL_RUN_DATE, MODEL_VERSION,
            ))

    if rows:
        pool.execute_many(sql, rows)
    print(f"  [MODEL_RESULTS] EKC: {len(rows)} filas actualizadas")


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
    print(f"  Modelo: ln(NDVI) = alpha + beta1*ln(GDP) + beta2*[ln(GDP)]^2 + FE")
    print(f"  Turning Point: Y* = exp(-beta1 / 2*beta2)")
    print(f"  Fuente: MariaDB Silver/Gold")
    print("=" * 65)

    pool = get_pool()

    # 1. Cargar datos de panel
    pdf = load_panel_data(pool)
    if pdf is None:
        print("[SKIP] EKC regression omitida.")
        return

    # 2. Estimar por cluster
    clusters_to_run = args.clusters or sorted(pdf["cluster_id"].dropna().unique().astype(int))
    ekc_results = []

    print(f"\n  Estimando EKC para {len(clusters_to_run)} cluster(s) ...")
    for cluster_id in clusters_to_run:
        pdf_cluster = pdf[pdf["cluster_id"] == cluster_id].copy()
        n_obs    = len(pdf_cluster)
        n_cities = pdf_cluster["city_code"].nunique()
        print(f"\n  --- Cluster {cluster_id}: {n_obs} obs, {n_cities} ciudades ---")
        result = estimate_ekc_for_cluster(pdf_cluster, cluster_id)
        if result:
            ekc_results.append(result)
            tp = result.get("turning_point_y_star")
            b1 = result.get("beta1")
            b2 = result.get("beta2")
            r2 = result.get("r_squared")
            print(f"    beta1={b1:.4f}  beta2={b2:.4f}  Y*={tp:.0f} PPS  "
                  f"R2={r2:.3f}  Forma={result['ekc_shape']}")

    # 3. Actualizar MariaDB
    if ekc_results:
        update_dim_cluster_ekc(pool, ekc_results)
        update_fact_kuznets_ekc(pool, ekc_results)
        update_model_results_ekc(pool, ekc_results)
    else:
        print("\n[WARN] No se generaron resultados EKC.")

    print("\n" + "=" * 65)
    print(f"  EKC REGRESSION COMPLETADA | {len(ekc_results)}/{len(clusters_to_run)} clusters")
    print("=" * 65)


if __name__ == "__main__":
    main()
