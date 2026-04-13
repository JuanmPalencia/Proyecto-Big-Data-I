"""
clustering.py — GTP (Green Turning Point) | Modelo 1: K-Means Clustering
Segmenta ciudades europeas en grupos homogeneos antes de la regresion EKC.

Lee Silver desde MariaDB (fact_environmental + fact_economic).
Usa sklearn KMeans (datos pequenos: ~237 ciudades).
Escribe resultados en MariaDB:
  - fact_kuznets: cluster_id, cluster_label, cluster_silhouette
  - model_results: columnas K-Means
  - dim_cluster: entrada SCD-2 con parametros del clustering

Ejecucion:
  python models/clustering.py [--k-min 2] [--k-max 8]
"""

import sys
import json
import argparse
import warnings
import os
import numpy as np
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

CLUSTER_FEATURES = [
    "ndvi_mean_city",
    "ndvi_yoy_change_city",
    "no2_mean_city",
    "imperviousness_mean_city",
    "ln_gdp_mean_city",
    "gdp_growth_mean_city",
    "ntl_mean_city",
    "lst_day_c_city",
    "pm25_mean_city",   # restaurado: ahora disponible via WHO GHO API (35 paises EU)
]


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
# CARGA DE DATOS DESDE MARIADB
# Agrega a nivel ciudad (promedio historico) para capturar perfil estructural
# ==============================================================================
def build_feature_matrix(pool):
    import pandas as pd
    print("  Cargando fact_environmental y fact_economic desde MariaDB ...")

    env_rows = pool.execute_query("""
        SELECT city_sk, AVG(ndvi_mean) AS ndvi_mean_city,
               AVG(ndvi_yoy_change) AS ndvi_yoy_change_city,
               AVG(no2_mean) AS no2_mean_city,
               AVG(imperviousness_mean) AS imperviousness_mean_city,
               AVG(CASE WHEN year >= 2012 THEN ntl_mean END) AS ntl_mean_city,
               AVG(lst_day_c) AS lst_day_c_city,
               AVG(pm25_mean) AS pm25_mean_city
        FROM fact_environmental
        GROUP BY city_sk
    """)

    eco_rows = pool.execute_query("""
        SELECT city_sk,
               AVG(ln_gdp_pps) AS ln_gdp_mean_city,
               AVG(gdp_growth_rate) AS gdp_growth_mean_city
        FROM fact_economic
        GROUP BY city_sk
    """)

    city_rows = pool.execute_query("""
        SELECT city_sk, city_code, city_name, country_code
        FROM dim_city
    """)

    if not env_rows:
        print("  [SKIP] fact_environmental vacia.")
        return None, None

    env_df  = pd.DataFrame(env_rows)
    eco_df  = pd.DataFrame(eco_rows) if eco_rows else pd.DataFrame(columns=["city_sk"])
    city_df = pd.DataFrame(city_rows)

    df = city_df.merge(env_df, on="city_sk", how="left")
    if not eco_df.empty:
        df = df.merge(eco_df, on="city_sk", how="left")
    else:
        df["ln_gdp_mean_city"]    = None
        df["gdp_growth_mean_city"] = None

    # Rellenar nulos con la mediana
    for col in CLUSTER_FEATURES:
        if col in df.columns:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val if median_val == median_val else 0.0)
        else:
            df[col] = 0.0

    print(f"  Feature matrix: {len(df)} ciudades x {len(CLUSTER_FEATURES)} features")
    return df, city_df


# ==============================================================================
# SELECCION DE K POR SILHOUETTE
# ==============================================================================
def select_best_k(X_scaled, k_min: int, k_max: int, seed: int = 42):
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    print(f"\n  Evaluando K-Means K={k_min}..{k_max} (Silhouette) ...")
    results = []

    for k in range(k_min, k_max + 1):
        km      = KMeans(n_clusters=k, random_state=seed, n_init=10, max_iter=300)
        labels  = km.fit_predict(X_scaled)
        if len(set(labels)) < 2:
            print(f"    K={k:2d}  [SKIP] solo un cluster")
            continue
        sil = silhouette_score(X_scaled, labels, metric="euclidean")
        results.append((k, sil, km, labels))
        print(f"    K={k:2d}  Silhouette={sil:.4f}")

    if not results:
        print("  [ERROR] No se pudo calcular Silhouette.")
        return None

    best = max(results, key=lambda x: x[1])
    print(f"\n  Optimo: K={best[0]} (Silhouette={best[1]:.4f})")
    return best   # (k, sil, km_model, labels)


# ==============================================================================
# ASIGNACION DE ETIQUETAS INTERPRETABLES
# ==============================================================================
def label_clusters(km_model, feature_names, scaler):
    """Asigna etiquetas semanticas a cada cluster basadas en los centroides."""
    centers_original = scaler.inverse_transform(km_model.cluster_centers_)
    idx = {name: i for i, name in enumerate(feature_names)}
    labels = []

    for cluster_id, center in enumerate(centers_original):
        ndvi_yoy = center[idx.get("ndvi_yoy_change_city", 0)]
        ln_gdp   = center[idx.get("ln_gdp_mean_city", 0)]
        no2      = center[idx.get("no2_mean_city", 0)]
        imperv   = center[idx.get("imperviousness_mean_city", 0)]

        # Mediana global como referencia (post-scaling centro = 0 → mediana)
        gdp_high    = ln_gdp > 0
        green_trend = ndvi_yoy > 0
        polluted    = no2 > 0
        built_up    = imperv > 0

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

    return labels


# ==============================================================================
# ACTUALIZAR DIM_CLUSTER (SCD-2)
# ==============================================================================
def update_dim_cluster(pool, cluster_labels: list, best_k: int, best_sil: float,
                        km_model, feature_names: list, city_assignments: dict):
    """
    Inserta o actualiza dim_cluster con los parametros del clustering actual.
    SCD-2: cierra versiones anteriores e inserta nuevas.
    """
    today     = MODEL_RUN_DATE
    yesterday = datetime.now(timezone.utc).date()
    yesterday_str = (datetime(yesterday.year, yesterday.month, yesterday.day)
                     - __import__("datetime").timedelta(days=1)).strftime("%Y-%m-%d")

    # Contar ciudades por cluster
    from collections import Counter
    city_counts = Counter(city_assignments.values())

    for cluster_id, cluster_label in cluster_labels:
        n_cities = city_counts.get(cluster_id, 0)

        # Cerrar version activa si existe
        pool.execute_write(
            """UPDATE dim_cluster SET valid_to=%s, is_current=0
               WHERE cluster_id=%s AND is_current=1""",
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
             None, None, None,
             None, safe_float(best_sil),
             "PENDING", 0,
             n_cities,
             today, "9999-12-31", 1)
        )

    print(f"  [DIM_CLUSTER] {len(cluster_labels)} clusters insertados/actualizados (SCD-2)")


# ==============================================================================
# ACTUALIZAR FACT_KUZNETS CON CLUSTERS
# ==============================================================================
def update_fact_kuznets(pool, city_cluster_map: dict, cluster_label_map: dict, best_sil: float):
    """
    Actualiza cluster_id, cluster_label, cluster_silhouette en fact_kuznets.
    city_cluster_map: city_sk -> cluster_id
    """
    print("  Actualizando fact_kuznets con cluster_id ...")
    for city_sk, cluster_id in city_cluster_map.items():
        label = cluster_label_map.get(cluster_id, f"Cluster_{cluster_id}")
        pool.execute_write(
            """UPDATE fact_kuznets
               SET cluster_id=%s, cluster_label=%s, cluster_silhouette=%s
               WHERE city_sk=%s""",
            (cluster_id, label, safe_float(best_sil), int(city_sk))
        )
    print(f"  [FACT_KUZNETS] cluster_id actualizado para {len(city_cluster_map)} ciudades")


# ==============================================================================
# ACTUALIZAR MODEL_RESULTS
# ==============================================================================
def update_model_results(pool, df, cluster_label_map: dict, best_k: int, best_sil: float):
    """Inserta/actualiza model_results con resultados K-Means."""
    # Obtener city_sk -> (year_list, month_list) de fact_kuznets
    fk_rows = pool.execute_query(
        "SELECT DISTINCT city_sk, year, month FROM fact_kuznets"
    )
    fk_lookup = {}
    for r in fk_rows:
        sk = r["city_sk"]
        if sk not in fk_lookup:
            fk_lookup[sk] = []
        fk_lookup[sk].append((r["year"], r["month"]))

    sql = """
        INSERT INTO model_results
            (city_sk, year, month,
             kmeans_cluster_id, kmeans_cluster_label,
             kmeans_silhouette, kmeans_k_selected,
             _model_run_date, _pipeline_version)
        VALUES (%s,%s,%s, %s,%s, %s,%s, %s,%s)
        ON DUPLICATE KEY UPDATE
            kmeans_cluster_id=VALUES(kmeans_cluster_id),
            kmeans_cluster_label=VALUES(kmeans_cluster_label),
            kmeans_silhouette=VALUES(kmeans_silhouette),
            kmeans_k_selected=VALUES(kmeans_k_selected),
            _model_run_date=VALUES(_model_run_date)
    """
    rows = []
    for _, row in df.iterrows():
        city_sk    = safe_int(row["city_sk"])
        cluster_id = safe_int(row.get("cluster_id"))
        label      = cluster_label_map.get(cluster_id, f"Cluster_{cluster_id}")
        ym_list    = fk_lookup.get(city_sk, [])
        for year, month in ym_list:
            rows.append((
                city_sk, year, month,
                cluster_id, label,
                safe_float(best_sil), best_k,
                MODEL_RUN_DATE, MODEL_VERSION,
            ))

    pool.execute_many(sql, rows) if rows else None
    print(f"  [MODEL_RESULTS] {len(rows)} filas actualizadas con K-Means")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP K-Means Clustering")
    parser.add_argument("--k-min",  type=int, default=2)
    parser.add_argument("--k-max",  type=int, default=8)
    parser.add_argument("--seed",   type=int, default=42)
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — MODELO 1: K-MEANS CLUSTERING")
    print(f"  K range: {args.k_min}-{args.k_max}  |  Seed: {args.seed}")
    print(f"  Features: {CLUSTER_FEATURES}")
    print(f"  Fuente: MariaDB Silver")
    print("=" * 65)

    try:
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        print("[ERROR] scikit-learn no instalado: pip install scikit-learn")
        sys.exit(1)

    pool = get_pool()

    # 1. Construir feature matrix
    df, city_df = build_feature_matrix(pool)
    if df is None:
        print("[SKIP] Clustering omitido.")
        return

    used_features = [c for c in CLUSTER_FEATURES if c in df.columns]
    X = df[used_features].values

    # 2. Escalar
    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 3. Seleccionar K optimo
    result = select_best_k(X_scaled, args.k_min, args.k_max, args.seed)
    if result is None:
        print("[ERROR] No se pudo determinar K optimo.")
        return

    best_k, best_sil, best_model, cluster_labels_arr = result

    # 4. Etiquetas por cluster
    labels_list    = label_clusters(best_model, used_features, scaler)
    cluster_label_map = {cid: lbl for cid, lbl in labels_list}

    df["cluster_id"] = cluster_labels_arr.tolist()

    print("\n  Asignaciones:")
    from collections import Counter
    cnt = Counter(cluster_labels_arr)
    for cid, lbl in labels_list:
        print(f"    Cluster {cid} [{lbl}]: {cnt[cid]} ciudades")

    # 5. Mapas
    city_cluster_map = dict(zip(df["city_sk"].tolist(), df["cluster_id"].tolist()))
    city_assignments = city_cluster_map

    # 6. Actualizar dim_cluster (SCD-2)
    update_dim_cluster(pool, labels_list, best_k, best_sil,
                       best_model, used_features, city_assignments)

    # 7. Actualizar fact_kuznets
    update_fact_kuznets(pool, city_cluster_map, cluster_label_map, best_sil)

    # 8. Actualizar model_results
    update_model_results(pool, df, cluster_label_map, best_k, best_sil)

    # 9. Persistir dim_cluster en HDFS Silver
    import pandas as pd
    from hdfs_writer import write_df_to_hdfs
    dc_rows = pool.execute_query("SELECT * FROM dim_cluster WHERE is_current=1")
    if dc_rows:
        write_df_to_hdfs(pd.DataFrame(dc_rows),
                         os.environ.get("DIM_CLUSTER_HDFS_PATH", "hdfs:///user/gtp/silver/dim_cluster/"),
                         partition_cols=["cluster_id"])

    print("\n" + "=" * 65)
    print(f"  CLUSTERING COMPLETADO | K={best_k} | Silhouette={best_sil:.4f}")
    print("=" * 65)


if __name__ == "__main__":
    main()
