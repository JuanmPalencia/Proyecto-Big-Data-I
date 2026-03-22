"""
gold_build.py — GTP (Green Turning Point)
Capa Gold: lee Silver desde MariaDB y construye tablas analíticas finales.

Schema activo: mariadb_dw_ddl.sql (granularidad MENSUAL)
  fact_kuznets   PK (city_sk, year, month) — cluster_sk NOT NULL → placeholder 0
  city_ranking   PK (city_sk, year, month) — rank_change MoM

Flujo:
  MariaDB Silver (fact_environmental, fact_economic, fact_policy, fact_financial, dim_city)
    → JOIN en pandas
    → MariaDB Gold (fact_kuznets, city_ranking)

Ejecución:
  python gold_build.py
"""

import sys
import math
import argparse
import numpy as np
from datetime import datetime, timezone
from pathlib import Path

# ==============================================================================
# CONFIGURACION
# ==============================================================================
CONFIG_FILE    = str(Path(__file__).resolve().parent.parent / "config.ini")
GOLD_LOAD_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

BATCH_SIZE = 1000

# cluster_sk placeholder (NOT NULL en DDL; los modelos ML lo actualizan)
CLUSTER_SK_DEFAULT = 0


# ==============================================================================
# DB POOL
# ==============================================================================
def get_pool():
    bbdd_dir = str(Path(__file__).resolve().parent)
    if bbdd_dir not in sys.path:
        sys.path.insert(0, bbdd_dir)
    from db_pool import MariaDBPool
    return MariaDBPool(config_file=CONFIG_FILE)


# ==============================================================================
# UTILIDADES
# ==============================================================================
def sanitize_row(t: tuple) -> tuple:
    """Replace any float NaN/inf in a tuple with None (MySQL can't handle them)."""
    result = []
    for v in t:
        if isinstance(v, float) and not math.isfinite(v):
            result.append(None)
        else:
            result.append(v)
    return tuple(result)


def batch_upsert(pool, sql: str, rows: list, batch_size: int = BATCH_SIZE):
    total = len(rows)
    if total == 0:
        return
    clean = [sanitize_row(r) for r in rows]
    for i in range(0, total, batch_size):
        pool.execute_many(sql, clean[i: i + batch_size])
    print(f"    {total:,} filas procesadas")


def safe_float(v):
    if v is None:
        return None
    try:
        f = float(v)
        return None if not math.isfinite(f) else f
    except Exception:
        return None


def safe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def safe_str(v, max_len=None):
    if v is None:
        return None
    # pandas NaN / numpy.nan are floats — avoid returning the string 'nan'
    if isinstance(v, float) and not math.isfinite(v):
        return None
    s = str(v)
    return s[:max_len] if max_len else s


def read_silver(pool, query: str):
    """Ejecuta una SELECT en MariaDB y devuelve lista de dicts."""
    try:
        return pool.execute_query(query)
    except Exception as e:
        print(f"  [WARN] Error leyendo Silver: {e}")
        return []


# ==============================================================================
# TABLA 1: FACT_KUZNETS
# PK: (city_sk, year, month)  — granularidad MENSUAL (DW schema)
# cluster_sk: NOT NULL → placeholder 0
# ==============================================================================
def build_fact_kuznets(pool):
    print("\n[GOLD 1/3] Construyendo fact_kuznets ...")

    import pandas as pd

    # ------------------------------------------------------------------
    # 1. FACT_ENVIRONMENTAL — base de datos mensual por ciudad
    # ------------------------------------------------------------------
    env_rows = read_silver(pool, """
        SELECT city_sk, year, month, date_sk,
               ndvi_mean, ndvi_yoy_change,
               no2_mean, imperviousness_mean, green_index,
               temp_mean_c,
               lst_day_c, lst_night_c, ntl_mean,
               pm25_mean, pm10_mean
        FROM fact_environmental
    """)

    if not env_rows:
        print("  [ERROR] fact_environmental vacía. Abortando fact_kuznets.")
        return False

    env_df = pd.DataFrame(env_rows)
    env_df["year"]  = env_df["year"].astype(int)
    env_df["month"] = env_df["month"].astype(int)

    # ------------------------------------------------------------------
    # ndvi_trend_slope — OLS slope de ndvi_mean vs year por ciudad
    # Se calcula sobre todo el histórico y se replica a todos los meses.
    # ------------------------------------------------------------------
    slopes = {}
    for city_sk, grp in env_df.groupby("city_sk"):
        grp_valid = grp.dropna(subset=["ndvi_mean"]).sort_values("year")
        if len(grp_valid) >= 2:
            x = grp_valid["year"].values.astype(float)
            y = grp_valid["ndvi_mean"].values.astype(float)
            n = len(x)
            sx  = x.sum()
            sy  = y.sum()
            sxy = (x * y).sum()
            sx2 = (x ** 2).sum()
            denom = n * sx2 - sx ** 2
            slopes[city_sk] = (n * sxy - sx * sy) / denom if denom != 0 else None
    env_df["ndvi_trend_slope"] = env_df["city_sk"].map(slopes)

    # ------------------------------------------------------------------
    # 2. DIM_CITY — metadatos de ciudad
    # ------------------------------------------------------------------
    city_rows = read_silver(pool, """
        SELECT city_sk, city_code, city_name, country_code, country_name
        FROM dim_city
    """)
    if not city_rows:
        print("  [ERROR] dim_city vacía.")
        return False
    city_df = pd.DataFrame(city_rows)

    # ------------------------------------------------------------------
    # 3. FACT_ECONOMIC — GDP mensual por ciudad (LEFT JOIN)
    # ------------------------------------------------------------------
    eco_rows = read_silver(pool, """
        SELECT city_sk, year, month,
               gdp_pps_per_capita, ln_gdp_pps, ln_gdp_pps_sq,
               gdp_growth_rate, fua_population,
               renewables_pct, tourism_nights, ets_price_eur
        FROM fact_economic
    """)
    eco_df = pd.DataFrame(eco_rows) if eco_rows else pd.DataFrame()
    if not eco_df.empty:
        eco_df["year"]  = eco_df["year"].astype(int)
        eco_df["month"] = eco_df["month"].astype(int)

    # ------------------------------------------------------------------
    # 4. FACT_POLICY — indicadores de política ambiental por país/mes
    # Incluye co2_country_kt, eps_index, env_tax_usd, investeu_total_eur
    # ------------------------------------------------------------------
    policy_rows = read_silver(pool, """
        SELECT country_code, year, month,
               co2_country_kt,
               eps_index, env_tax_usd,
               investeu_total_eur
        FROM fact_policy
    """)
    policy_df = pd.DataFrame(policy_rows) if policy_rows else pd.DataFrame()
    if not policy_df.empty:
        policy_df["year"]  = policy_df["year"].astype(int)
        policy_df["month"] = policy_df["month"].astype(int)

    # ------------------------------------------------------------------
    # 5. FACT_FINANCIAL — agregado por país/año/mes
    # ------------------------------------------------------------------
    fin_rows = read_silver(pool, """
        SELECT dc.country_code,
               ff.year,
               ff.month,
               GROUP_CONCAT(DISTINCT dc.ticker
                            ORDER BY dc.ticker SEPARATOR ',') AS fin_tickers,
               AVG(ff.close_price)                            AS fin_close_price_avg,
               AVG(ff.monthly_volatility)                     AS fin_annual_volatility
        FROM fact_financial ff
        JOIN dim_company dc
             ON ff.company_sk = dc.company_sk AND dc.is_current = 1
        GROUP BY dc.country_code, ff.year, ff.month
    """)
    fin_df = pd.DataFrame(fin_rows) if fin_rows else pd.DataFrame()
    if not fin_df.empty:
        fin_df["year"]  = fin_df["year"].astype(int)
        fin_df["month"] = fin_df["month"].astype(int)

    # ------------------------------------------------------------------
    # MERGE
    # ------------------------------------------------------------------
    fk = env_df.merge(city_df, on="city_sk", how="left")

    for col in ["lst_day_c", "lst_night_c", "ntl_mean", "pm25_mean", "pm10_mean"]:
        if col not in fk.columns:
            fk[col] = None

    if not eco_df.empty:
        fk = fk.merge(eco_df, on=["city_sk", "year", "month"], how="left")
    else:
        for col in ["gdp_pps_per_capita", "ln_gdp_pps", "ln_gdp_pps_sq",
                    "gdp_growth_rate", "fua_population",
                    "renewables_pct", "tourism_nights", "ets_price_eur"]:
            fk[col] = None

    if not policy_df.empty and "country_code" in fk.columns:
        fk = fk.merge(policy_df, on=["country_code", "year", "month"], how="left")
    else:
        for col in ["co2_country_kt", "eps_index", "env_tax_usd", "investeu_total_eur"]:
            fk[col] = None

    if not fin_df.empty and "country_code" in fk.columns:
        fk = fk.merge(fin_df, on=["country_code", "year", "month"], how="left")
    else:
        for col in ["fin_tickers", "fin_close_price_avg", "fin_annual_volatility"]:
            fk[col] = None

    # ------------------------------------------------------------------
    # Columnas de negocio
    # ------------------------------------------------------------------
    # Investment score: composite 0-100 from multiple signals
    score_env = fk["green_index"].fillna(0.0) * 60.0          # 0-60 (NDVI-based green index)
    score_ren = (fk["renewables_pct"].fillna(0.0) / 100.0) * 20.0  # 0-20 (renewables %)
    pm25_raw  = fk["pm25_mean"].fillna(15.0).clip(lower=0.0)
    score_air = (10.0 - (pm25_raw / 35.0 * 10.0)).clip(0.0, 10.0)  # 0-10 (air quality bonus)
    ets_raw   = fk["ets_price_eur"].fillna(0.0).clip(lower=0.0)
    score_ets = (ets_raw / 100.0 * 10.0).clip(0.0, 10.0)           # 0-10 (ETS price signal)
    fk["investment_score"] = (score_env + score_ren + score_air + score_ets).clip(0.0, 100.0).round(2)
    fk["investment_recommendation"] = fk["investment_score"].apply(
        lambda s: (
            "STRONG_BUY" if s >= 75
            else ("BUY" if s >= 55
                  else ("HOLD" if s >= 35
                        else "AVOID"))
        )
    )

    print(f"  fact_kuznets preparado: {len(fk):,} filas (ciudad × año × mes)")

    # ------------------------------------------------------------------
    # INSERT — columnas exactas del DW DDL (mariadb_dw_ddl.sql)
    # PK: (city_sk, year, month)
    # cluster_sk NOT NULL → placeholder CLUSTER_SK_DEFAULT
    # ------------------------------------------------------------------
    sql = """
        INSERT INTO fact_kuznets
            (city_sk, cluster_sk, date_sk, year, month,
             city_code, city_name, country_code, country_name,
             ndvi_mean, ndvi_trend_slope, ndvi_yoy_change,
             no2_mean, imperviousness_mean, green_index,
             temp_mean_c, co2_country_kt,
             lst_day_c, lst_night_c, ntl_mean, pm25_mean, pm10_mean,
             gdp_pps_per_capita, ln_gdp_pps, ln_gdp_pps_sq,
             gdp_growth_rate, fua_population,
             renewables_pct, tourism_nights, ets_price_eur,
             cluster_id, cluster_label, cluster_silhouette, distance_to_centroid,
             ekc_beta1, ekc_beta2, ekc_alpha,
             ekc_turning_point_y, ekc_r_squared, ekc_shape,
             turning_point_phase, phase_confidence, gdp_gap_to_turning,
             prophet_ndvi_forecast_1y, prophet_ndvi_forecast_3y, prophet_ndvi_forecast_5y,
             prophet_turning_year, prophet_forecast_lower_95, prophet_forecast_upper_95,
             eps_index, env_tax_usd, investeu_total_eur,
             fin_tickers, fin_close_price_avg, fin_annual_volatility,
             investment_score, investment_recommendation,
             _gold_load_date)
        VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,
            %s,%s,
            %s
        )
        ON DUPLICATE KEY UPDATE
            city_code=VALUES(city_code),
            ndvi_mean=VALUES(ndvi_mean),
            ndvi_trend_slope=VALUES(ndvi_trend_slope),
            ndvi_yoy_change=VALUES(ndvi_yoy_change),
            no2_mean=VALUES(no2_mean),
            green_index=VALUES(green_index),
            temp_mean_c=VALUES(temp_mean_c),
            co2_country_kt=VALUES(co2_country_kt),
            lst_day_c=VALUES(lst_day_c),
            lst_night_c=VALUES(lst_night_c),
            ntl_mean=VALUES(ntl_mean),
            pm25_mean=VALUES(pm25_mean),
            pm10_mean=VALUES(pm10_mean),
            gdp_pps_per_capita=VALUES(gdp_pps_per_capita),
            ln_gdp_pps=VALUES(ln_gdp_pps),
            ln_gdp_pps_sq=VALUES(ln_gdp_pps_sq),
            renewables_pct=VALUES(renewables_pct),
            tourism_nights=VALUES(tourism_nights),
            ets_price_eur=VALUES(ets_price_eur),
            eps_index=VALUES(eps_index),
            investeu_total_eur=VALUES(investeu_total_eur),
            investment_score=VALUES(investment_score),
            investment_recommendation=VALUES(investment_recommendation),
            _gold_load_date=VALUES(_gold_load_date)
    """

    rows = []
    for _, row in fk.iterrows():
        city_sk = safe_int(row.get("city_sk"))
        if city_sk is None:
            continue
        rows.append((
            # PK + required
            city_sk,
            CLUSTER_SK_DEFAULT,
            safe_int(row.get("date_sk")),
            safe_int(row.get("year")),
            safe_int(row.get("month")),
            # city metadata
            safe_str(row.get("city_code")),
            safe_str(row.get("city_name")),
            safe_str(row.get("country_code")),
            safe_str(row.get("country_name")),
            # environmental
            safe_float(row.get("ndvi_mean")),
            safe_float(row.get("ndvi_trend_slope")),
            safe_float(row.get("ndvi_yoy_change")),
            safe_float(row.get("no2_mean")),
            safe_float(row.get("imperviousness_mean")),
            safe_float(row.get("green_index")),
            safe_float(row.get("temp_mean_c")),
            safe_float(row.get("co2_country_kt")),
            safe_float(row.get("lst_day_c")),
            safe_float(row.get("lst_night_c")),
            safe_float(row.get("ntl_mean")),
            safe_float(row.get("pm25_mean")),
            safe_float(row.get("pm10_mean")),
            # economic
            safe_float(row.get("gdp_pps_per_capita")),
            safe_float(row.get("ln_gdp_pps")),
            safe_float(row.get("ln_gdp_pps_sq")),
            safe_float(row.get("gdp_growth_rate")),
            safe_int(row.get("fua_population")),
            safe_float(row.get("renewables_pct")),
            safe_float(row.get("tourism_nights")),
            safe_float(row.get("ets_price_eur")),
            # clustering placeholders (NULL until models run)
            None, None, None, None,
            # EKC placeholders
            None, None, None,
            None, None, "PENDING",
            # XGBoost placeholders
            "PENDING", None, None,
            # Prophet placeholders
            None, None, None,
            None, None, None,
            # policy
            safe_float(row.get("eps_index")),
            safe_float(row.get("env_tax_usd")),
            safe_float(row.get("investeu_total_eur")),
            # financial
            safe_str(row.get("fin_tickers")),
            safe_float(row.get("fin_close_price_avg")),
            safe_float(row.get("fin_annual_volatility")),
            # score
            safe_float(row.get("investment_score")),
            safe_str(row.get("investment_recommendation", "AVOID")),
            GOLD_LOAD_DATE,
        ))

    batch_upsert(pool, sql, rows)
    print(f"  [GOLD] fact_kuznets: {len(rows)} filas escritas en MariaDB")
    return True


# ==============================================================================
# TABLA 2: CITY_RANKING
# PK: (city_sk, year, month)  — rank_change MoM (vs mes anterior)
# ==============================================================================
def build_city_ranking(pool):
    print("\n[GOLD 2/3] Construyendo city_ranking ...")

    import pandas as pd

    fk_rows = read_silver(pool, """
        SELECT fk.city_sk, fk.date_sk, fk.year, fk.month,
               fk.city_code, fk.city_name, fk.country_code, fk.country_name,
               fk.investment_score, fk.investment_recommendation,
               fk.green_index, fk.turning_point_phase, fk.prophet_turning_year,
               fk.fin_tickers
        FROM fact_kuznets fk
        ORDER BY fk.year, fk.month, fk.investment_score DESC
    """)

    if not fk_rows:
        print("  [SKIP] fact_kuznets vacía.")
        return

    df = pd.DataFrame(fk_rows)
    df["year"]  = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    # Rank global dentro de cada (year, month)
    df = df.sort_values(["year", "month", "investment_score"], ascending=[True, True, False])
    df["rank_position"] = (
        df.groupby(["year", "month"])["investment_score"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    # Rank dentro del país
    df["rank_within_country"] = (
        df.groupby(["year", "month", "country_code"])["investment_score"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    # rank_change MoM (positivo = mejora = rank_position bajó)
    df = df.sort_values(["city_sk", "year", "month"])
    df["rank_change"] = (
        df.groupby("city_sk")["rank_position"]
        .diff()
        .apply(lambda x: -int(x) if x == x else None)
    )
    df["score_change"] = (
        df.groupby("city_sk")["investment_score"]
        .diff()
        .apply(safe_float)
    )

    # top_ticker / top_company (primer elemento de la lista CSV)
    def _first(s, max_len):
        if s and isinstance(s, str):
            return s.split(",")[0].strip()[:max_len]
        return None

    df["top_ticker"]  = df["fin_tickers"].apply(lambda s: _first(s, 20))
    df["top_company"] = df.get("fin_companies", pd.Series(None, index=df.index))

    sql = """
        INSERT INTO city_ranking
            (city_sk, date_sk, year, month,
             rank_position, rank_within_country,
             city_code, city_name, country_code, country_name,
             investment_score, investment_recommendation,
             green_index, turning_point_phase,
             prophet_turning_year,
             rank_change, score_change,
             top_ticker, top_company,
             _gold_load_date)
        VALUES (%s,%s,%s,%s, %s,%s, %s,%s,%s,%s, %s,%s, %s,%s, %s, %s,%s, %s,%s, %s)
        ON DUPLICATE KEY UPDATE
            rank_position=VALUES(rank_position),
            rank_within_country=VALUES(rank_within_country),
            investment_score=VALUES(investment_score),
            investment_recommendation=VALUES(investment_recommendation),
            rank_change=VALUES(rank_change),
            score_change=VALUES(score_change),
            _gold_load_date=VALUES(_gold_load_date)
    """

    rows = []
    for _, row in df.iterrows():
        city_sk = safe_int(row["city_sk"])
        if city_sk is None:
            continue
        rc = row.get("rank_change")
        rows.append((
            city_sk,
            safe_int(row["date_sk"]),
            safe_int(row["year"]),
            safe_int(row["month"]),
            safe_int(row["rank_position"]),
            safe_int(row["rank_within_country"]),
            safe_str(row["city_code"]),
            safe_str(row.get("city_name")),
            safe_str(row.get("country_code")),
            safe_str(row.get("country_name")),
            safe_float(row["investment_score"]),
            safe_str(row.get("investment_recommendation", "AVOID")),
            safe_float(row.get("green_index")),
            safe_str(row.get("turning_point_phase", "PENDING")),
            safe_int(row.get("prophet_turning_year")),
            safe_int(rc) if (rc is not None and rc == rc) else None,
            safe_float(row.get("score_change")),
            safe_str(row.get("top_ticker"), 20),
            safe_str(row.get("top_company"), 255),
            GOLD_LOAD_DATE,
        ))

    batch_upsert(pool, sql, rows)
    print(f"  [GOLD] city_ranking: {len(rows)} filas escritas en MariaDB")


# ==============================================================================
# TABLA 3: MODEL_RESULTS — placeholder vacío (lo llenan los modelos ML)
# ==============================================================================
def build_model_results_placeholder(pool):
    print("\n[GOLD 3/3] model_results: placeholder (vacía hasta que corran los modelos ML)")
    pass


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP Gold Build → MariaDB")
    parser.add_argument("--table", default="all",
                        choices=["all", "fact_kuznets", "city_ranking", "model_results"],
                        help="Tabla a construir (default: all)")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — GOLD BUILD (MariaDB DW schema — monthly)")
    print(f"  Inicio: {GOLD_LOAD_DATE}")
    print(f"  Fuente: MariaDB Silver (fact_environmental, fact_economic, fact_policy, fact_financial)")
    print("=" * 65)

    pool = get_pool()

    ok = True
    if args.table in ("all", "fact_kuznets"):
        ok = build_fact_kuznets(pool)

    if ok and args.table in ("all", "city_ranking"):
        build_city_ranking(pool)

    if args.table in ("all", "model_results"):
        build_model_results_placeholder(pool)

    print("\n" + "=" * 65)
    print("  GOLD BUILD COMPLETADO → MariaDB")
    print("=" * 65)


if __name__ == "__main__":
    main()
