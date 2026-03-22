"""
prophet_forecast.py — GTP (Green Turning Point) | Modelo 4: Prophet Time Series
Proyecta el indice NDVI de cada ciudad 1, 3 y 5 anos hacia el futuro.

Lee fact_environmental desde MariaDB (serie NDVI mensual por ciudad).
Ejecuta Prophet en serie por ciudad (datos pequenos: ~237 ciudades * ~80 meses).
Actualiza MariaDB:
  - fact_kuznets: prophet_ndvi_forecast_1y/3y/5y, prophet_turning_year,
                  prophet_forecast_lower_95, prophet_forecast_upper_95
  - model_results: prophet_ndvi_fitted, prophet_trend, prophet_seasonality,
                   prophet_forecast_*, prophet_mape

Ejecucion:
  python models/prophet_forecast.py [--horizon 5] [--min-months 24]
"""

import sys
import argparse
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, date, timezone
from pathlib import Path

warnings.filterwarnings("ignore")

# ==============================================================================
# CONFIGURACION
# ==============================================================================
CONFIG_FILE   = str(Path(__file__).resolve().parent.parent.parent / "config.ini")
MODEL_VERSION = "1.0"
MODEL_RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

MIN_MONTHS_PROPHET = 24


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
# CARGA DE SERIE NDVI MENSUAL DESDE MARIADB
# ==============================================================================
def load_monthly_series(pool):
    print("  Cargando serie NDVI mensual desde fact_environmental ...")

    rows = pool.execute_query("""
        SELECT fe.city_sk, dc.city_code, fe.year, fe.month, fe.ndvi_mean
        FROM fact_environmental fe
        JOIN dim_city dc ON fe.city_sk = dc.city_sk
        WHERE fe.ndvi_mean IS NOT NULL
        ORDER BY fe.city_sk, fe.year, fe.month
    """)

    if not rows:
        print("  [SKIP] fact_environmental vacia.")
        return None

    df = pd.DataFrame(rows)
    df["ds"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
    )
    print(f"  {len(df):,} observaciones | {df['city_sk'].nunique()} ciudades")
    return df


# ==============================================================================
# PROPHET POR CIUDAD
# ==============================================================================
def fit_prophet_for_city(city_df: pd.DataFrame, city_sk: int,
                          horizon_years: int = 5, min_months: int = MIN_MONTHS_PROPHET):
    """Ajusta Prophet a la serie NDVI de una ciudad. Devuelve dict de resultados."""
    if len(city_df) < min_months:
        return None

    try:
        from prophet import Prophet
    except ImportError:
        print("  [ERROR] prophet no instalado: pip install prophet")
        return None

    df_prophet = (
        city_df[["ds", "ndvi_mean"]]
        .rename(columns={"ndvi_mean": "y"})
        .sort_values("ds")
        .dropna(subset=["y"])
    )

    if len(df_prophet) < min_months:
        return None

    try:
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            seasonality_mode="multiplicative",
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=5.0,
            interval_width=0.95,
            growth="linear",
        )
        model.fit(df_prophet)

        future   = model.make_future_dataframe(periods=horizon_years * 12 + 2, freq="MS")
        forecast = model.predict(future)

        # In-sample
        in_sample = forecast[forecast["ds"].isin(df_prophet["ds"])].copy()
        actual    = df_prophet.set_index("ds")["y"]
        fitted    = in_sample.set_index("ds")["yhat"]
        common    = actual.index.intersection(fitted.index)
        if len(common) > 0:
            # clip(lower=0.05) evita MAPE explosivo en ciudades con NDVI
            # bajo o negativo (costeras, muy urbanizadas).
            mape = float(
                np.abs((actual.loc[common] - fitted.loc[common]) /
                       actual.loc[common].clip(lower=0.05)).mean() * 100
            )
        else:
            mape = None

        # Forecast horizons
        current_date = pd.Timestamp(MODEL_RUN_DATE)
        future_only  = forecast[forecast["ds"] > df_prophet["ds"].max()].copy()

        def forecast_at(months_ahead):
            target = current_date + pd.DateOffset(months=months_ahead)
            row    = future_only[future_only["ds"] >= target].head(1)
            if len(row) == 0:
                return None, None, None
            return float(row["yhat"].values[0]), float(row["yhat_lower"].values[0]), float(row["yhat_upper"].values[0])

        f1y_mid, f1y_lo, f1y_hi   = forecast_at(12)
        f3y_mid, f3y_lo, f3y_hi   = forecast_at(36)
        f5y_mid, f5y_lo, f5y_hi   = forecast_at(60)
        f10y_mid, f10y_lo, f10y_hi = forecast_at(120)

        # Turning year: primer anio futuro donde la tendencia deja de caer
        if len(future_only) > 0:
            future_annual = (
                future_only
                .assign(year=future_only["ds"].dt.year)
                .groupby("year")["trend"]
                .mean()
                .reset_index()
                .sort_values("year")
            )
            future_annual["trend_diff"] = future_annual["trend"].diff()
            turning_rows = future_annual[future_annual["trend_diff"] > 0]
            turning_year = safe_int(turning_rows["year"].iloc[0]) if len(turning_rows) > 0 else None
        else:
            turning_year = None

        # In-sample components por mes (para model_results)
        in_sample_by_ym = {}
        for _, row in in_sample.iterrows():
            ym = (row["ds"].year, row["ds"].month)
            in_sample_by_ym[ym] = {
                "prophet_trend":       safe_float(row.get("trend")),
                "prophet_seasonality": safe_float(row.get("yearly", row.get("seasonal"))),
                "prophet_fitted":      safe_float(row.get("yhat")),
            }

        return {
            "city_sk":                 city_sk,
            "mape":                    mape,
            "forecast_1y":             f1y_mid,
            "forecast_3y":             f3y_mid,
            "forecast_5y":             f5y_mid,
            "forecast_10y":            f10y_mid,
            "forecast_lower_95_1y":    f1y_lo,
            "forecast_upper_95_1y":    f1y_hi,
            "forecast_lower_95_5y":    f5y_lo,
            "forecast_upper_95_5y":    f5y_hi,
            "forecast_lower_95_10y":   f10y_lo,
            "forecast_upper_95_10y":   f10y_hi,
            "turning_year":            turning_year,
            "in_sample_by_ym":         in_sample_by_ym,
        }

    except Exception as e:
        print(f"    [WARN] Prophet fallo para city_sk={city_sk}: {e}")
        return None


# ==============================================================================
# ACTUALIZAR MARIADB CON RESULTADOS PROPHET
# ==============================================================================
def update_mariadb(pool, results_by_city: dict):
    print("\n  Actualizando MariaDB con resultados Prophet ...")

    # Actualizar fact_kuznets con forecasts (para todos los meses de cada anio)
    fk_update_sql = """
        UPDATE fact_kuznets
        SET prophet_ndvi_forecast_1y=%s,
            prophet_ndvi_forecast_3y=%s,
            prophet_ndvi_forecast_5y=%s,
            prophet_ndvi_forecast_10y=%s,
            prophet_turning_year=%s,
            prophet_forecast_lower_95=%s,
            prophet_forecast_upper_95=%s
        WHERE city_sk=%s
    """
    for city_sk, r in results_by_city.items():
        pool.execute_write(
            fk_update_sql,
            (
                safe_float(r.get("forecast_1y")),
                safe_float(r.get("forecast_3y")),
                safe_float(r.get("forecast_5y")),
                safe_float(r.get("forecast_10y")),
                safe_int(r.get("turning_year")),
                safe_float(r.get("forecast_lower_95_5y")),
                safe_float(r.get("forecast_upper_95_5y")),
                int(city_sk),
            )
        )

    print(f"  [FACT_KUZNETS] Prophet: {len(results_by_city)} ciudades actualizadas")

    # Actualizar model_results con componentes in-sample
    mr_sql = """
        INSERT INTO model_results
            (city_sk, year, month,
             prophet_ndvi_fitted, prophet_trend, prophet_seasonality,
             prophet_forecast_1y, prophet_forecast_3y, prophet_forecast_5y, prophet_forecast_10y,
             prophet_lower_95_5y, prophet_upper_95_5y,
             prophet_lower_95_10y, prophet_upper_95_10y,
             prophet_turning_year, prophet_mape,
             _model_run_date, _pipeline_version)
        VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s, %s,%s, %s,%s, %s,%s)
        ON DUPLICATE KEY UPDATE
            prophet_ndvi_fitted=VALUES(prophet_ndvi_fitted),
            prophet_trend=VALUES(prophet_trend),
            prophet_seasonality=VALUES(prophet_seasonality),
            prophet_forecast_1y=VALUES(prophet_forecast_1y),
            prophet_forecast_3y=VALUES(prophet_forecast_3y),
            prophet_forecast_5y=VALUES(prophet_forecast_5y),
            prophet_forecast_10y=VALUES(prophet_forecast_10y),
            prophet_lower_95_5y=VALUES(prophet_lower_95_5y),
            prophet_upper_95_5y=VALUES(prophet_upper_95_5y),
            prophet_lower_95_10y=VALUES(prophet_lower_95_10y),
            prophet_upper_95_10y=VALUES(prophet_upper_95_10y),
            prophet_turning_year=VALUES(prophet_turning_year),
            prophet_mape=VALUES(prophet_mape),
            _model_run_date=VALUES(_model_run_date)
    """
    mr_rows = []
    for city_sk, r in results_by_city.items():
        in_sample = r.get("in_sample_by_ym", {})
        # Obtener los (year, month) disponibles en fact_environmental
        env_rows = pool.execute_query(
            "SELECT year, month FROM fact_environmental WHERE city_sk=%s ORDER BY year, month",
            (int(city_sk),)
        )
        for env_row in env_rows:
            year  = env_row["year"]
            month = env_row["month"]
            ym    = (year, month)
            comp  = in_sample.get(ym, {})
            mr_rows.append((
                int(city_sk), year, month,
                safe_float(comp.get("prophet_fitted")),
                safe_float(comp.get("prophet_trend")),
                safe_float(comp.get("prophet_seasonality")),
                safe_float(r.get("forecast_1y")),
                safe_float(r.get("forecast_3y")),
                safe_float(r.get("forecast_5y")),
                safe_float(r.get("forecast_10y")),
                safe_float(r.get("forecast_lower_95_5y")),
                safe_float(r.get("forecast_upper_95_5y")),
                safe_float(r.get("forecast_lower_95_10y")),
                safe_float(r.get("forecast_upper_95_10y")),
                safe_int(r.get("turning_year")),
                safe_float(r.get("mape")),
                MODEL_RUN_DATE, MODEL_VERSION,
            ))

    if mr_rows:
        pool.execute_many(mr_sql, mr_rows)
    print(f"  [MODEL_RESULTS] Prophet: {len(mr_rows)} filas actualizadas")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP Prophet Time Series Forecast")
    parser.add_argument("--horizon",    type=int, default=5,
                        help="Anos de forecast (default: 5)")
    parser.add_argument("--min-months", type=int, default=MIN_MONTHS_PROPHET,
                        help=f"Minimo meses por ciudad (default: {MIN_MONTHS_PROPHET})")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — MODELO 4: PROPHET TIME SERIES FORECAST")
    print(f"  Horizonte: {args.horizon} anos | Min. meses: {args.min_months}")
    print(f"  Fuente: MariaDB Silver (fact_environmental)")
    print("=" * 65)

    pool = get_pool()

    # 1. Cargar serie mensual
    monthly_df = load_monthly_series(pool)
    if monthly_df is None:
        print("[SKIP] Prophet omitido.")
        return

    # 2. Ajustar Prophet por ciudad
    print(f"\n  Ejecutando Prophet para {monthly_df['city_sk'].nunique()} ciudades ...")
    results_by_city = {}
    n_ok = n_skip = 0

    for city_sk, city_df in monthly_df.groupby("city_sk"):
        city_code = city_df["city_code"].iloc[0]
        result = fit_prophet_for_city(
            city_df, city_sk,
            horizon_years=args.horizon,
            min_months=args.min_months
        )
        if result:
            results_by_city[city_sk] = result
            n_ok += 1
        else:
            n_skip += 1
            if n_skip <= 5:
                print(f"    [SKIP] {city_code}: insuficientes datos (<{args.min_months} meses)")

    print(f"\n  Resultados: {n_ok} ciudades con forecast | {n_skip} sin datos suficientes")

    # 3. Actualizar MariaDB
    if results_by_city:
        update_mariadb(pool, results_by_city)

    # 4. Persistir fact_kuznets y model_results en HDFS Gold
    from hdfs_writer import write_df_to_hdfs
    fk_rows = pool.execute_query("""
        SELECT fk.*, dc.country_code AS country
        FROM fact_kuznets fk
        JOIN dim_city dc ON fk.city_sk = dc.city_sk
    """)
    if fk_rows:
        write_df_to_hdfs(pd.DataFrame(fk_rows),
                         "hdfs:///user/gtp/gold/fact_kuznets/",
                         partition_cols=["country"])
    mr_rows = pool.execute_query("SELECT * FROM model_results")
    if mr_rows:
        write_df_to_hdfs(pd.DataFrame(mr_rows),
                         "hdfs:///user/gtp/gold/model_results/",
                         partition_cols=["year"])

    # 5. Resumen
    if results_by_city:
        mapes = [(c, r.get("mape") or 999) for c, r in results_by_city.items() if r.get("mape")]
        mapes.sort(key=lambda x: x[1])
        print("\n  Top 10 ciudades con mejor ajuste Prophet (menor MAPE):")
        for city_sk, mape in mapes[:10]:
            r   = results_by_city[city_sk]
            tp  = r.get("turning_year") or "N/A"
            f5y = safe_float(r.get("forecast_5y"))
            print(f"    city_sk={city_sk}  MAPE={mape:.2f}%  TurningYear={tp}  NDVI_5y={f5y}")

    print("\n" + "=" * 65)
    print("  PROPHET FORECAST COMPLETADO")
    print("=" * 65)


if __name__ == "__main__":
    main()
