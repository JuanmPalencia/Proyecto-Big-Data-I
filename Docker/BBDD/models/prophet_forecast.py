"""
prophet_forecast.py — GTP (Green Turning Point) | Modelo 4: Prophet Time Series
Proyecta el índice NDVI de cada ciudad 1, 3 y 5 años hacia el futuro.

Por qué Prophet:
  - Diseñado para series temporales con estacionalidad y tendencia.
  - Robusto a datos faltantes (sin necesidad de interpolación manual).
  - Genera intervalos de confianza calibrados (IC 95%).
  - No requiere estacionariedad (a diferencia de ARIMA).

Estrategia en Spark:
  - Los datos (237 ciudades × ~8 años × 12 meses = ~22k filas) caben en RAM.
  - Usamos pandas UDF (Grouped Map) sobre Spark para paralelizar el ajuste:
    una función Python por ciudad, distribuida entre workers.
  - Esto aprovecha el paralelismo de Lorca sin mover datos innecesariamente.

Estimación del Turning Year:
  El año en que la tendencia de NDVI se estabiliza (derivada ≈ 0).
  Se detecta buscando el año futuro donde el forecast cambia de signo
  en su primera diferencia (de bajada a subida, o de bajada a plano).

Output:
  - Actualiza Gold fact_kuznets con forecast_1y/3y/5y y prophet_turning_year
  - Actualiza Gold model_results con componentes Prophet completas

Ejecución:
  spark-submit --master yarn models/prophet_forecast.py [--horizon 5]
"""

import sys
import argparse
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timezone

warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, DateType
)

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HDFS_SILVER     = "hdfs:///user/gtp/silver"
HDFS_GOLD       = "hdfs:///user/gtp/gold"
HDFS_MODELS     = "hdfs:///user/gtp/models/prophet"
MODEL_VERSION   = "1.0"
MODEL_RUN_DATE  = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Mínimo de meses para que Prophet tenga sentido
MIN_MONTHS_PROPHET = 24

# ==============================================================================
# SPARK
# ==============================================================================
def get_spark():
    return (
        SparkSession.builder
        .appName("GTP_Prophet_Forecast")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )

# ==============================================================================
# CARGAR SERIE TEMPORAL MENSUAL DESDE BRONZE
# ==============================================================================
def load_monthly_series(spark):
    print("  Cargando serie NDVI mensual desde Bronze sentinel2_raw ...")

    try:
        bronze = spark.read.parquet("hdfs:///user/gtp/bronze/sentinel2_raw")
    except Exception as e:
        print(f"[SKIP] sentinel2_raw no disponible en Bronze (prophet): {e}")
        return None
    required = ["city", "year", "month", "ndvi_mean"]
    existing = [c for c in required if c in bronze.columns]

    df = (
        bronze
        .select(existing)
        .filter(F.col("ndvi_mean").isNotNull())
        .filter(F.col("year").isNotNull() & F.col("month").isNotNull())
    )

    # Construir columna de fecha mensual (primer día del mes)
    df = df.withColumn(
        "ds",
        F.to_date(
            F.concat(F.col("year").cast(StringType()), F.lit("-"),
                     F.lpad(F.col("month").cast(StringType()), 2, "0"), F.lit("-01"))
        )
    )

    n = df.count()
    n_cities = df.select("city").distinct().count()
    print(f"  {n:,} observaciones mensuales | {n_cities} ciudades")
    return df


# ==============================================================================
# FUNCIÓN PROPHET POR CIUDAD (pandas UDF — Grouped Map)
# Esta función se ejecuta en paralelo en cada worker de Spark.
# ==============================================================================
def fit_prophet_city(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe el DataFrame de UNA ciudad (todas sus observaciones mensuales).
    Ajusta Prophet, genera forecast, y devuelve un DataFrame con resultados.
    """
    try:
        from prophet import Prophet
    except ImportError:
        # Si Prophet no está instalado, devolver resultado vacío
        empty = pdf.copy()
        for col in ["prophet_trend", "prophet_seasonality", "prophet_fitted",
                    "prophet_forecast_1y", "prophet_forecast_3y", "prophet_forecast_5y",
                    "prophet_lower_95_1y", "prophet_upper_95_1y",
                    "prophet_lower_95_5y", "prophet_upper_95_5y",
                    "prophet_turning_year", "prophet_mape"]:
            empty[col] = None
        return empty

    city_code = pdf["city"].iloc[0]
    n         = len(pdf)

    if n < MIN_MONTHS_PROPHET:
        # No hay suficientes datos — devolver vacío
        empty = pdf[["city", "year", "month", "ds", "ndvi_mean"]].copy()
        for col in ["prophet_trend", "prophet_seasonality", "prophet_fitted",
                    "prophet_forecast_1y", "prophet_forecast_3y", "prophet_forecast_5y",
                    "prophet_lower_95_1y", "prophet_upper_95_1y",
                    "prophet_lower_95_5y", "prophet_upper_95_5y",
                    "prophet_turning_year", "prophet_mape"]:
            empty[col] = None
        return empty

    # Preparar formato Prophet (ds, y)
    df_prophet = pdf[["ds", "ndvi_mean"]].rename(columns={"ndvi_mean": "y"}).sort_values("ds")
    df_prophet = df_prophet.dropna(subset=["y"])

    try:
        # Configuración Prophet
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            seasonality_mode="multiplicative",  # NDVI es siempre positivo
            changepoint_prior_scale=0.05,       # Regularización conservadora (pocos datos)
            seasonality_prior_scale=5.0,
            interval_width=0.95,
            growth="linear",
        )
        model.fit(df_prophet)

        # Horizonte de forecast: 60 meses (5 años)
        future = model.make_future_dataframe(periods=60, freq="MS")
        forecast = model.predict(future)

        # In-sample fitted values
        in_sample = forecast[forecast["ds"].isin(df_prophet["ds"])].copy()
        mape = (
            np.abs((df_prophet.set_index("ds")["y"] -
                    in_sample.set_index("ds")["yhat"]) /
                   df_prophet.set_index("ds")["y"].clip(lower=1e-6))
            .mean() * 100
        )

        # Fechas de forecast futuro
        future_only = forecast[~forecast["ds"].isin(df_prophet["ds"])].copy()
        current_date = pd.Timestamp(MODEL_RUN_DATE)

        def get_forecast_at_months(months_ahead):
            target_date = current_date + pd.DateOffset(months=months_ahead)
            row = future_only[future_only["ds"] >= target_date].head(1)
            if len(row) == 0:
                return None, None, None
            return row["yhat"].values[0], row["yhat_lower"].values[0], row["yhat_upper"].values[0]

        fc_1y_mid, fc_1y_lo, fc_1y_hi = get_forecast_at_months(12)
        fc_3y_mid, fc_3y_lo, fc_3y_hi = get_forecast_at_months(36)
        fc_5y_mid, fc_5y_lo, fc_5y_hi = get_forecast_at_months(60)

        # Estimar Turning Year:
        # Año en el que la tendencia (trend component) deja de decrecer.
        # Buscamos el primer año futuro donde trend_diff > 0 (de caída a subida).
        future_yearly = (
            future_only
            .assign(year=future_only["ds"].dt.year)
            .groupby("year")
            .agg(trend_mean=("trend", "mean"))
            .reset_index()
            .sort_values("year")
        )
        future_yearly["trend_diff"] = future_yearly["trend_mean"].diff()
        turning_rows = future_yearly[future_yearly["trend_diff"] > 0]
        turning_year = int(turning_rows["year"].iloc[0]) if len(turning_rows) > 0 else None

        # Unir con los datos originales
        result = pdf[["city", "year", "month", "ds", "ndvi_mean"]].copy()
        result["prophet_mape"] = round(float(mape), 4)
        result["prophet_forecast_1y"]   = round(float(fc_1y_mid), 6) if fc_1y_mid is not None else None
        result["prophet_forecast_3y"]   = round(float(fc_3y_mid), 6) if fc_3y_mid is not None else None
        result["prophet_forecast_5y"]   = round(float(fc_5y_mid), 6) if fc_5y_mid is not None else None
        result["prophet_lower_95_1y"]   = round(float(fc_1y_lo), 6)  if fc_1y_lo  is not None else None
        result["prophet_upper_95_1y"]   = round(float(fc_1y_hi), 6)  if fc_1y_hi  is not None else None
        result["prophet_lower_95_5y"]   = round(float(fc_5y_lo), 6)  if fc_5y_lo  is not None else None
        result["prophet_upper_95_5y"]   = round(float(fc_5y_hi), 6)  if fc_5y_hi  is not None else None
        result["prophet_turning_year"]  = turning_year

        # Componentes in-sample por fila (trend + seasonality)
        in_sample_idx = in_sample.set_index("ds")
        result["ds_str"] = result["ds"].astype(str)
        in_sample_idx.index = in_sample_idx.index.astype(str)
        result["prophet_trend"]       = result["ds_str"].map(in_sample_idx["trend"])
        result["prophet_seasonality"] = result["ds_str"].map(
            in_sample_idx.get("yearly", pd.Series(dtype=float))
        )
        result["prophet_fitted"]      = result["ds_str"].map(in_sample_idx["yhat"])
        result = result.drop(columns=["ds_str"], errors="ignore")

        return result

    except Exception as e:
        # Si Prophet falla para esta ciudad, devolver NaN en columnas forecast
        result = pdf[["city", "year", "month", "ds", "ndvi_mean"]].copy()
        for col in ["prophet_trend", "prophet_seasonality", "prophet_fitted",
                    "prophet_forecast_1y", "prophet_forecast_3y", "prophet_forecast_5y",
                    "prophet_lower_95_1y", "prophet_upper_95_1y",
                    "prophet_lower_95_5y", "prophet_upper_95_5y",
                    "prophet_turning_year", "prophet_mape"]:
            result[col] = None
        return result


# Schema de retorno del pandas UDF
PROPHET_OUTPUT_SCHEMA = StructType([
    StructField("city",                StringType(),  True),
    StructField("year",                IntegerType(), True),
    StructField("month",               IntegerType(), True),
    StructField("ds",                  DateType(),    True),
    StructField("ndvi_mean",           DoubleType(),  True),
    StructField("prophet_mape",        DoubleType(),  True),
    StructField("prophet_forecast_1y", DoubleType(),  True),
    StructField("prophet_forecast_3y", DoubleType(),  True),
    StructField("prophet_forecast_5y", DoubleType(),  True),
    StructField("prophet_lower_95_1y", DoubleType(),  True),
    StructField("prophet_upper_95_1y", DoubleType(),  True),
    StructField("prophet_lower_95_5y", DoubleType(),  True),
    StructField("prophet_upper_95_5y", DoubleType(),  True),
    StructField("prophet_turning_year",IntegerType(), True),
    StructField("prophet_trend",       DoubleType(),  True),
    StructField("prophet_seasonality", DoubleType(),  True),
    StructField("prophet_fitted",      DoubleType(),  True),
])


# ==============================================================================
# ACTUALIZAR GOLD CON RESULTADOS PROPHET
# ==============================================================================
def update_gold(spark, prophet_results_df):
    print("\n  Actualizando Gold con resultados Prophet ...")

    # Agregar forecast a nivel año (tomamos el valor del último mes del año)
    annual = (
        prophet_results_df
        .filter(F.col("month") == 12)
        .select(
            "city", "year",
            "prophet_forecast_1y", "prophet_forecast_3y", "prophet_forecast_5y",
            "prophet_lower_95_5y", "prophet_upper_95_5y",
            "prophet_turning_year", "prophet_mape",
            "prophet_trend", "prophet_seasonality", "prophet_fitted",
        )
        .withColumnRenamed("city", "city_code")
    )

    # Si no hay datos de diciembre para algún año, usar el máximo mes disponible
    max_month = (
        prophet_results_df
        .groupBy("city", "year")
        .agg(F.max("month").alias("max_month"))
    )
    fallback = (
        prophet_results_df
        .join(max_month, on=["city", "year"])
        .filter(F.col("month") == F.col("max_month"))
        .select(
            F.col("city").alias("city_code"), "year",
            "prophet_forecast_1y", "prophet_forecast_3y", "prophet_forecast_5y",
            "prophet_lower_95_5y", "prophet_upper_95_5y",
            "prophet_turning_year", "prophet_mape",
            "prophet_trend", "prophet_seasonality", "prophet_fitted",
        )
    )
    # Usar annual donde esté disponible, fallback para el resto
    annual_full = annual.unionByName(
        fallback.join(annual.select("city_code", "year"), on=["city_code", "year"], how="left_anti")
    )

    # --- Actualizar fact_kuznets ---
    try:
        fk = spark.read.parquet(f"{HDFS_GOLD}/fact_kuznets")
    except Exception as e:
        print(f"[SKIP] fact_kuznets no disponible para actualización Prophet: {e}")
        return

    drop_prophet = [
        "prophet_ndvi_forecast_1y", "prophet_ndvi_forecast_3y", "prophet_ndvi_forecast_5y",
        "prophet_turning_year", "prophet_forecast_lower_95", "prophet_forecast_upper_95"
    ]
    fk_clean = fk.drop(*[c for c in drop_prophet if c in fk.columns])

    fk_updated = fk_clean.join(
        annual_full.select(
            "city_code", "year",
            F.col("prophet_forecast_1y").alias("prophet_ndvi_forecast_1y"),
            F.col("prophet_forecast_3y").alias("prophet_ndvi_forecast_3y"),
            F.col("prophet_forecast_5y").alias("prophet_ndvi_forecast_5y"),
            "prophet_turning_year",
            F.col("prophet_lower_95_5y").alias("prophet_forecast_lower_95"),
            F.col("prophet_upper_95_5y").alias("prophet_forecast_upper_95"),
        ),
        on=["city_code", "year"],
        how="left"
    )

    # Materializar antes del overwrite para evitar FileNotFoundException por self-overwrite
    # (Spark lazy evaluation + mode="overwrite" en el mismo path fuente puede borrar
    # archivos antes de haberlos leído para ejecutar la transformación)
    fk_updated = fk_updated.cache()
    fk_updated.count()

    (
        fk_updated.write
        .mode("overwrite")
        .partitionBy("year", "country")
        .parquet(f"{HDFS_GOLD}/fact_kuznets")
    )
    fk_updated.unpersist()
    print("  [GOLD] fact_kuznets actualizado con forecast Prophet")

    # --- Actualizar model_results ---
    try:
        mr = spark.read.parquet(f"{HDFS_GOLD}/model_results")
        drop_cols_mr = [
            "prophet_ndvi_fitted", "prophet_trend", "prophet_seasonality",
            "prophet_forecast_1y", "prophet_forecast_3y", "prophet_forecast_5y",
            "prophet_lower_95_5y", "prophet_upper_95_5y",
            "prophet_turning_year", "prophet_mape"
        ]
        mr_clean = mr.drop(*[c for c in drop_cols_mr if c in mr.columns])
        mr_updated = mr_clean.join(
            annual_full.withColumnRenamed("city_code", "city_code_p"),
            on=mr_clean["city_code"] == annual_full["city_code"],
            how="left"
        ).drop("city_code_p")
    except Exception:
        mr_updated = annual_full

    try:
        (
            mr_updated.write
            .mode("overwrite")
            .partitionBy("year", "country")
            .parquet(f"{HDFS_GOLD}/model_results")
        )
    except Exception:
        mr_updated.write.mode("overwrite").parquet(f"{HDFS_GOLD}/model_results")
    print("  [GOLD] model_results actualizado con resultados Prophet")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="GTP Prophet Time Series Forecast")
    parser.add_argument("--horizon", type=int, default=5,
                        help="Años de forecast (default: 5)")
    parser.add_argument("--min-months", type=int, default=MIN_MONTHS_PROPHET,
                        help=f"Mínimo meses por ciudad (default: {MIN_MONTHS_PROPHET})")
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — MODELO 4: PROPHET TIME SERIES FORECAST")
    print(f"  Horizonte: {args.horizon} años  |  Mín. meses: {args.min_months}")
    print("=" * 65)

    spark = get_spark()

    # 1. Cargar serie mensual
    monthly_df = load_monthly_series(spark)
    if monthly_df is None:
        print("[SKIP] Prophet omitido: Bronze sentinel2 no disponible.")
        spark.stop()
        return

    # 2. Aplicar Prophet por ciudad via pandas UDF (Grouped Map)
    print("\n  Ejecutando Prophet en paralelo por ciudad (pandas UDF) ...")
    prophet_results = (
        monthly_df
        .groupby("city")
        .applyInPandas(fit_prophet_city, schema=PROPHET_OUTPUT_SCHEMA)
    )

    # Cachear para evitar recomputación
    prophet_results.cache()
    n_results = prophet_results.count()
    n_with_forecast = prophet_results.filter(F.col("prophet_forecast_1y").isNotNull()).select("city").distinct().count()
    print(f"  Resultados: {n_results:,} filas | Ciudades con forecast: {n_with_forecast}")

    # 3. Actualizar Gold
    update_gold(spark, prophet_results)

    # 4. Log resumen por ciudad
    summary = (
        prophet_results
        .filter(F.col("month") == 12)
        .groupBy("city")
        .agg(
            F.last("prophet_turning_year").alias("turning_year"),
            F.last("prophet_mape").alias("mape"),
            F.last("prophet_forecast_5y").alias("ndvi_5y"),
        )
        .orderBy("mape")
    )
    print("\n  Top 10 ciudades con mejor ajuste Prophet (menor MAPE):")
    summary.show(10, truncate=False)

    print("\n" + "=" * 65)
    print("  PROPHET FORECAST COMPLETADO")
    print("=" * 65)
    spark.stop()


if __name__ == "__main__":
    main()
