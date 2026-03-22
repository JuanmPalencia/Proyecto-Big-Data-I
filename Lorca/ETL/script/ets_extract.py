"""
ets_extract.py — GTP (Green Turning Point)
Extrae el precio mensual de los derechos de emisión de CO2 en el EU ETS
(€/tonelada CO2), 2005-2024. Años 2000-2004 → NULL (el mercado no existía).

Metodología:
  1. Intenta descargar datos de precios mensuales desde EMBER Climate (CSV libre).
  2. Si la descarga falla (URL obsoleta o sin conexión), usa una tabla de precios
     históricos anuales codificada (Fases 1-4 del EU ETS) y expande a 12 meses
     con una variación aleatoria determinista (±5 %) para simular estacionalidad.
  3. El resultado es una serie temporal única (no por ciudad/país); todos los
     países europeos comparten el mismo precio EUA.

Fuentes:
  - EMBER Climate ETS price CSV (cuando disponible)
  - Tabla histórica codificada (fallback robusto)

Output: DatosProcesados/ets_price.csv
Columnas: Year, Month, ETS_Price_EUR, ETS_Source

Ejecución:
  python ets_extract.py
"""

import os
import gzip
import io
import requests
import pandas as pd
import numpy as np
import config

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "ets_price.csv"
RAW_DIR    = config.DATA_DIR / "raw" / "ets"

YEARS  = list(range(2000, 2025))
MONTHS = list(range(1, 13))

DOWNLOAD_TIMEOUT = 30

# URLs candidatas de EMBER Climate para precios mensuales ETS
EMBER_URLS = [
    "https://ember-climate.org/app/uploads/2022/07/EU-ETS-carbon-price.csv",
    "https://ember-climate.org/wp-content/uploads/2022/07/EU-ETS-carbon-price.csv",
    "https://raw.githubusercontent.com/ember-climate/carbon-price-viewer/main/data/eu_ets_price.csv",
]

# ==============================================================================
# TABLA HISTÓRICA DE PRECIOS EU ETS (FALLBACK)
# Fuentes de referencia: ICAP, Sandbag, Carbon Tracker, literature review.
# Precios son promedios anuales aproximados en €/tCO2.
# ==============================================================================
ETS_ANNUAL_FALLBACK: dict[int, float] = {
    # Fase 1 (2005-2007): sobre-asignación de permisos, colapso de precio
    2005: 22.5,
    2006: 17.0,
    2007: 0.8,     # colapso al final de Fase 1
    # Fase 2 (2008-2012): crisis financiera deprimió demanda industrial
    2008: 22.0,
    2009: 13.5,
    2010: 14.5,
    2011: 13.0,
    2012: 7.5,
    # Fase 3 (2013-2020): exceso de permisos, mínimos históricos
    2013: 4.5,
    2014: 6.0,
    2015: 7.5,
    2016: 5.0,
    2017: 6.5,
    2018: 15.5,    # fuerte subida por reforma MSR (Market Stability Reserve)
    2019: 25.0,
    2020: 24.5,
    # Fase 4 (2021+): reducción acelerada de permisos, "Fit for 55"
    2021: 50.0,
    2022: 80.0,
    2023: 84.0,
    2024: 65.0,
}

# Variaciones mensuales relativas (índice estacional histórico aproximado).
# Basado en patrones observados: verano bajo, invierno alto.
MONTHLY_SEASONAL_INDEX: dict[int, float] = {
    1: 1.04,   # enero: demanda calefacción elevada
    2: 1.03,
    3: 1.02,
    4: 1.00,
    5: 0.99,
    6: 0.97,   # verano: menor demanda industrial
    7: 0.96,
    8: 0.96,
    9: 0.98,
    10: 1.00,
    11: 1.02,
    12: 1.03,  # diciembre: cumplimiento anual (compras de última hora)
}


# ==============================================================================
# DESCARGA EMBER (FUENTE PRIMARIA)
# ==============================================================================
def _try_download_ember() -> pd.DataFrame | None:
    """
    Intenta descargar datos de precios mensuales ETS desde EMBER Climate.
    Retorna DataFrame con columnas (year, month, price) o None si falla.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_DIR / "ember_ets_price.csv"

    if cache_path.exists():
        print("  [CACHE] ember_ets_price.csv")
        try:
            df = pd.read_csv(cache_path, encoding="utf-8")
            return _parse_ember_df(df)
        except Exception as e:
            print(f"  [WARN] Error al leer caché EMBER: {e}")

    for url in EMBER_URLS:
        print(f"  [DOWNLOAD] Intentando EMBER: {url}")
        try:
            r = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
            r.raise_for_status()
            content = r.content
            # Intentar descomprimir si es gzip
            try:
                with gzip.open(io.BytesIO(content), "rt", encoding="utf-8") as f:
                    df = pd.read_csv(f)
            except Exception:
                df = pd.read_csv(io.BytesIO(content), encoding="utf-8")

            parsed = _parse_ember_df(df)
            if parsed is not None and not parsed.empty:
                # Guardar en caché
                df.to_csv(cache_path, index=False)
                print(f"  [OK] EMBER descargado: {len(parsed)} registros")
                return parsed
        except Exception as e:
            print(f"  [WARN] URL fallida ({url}): {e}")

    print("  [WARN] Todas las URLs de EMBER fallaron. Se usará fallback.")
    return None


def _parse_ember_df(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Intenta interpretar un DataFrame de EMBER en (year, month, price).
    EMBER puede tener columnas: 'date', 'price', 'year', 'month', etc.
    """
    if df is None or df.empty:
        return None

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    price_col = next(
        (c for c in df.columns if "price" in c or "eur" in c or "ets" in c),
        None
    )
    if price_col is None and len(df.columns) >= 2:
        # Segunda columna es probablemente el precio
        price_col = df.columns[1]

    if price_col is None:
        return None

    # Detectar año y mes
    if "date" in df.columns:
        try:
            df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date_parsed"])
            df["year"]  = df["date_parsed"].dt.year.astype(int)
            df["month"] = df["date_parsed"].dt.month.astype(int)
        except Exception:
            return None
    elif "year" in df.columns and "month" in df.columns:
        df["year"]  = pd.to_numeric(df["year"],  errors="coerce").astype("Int64")
        df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["year", "month"])
    else:
        # Intentar primera columna como fecha
        try:
            df["date_parsed"] = pd.to_datetime(df.iloc[:, 0], errors="coerce")
            df = df.dropna(subset=["date_parsed"])
            df["year"]  = df["date_parsed"].dt.year.astype(int)
            df["month"] = df["date_parsed"].dt.month.astype(int)
        except Exception:
            return None

    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[price_col])
    df = df[df["year"].isin(range(2004, 2026))].copy()

    result = (
        df.groupby(["year", "month"], as_index=False)[price_col]
        .mean()
        .rename(columns={"year": "Year", "month": "Month", price_col: "ETS_Price_EUR"})
    )
    result["ETS_Price_EUR"] = result["ETS_Price_EUR"].round(4)
    return result if not result.empty else None


# ==============================================================================
# GENERACIÓN DE SERIE MENSUAL DESDE FALLBACK ANUAL
# ==============================================================================
def _build_from_fallback() -> pd.DataFrame:
    """
    Construye una serie mensual de precios ETS usando la tabla histórica codificada.
    Aplica un índice estacional mensual para mayor realismo.
    """
    print("  [FALLBACK] Generando precios ETS desde tabla histórica codificada...")
    rows = []
    for year in YEARS:
        annual_price = ETS_ANNUAL_FALLBACK.get(year)  # None para 2000-2004
        for month in MONTHS:
            if annual_price is None:
                rows.append({
                    "Year":          year,
                    "Month":         month,
                    "ETS_Price_EUR": None,
                    "ETS_Source":    "NO_MARKET",
                })
            else:
                seasonal = MONTHLY_SEASONAL_INDEX.get(month, 1.0)
                price = round(annual_price * seasonal, 4)
                rows.append({
                    "Year":          year,
                    "Month":         month,
                    "ETS_Price_EUR": price,
                    "ETS_Source":    "FALLBACK_ANNUAL",
                })
    return pd.DataFrame(rows)


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    print("=" * 60)
    print("  GTP — EXTRACCIÓN PRECIOS EU ETS (EMBER / Fallback histórico)")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)
    print()

    # --- Intentar fuente primaria EMBER ---
    print("[1/2] Intentando descarga EMBER Climate...")
    df_ember = _try_download_ember()

    if df_ember is not None and not df_ember.empty:
        # EMBER tiene datos: complementar con fallback para años sin cobertura
        print(f"  [OK] EMBER: {len(df_ember)} registros mensuales")
        df_ember["ETS_Source"] = "EMBER_MONTHLY"

        # Cubrir años no presentes en EMBER con fallback
        ember_keys = set(zip(df_ember["Year"].astype(int),
                             df_ember["Month"].astype(int)))
        fallback_rows = []
        for year in YEARS:
            annual_price = ETS_ANNUAL_FALLBACK.get(year)
            for month in MONTHS:
                if (year, month) not in ember_keys:
                    if annual_price is None:
                        fallback_rows.append({
                            "Year": year, "Month": month,
                            "ETS_Price_EUR": None,
                            "ETS_Source": "NO_MARKET",
                        })
                    else:
                        seasonal = MONTHLY_SEASONAL_INDEX.get(month, 1.0)
                        fallback_rows.append({
                            "Year": year, "Month": month,
                            "ETS_Price_EUR": round(annual_price * seasonal, 4),
                            "ETS_Source": "FALLBACK_ANNUAL",
                        })

        frames = [df_ember]
        if fallback_rows:
            frames.append(pd.DataFrame(fallback_rows))
        df_final = pd.concat(frames, ignore_index=True)
    else:
        # EMBER no disponible: usar fallback completo
        print("[2/2] Usando tabla histórica codificada (fallback completo)...")
        df_final = _build_from_fallback()

    # Ordenar y garantizar cobertura 2000-2024
    df_final["Year"]  = df_final["Year"].astype(int)
    df_final["Month"] = df_final["Month"].astype(int)
    df_final = df_final.sort_values(["Year", "Month"]).reset_index(drop=True)

    # Asegurarse de que todos los años×meses están presentes
    all_periods = pd.MultiIndex.from_product([YEARS, MONTHS], names=["Year", "Month"])
    df_all = pd.DataFrame(index=all_periods).reset_index()
    df_final = df_all.merge(df_final, on=["Year", "Month"], how="left")

    # Rellenar ETS_Source para filas que quedaron en blanco tras el merge
    df_final["ETS_Source"] = df_final["ETS_Source"].fillna(
        df_final["Year"].apply(lambda y: "NO_MARKET" if y < 2005 else "MISSING")
    )

    # Guardar
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    has_price  = df_final["ETS_Price_EUR"].notna().sum()
    no_market  = (df_final["ETS_Source"] == "NO_MARKET").sum()
    sources    = df_final["ETS_Source"].value_counts().to_dict()

    print()
    print("=" * 60)
    print("  FIN — Precios EU ETS extraídos")
    print(f"  Total filas:          {len(df_final):,}")
    print(f"  Filas con precio:     {has_price:,}")
    print(f"  Filas sin mercado:    {no_market:,} (2000-2004)")
    print(f"  Distribución fuentes: {sources}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
