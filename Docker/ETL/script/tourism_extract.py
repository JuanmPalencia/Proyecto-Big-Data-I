"""
tourism_extract.py — GTP (Green Turning Point)
Extrae las pernoctaciones mensuales en establecimientos turísticos por país,
2000-2024, desde el dataset Eurostat tour_occ_nim.

Metodología:
  1. Descarga tour_occ_nim (TSV gzip) — pernoctaciones mensuales brutas.
     Filtro: unit=NR (número de noches), c_resid=TOTAL.
     Nivel geográfico: códigos de 2 letras ISO2 (nivel país) o NUTS-2.
  2. Para cada país en EURO_FUAS, asigna el mismo valor de pernoctaciones
     a TODAS las ciudades de ese país (expansión 1-a-N).
     Columna Tourism_Source indica si el dato es de país (COUNTRY) o
     imputado por falta de datos (MISSING).
  3. Los períodos sin datos (antes de 2004 para varios países) se dejan NULL.

Fuente:
  tour_occ_nim: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tour_occ_nim

Output: DatosProcesados/tourism.csv
Columnas: City, Country, Year, Month, Tourism_Nights, Tourism_Source

Ejecución:
  python tourism_extract.py
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
URL_TOUR = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "tour_occ_nim?format=TSV&compressed=true"
)

OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "tourism.csv"
RAW_DIR    = config.DATA_DIR / "raw" / "tourism"

YEARS  = list(range(2000, 2025))
MONTHS = list(range(1, 13))

DOWNLOAD_TIMEOUT = 120


# ==============================================================================
# UTILIDADES
# ==============================================================================
def _download_tsv_gz(url: str, cache_name: str) -> pd.DataFrame | None:
    """Descarga un TSV comprimido de Eurostat con caché en disco."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = RAW_DIR / cache_name

    if cache_path.exists():
        print(f"  [CACHE] {cache_name}")
        with gzip.open(cache_path, "rt", encoding="utf-8") as f:
            return pd.read_csv(f, sep="\t", low_memory=False)

    print(f"  [DOWNLOAD] {url}")
    try:
        r = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"  [ERROR] Descarga fallida: {e}")
        return None

    with open(cache_path, "wb") as f:
        f.write(r.content)

    with gzip.open(io.BytesIO(r.content), "rt", encoding="utf-8") as f:
        df = pd.read_csv(f, sep="\t", low_memory=False)

    print(f"  [OK] {cache_name} descargado ({len(df):,} filas)")
    return df


def _split_first_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expande la columna combinada de Eurostat (e.g. 'freq,unit,c_resid,geo\\time')
    en columnas individuales.
    """
    first_col = df.columns[0]
    dim_str = first_col.split("\\")[0]
    dim_names = [d.strip() for d in dim_str.split(",")]
    dims = df[first_col].str.split(",", expand=True)
    dims.columns = dim_names
    rest = df.iloc[:, 1:].copy()
    return pd.concat([dims, rest], axis=1)


def _clean_value(val) -> float | None:
    """Elimina flags Eurostat ('p', 'b', 'e', 'u', ':') y convierte a float."""
    s = str(val).strip()
    if s in (":", "", "nan"):
        return np.nan
    cleaned = "".join(c for c in s if c.isdigit() or c in (".", "-"))
    try:
        return float(cleaned) if cleaned else np.nan
    except ValueError:
        return np.nan


def _parse_period(p: str):
    """
    Convierte un string de período Eurostat a (year, month).
    Formatos esperados: 'YYYY-MM', 'YYYYMM', 'YYYY-MXX'.
    """
    p = str(p).strip()
    # Formato YYYY-MM (mensual estándar)
    if len(p) == 7 and p[4] == "-" and p[:4].isdigit() and p[5:].isdigit():
        return int(p[:4]), int(p[5:])
    # Formato YYYYMM
    if len(p) == 6 and p.isdigit():
        return int(p[:4]), int(p[4:])
    # Formato YYYY-M01 (a veces usado por Eurostat para meses)
    if len(p) == 8 and p[4] == "-" and p[5] == "M":
        try:
            return int(p[:4]), int(p[6:])
        except ValueError:
            pass
    return None, None


# ==============================================================================
# PARSEO DE tour_occ_nim
# ==============================================================================
def _parse_tour_occ_nim(target_countries: set[str]) -> pd.DataFrame:
    """
    Procesa tour_occ_nim y devuelve pernoctaciones mensuales por país.
    Columnas resultado: Country, Year, Month, Tourism_Nights
    """
    df_raw = _download_tsv_gz(URL_TOUR, "tour_occ_nim.tsv.gz")
    if df_raw is None:
        return pd.DataFrame()

    df = _split_first_col(df_raw)

    # Identificar columna geográfica
    geo_col = None
    for candidate in ("geo", "geo\\time", "ref_area", "country"):
        if candidate in df.columns:
            geo_col = candidate
            break
    if geo_col is None:
        for col in df.columns:
            sample = df[col].dropna().head(30).astype(str).str.strip()
            if sample.str.match(r"^[A-Z]{2}$").mean() > 0.4:
                geo_col = col
                break
    if geo_col is None:
        print("  [WARN] tour_occ_nim: columna geográfica no encontrada")
        return pd.DataFrame()

    print(f"  [OK] Columna geográfica: '{geo_col}'")

    # Filtrar: unit=NR y c_resid=TOTAL si existen esas columnas
    unit_col    = next((c for c in df.columns if c.lower() == "unit"), None)
    resid_col   = next((c for c in df.columns if c.lower() in ("c_resid", "c_resid_nres")), None)

    if unit_col:
        df = df[df[unit_col].str.strip().str.upper() == "NR"].copy()
    if resid_col:
        df = df[df[resid_col].str.strip().str.upper() == "TOTAL"].copy()

    # Filtrar solo países de 2 letras ISO2 (excluir NUTS-2/3 con 4-5 caracteres)
    df = df[df[geo_col].astype(str).str.strip().str.match(r"^[A-Z]{2}$")].copy()
    df = df[df[geo_col].isin(target_countries)].copy()

    if df.empty:
        print("  [WARN] tour_occ_nim: sin datos tras filtrar")
        return pd.DataFrame()

    # Identificar columnas de período (YYYY-MM o similar)
    period_cols = []
    for col in df.columns:
        if col in (geo_col, unit_col, resid_col):
            continue
        year, month = _parse_period(col)
        if year is not None and year in range(1995, 2030):
            period_cols.append(col)

    if not period_cols:
        print("  [WARN] tour_occ_nim: no se detectaron columnas de período mensual")
        return pd.DataFrame()

    id_cols = [c for c in df.columns if c not in period_cols]
    df_melt = df.melt(id_vars=id_cols, value_vars=period_cols,
                      var_name="period", value_name="value_raw")

    df_melt["Tourism_Nights"] = df_melt["value_raw"].apply(_clean_value)
    df_melt[["Year", "Month"]] = df_melt["period"].apply(
        lambda p: pd.Series(_parse_period(p))
    )
    df_melt = df_melt.dropna(subset=["Year", "Month"])
    df_melt["Year"]  = df_melt["Year"].astype(int)
    df_melt["Month"] = df_melt["Month"].astype(int)
    df_melt = df_melt[df_melt["Year"].isin(YEARS) & df_melt["Month"].isin(MONTHS)]

    # Agregar: media por país × año × mes (podría haber sub-categorías)
    df_agg = (
        df_melt.groupby([geo_col, "Year", "Month"], as_index=False)["Tourism_Nights"]
        .sum()
        .rename(columns={geo_col: "Country"})
    )
    # Donde la suma es 0, tratarlo como no disponible
    df_agg.loc[df_agg["Tourism_Nights"] == 0, "Tourism_Nights"] = np.nan

    valid = df_agg["Tourism_Nights"].notna().sum()
    print(f"  [OK] tour_occ_nim: {valid:,} registros válidos (país×mes)")
    return df_agg


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    print("=" * 60)
    print("  GTP — EXTRACCIÓN TURISMO (EUROSTAT tour_occ_nim)")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)

    # Mapa ISO2 → lista de city_codes
    iso2_to_cities: dict[str, list[str]] = {}
    for city_code in config.EURO_FUAS:
        iso2 = city_code.rsplit("_", 1)[-1]
        iso2_to_cities.setdefault(iso2, []).append(city_code)

    target_countries = set(iso2_to_cities.keys())
    print(f"  Países objetivo: {len(target_countries)}")
    print()

    # Descargar y parsear
    print("[1/2] Descargando tour_occ_nim...")
    df_country = _parse_tour_occ_nim(target_countries)

    if df_country.empty:
        print("[WARN] Sin datos de turismo. Generando CSV vacío.")
        df_out = pd.DataFrame(columns=["City", "Country", "Year", "Month",
                                       "Tourism_Nights", "Tourism_Source"])
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        return

    # Expandir país → todas las ciudades de ese país
    print("\n[2/2] Expandiendo datos de país a ciudades...")
    output_rows = []
    for _, row in df_country.iterrows():
        country = row["Country"]
        cities  = iso2_to_cities.get(country, [])
        if not cities:
            continue
        nights = row["Tourism_Nights"]
        source = "COUNTRY" if pd.notna(nights) else "MISSING"
        for city_code in cities:
            output_rows.append({
                "City":            city_code,
                "Country":         country,
                "Year":            int(row["Year"]),
                "Month":           int(row["Month"]),
                "Tourism_Nights":  nights if pd.notna(nights) else None,
                "Tourism_Source":  source,
            })

    df_out = pd.DataFrame(output_rows)
    df_out = df_out.sort_values(["City", "Year", "Month"]).reset_index(drop=True)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    valid_rows   = df_out["Tourism_Nights"].notna().sum()
    missing_rows = df_out["Tourism_Nights"].isna().sum()

    print()
    print("=" * 60)
    print("  FIN — Turismo extraído")
    print(f"  Total filas:       {len(df_out):,}")
    print(f"  Filas con dato:    {valid_rows:,}")
    print(f"  Filas sin dato:    {missing_rows:,}")
    print(f"  Ciudades:          {df_out['City'].nunique()}")
    print(f"  Países cubiertos:  {df_out['Country'].nunique()}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
