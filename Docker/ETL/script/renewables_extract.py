"""
renewables_extract.py — GTP (Green Turning Point)
Extrae el porcentaje mensual de electricidad procedente de energías renovables
por país europeo, para los años 2000-2024.

Metodología:
  1. Descarga nrg_cb_pem (Eurostat, TSV gzip) — producción mensual de electricidad
     por tipo de combustible (GWh). Calcula renewables_pct = RA000 / TOTAL * 100.
  2. Para países o períodos sin cobertura en nrg_cb_pem (principalmente 2000-2005),
     usa nrg_ind_ren (Eurostat, TSV gzip) — dato anual, repetido en los 12 meses.
  3. Filtra por los países de EURO_FUAS (ISO2).

Fuentes:
  - nrg_cb_pem: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nrg_cb_pem
  - nrg_ind_ren: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nrg_ind_ren

Output: DatosProcesados/renewables.csv
Columnas: Country, Year, Month, Renewables_Pct, Source

Ejecución:
  python renewables_extract.py
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
URL_NRG_CB_PEM = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "nrg_cb_pem?format=TSV&compressed=true"
)
URL_NRG_IND_REN = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "nrg_ind_ren?format=TSV&compressed=true"
)

OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "renewables.csv"
RAW_DIR    = config.DATA_DIR / "raw" / "renewables"

YEARS  = list(range(2000, 2025))
MONTHS = list(range(1, 13))

# Códigos de combustibles renovables en nrg_cb_pem
RENEW_FUELS = {"RA000", "RA100", "RA200", "RA300", "RA400", "RA500", "RA600"}
TOTAL_FUEL  = "TOTAL"

DOWNLOAD_TIMEOUT = 120   # segundos


# ==============================================================================
# UTILIDADES: descarga con caché
# ==============================================================================
def _download_tsv_gz(url: str, cache_name: str) -> pd.DataFrame | None:
    """
    Descarga un TSV comprimido de Eurostat y lo cachea en disco.
    Devuelve el DataFrame crudo (primera columna combinada sin separar).
    """
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
        print(f"  [ERROR] No se pudo descargar {cache_name}: {e}")
        return None

    with open(cache_path, "wb") as f:
        f.write(r.content)

    with gzip.open(io.BytesIO(r.content), "rt", encoding="utf-8") as f:
        df = pd.read_csv(f, sep="\t", low_memory=False)

    print(f"  [OK] {cache_name} descargado ({len(df):,} filas)")
    return df


# ==============================================================================
# PARSEO del TSV de Eurostat (formato ancho con dimensiones en la 1ª columna)
# ==============================================================================
def _split_first_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    El TSV de Eurostat tiene la primera columna combinada tipo:
      'freq,nrg_bal,siec,unit,geo\\time'  con valores 'M,GWH,RA000,GWH,ES'
    Esta función la expande en columnas individuales.
    """
    first_col = df.columns[0]
    dim_str, _ = first_col.split("\\")
    dim_names  = [d.strip() for d in dim_str.split(",")]
    dims       = df[first_col].str.split(",", expand=True)
    dims.columns = dim_names

    rest = df.iloc[:, 1:].copy()
    return pd.concat([dims, rest], axis=1)


def _melt_years(df: pd.DataFrame, id_cols: list[str]) -> pd.DataFrame:
    """
    Convierte columnas de año (o año+mes) en filas.
    Limpia flags Eurostat ('p', 'b', 'e', 'u') de los valores.
    """
    value_cols = [c for c in df.columns if c not in id_cols]
    df_melt = df.melt(id_vars=id_cols, value_vars=value_cols,
                      var_name="period", value_name="value_raw")
    df_melt["value"] = pd.to_numeric(
        df_melt["value_raw"].astype(str).str.replace(r"[^\d\.\-]", "", regex=True),
        errors="coerce"
    )
    return df_melt.dropna(subset=["value"])


# ==============================================================================
# FUENTE PRIMARIA: nrg_cb_pem (mensual, GWh por tipo de combustible)
# ==============================================================================
def _parse_nrg_cb_pem(target_countries: set[str]) -> pd.DataFrame:
    """
    Procesa nrg_cb_pem para obtener renewables_pct mensual por país.
    Columnas resultado: Country, Year, Month, Renewables_Pct
    """
    df_raw = _download_tsv_gz(URL_NRG_CB_PEM, "nrg_cb_pem.tsv.gz")
    if df_raw is None:
        return pd.DataFrame()

    df = _split_first_col(df_raw)
    # Columnas esperadas: freq, nrg_bal (o siec), unit, geo
    # Identificar columna geográfica
    geo_col = next((c for c in df.columns if c.lower() in ("geo", "geo\\time")), None)
    if geo_col is None:
        # buscar la columna que contiene códigos ISO2 de 2 letras
        for col in df.columns:
            sample = df[col].dropna().head(20).astype(str).str.strip()
            if sample.str.match(r"^[A-Z]{2}$").mean() > 0.5:
                geo_col = col
                break
    if geo_col is None:
        print("  [WARN] nrg_cb_pem: no se encontró columna geográfica")
        return pd.DataFrame()

    # Identificar columna de combustible (siec o nrg_bal)
    fuel_col = next(
        (c for c in df.columns if c.lower() in ("siec", "nrg_bal", "products")),
        None
    )
    if fuel_col is None:
        print("  [WARN] nrg_cb_pem: no se encontró columna de combustible")
        return pd.DataFrame()

    id_cols = [c for c in df.columns
               if not _is_period_col(c) and c != "value_raw"]

    # Filtrar países objetivo y combustibles relevantes
    all_fuels = RENEW_FUELS | {TOTAL_FUEL}
    df = df[df[geo_col].isin(target_countries) & df[fuel_col].isin(all_fuels)].copy()

    if df.empty:
        print("  [WARN] nrg_cb_pem: sin datos para los países objetivo")
        return pd.DataFrame()

    df_melt = _melt_years(df, id_cols)

    # Parsear período: formato 'YYYY-MM' o 'YYYYMM'
    def _parse_period(p: str):
        p = str(p).strip()
        if len(p) == 7 and p[4] == "-":          # YYYY-MM
            return int(p[:4]), int(p[5:])
        if len(p) == 6 and p.isdigit():           # YYYYMM
            return int(p[:4]), int(p[4:])
        return None, None

    df_melt[["Year", "Month"]] = df_melt["period"].apply(
        lambda p: pd.Series(_parse_period(p))
    )
    df_melt = df_melt.dropna(subset=["Year", "Month"])
    df_melt["Year"]  = df_melt["Year"].astype(int)
    df_melt["Month"] = df_melt["Month"].astype(int)
    df_melt = df_melt[df_melt["Year"].isin(YEARS) & df_melt["Month"].isin(MONTHS)]

    # Separar renovables vs total
    df_renew = (
        df_melt[df_melt[fuel_col].isin(RENEW_FUELS)]
        .groupby([geo_col, "Year", "Month"], as_index=False)["value"].sum()
        .rename(columns={"value": "renew_gwh"})
    )
    df_total = (
        df_melt[df_melt[fuel_col] == TOTAL_FUEL]
        .groupby([geo_col, "Year", "Month"], as_index=False)["value"].sum()
        .rename(columns={"value": "total_gwh"})
    )

    df_merged = df_renew.merge(df_total, on=[geo_col, "Year", "Month"], how="inner")
    df_merged = df_merged[df_merged["total_gwh"] > 0].copy()
    df_merged["Renewables_Pct"] = (df_merged["renew_gwh"] / df_merged["total_gwh"] * 100).round(4)
    df_merged.rename(columns={geo_col: "Country"}, inplace=True)
    df_merged["Source"] = "MONTHLY_NRG"

    result = df_merged[["Country", "Year", "Month", "Renewables_Pct", "Source"]].copy()
    print(f"  [OK] nrg_cb_pem: {len(result):,} registros país×mes con renovables")
    return result


def _is_period_col(col: str) -> bool:
    """True si el nombre de columna parece un año o año-mes."""
    c = str(col).strip()
    # YYYY-MM o YYYYMM
    if len(c) == 7 and c[4] == "-" and c[:4].isdigit() and c[5:].isdigit():
        return True
    if len(c) == 6 and c.isdigit():
        return True
    # YYYY (solo año)
    if len(c) == 4 and c.isdigit() and 1990 <= int(c) <= 2030:
        return True
    return False


# ==============================================================================
# FUENTE SECUNDARIA: nrg_ind_ren (anual, % de renovables en energía final bruta)
# ==============================================================================
def _parse_nrg_ind_ren(target_countries: set[str]) -> pd.DataFrame:
    """
    Procesa nrg_ind_ren para obtener renewables_pct anual por país.
    Columna principal: REN_SHARE (% de energías renovables sobre energía final bruta).
    Resultado expandido a 12 meses con Source = 'ANNUAL_FILL'.
    """
    df_raw = _download_tsv_gz(URL_NRG_IND_REN, "nrg_ind_ren.tsv.gz")
    if df_raw is None:
        return pd.DataFrame()

    df = _split_first_col(df_raw)

    geo_col = next((c for c in df.columns if c.lower() in ("geo", "geo\\time")), None)
    if geo_col is None:
        for col in df.columns:
            sample = df[col].dropna().head(20).astype(str).str.strip()
            if sample.str.match(r"^[A-Z]{2}$").mean() > 0.5:
                geo_col = col
                break

    if geo_col is None:
        print("  [WARN] nrg_ind_ren: no se encontró columna geográfica")
        return pd.DataFrame()

    # Filtrar por indicador de cuota de renovables (nrg_bal = REN o similar)
    nrg_bal_col = next(
        (c for c in df.columns if c.lower() in ("nrg_bal", "siec", "indic_nrg")),
        None
    )
    if nrg_bal_col:
        ren_vals = {"REN", "REN_ELC", "SHARE_REN", "REN_SHARE"}
        df_ren = df[df[nrg_bal_col].isin(ren_vals)].copy()
        if df_ren.empty:
            # Intentar cualquier fila que contenga "REN" en ese campo
            df_ren = df[df[nrg_bal_col].str.contains("REN", na=False)].copy()
        if df_ren.empty:
            df_ren = df.copy()  # Usar todo si no hay filtro claro
    else:
        df_ren = df.copy()

    df_ren = df_ren[df_ren[geo_col].isin(target_countries)].copy()
    if df_ren.empty:
        print("  [WARN] nrg_ind_ren: sin datos para los países objetivo")
        return pd.DataFrame()

    id_cols = [c for c in df_ren.columns if not _is_period_col(c)]
    df_melt = _melt_years(df_ren, id_cols)

    # Solo columnas de año (4 dígitos)
    df_melt = df_melt[df_melt["period"].astype(str).str.match(r"^\d{4}$")].copy()
    df_melt["Year"] = df_melt["period"].astype(int)
    df_melt = df_melt[df_melt["Year"].isin(YEARS)]

    # Agrupar: media por país y año (puede haber múltiples indicadores)
    df_annual = (
        df_melt.groupby([geo_col, "Year"], as_index=False)["value"].mean()
        .rename(columns={geo_col: "Country", "value": "Renewables_Pct"})
    )
    df_annual["Renewables_Pct"] = df_annual["Renewables_Pct"].round(4)

    # Expandir a 12 meses
    rows = []
    for _, row in df_annual.iterrows():
        for month in MONTHS:
            rows.append({
                "Country":        row["Country"],
                "Year":           int(row["Year"]),
                "Month":          month,
                "Renewables_Pct": row["Renewables_Pct"],
                "Source":         "ANNUAL_FILL",
            })

    result = pd.DataFrame(rows)
    print(f"  [OK] nrg_ind_ren: {len(result):,} registros (anual expandido a mensual)")
    return result


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    print("=" * 60)
    print("  GTP — EXTRACCIÓN ENERGÍAS RENOVABLES (EUROSTAT)")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)

    # Construir conjunto de países ISO2 presentes en EURO_FUAS
    target_countries: set[str] = set()
    for city_code in config.EURO_FUAS:
        iso2 = city_code.rsplit("_", 1)[-1]
        target_countries.add(iso2)
    print(f"  Países objetivo: {len(target_countries)} ({sorted(target_countries)})")
    print()

    # --- FUENTE PRIMARIA: datos mensuales ---
    print("[1/3] Descargando nrg_cb_pem (producción mensual por combustible)...")
    df_monthly = _parse_nrg_cb_pem(target_countries)

    # --- FUENTE SECUNDARIA: datos anuales para rellenar huecos ---
    print("\n[2/3] Descargando nrg_ind_ren (cuota anual de renovables, fallback)...")
    df_annual = _parse_nrg_ind_ren(target_countries)

    # --- COMBINAR: mensual tiene prioridad; anual cubre lo que falta ---
    print("\n[3/3] Combinando fuentes...")

    # Conjunto de claves ya cubiertas por la fuente mensual
    if not df_monthly.empty:
        covered = set(zip(df_monthly["Country"], df_monthly["Year"], df_monthly["Month"]))
    else:
        covered = set()

    if not df_annual.empty:
        mask_new = ~df_annual.apply(
            lambda r: (r["Country"], int(r["Year"]), int(r["Month"])) in covered,
            axis=1
        )
        df_fill = df_annual[mask_new].copy()
    else:
        df_fill = pd.DataFrame()

    frames = [f for f in [df_monthly, df_fill] if not f.empty]
    if not frames:
        print("[WARN] Sin datos para exportar.")
        return

    df_final = pd.concat(frames, ignore_index=True)
    df_final = df_final.sort_values(["Country", "Year", "Month"]).reset_index(drop=True)

    # Solo países en EURO_FUAS
    df_final = df_final[df_final["Country"].isin(target_countries)].copy()

    # Guardar
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    monthly_rows = len(df_final[df_final["Source"] == "MONTHLY_NRG"])
    annual_rows  = len(df_final[df_final["Source"] == "ANNUAL_FILL"])

    print()
    print("=" * 60)
    print("  FIN — Renovables extraídas")
    print(f"  Total filas:      {len(df_final):,}")
    print(f"  Fuente MONTHLY:   {monthly_rows:,}")
    print(f"  Fuente ANNUAL_FILL: {annual_rows:,}")
    print(f"  Países cubiertos: {df_final['Country'].nunique()}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
