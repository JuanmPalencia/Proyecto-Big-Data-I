"""
oecd_process.py — GTP (Green Turning Point)
Post-procesador OECD: lee CSVs crudos de data/raw/oecd/ y genera
oecd_indicators.csv (por ciudad × año) listo para ingestar en Bronze.

Indicadores extraídos:
  env_policy.csv       → EPS_Index (Environmental Policy Stringency index)
  env_tax.csv          → Env_Tax_USD (recaudación impuestos ambientales, USD)
  env_expenditure.csv  → Env_Expenditure (gasto público ambiental NEEP)
  air_ghg.csv          → GHG_Total_kt (emisiones GHG totales, suma todos gases)

Granularidad fuente: país (ISO3) × año
Granularidad salida: ciudad (city_code) × año (expandida desde país)

Output: DatosProcesados/oecd_indicators.csv
Columnas: City, Year, EPS_Index, Env_Tax_USD, Env_Expenditure, GHG_Total_kt

Ejecución:
  python oecd_process.py
"""

import pandas as pd
import sys
from pathlib import Path

# Importar config (fuente de verdad de ciudades y rutas)
sys.path.insert(0, str(Path(__file__).resolve().parent))
import config

# ==============================================================================
# RUTAS
# ==============================================================================
RAW_DIR    = config.DATA_DIR / "raw" / "oecd"
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "oecd_indicators.csv"

# ==============================================================================
# MAPEO ISO3 → ISO2 (países europeos cubiertos por EURO_FUAS)
# ==============================================================================
ISO3_TO_ISO2 = {
    "ESP": "ES", "PRT": "PT", "FRA": "FR", "ITA": "IT", "DEU": "DE",
    "GBR": "UK", "NLD": "NL", "BEL": "BE", "POL": "PL", "ROU": "RO",
    "CZE": "CZ", "HUN": "HU", "AUT": "AT", "SWE": "SE", "DNK": "DK",
    "FIN": "FI", "IRL": "IE", "GRC": "GR", "CHE": "CH", "NOR": "NO",
}


# ==============================================================================
# UTILIDADES
# ==============================================================================
def load_oecd_csv(filename: str, value_col: str) -> "pd.DataFrame | None":
    """
    Lee un CSV SDMX (csvfilewithlabels) de OECD.
    Busca columnas REF_AREA (ISO3), TIME_PERIOD (año), OBS_VALUE (valor).
    Devuelve DataFrame con columnas: iso3, year, <value_col>
    """
    path = RAW_DIR / filename
    if not path.exists():
        print(f"  [WARN] {filename} no encontrado. Saltando.")
        return None

    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"  [WARN] Error leyendo {filename}: {e}. Saltando.")
        return None

    df.columns = [c.strip() for c in df.columns]

    required = {"REF_AREA", "TIME_PERIOD", "OBS_VALUE"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        print(f"  [WARN] {filename}: faltan columnas {missing}. Saltando.")
        return None

    out = df[["REF_AREA", "TIME_PERIOD", "OBS_VALUE"]].copy()
    out.columns = ["iso3", "year", value_col]

    out["year"]    = pd.to_numeric(out["year"],    errors="coerce")
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=["iso3", "year"])
    out["year"] = out["year"].astype(int)

    return out


def expand_country_to_cities(df: pd.DataFrame, country_col: str = "iso2") -> pd.DataFrame:
    """
    Dado DataFrame país×año con columna <country_col> (ISO2),
    devuelve DataFrame ciudad×año con columna City.
    """
    iso2_to_cities: dict[str, list[str]] = {}
    for city_code in config.EURO_FUAS:
        iso2 = city_code.rsplit("_", 1)[-1]
        iso2_to_cities.setdefault(iso2, []).append(city_code)

    rows = []
    for _, row in df.iterrows():
        iso2 = row[country_col]
        for city in iso2_to_cities.get(iso2, []):
            r = row.to_dict()
            r["City"] = city
            rows.append(r)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    print("=" * 60)
    print("  GTP — OECD POST-PROCESADOR")
    print(f"  Raw dir:  {RAW_DIR}")
    print(f"  Output:   {OUTPUT_CSV}")
    print("=" * 60)

    dfs: list[pd.DataFrame] = []

    # --- 1. EPS Index (Environmental Policy Stringency) ---
    eps = load_oecd_csv("env_policy.csv", "EPS_Index")
    if eps is not None:
        # Deduplicar por iso3×year (tomar media si hay sub-indicadores)
        eps = eps.groupby(["iso3", "year"], as_index=False)["EPS_Index"].mean()
        dfs.append(eps)
        print(f"  env_policy.csv: {len(eps):,} filas país×año")

    # --- 2. Impuestos ambientales (USD) ---
    tax = load_oecd_csv("env_tax.csv", "Env_Tax_USD")
    if tax is not None:
        tax = tax.groupby(["iso3", "year"], as_index=False)["Env_Tax_USD"].sum()
        dfs.append(tax)
        print(f"  env_tax.csv: {len(tax):,} filas país×año")

    # --- 3. Gasto público ambiental (NEEP) ---
    exp = load_oecd_csv("env_expenditure.csv", "Env_Expenditure")
    if exp is not None:
        exp = exp.groupby(["iso3", "year"], as_index=False)["Env_Expenditure"].sum()
        dfs.append(exp)
        print(f"  env_expenditure.csv: {len(exp):,} filas país×año")

    # --- 4. Emisiones GHG totales (suma todos los gases, kt CO2eq) ---
    ghg_path = RAW_DIR / "air_ghg.csv"
    if ghg_path.exists():
        try:
            ghg_raw = pd.read_csv(ghg_path, low_memory=False)
            ghg_raw.columns = [c.strip() for c in ghg_raw.columns]
            if all(c in ghg_raw.columns for c in ["REF_AREA", "TIME_PERIOD", "OBS_VALUE"]):
                ghg_raw = ghg_raw[["REF_AREA", "TIME_PERIOD", "OBS_VALUE"]].copy()
                ghg_raw.columns = ["iso3", "year", "GHG_Total_kt"]
                ghg_raw["year"]        = pd.to_numeric(ghg_raw["year"],        errors="coerce")
                ghg_raw["GHG_Total_kt"] = pd.to_numeric(ghg_raw["GHG_Total_kt"], errors="coerce")
                ghg_raw = ghg_raw.dropna(subset=["iso3", "year"])
                ghg_raw["year"] = ghg_raw["year"].astype(int)
                # Suma de todos los gases por país×año = GHG total
                ghg = ghg_raw.groupby(["iso3", "year"], as_index=False)["GHG_Total_kt"].sum()
                dfs.append(ghg)
                print(f"  air_ghg.csv: {len(ghg):,} filas país×año (agregado)")
            else:
                print("  [WARN] air_ghg.csv: columnas no encontradas. Saltando.")
        except Exception as e:
            print(f"  [WARN] Error leyendo air_ghg.csv: {e}. Saltando.")
    else:
        print(f"  [WARN] air_ghg.csv no encontrado. Saltando.")

    if not dfs:
        print("[ERROR] No hay datos OECD disponibles. Abortando.")
        return

    # --- Merge por iso3 × year ---
    df_merged = dfs[0]
    for other in dfs[1:]:
        df_merged = pd.merge(df_merged, other, on=["iso3", "year"], how="outer")

    # Filtrar solo países europeos cubiertos por EURO_FUAS
    df_merged["iso2"] = df_merged["iso3"].map(ISO3_TO_ISO2)
    df_merged = df_merged[df_merged["iso2"].notna()].copy()
    print(f"\n  Registros país×año europeos: {len(df_merged):,}")

    # Expandir país → ciudades
    df_cities = expand_country_to_cities(df_merged, country_col="iso2")
    if df_cities.empty:
        print("[WARN] No se generaron filas ciudad×año. Revisa el mapping ISO3→ISO2.")
        return

    print(f"  Registros ciudad×año: {len(df_cities):,}")

    # Preparar output
    df_cities = df_cities.rename(columns={"year": "Year"})

    out_cols = ["City", "Year", "EPS_Index", "Env_Tax_USD", "Env_Expenditure", "GHG_Total_kt"]
    for col in out_cols:
        if col not in df_cities.columns:
            df_cities[col] = None

    df_out = df_cities[out_cols].sort_values(["City", "Year"]).reset_index(drop=True)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False)

    print(f"\n  [OK] oecd_indicators.csv guardado: {len(df_out):,} filas")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
