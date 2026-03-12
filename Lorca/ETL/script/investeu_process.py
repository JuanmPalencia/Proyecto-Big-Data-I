"""
investeu_process.py — GTP (Green Turning Point)
Post-procesador InvestEU: lee final_recipients.csv de data/raw/investeu/ y genera
investeu_summary.csv (por ciudad × año) listo para ingestar en Bronze.

Fuente: final_recipients.csv — beneficiarios finales EIB/InvestEU
  Columnas esperadas: Financial Product, Operation Name, Borrower Name,
    Borrower Address, Borrower Country, Financing Form, Policy Area supported,
    Amount of financial support received in EUR, page, pdf_url, extraction_date

Estrategia de año:
  1. Extraer año del pdf_url (regex 20\\d{2}) — los PDFs EIB suelen incluir
     el año en el nombre del archivo (ej: investeu_final_recipients_2023_en.pdf)
  2. Fallback: año del campo extraction_date

Granularidad salida: ciudad (city_code) × año — expandida desde país

Output: DatosProcesados/investeu_summary.csv
Columnas: City, Year, InvestEU_Ops_Count, InvestEU_Total_EUR

Ejecución:
  python investeu_process.py
"""

import re
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config

# ==============================================================================
# RUTAS
# ==============================================================================
RAW_DIR    = config.DATA_DIR / "raw" / "investeu"
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "investeu_summary.csv"

# ==============================================================================
# MAPEO NOMBRE DE PAÍS (EN) → ISO2 (países europeos cubiertos por EURO_FUAS)
# ==============================================================================
COUNTRY_NAME_TO_ISO2 = {
    "Spain": "ES", "Portugal": "PT", "France": "FR", "Italy": "IT",
    "Germany": "DE", "United Kingdom": "UK", "Netherlands": "NL",
    "Belgium": "BE", "Poland": "PL", "Romania": "RO",
    "Czech Republic": "CZ", "Czechia": "CZ", "Hungary": "HU",
    "Austria": "AT", "Sweden": "SE", "Denmark": "DK", "Finland": "FI",
    "Ireland": "IE", "Greece": "GR", "Switzerland": "CH", "Norway": "NO",
    "Luxembourg": "LU",  # no en EURO_FUAS pero puede aparecer
}

# Año base InvestEU (lanzado en 2021 — si no detectamos año usamos este)
DEFAULT_YEAR = 2022


# ==============================================================================
# UTILIDADES
# ==============================================================================
def extract_year_from_url(url: str) -> int | None:
    """
    Intenta extraer el año (20XX) del pdf_url.
    Si hay varios, toma el primero >= 2021 (InvestEU inicio).
    """
    if not isinstance(url, str):
        return None
    candidates = re.findall(r"20\d{2}", url)
    for c in candidates:
        y = int(c)
        if 2021 <= y <= 2030:
            return y
    return None


def extract_year_from_date(date_str: str) -> int | None:
    """Extrae el año de una cadena de fecha (YYYY-... formato)."""
    if not isinstance(date_str, str):
        return None
    m = re.match(r"(20\d{2})", date_str.strip())
    return int(m.group(1)) if m else None


def expand_country_to_cities(df: pd.DataFrame, country_col: str = "iso2") -> pd.DataFrame:
    """Expande DataFrame país×año a ciudad×año."""
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
    print("  GTP — InvestEU POST-PROCESADOR")
    print(f"  Raw dir:  {RAW_DIR}")
    print(f"  Output:   {OUTPUT_CSV}")
    print("=" * 60)

    # Buscar final_recipients.csv (único o múltiples por año)
    csv_files = list(RAW_DIR.glob("final_recipients*.csv"))
    if not csv_files:
        print("[ERROR] No se encontraron archivos final_recipients*.csv en:")
        print(f"        {RAW_DIR}")
        print("        Ejecuta primero InvestEU_extract.py.")
        return

    print(f"  Archivos encontrados: {[f.name for f in csv_files]}")

    frames = []
    for fp in csv_files:
        try:
            df = pd.read_csv(fp, low_memory=False)
            frames.append(df)
            print(f"  Cargado {fp.name}: {len(df):,} filas")
        except Exception as e:
            print(f"  [WARN] Error leyendo {fp.name}: {e}. Saltando.")

    if not frames:
        print("[ERROR] No se pudo cargar ningún archivo. Abortando.")
        return

    df = pd.concat(frames, ignore_index=True)
    print(f"\n  Total filas raw: {len(df):,}")

    # Identificar columna de país
    country_col = next(
        (c for c in df.columns if "country" in c.lower() and "borrower" in c.lower()),
        next((c for c in df.columns if "country" in c.lower()), None)
    )
    # Identificar columna de importe
    amount_col = next(
        (c for c in df.columns if "amount" in c.lower() and "eur" in c.lower()),
        next((c for c in df.columns if "amount" in c.lower()), None)
    )

    if country_col is None:
        print("[ERROR] No se encontró columna de país. Abortando.")
        return

    print(f"  Columna país:    '{country_col}'")
    print(f"  Columna importe: '{amount_col}'")

    # --- Asignar año ---
    def get_year(row) -> int:
        # Intento 1: pdf_url
        if "pdf_url" in df.columns:
            y = extract_year_from_url(row.get("pdf_url"))
            if y:
                return y
        # Intento 2: extraction_date
        if "extraction_date" in df.columns:
            y = extract_year_from_date(row.get("extraction_date"))
            if y:
                return y
        return DEFAULT_YEAR

    df["year"] = df.apply(get_year, axis=1)

    # --- Mapear país → ISO2 ---
    df["iso2"] = df[country_col].str.strip().map(COUNTRY_NAME_TO_ISO2)
    n_unmapped = df["iso2"].isna().sum()
    if n_unmapped > 0:
        unique_unknown = df.loc[df["iso2"].isna(), country_col].dropna().unique()[:10]
        print(f"  [WARN] {n_unmapped:,} filas sin mapeo de país. Ejemplos: {list(unique_unknown)}")

    df = df[df["iso2"].notna()].copy()
    print(f"  Filas con país europeo mapeado: {len(df):,}")

    # --- Importe numérico ---
    if amount_col:
        df["amount_eur"] = (
            df[amount_col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )
    else:
        df["amount_eur"] = 0.0

    # --- Agregar por país×año ---
    agg = (
        df.groupby(["iso2", "year"], as_index=False)
        .agg(
            InvestEU_Ops_Count=("iso2", "count"),
            InvestEU_Total_EUR=("amount_eur", "sum"),
        )
    )
    print(f"  Registros país×año: {len(agg):,}")

    # --- Expandir a ciudades ---
    df_cities = expand_country_to_cities(agg, country_col="iso2")
    if df_cities.empty:
        print("[WARN] No se generaron filas ciudad×año. Revisa el mapping de países.")
        return

    print(f"  Registros ciudad×año: {len(df_cities):,}")

    # Preparar output
    df_cities = df_cities.rename(columns={"year": "Year"})
    out_cols = ["City", "Year", "InvestEU_Ops_Count", "InvestEU_Total_EUR"]
    df_out = df_cities[out_cols].sort_values(["City", "Year"]).reset_index(drop=True)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False)

    print(f"\n  [OK] investeu_summary.csv guardado: {len(df_out):,} filas")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
