"""
edgar_co2_extract.py — GTP (Green Turning Point)
Descarga y procesa las emisiones de CO2 por país y año desde EDGAR v8 (JRC/EU Commission).

Metodología completamente automatizada — sin descargas manuales:
  1. Descarga el archivo ZIP de EDGAR directamente desde el servidor JRC
  2. Extrae y parsea el CSV interno (formato semicolón, años como columnas Y_XXXX)
  3. Filtra por países europeos (los de EURO_FUAS en config.py)
  4. Mapea cada país → todas las ciudades de ese país (ISO3 → ISO2)
  5. Genera edgar_co2.csv con granularidad ciudad × año

Fuente: EDGAR 2024 — Fossil CO2 emissions by country
  URL: https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/EDGAR/datasets/EDGAR_2024_GHG/
  Dataset: IEA_EDGAR_CO2_1970_2023
  Unidades: kt CO2/año por país

Output: DatosProcesados/edgar_co2.csv
Columnas: City, Year, CO2_kt, Country_Code

Nota: Los datos son a nivel país (no ciudad). Cada ciudad hereda el valor de su país.
      Para el análisis EKC, se puede combinar con población para CO2 per cápita.

Ejecución:
  python edgar_co2_extract.py
  python edgar_co2_extract.py --force-download   (ignora el caché del ZIP)
"""

import csv
import io
import zipfile
import argparse
from pathlib import Path

import requests
import pandas as pd
import config

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
# EDGAR 2024 — Fossil CO2 country totals, 1970-2023
EDGAR_URL     = (
    "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/EDGAR/datasets/"
    "EDGAR_2024_GHG/IEA_EDGAR_CO2_1970_2023.zip"
)
OUTPUT_CSV        = config.INPUT_DIR_PROCESSED / "edgar_co2.csv"
CACHE_ZIP         = config.INPUT_DIR_PROCESSED / "edgar_co2_raw.zip"
CSV_COLUMNS       = ["City", "Year", "CO2_kt", "Country_Code"]
DOWNLOAD_TIMEOUT  = 180          # segundos

# Mapeo ISO3 → ISO2 para países europeos en EURO_FUAS
ISO3_TO_ISO2 = {
    "AUT": "AT", "BEL": "BE", "BGR": "BG", "HRV": "HR", "CYP": "CY",
    "CZE": "CZ", "DNK": "DK", "EST": "EE", "FIN": "FI", "FRA": "FR",
    "DEU": "DE", "GRC": "GR", "HUN": "HU", "IRL": "IE", "ITA": "IT",
    "LVA": "LV", "LTU": "LT", "LUX": "LU", "MLT": "MT", "NLD": "NL",
    "POL": "PL", "PRT": "PT", "ROU": "RO", "SVK": "SK", "SVN": "SI",
    "ESP": "ES", "SWE": "SE", "GBR": "GB", "NOR": "NO", "CHE": "CH",
    "ISL": "IS", "LIE": "LI", "MKD": "MK", "ALB": "AL", "SRB": "RS",
    "MNE": "ME", "BIH": "BA", "MDA": "MD", "UKR": "UA", "BLR": "BY",
    "TUR": "TR", "NOR": "NO",
}


# ==============================================================================
# DESCARGA CON CACHÉ
# ==============================================================================
def download_edgar_zip(force: bool = False):
    """Descarga el ZIP de EDGAR y lo cachea en disco para no re-descargar."""
    CACHE_ZIP.parent.mkdir(parents=True, exist_ok=True)

    if CACHE_ZIP.exists() and not force:
        print(f"  [CACHE] Usando ZIP cacheado: {CACHE_ZIP}")
        return

    print(f"  [DOWNLOAD] Descargando EDGAR CO2 desde JRC...")
    print(f"    {EDGAR_URL}")

    resp = requests.get(EDGAR_URL, timeout=DOWNLOAD_TIMEOUT, stream=True)
    resp.raise_for_status()

    total_bytes = 0
    with open(CACHE_ZIP, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            total_bytes += len(chunk)

    print(f"  [OK] ZIP descargado ({total_bytes / 1e6:.1f} MB) → {CACHE_ZIP}")


# ==============================================================================
# PROCESADO COMÚN (CSV y XLSX) DEL DATAFRAME EDGAR
# ==============================================================================
def _process_edgar_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa un DataFrame de EDGAR (leído desde CSV o XLSX) y devuelve:
      Country_code_A3, iso2, year (int), co2_kt (float)
    Funciona con cualquier formato que EDGAR haya publicado.
    """
    print(f"  [OK] {len(df_raw):,} filas | Columnas: {list(df_raw.columns[:8])}")

    # Detectar columna de código ISO3
    country_col = None
    for candidate in ["Country_code_A3", "ISO_A3", "country_code_A3",
                       "Alpha3", "Country code A3", "country"]:
        if candidate in df_raw.columns:
            country_col = candidate
            break

    if country_col is None:
        for col in df_raw.columns:
            sample = df_raw[col].dropna().head(10).tolist()
            if all(len(str(v).strip()) == 3 and str(v).strip().isalpha()
                   for v in sample if str(v).strip()):
                country_col = col
                break

    if country_col is None:
        raise RuntimeError(
            f"No se detectó columna de código ISO3.\n"
            f"Columnas disponibles: {list(df_raw.columns)}"
        )

    print(f"  [OK] Columna país: '{country_col}'")

    # Detectar columnas de año (formato "Y_1970" o "1970")
    year_cols = {}
    for col in df_raw.columns:
        col_clean = str(col).strip().lstrip("Y_").lstrip("y_")
        try:
            yr = int(col_clean)
            if 1970 <= yr <= 2030:
                year_cols[col] = yr
        except ValueError:
            pass

    if not year_cols:
        raise RuntimeError(
            f"No se detectaron columnas de año.\n"
            f"Columnas: {list(df_raw.columns)}"
        )

    yr_min = min(year_cols.values())
    yr_max = max(year_cols.values())
    print(f"  [OK] Años detectados: {yr_min}–{yr_max} ({len(year_cols)} columnas)")

    # Convertir a string para homogeneizar
    df_raw = df_raw.copy()
    df_raw[country_col] = df_raw[country_col].astype(str).str.strip()

    records = []
    for _, row in df_raw.iterrows():
        iso3 = row.get(country_col, "").strip()
        iso2 = ISO3_TO_ISO2.get(iso3)
        if iso2 is None:
            continue
        for year_col, year in year_cols.items():
            try:
                val = float(str(row.get(year_col, "")).strip().replace(",", "."))
            except (ValueError, TypeError):
                val = None
            records.append({"iso3": iso3, "iso2": iso2, "year": year, "co2_kt": val})

    df = pd.DataFrame(records)
    df = df.groupby(["iso3", "iso2", "year"], as_index=False)["co2_kt"].sum()
    print(f"  [OK] {len(df):,} registros país×año después de agregar sectores")
    return df


# ==============================================================================
# PARSEO DEL CSV/XLSX DE EDGAR
# ==============================================================================
def parse_edgar_csv() -> pd.DataFrame:
    """
    Lee el CSV dentro del ZIP de EDGAR y devuelve un DataFrame con columnas:
      Country_code_A3, iso2, year (int), co2_kt (float)

    EDGAR v8 CSV format (semicolon-delimited):
      IPCC_annex;C_group_IM24_sh;Country_code_A3;Name;Substance;Y_1970;...;Y_2022
    Unidades: kt CO2 equivalente por año por país (total nacional).
    Si hay filas por sector, se suman para obtener el total nacional.
    """
    with zipfile.ZipFile(CACHE_ZIP) as zf:
        all_names = zf.namelist()
        csv_names  = [n for n in all_names if n.lower().endswith(".csv")]
        xlsx_names = [n for n in all_names if n.lower().endswith(".xlsx")]

        if xlsx_names:
            # EDGAR 2024 distribuye .xlsx en lugar de .csv
            xlsx_name = xlsx_names[0]
            print(f"  [PARSE] Leyendo {xlsx_name} del ZIP (formato XLSX) ...")
            with zf.open(xlsx_name) as f:
                # La hoja de datos suele llamarse "IPCC 2006" o ser la primera hoja
                xl = pd.ExcelFile(f, engine="openpyxl")
                # Elegir la hoja que contiene datos de emisiones (años como columnas)
                sheet = xl.sheet_names[0]
                for sh in xl.sheet_names:
                    if any(kw in sh for kw in ["IPCC", "data", "CO2", "GHG"]):
                        sheet = sh
                        break

                # Escanear las primeras 20 filas para encontrar la fila de cabecera real
                # (EDGAR XLSX tiene filas de metadata al principio: "Content:", "Last update:", etc.)
                df_preview = xl.parse(sheet, header=None, nrows=20)
                header_row = 0
                for i, row in df_preview.iterrows():
                    row_vals = [str(v) for v in row.values]
                    if any(
                        kw in v
                        for v in row_vals
                        for kw in ["Country_code_A3", "ISO_A3", "country_code", "Country code",
                                   "Alpha3", "IPCC_annex", "C_group"]
                    ):
                        header_row = i
                        break
                print(f"    Hoja: {sheet} | Fila cabecera detectada: {header_row}")

                df_raw = xl.parse(sheet, skiprows=header_row)
                print(f"    Filas: {len(df_raw)} | Cols: {len(df_raw.columns)}")
                xl.close()
            return _process_edgar_df(df_raw)
        elif csv_names:
            csv_name = csv_names[0]
        else:
            raise RuntimeError(f"ZIP de EDGAR no contiene .csv ni .xlsx: {all_names[:10]}")

        print(f"  [PARSE] Leyendo {csv_name} del ZIP ...")
        with zf.open(csv_name) as f:
            raw = f.read().decode("utf-8", errors="replace")

    # Detectar separador y fila de cabecera real
    # EDGAR CSVs a veces tienen líneas de disclaimer al principio
    lines = raw.splitlines()
    header_idx = 0
    for i, line in enumerate(lines):
        # La cabecera real tiene "Country_code_A3" o columnas numéricas de año
        if "Country_code_A3" in line or "ISO_A3" in line:
            header_idx = i
            break
        # Heurística: primera línea con muchos separadores (¿o punto y coma?)
        if line.count(";") > 5 or line.count(",") > 5:
            header_idx = i
            break

    # Detectar separador
    sample_line = lines[header_idx]
    sep = ";" if sample_line.count(";") >= sample_line.count(",") else ","

    print(f"  [OK] Separador detectado: '{sep}' | Cabecera en fila {header_idx}")

    df_raw = pd.read_csv(
        io.StringIO("\n".join(lines[header_idx:])),
        sep=sep,
        encoding="utf-8",
        on_bad_lines="skip",
        dtype=str,
    )

    df_raw = pd.read_csv(
        io.StringIO("\n".join(lines[header_idx:])),
        sep=sep,
        encoding="utf-8",
        on_bad_lines="skip",
        dtype=str,
    )
    return _process_edgar_df(df_raw)


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP EDGAR CO2 — emisiones por país, descarga automática JRC"
    )
    parser.add_argument("--force-download", action="store_true",
                        help="Ignorar el ZIP cacheado y volver a descargar")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN CO2 (EDGAR 2024 — JRC/EU)")
    print(f"  Fuente:  EDGAR 2024 (IEA_EDGAR_CO2_1970_2023)")
    print(f"  Output:  {OUTPUT_CSV}")
    print("=" * 60)

    # Construir mapa ISO2 → lista de city_codes del proyecto
    iso2_to_cities = {}
    for city_code in config.EURO_FUAS:
        iso2 = city_code.rsplit("_", 1)[-1]
        iso2_to_cities.setdefault(iso2, []).append(city_code)

    print(f"  Países en EURO_FUAS: {len(iso2_to_cities)}")

    # Descargar ZIP (con caché)
    download_edgar_zip(force=args.force_download)

    # Parsear CSV
    df_edgar = parse_edgar_csv()

    # Expandir país × año → ciudad × año
    output_rows = []
    for _, row in df_edgar.iterrows():
        iso2   = row["iso2"]
        year   = int(row["year"])
        co2_kt = row["co2_kt"]
        cities = iso2_to_cities.get(iso2, [])
        for city_code in cities:
            output_rows.append({
                "City":         city_code,
                "Year":         year,
                "CO2_kt":       co2_kt,
                "Country_Code": iso2,
            })

    print(f"  Filas expandidas (ciudad×año): {len(output_rows):,}")

    if not output_rows:
        print("[WARN] Sin datos para exportar. Revisa la URL o el formato del CSV de EDGAR.")
        return

    # Guardar CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(output_rows)

    print()
    print("=" * 60)
    print(f"  FIN — CO2 EDGAR extraído")
    print(f"  Filas escritas: {len(output_rows):,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
