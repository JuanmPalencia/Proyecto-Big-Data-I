"""
hrl_to_csv.py — GTP (Green Turning Point)
Convierte los GeoTIFFs descargados por HRL_extract.py en hrl.csv.

Flujo:
  HRL_extract.py descarga:
    data/hrl/{year}/{layer}/{city}_{layer}_{year}.tif
  Este script lee cada .tif con rasterio y calcula la media de píxeles válidos.
  Output: DatosProcesados/hrl.csv

Columnas output:
  City, Year, Imperviousness_Mean, Tree_Cover_Pct, Small_Woody_Mean

Valores de nodata:
  Los productos EEA HRL usan 255 (uint8) como nodata. Si el TIFF tiene
  nodata declarado en sus metadatos, se usa ese. Se enmascaran también
  valores fuera del rango válido (>100 para capas de porcentaje).

Dependencia:
  pip install rasterio numpy

Ejecución:
  python hrl_to_csv.py
  python hrl_to_csv.py --hrl-dir data/hrl --output DatosProcesados/hrl.csv
"""

import csv
import sys
import argparse
import numpy as np
from pathlib import Path

try:
    import rasterio
except ImportError:
    print("[ERROR] rasterio no instalado. Ejecuta: pip install rasterio")
    sys.exit(1)

import config

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HRL_DIR    = config.DATA_DIR / "hrl"
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "hrl.csv"

# Capas que descarga HRL_extract.py y sus rangos válidos
LAYER_CONFIG = {
    "Imperviousness": {
        "col":       "Imperviousness_Mean",
        "valid_max": 100,          # porcentaje 0–100
        "nodata":    255,
    },
    "Tree_Cover": {
        "col":       "Tree_Cover_Pct",
        "valid_max": 100,
        "nodata":    255,
    },
    "Small_Woody": {
        "col":       "Small_Woody_Mean",
        "valid_max": 100,
        "nodata":    255,
    },
}

CSV_COLUMNS = [
    "City", "Year",
    "Imperviousness_Mean", "Tree_Cover_Pct", "Small_Woody_Mean",
]


# ==============================================================================
# LECTURA DE UN GEOTIFF → MEDIA DE PÍXELES VÁLIDOS
# ==============================================================================
def read_tif_mean(tif_path: Path, valid_max: float, nodata_hint: int = 255) -> float | None:
    """
    Abre un GeoTIFF y devuelve la media de píxeles válidos.

    Aplica tres filtros para excluir píxeles inválidos:
      1. nodata declarado en los metadatos del TIFF (o nodata_hint si no hay)
      2. Valores > valid_max (fuera de rango lógico del producto)
      3. Valores < 0 (artefacto de conversión)

    Retorna None si el fichero no existe o no hay píxeles válidos.
    """
    if not tif_path.exists():
        return None

    try:
        with rasterio.open(tif_path) as src:
            data = src.read(1).astype(float)

            # Nodata declarado en metadatos del TIFF
            nd = src.nodata if src.nodata is not None else nodata_hint

            # Máscara de píxeles inválidos
            mask = (data == nd) | (data > valid_max) | (data < 0)
            data[mask] = np.nan

            valid = data[~np.isnan(data)]
            if valid.size == 0:
                return None

            return round(float(np.mean(valid)), 4)

    except Exception as e:
        print(f"  [WARN] No se pudo leer {tif_path.name}: {e}")
        return None


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP HRL GeoTIFF → hrl.csv"
    )
    parser.add_argument("--hrl-dir", type=Path, default=HRL_DIR,
                        help=f"Directorio raíz de los TIFFs HRL (default: {HRL_DIR})")
    parser.add_argument("--output", type=Path, default=OUTPUT_CSV,
                        help=f"Ruta del CSV de salida (default: {OUTPUT_CSV})")
    args = parser.parse_args()

    hrl_dir    = args.hrl_dir
    output_csv = args.output

    print("=" * 60)
    print("  GTP — HRL GeoTIFF → CSV")
    print(f"  Directorio HRL: {hrl_dir}")
    print(f"  Output:         {output_csv}")
    print("=" * 60)

    if not hrl_dir.exists():
        print(f"[ERROR] El directorio HRL no existe: {hrl_dir}")
        print("  Ejecuta primero: python HRL_extract.py")
        sys.exit(1)

    # Descubrir qué años están disponibles
    available_years = sorted([
        int(p.name) for p in hrl_dir.iterdir()
        if p.is_dir() and p.name.isdigit()
    ])

    if not available_years:
        print(f"[ERROR] No se encontraron subdirectorios de año en {hrl_dir}")
        sys.exit(1)

    print(f"  Años encontrados: {available_years}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    rows_missing = 0

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)

        for year in available_years:
            year_dir = hrl_dir / str(year)
            print(f"\n  Procesando año {year} ...")

            for city in config.EURO_FUAS:
                row_values = {}

                for layer_name, cfg in LAYER_CONFIG.items():
                    tif_path = year_dir / layer_name / f"{city}_{layer_name}_{year}.tif"
                    val = read_tif_mean(tif_path, cfg["valid_max"], cfg["nodata"])
                    row_values[cfg["col"]] = val  # None si no existe

                # Escribir fila aunque falten algunas capas (habrá nulls en el CSV)
                row = [
                    city,
                    year,
                    row_values.get("Imperviousness_Mean"),
                    row_values.get("Tree_Cover_Pct"),
                    row_values.get("Small_Woody_Mean"),
                ]
                writer.writerow(row)

                if all(v is not None for v in row[2:]):
                    rows_written += 1
                else:
                    rows_missing += 1

    print()
    print("=" * 60)
    print(f"  FIN — HRL procesado")
    print(f"  Filas completas:   {rows_written:,}")
    print(f"  Filas con nulls:   {rows_missing:,}  (capas no descargadas aún)")
    print(f"  Output:            {output_csv}")
    print("=" * 60)


if __name__ == "__main__":
    main()
