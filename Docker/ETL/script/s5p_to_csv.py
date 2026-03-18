"""
s5p_to_csv.py — GTP (Green Turning Point)
Convierte los ZIPs descargados por Sentinel-5p_extract.py en s5p.csv.

Flujo:
  Sentinel-5p_extract.py descarga:
    data/sentinel5p/{City}/{City}_NO2_{YYYY}-{MM}.zip   (contiene un .tif)
  Este script lee cada .tif en memoria con rasterio, calcula NO2_Mean por
  ciudad/mes, escribe s5p.csv y elimina el ZIP para liberar espacio.

Output: DatosProcesados/s5p.csv
Columnas: City, Year, Month, NO2_Mean

Banda: tropospheric_NO2_column_number_density (mol/m²)
  Valores válidos: >= 0. Negativos son artefactos de retrieval, se enmascaran.

Idempotente: si s5p.csv ya existe, carga las entradas previas y
             solo procesa los ZIPs que aún no han sido convertidos.

Ejecución:
  python s5p_to_csv.py
  python s5p_to_csv.py --s5p-dir data/sentinel5p --output DatosProcesados/s5p.csv
"""

import csv
import sys
import zipfile
import argparse
from pathlib import Path

import numpy as np

try:
    import rasterio
    from rasterio.io import MemoryFile
except ImportError:
    print("[ERROR] rasterio no instalado. Ejecuta: pip install rasterio")
    sys.exit(1)

import config

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
S5P_DIR    = config.DATA_DIR / "sentinel5p"
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "s5p.csv"

CSV_COLUMNS = ["City", "Year", "Month", "NO2_Mean"]


# ==============================================================================
# LECTURA ZIP → NO2 mean (todo en memoria, sin extraer al disco)
# ==============================================================================
def read_no2_from_zip(zip_path: Path) -> float | None:
    """
    Abre el ZIP en memoria, lee el TIF con rasterio MemoryFile y devuelve
    la media de NO2 (mol/m²) de píxeles válidos (>= 0, no nodata).
    Devuelve None si la imagen está vacía o hay error.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            tif_names = [n for n in zf.namelist() if n.endswith(".tif")]
            if not tif_names:
                return None
            tif_bytes = zf.read(tif_names[0])

        with MemoryFile(tif_bytes) as memfile:
            with memfile.open() as src:
                data = src.read(1).astype(np.float32)

                # Enmascarar nodata declarado en los metadatos del TIF
                nd = src.nodata
                if nd is not None:
                    data[data == nd] = np.nan

                # Enmascarar valores negativos (artefactos de retrieval)
                data[data < 0] = np.nan

                valid = data[~np.isnan(data)]
                if valid.size == 0:
                    return None

                return round(float(np.mean(valid)), 10)

    except Exception as e:
        print(f"    [WARN] No se pudo leer {zip_path.name}: {e}")
        return None


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP Sentinel-5P ZIP/TIF → s5p.csv"
    )
    parser.add_argument("--s5p-dir", type=Path, default=S5P_DIR,
                        help=f"Directorio raíz con los ZIPs S5P (default: {S5P_DIR})")
    parser.add_argument("--output", type=Path, default=OUTPUT_CSV,
                        help=f"CSV de salida (default: {OUTPUT_CSV})")
    args = parser.parse_args()

    s5p_dir    = args.s5p_dir
    output_csv = args.output

    print("=" * 60)
    print("  GTP — SENTINEL-5P ZIP → CSV")
    print(f"  Directorio S5P: {s5p_dir}")
    print(f"  Output:         {output_csv}")
    print("=" * 60)

    if not s5p_dir.exists():
        print(f"[ERROR] El directorio S5P no existe: {s5p_dir}")
        print("  Ejecuta primero: python Sentinel-5p_extract.py")
        sys.exit(1)

    # ── Cargar entradas ya procesadas (idempotencia) ───────────────────────────
    already_done: set = set()
    if output_csv.exists():
        with open(output_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    already_done.add((row["City"], int(row["Year"]), int(row["Month"])))
                except (KeyError, ValueError):
                    pass
        print(f"  Entradas previas en CSV: {len(already_done):,}")

        # Crear marcadores .done retroactivos para que el script de extracción
        # no vuelva a descargar meses ya convertidos a CSV
        markers_created = 0
        for city, year, month in already_done:
            city_dir = s5p_dir / city
            if city_dir.exists():
                done_path = city_dir / f"{city}_NO2_{year}-{month:02d}.done"
                if not done_path.exists():
                    done_path.touch()
                    markers_created += 1
        if markers_created:
            print(f"  Marcadores .done creados: {markers_created:,}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    mode = "a" if already_done else "w"

    processed = 0
    skipped   = 0
    errors    = 0
    deleted   = 0

    with open(output_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not already_done:
            writer.writerow(CSV_COLUMNS)

        city_dirs = sorted([d for d in s5p_dir.iterdir() if d.is_dir()])
        total_cities = len(city_dirs)

        for ci, city_dir in enumerate(city_dirs, 1):
            city = city_dir.name
            zips = sorted(city_dir.glob("*.zip"))

            if not zips:
                continue

            print(f"  [{ci:>3}/{total_cities}] {city:<25} {len(zips)} ZIPs")

            for zip_path in zips:
                # Parsear año y mes: {City}_NO2_{YYYY}-{MM}.zip
                stem = zip_path.stem  # e.g. "Madrid_ES_NO2_2022-06"
                try:
                    date_part = stem.split("_NO2_")[1]   # "2022-06"
                    year  = int(date_part[:4])
                    month = int(date_part[5:7])
                except (IndexError, ValueError):
                    print(f"    [WARN] Nombre inesperado: {zip_path.name} — saltando")
                    errors += 1
                    continue

                if (city, year, month) in already_done:
                    skipped += 1
                    continue

                no2_mean = read_no2_from_zip(zip_path)

                writer.writerow([city, year, month, no2_mean])
                processed += 1

                # Eliminar ZIP y crear marcador .done para que el extractor no redescargue
                try:
                    zip_path.unlink()
                    zip_path.with_suffix(".done").touch()
                    deleted += 1
                except Exception as e:
                    print(f"    [WARN] No se pudo borrar {zip_path.name}: {e}")

    print()
    print("=" * 60)
    print(f"  FIN — Sentinel-5P procesado")
    print(f"  Filas escritas:       {processed:,}")
    print(f"  Saltadas (ya en CSV): {skipped:,}")
    print(f"  ZIPs eliminados:      {deleted:,}")
    print(f"  Errores:              {errors:,}")
    print(f"  Output:               {output_csv}")
    print("=" * 60)


if __name__ == "__main__":
    main()
