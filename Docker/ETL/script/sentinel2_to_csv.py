"""
sentinel2_to_csv.py — GTP (Green Turning Point)
Convierte los ZIPs descargados por Sentinel-2_extract.py en sentinel2.csv.

Flujo:
  Sentinel-2_extract.py descarga:
    data/sentinel2/{City}/{City}_NDVI_{YYYY}-{MM}.zip   (contiene un .tif)
  Este script lee cada .tif en memoria (sin extraer al disco) con rasterio,
  calcula NDVI_Mean y NDVI_Std por ciudad/mes, escribe sentinel2.csv
  y elimina el ZIP para liberar espacio.

Output: DatosProcesados/sentinel2.csv
Columnas: City, Year, Month, NDVI_Mean, NDVI_Std

Idempotente: si sentinel2.csv ya existe, carga las entradas previas y
             solo procesa los ZIPs que aún no han sido convertidos.

Ejecución:
  python sentinel2_to_csv.py
  python sentinel2_to_csv.py --s2-dir data/sentinel2 --output DatosProcesados/sentinel2.csv
"""

import csv
import io
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
S2_DIR     = config.DATA_DIR / "sentinel2"
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "sentinel2.csv"

CSV_COLUMNS = ["City", "Year", "Month", "NDVI_Mean", "NDVI_Std"]

# NDVI válido: [-1, 1]. Valores fuera de rango son artefactos o nodata.
NDVI_MIN = -1.0
NDVI_MAX =  1.0


# ==============================================================================
# LECTURA ZIP → NDVI mean + std (todo en memoria, sin extraer al disco)
# ==============================================================================
def read_ndvi_from_zip(zip_path: Path) -> tuple:
    """
    Abre el ZIP en memoria, lee el TIF interno con rasterio MemoryFile
    y devuelve (ndvi_mean, ndvi_std).
    Devuelve (None, None) si la imagen está vacía (todo nubes) o hay error.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            tif_names = [n for n in zf.namelist() if n.endswith(".tif")]
            if not tif_names:
                return None, None
            tif_bytes = zf.read(tif_names[0])

        with MemoryFile(tif_bytes) as memfile:
            with memfile.open() as src:
                data = src.read(1).astype(np.float32)

                # Enmascarar nodata declarado en los metadatos del TIF
                nd = src.nodata
                if nd is not None:
                    data[data == nd] = np.nan

                # Enmascarar valores fuera del rango NDVI físicamente válido
                data[(data < NDVI_MIN) | (data > NDVI_MAX)] = np.nan

                valid = data[~np.isnan(data)]
                if valid.size == 0:
                    return None, None

                return round(float(np.mean(valid)), 6), round(float(np.std(valid)), 6)

    except Exception as e:
        print(f"    [WARN] No se pudo leer {zip_path.name}: {e}")
        return None, None


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP Sentinel-2 ZIP/TIF → sentinel2.csv"
    )
    parser.add_argument("--s2-dir", type=Path, default=S2_DIR,
                        help=f"Directorio raíz con los ZIPs Sentinel-2 (default: {S2_DIR})")
    parser.add_argument("--output", type=Path, default=OUTPUT_CSV,
                        help=f"CSV de salida (default: {OUTPUT_CSV})")
    args = parser.parse_args()

    s2_dir     = args.s2_dir
    output_csv = args.output

    print("=" * 60)
    print("  GTP — SENTINEL-2 ZIP → CSV")
    print(f"  Directorio S2: {s2_dir}")
    print(f"  Output:        {output_csv}")
    print("=" * 60)

    if not s2_dir.exists():
        print(f"[ERROR] El directorio Sentinel-2 no existe: {s2_dir}")
        print("  Ejecuta primero: python Sentinel-2_extract.py")
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
            city_dir = s2_dir / city
            if city_dir.exists():
                done_path = city_dir / f"{city}_NDVI_{year}-{month:02d}.done"
                if not done_path.exists():
                    done_path.touch()
                    markers_created += 1
        if markers_created:
            print(f"  Marcadores .done creados: {markers_created:,}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    # Append si ya hay datos, write si es nuevo
    mode = "a" if already_done else "w"

    processed = 0
    skipped   = 0
    errors    = 0
    deleted   = 0

    with open(output_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not already_done:
            writer.writerow(CSV_COLUMNS)

        city_dirs = sorted([d for d in s2_dir.iterdir() if d.is_dir()])
        total_cities = len(city_dirs)

        for ci, city_dir in enumerate(city_dirs, 1):
            city = city_dir.name
            zips = sorted(city_dir.glob("*.zip"))

            if not zips:
                continue

            print(f"  [{ci:>3}/{total_cities}] {city:<25} {len(zips)} ZIPs")

            for zip_path in zips:
                # Parsear año y mes: {City}_NDVI_{YYYY}-{MM}.zip
                stem = zip_path.stem  # e.g. "Madrid_ES_NDVI_2022-06"
                try:
                    date_part = stem.split("_NDVI_")[1]   # "2022-06"
                    year  = int(date_part[:4])
                    month = int(date_part[5:7])
                except (IndexError, ValueError):
                    print(f"    [WARN] Nombre inesperado: {zip_path.name} — saltando")
                    errors += 1
                    continue

                if (city, year, month) in already_done:
                    skipped += 1
                    continue

                ndvi_mean, ndvi_std = read_ndvi_from_zip(zip_path)

                # Escribir fila (con None si imagen vacía = todo nubes ese mes)
                writer.writerow([city, year, month, ndvi_mean, ndvi_std])
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
    print(f"  FIN — Sentinel-2 procesado")
    print(f"  Filas escritas:  {processed:,}")
    print(f"  Saltadas (ya en CSV): {skipped:,}")
    print(f"  ZIPs eliminados: {deleted:,}")
    print(f"  Errores:         {errors:,}")
    print(f"  Output:          {output_csv}")
    print("=" * 60)


if __name__ == "__main__":
    main()
