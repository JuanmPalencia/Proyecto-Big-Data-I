"""
urban_atlas_extract.py — GTP (Green Turning Point)
Extrae la distribución de uso de suelo por ciudad usando ESA WorldCover via GEE.

Estrategia: histograma de frecuencia via reduceRegion (una sola llamada GEE por ciudad).
GEE calcula la proporción de cada clase de uso del suelo dentro del buffer de la ciudad.

Dataset: ESA/WorldCover/v200 (2021, 10m resolución global)
         ESA/WorldCover/v100 (2020, 10m resolución global)
Clases de uso del suelo (WorldCover):
  10 = Tree cover (bosque/arbolado)
  20 = Shrubland (matorral)
  30 = Grassland (pradera/herbazal)
  40 = Cropland (cultivos)
  50 = Built-up (urbanizado/construido)
  60 = Bare/sparse vegetation (suelo desnudo)
  80 = Permanent water bodies (agua permanente)
  90 = Herbaceous wetland (humedal herbáceo)

Output: DatosProcesados/urban_atlas.csv
Columnas: City, Year, Tree_Pct, Shrub_Pct, Grass_Pct, Crop_Pct,
          Built_Pct, Bare_Pct, Water_Pct

Nota: Se generan 2 filas por ciudad (Year=2020 y Year=2021 — los dos epochs disponibles).
      Para años fuera de 2020–2021, el pipeline usará NULL en las columnas WorldCover.

Ejecución:
  python urban_atlas_extract.py
  python urban_atlas_extract.py --workers 4
"""

import csv
import time
import random
import threading
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import ee
import config

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
GOOGLE_PROJECT = "gtpuem23"
SCALE_M        = 30             # 10m nativo → 30m reduce tiempo de cómputo sin perder precisión
MAX_WORKERS    = 8
MAX_RETRIES    = 4
BUFFER_M       = 20_000         # 20 km buffer alrededor del centroide

# Dos epochs disponibles de ESA WorldCover: (collection_id, year)
WORLDCOVER_VERSIONS = [
    ("ESA/WorldCover/v100", 2020),
    ("ESA/WorldCover/v200", 2021),
]

# Mapeo: nombre de columna → valor de píxel WorldCover
CLASS_MAP = {
    "Tree_Pct":  10,
    "Shrub_Pct": 20,
    "Grass_Pct": 30,
    "Crop_Pct":  40,
    "Built_Pct": 50,
    "Bare_Pct":  60,
    "Water_Pct": 80,
}

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "urban_atlas.csv"
CSV_COLUMNS = ["City", "Year"] + list(CLASS_MAP.keys())

_csv_lock = threading.Lock()

# ==============================================================================
# INICIALIZACIÓN GEE
# ==============================================================================
try:
    ee.Initialize(project=GOOGLE_PROJECT)
    print("[INFO] GEE inicializado correctamente.")
except Exception as e:
    raise RuntimeError(
        f"Fallo de autenticación GEE: {e}\n"
        "Ejecuta: earthengine authenticate"
    )


# ==============================================================================
# IDEMPOTENCIA
# ==============================================================================
def load_done(csv_path: Path) -> set:
    """Devuelve el conjunto de (City, Year) ya escritos en el CSV."""
    if not csv_path.exists():
        return set()
    done = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                done.add((row["City"], int(row["Year"])))
            except (KeyError, ValueError):
                pass
    return done


def init_csv(csv_path: Path):
    """Crea el directorio y la cabecera del CSV si no existe."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists():
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(CSV_COLUMNS)


def append_row(csv_path: Path, row: tuple):
    """Escritura thread-safe de una fila al CSV."""
    with _csv_lock:
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)


# ==============================================================================
# WORKER — procesa una ciudad × epoch WorldCover
# ==============================================================================
def process_city(task: tuple):
    """
    Calcula el porcentaje de cada clase de uso del suelo para una ciudad.
    Usa ee.Reducer.frequencyHistogram(): una sola llamada GEE (no 7 llamadas separadas).

    Retorna:
      - tuple (City, Year, Tree_Pct, Shrub_Pct, ...) si OK
      - str con prefijo [WARN/ERROR] si hay problema
    """
    city, (lat, lon), collection_id, year = task
    roi = ee.Geometry.Point([lon, lat]).buffer(BUFFER_M).bounds()

    for attempt in range(MAX_RETRIES):
        try:
            img = (
                ee.ImageCollection(collection_id)
                .filterBounds(roi)
                .first()
                .select("Map")
                .clip(roi)
            )

            # frequencyHistogram: devuelve dict {str(class_value): pixel_count}
            # Una sola llamada GEE para todos los clases — eficiente
            hist_stats = img.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e9,
                bestEffort=True,
            ).getInfo()

            hist = hist_stats.get("Map", {}) or {}
            total_pixels = sum(hist.values()) or 1  # evitar división por cero

            class_pcts = {}
            for col_name, class_val in CLASS_MAP.items():
                n = hist.get(str(class_val), 0)
                class_pcts[col_name] = round(n / total_pixels * 100, 4)

            return (
                city, year,
                class_pcts.get("Tree_Pct",  0.0),
                class_pcts.get("Shrub_Pct", 0.0),
                class_pcts.get("Grass_Pct", 0.0),
                class_pcts.get("Crop_Pct",  0.0),
                class_pcts.get("Built_Pct", 0.0),
                class_pcts.get("Bare_Pct",  0.0),
                class_pcts.get("Water_Pct", 0.0),
            )

        except ee.EEException as e:
            if attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                time.sleep(wait)
            else:
                return f"[ERROR-GEE] {city} {year}: {e}"
        except Exception as e:
            return f"[ERROR] {city} {year}: {e}"

    return f"[ERROR] {city} {year}: máx reintentos alcanzados"


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP Urban Atlas — uso de suelo ESA WorldCover vía GEE"
    )
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN USO DE SUELO (ESA WorldCover via GEE)")
    print(f"  Epochs:     2020 (v100) + 2021 (v200)")
    print(f"  Escala:     {SCALE_M}m | Buffer: {BUFFER_M//1000}km")
    print(f"  Hilos:      {args.workers}")
    print(f"  Output:     {OUTPUT_CSV}")
    print("=" * 60)

    init_csv(OUTPUT_CSV)
    done = load_done(OUTPUT_CSV)
    print(f"  Registros ya procesados: {len(done):,}")

    tasks = [
        (city, coords, col_id, year)
        for city, coords in config.EURO_FUAS.items()
        for col_id, year in WORLDCOVER_VERSIONS
        if (city, year) not in done
    ]

    print(f"  Pendientes:  {len(tasks):,} ciudad×año\n")

    ok = err = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_city, t): t for t in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()

            if isinstance(result, tuple):
                append_row(OUTPUT_CSV, result)
                ok += 1
            else:
                err += 1
                print(result)

            if i % 100 == 0:
                print(f"  [{i:,}/{len(tasks):,}] OK={ok:,} | ERR={err:,}")

    print()
    print("=" * 60)
    print(f"  FIN — Urban Atlas extraído")
    print(f"  Filas escritas: {ok:,}")
    print(f"  Errores:        {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
