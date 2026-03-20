"""
Sentinel-5p_extract.py — GTP (Green Turning Point)
Extrae NO2 troposférico mensual medio por ciudad usando Google Earth Engine.

Estrategia: reduceRegion en GEE sobre S5P OFFL L3_NO2 (sin descargas).
Dataset: COPERNICUS/S5P/OFFL/L3_NO2
  - tropospheric_NO2_column_number_density  (mol/m²)
  - Filtro de calidad: cloud_fraction < 0.3

Cobertura temporal: 2018–presente
  Sentinel-5P operativo desde abril 2018 (colección OFFL fiable desde 2018-07).
  Pre-2018: NO2 no disponible en GEE (OMI/Aura no está en el catálogo público).
  El gap 2004–2017 se gestiona en Silver como LEFT JOIN → columna no2_mean = NULL.

Output: DatosProcesados/s5p.csv
Columnas: City, Year, Month, NO2_Mean

Idempotente: reanuda desde donde se quedó si se interrumpe.

Ejecución:
  python Sentinel-5p_extract.py
  python Sentinel-5p_extract.py --workers 10
"""

import csv
import time
import random
import calendar
import datetime
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
START_YEAR     = 2018      # S5P OFFL fiable desde 2018; pre-2018 = NULL en Silver
SCALE_M        = 1_113     # ~1km cuadrado (resolución nativa S5P ~3.5×7 km)
MAX_WORKERS    = 10
MAX_RETRIES    = 4
BUFFER_M       = 20_000    # 20 km buffer alrededor del centroide

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "s5p.csv"
CSV_COLUMNS = ["City", "Year", "Month", "NO2_Mean"]

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
# IDEMPOTENCIA — cargar progreso previo
# ==============================================================================
def load_done(csv_path: Path, retry_missing: bool = False) -> set:
    """
    Devuelve el conjunto de (City, Year, Month) ya procesados.
    - retry_missing=True: excluye filas sin datos (NO2_Mean vacío) → se reintentarán.
    """
    if not csv_path.exists():
        return set()
    done = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                if retry_missing and not row.get("NO2_Mean"):
                    continue
                done.add((row["City"], int(row["Year"]), int(row["Month"])))
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
# WORKER — procesa una ciudad×mes
# ==============================================================================
def process_city_month(task: tuple):
    """
    Calcula NO2_Mean (mol/m²) para una ciudad y un mes.
    Filtra nubes con cloud_fraction < 0.3, luego media mensual + reduceRegion.

    Retorna:
      - tuple (City, Year, Month, NO2_Mean) si OK
      - str con prefijo [WARN] si no hay imágenes válidas
      - str con prefijo [ERROR] si hay fallo GEE
    """
    city, (lat, lon), year, month = task

    last_day = calendar.monthrange(year, month)[1]
    start    = f"{year}-{month:02d}-01"
    end      = f"{year}-{month:02d}-{last_day}"
    roi      = ee.Geometry.Point([lon, lat]).buffer(BUFFER_M).bounds()

    for attempt in range(MAX_RETRIES):
        try:
            col = (
                ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_NO2")
                .filterBounds(roi)
                .filterDate(start, end)
                .map(lambda img: img.updateMask(
                    img.select("cloud_fraction").lt(0.3)
                ))
                .select("tropospheric_NO2_column_number_density")
            )

            # Media mensual de todos los pases del satélite
            img = col.mean().clip(roi)

            stats = img.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            no2 = stats.get("tropospheric_NO2_column_number_density")

            if no2 is None:
                return f"[WARN] {city} {year}-{month:02d}: sin datos S5P (nubes o sin pasadas)"

            return (city, year, month, round(no2, 10))

        except ee.EEException as e:
            if attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                time.sleep(wait)
            else:
                return f"[ERROR-GEE] {city} {year}-{month:02d}: {e}"
        except Exception as e:
            return f"[ERROR] {city} {year}-{month:02d}: {e}"

    return f"[ERROR] {city} {year}-{month:02d}: máx reintentos alcanzados"


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP S5P — NO2 troposférico mensual vía GEE"
    )
    parser.add_argument("--workers",       type=int,  default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    parser.add_argument("--retry-missing", action="store_true",
                        help="Reintentar meses sin datos (nubosos/sin pasadas) del CSV anterior")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN NO2 (Sentinel-5P OFFL L3)")
    print(f"  Colección:  COPERNICUS/S5P/OFFL/L3_NO2")
    print(f"  Cobertura:  {START_YEAR}–presente (pre-{START_YEAR} = NULL en Silver)")
    print(f"  Escala:     {SCALE_M}m | Buffer: {BUFFER_M//1000}km")
    print(f"  Hilos:      {args.workers}")
    print(f"  Output:     {OUTPUT_CSV}")
    print("=" * 60)

    init_csv(OUTPUT_CSV)
    done = load_done(OUTPUT_CSV, retry_missing=args.retry_missing)
    print(f"  Registros ya procesados: {len(done):,}")
    if args.retry_missing:
        print("  [retry-missing] Se reintentarán meses sin datos previos.")

    now   = datetime.datetime.now()
    tasks = [
        (city, coords, year, month)
        for city, coords in config.EURO_FUAS.items()
        for year in range(START_YEAR, now.year + 1)
        for month in range(1, 13)
        if not (year == now.year and month > now.month)
        and (city, year, month) not in done
    ]

    print(f"  Pendientes:  {len(tasks):,} ciudad×mes\n")

    ok = warn = err = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_city_month, t): t for t in tasks}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            task   = futures[future]
            city, _, year, month = task

            if isinstance(result, tuple):
                append_row(OUTPUT_CSV, result)
                ok += 1
            elif isinstance(result, str) and "[WARN]" in result:
                # Sin pasadas válidas ese mes → escribir fila vacía para no reintentar
                append_row(OUTPUT_CSV, (city, year, month, ""))
                warn += 1
                if warn % 200 == 0:
                    print(result)
            else:
                err += 1
                print(result)

            if i % 1000 == 0:
                print(f"  [{i:,}/{len(tasks):,}] OK={ok:,} | WARN={warn:,} | ERR={err:,}")

    print()
    print("=" * 60)
    print(f"  FIN — S5P NO2 extraído")
    print(f"  Filas escritas:    {ok:,}")
    print(f"  Sin datos:         {warn:,}")
    print(f"  Errores GEE:       {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
