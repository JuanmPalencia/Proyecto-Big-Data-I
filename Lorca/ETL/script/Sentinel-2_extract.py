"""
Sentinel-2_extract.py — GTP (Green Turning Point)
Extrae estadísticas NDVI mensuales por ciudad usando Google Earth Engine.

Estrategia: reduceRegion en GEE (sin descargar GeoTIFFs).
GEE calcula la mediana mensual de NDVI, aplica máscara de nubes y devuelve
un único valor numérico por ciudad×mes — sin descarga de imágenes.

Output: DatosProcesados/sentinel2.csv
Columnas: City, Year, Month, NDVI_Mean, NDVI_Std, pixel_count

Ventajas vs descarga de GeoTIFFs:
  - Sin descargas (TB de datos → cero bytes locales)
  - Sin rasterio (GEE hace la agregación en sus servidores)
  - Idempotente: reanuda desde donde se quedó si se interrumpe
  - Reintentos automáticos con backoff exponencial

Ejecución:
  python Sentinel-2_extract.py
  python Sentinel-2_extract.py --start-year 2020 --workers 6
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
START_YEAR     = 2018
SCALE_M        = 60          # metros por píxel para reduceRegion
MAX_WORKERS    = 8           # hilos paralelos (GEE soporta 8-10 bien)
MAX_RETRIES    = 4           # reintentos ante error GEE
BUFFER_M       = 20_000      # radio buffer alrededor del centroide (20 km)

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "sentinel2.csv"
CSV_COLUMNS = ["City", "Year", "Month", "NDVI_Mean", "NDVI_Std", "pixel_count"]

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
def load_done(csv_path: Path) -> set:
    """Devuelve el conjunto de (City, Year, Month) ya escritos en el CSV."""
    if not csv_path.exists():
        return set()
    done = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
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
# GEE — COLECCIÓN NDVI CON MÁSCARA DE NUBES
# ==============================================================================
def get_ndvi_collection(roi, start_date: str, end_date: str):
    """
    Crea una colección Sentinel-2 con máscara de nubes y banda NDVI calculada.
    Usa S2_SR_HARMONIZED para evitar saltos de calibración históricos.
    """
    def mask_clouds(image):
        qa = image.select('QA60')
        cloud_mask  = qa.bitwiseAnd(1 << 10).eq(0)
        cirrus_mask = qa.bitwiseAnd(1 << 11).eq(0)
        return (image.updateMask(cloud_mask.And(cirrus_mask))
                     .divide(10000))  # escalar reflectancia a [0, 1]

    return (
        ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(roi)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 60))
        .map(mask_clouds)
        .map(lambda img: img.addBands(
            img.normalizedDifference(['B8', 'B4']).rename('NDVI')
        ))
    )


# ==============================================================================
# WORKER — procesa una ciudad×mes
# ==============================================================================
def process_city_month(task: tuple):
    """
    Calcula NDVI_Mean, NDVI_Std y pixel_count para una ciudad y un mes.
    Usa GEE reduceRegion: sin descargar ningún archivo.

    Retorna:
      - tuple (City, Year, Month, NDVI_Mean, NDVI_Std, pixel_count) si OK
      - str con prefijo [WARN] si no hay datos válidos ese mes
      - str con prefijo [ERROR] si hay fallo de GEE o de red
    """
    city, (lat, lon), year, month = task

    last_day = calendar.monthrange(year, month)[1]
    start    = f"{year}-{month:02d}-01"
    end      = f"{year}-{month:02d}-{last_day}"
    roi      = ee.Geometry.Point([lon, lat]).buffer(BUFFER_M).bounds()

    for attempt in range(MAX_RETRIES):
        try:
            col = get_ndvi_collection(roi, start, end)
            img = col.select('NDVI').median().clip(roi)

            # reduceRegion: media + desviación estándar + recuento de píxeles válidos
            # Todo se calcula en los servidores de GEE — cero bytes descargados
            reducer = (
                ee.Reducer.mean()
                .combine(ee.Reducer.stdDev(),  sharedInputs=True)
                .combine(ee.Reducer.count(),   sharedInputs=True)
            )

            stats = img.reduceRegion(
                reducer=reducer,
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True   # ajusta escala si maxPixels se supera
            ).getInfo()

            ndvi_mean = stats.get('NDVI_mean')
            ndvi_std  = stats.get('NDVI_stdDev', 0.0)
            n_pixels  = stats.get('NDVI_count',  0)

            if ndvi_mean is None:
                return f"[WARN] {city} {year}-{month:02d}: sin píxeles válidos (todo nubes)"

            return (
                city, year, month,
                round(ndvi_mean, 6),
                round(ndvi_std or 0.0, 6),
                int(n_pixels or 0),
            )

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
        description="GTP Sentinel-2 NDVI — extracción vía GEE reduceRegion"
    )
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help=f"Año de inicio (default: {START_YEAR})")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN NDVI (Sentinel-2 via GEE)")
    print(f"  Escala:     {SCALE_M}m | Buffer: {BUFFER_M//1000}km")
    print(f"  Hilos:      {args.workers} | Inicio: {args.start_year}")
    print(f"  Output:     {OUTPUT_CSV}")
    print("=" * 60)

    init_csv(OUTPUT_CSV)
    done = load_done(OUTPUT_CSV)
    print(f"  Registros ya procesados: {len(done):,}")

    now   = datetime.datetime.now()
    tasks = [
        (city, coords, year, month)
        for city, coords in config.EURO_FUAS.items()
        for year in range(args.start_year, now.year + 1)
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

            if isinstance(result, tuple):
                append_row(OUTPUT_CSV, result)
                ok += 1
            elif isinstance(result, str) and "[WARN]" in result:
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
    print(f"  FIN — NDVI extraído")
    print(f"  Filas escritas: {ok:,}")
    print(f"  Sin datos (nubes): {warn:,}")
    print(f"  Errores GEE: {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
