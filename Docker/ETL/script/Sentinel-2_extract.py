"""
Sentinel-2_extract.py — GTP (Green Turning Point)
Extrae NDVI mensual medio y desviación estándar por ciudad usando Google Earth Engine.

Estrategia multi-satélite (reduceRegion, sin descargas):
  - 2004–2012 : Landsat 7 ETM+  (LANDSAT/LE07/C02/T1_L2)  SR_B4/SR_B3
  - 2013–2016 : Landsat 8 OLI   (LANDSAT/LC08/C02/T1_L2)  SR_B5/SR_B4
  - 2017+     : Sentinel-2 L2A  (COPERNICUS/S2_SR_HARMONIZED) B8/B4

Procesamiento por satélite:
  Landsat     → scale × 0.0000275 + (-0.2), cloud mask QA_PIXEL bits 1/3/4
  Sentinel-2  → divide 10000, cloud mask QA60 bits 10/11

Output: DatosProcesados/sentinel2.csv
Columnas: City, Year, Month, NDVI_Mean, NDVI_Std, Satellite

Idempotente: reanuda desde donde se quedó si se interrumpe.

Ejecución:
  python Sentinel-2_extract.py
  python Sentinel-2_extract.py --start-year 2004 --workers 8
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
START_YEAR     = 2004
LANDSAT8_START = 2013      # Landsat 8 disponible desde 2013
S2_START       = 2017      # Sentinel-2 disponible desde 2017 (cobertura completa)

SCALE_M        = 100       # 100 m: balance cobertura/velocidad para análisis continental
MAX_WORKERS    = 8
MAX_RETRIES    = 4
BUFFER_M       = 20_000    # 20 km buffer alrededor del centroide

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "sentinel2.csv"
CSV_COLUMNS = ["City", "Year", "Month", "NDVI_Mean", "NDVI_Std", "Satellite"]

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
    - retry_missing=False (default): incluye filas sin datos (NDVI_Mean vacío)
      → no se reintentarán en esta ejecución.
    - retry_missing=True: excluye filas sin datos
      → se reintentarán aunque ya estén en el CSV.
    """
    if not csv_path.exists():
        return set()
    done = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                if retry_missing and not row.get("NDVI_Mean"):
                    continue  # fila vacía → dejar que se reintente
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
# FUNCIONES DE ENMASCARADO Y COLECCIÓN POR SATÉLITE
# ==============================================================================
def _mask_landsat_clouds(image):
    """
    Enmascara nubes en Landsat Collection 2 Surface Reflectance.
    QA_PIXEL: bit 1 = dilated cloud, bit 3 = cloud, bit 4 = cloud shadow.
    """
    qa = image.select("QA_PIXEL")
    mask = (
        qa.bitwiseAnd(1 << 1).eq(0)   # dilated cloud
          .And(qa.bitwiseAnd(1 << 3).eq(0))  # cloud
          .And(qa.bitwiseAnd(1 << 4).eq(0))  # cloud shadow
    )
    return image.updateMask(mask)


def _apply_landsat_scale(image):
    """
    Aplica factores de escala de Collection 2 SR:
    SR × 0.0000275 + (-0.2)  → reflectancia en rango 0-1
    """
    optical = image.select("SR_B.").multiply(0.0000275).add(-0.2)
    return image.addBands(optical, overwrite=True)


def _mask_s2_clouds(image):
    """Enmascara nubes en Sentinel-2 usando QA60 (bits 10 y 11)."""
    qa = image.select("QA60")
    mask = (
        qa.bitwiseAnd(1 << 10).eq(0)
          .And(qa.bitwiseAnd(1 << 11).eq(0))
    )
    return image.updateMask(mask).divide(10000)


def get_ndvi_collection(roi, start: str, end: str, year: int):
    """
    Devuelve (colección GEE con banda NDVI, nombre_satélite).
    Selecciona automáticamente la fuente según el año.
    """
    if year < LANDSAT8_START:
        # Landsat 7 ETM+: NIR=SR_B4, Red=SR_B3
        col = (
            ee.ImageCollection("LANDSAT/LE07/C02/T1_L2")
            .filterBounds(roi)
            .filterDate(start, end)
            .map(_mask_landsat_clouds)
            .map(_apply_landsat_scale)
            .map(lambda img: img.addBands(
                img.normalizedDifference(["SR_B4", "SR_B3"]).rename("NDVI")
            ))
        )
        satellite = "Landsat7"

    elif year < S2_START:
        # Landsat 8 OLI: NIR=SR_B5, Red=SR_B4
        col = (
            ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
            .filterBounds(roi)
            .filterDate(start, end)
            .map(_mask_landsat_clouds)
            .map(_apply_landsat_scale)
            .map(lambda img: img.addBands(
                img.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI")
            ))
        )
        satellite = "Landsat8"

    else:
        # Sentinel-2 SR Harmonized: NIR=B8, Red=B4
        col = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(roi)
            .filterDate(start, end)
            .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 60))
            .map(_mask_s2_clouds)
            .map(lambda img: img.addBands(
                img.normalizedDifference(["B8", "B4"]).rename("NDVI")
            ))
        )
        satellite = "Sentinel2"

    return col, satellite


# ==============================================================================
# WORKER — procesa una ciudad×mes
# ==============================================================================
def process_city_month(task: tuple):
    """
    Calcula NDVI_Mean y NDVI_Std para una ciudad y un mes.
    Usa mediana del compuesto mensual + reduceRegion (sin descargas).

    Retorna:
      - tuple (City, Year, Month, NDVI_Mean, NDVI_Std, Satellite) si OK
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
            col, satellite = get_ndvi_collection(roi, start, end, year)

            # Compuesto mensual: mediana (robusta frente a nubes residuales)
            img = col.select("NDVI").median().clip(roi)

            # Doble reductor: mean + stdDev en una sola llamada GEE
            stats = img.reduceRegion(
                reducer=ee.Reducer.mean().combine(
                    ee.Reducer.stdDev(), sharedInputs=True
                ),
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            ndvi_mean = stats.get("NDVI_mean")
            ndvi_std  = stats.get("NDVI_stdDev")

            if ndvi_mean is None:
                return f"[WARN] {city} {year}-{month:02d}: sin píxeles limpios ({satellite})"

            return (
                city, year, month,
                round(ndvi_mean, 6),
                round(ndvi_std or 0.0, 6),
                satellite,
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
        description="GTP NDVI — Landsat 7/8 + Sentinel-2 mensual vía GEE"
    )
    parser.add_argument("--start-year",    type=int,  default=START_YEAR,
                        help=f"Año de inicio (default: {START_YEAR})")
    parser.add_argument("--workers",       type=int,  default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    parser.add_argument("--retry-missing", action="store_true",
                        help="Reintentar meses sin datos (nubes/sin píxeles) del CSV anterior")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN NDVI (Landsat 7/8 + Sentinel-2)")
    print(f"  2004–{LANDSAT8_START-1} : Landsat 7  (LANDSAT/LE07/C02/T1_L2)")
    print(f"  {LANDSAT8_START}–{S2_START-1}  : Landsat 8  (LANDSAT/LC08/C02/T1_L2)")
    print(f"  {S2_START}+    : Sentinel-2 (S2_SR_HARMONIZED)")
    print(f"  Escala:     {SCALE_M}m | Buffer: {BUFFER_M//1000}km")
    print(f"  Hilos:      {args.workers} | Inicio: {args.start_year}")
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
            task   = futures[future]
            city, _, year, month = task

            if isinstance(result, tuple):
                append_row(OUTPUT_CSV, result)
                ok += 1
            elif isinstance(result, str) and "[WARN]" in result:
                # Sin píxeles limpios ese mes → escribir fila vacía para no reintentar
                append_row(OUTPUT_CSV, (city, year, month, "", "", "NO_DATA"))
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
    print(f"  Filas escritas:    {ok:,}")
    print(f"  Sin píxeles:       {warn:,}")
    print(f"  Errores GEE:       {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
