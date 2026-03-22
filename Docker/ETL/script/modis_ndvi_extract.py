"""
modis_ndvi_extract.py — GTP (Green Turning Point)
Extrae NDVI mensual medio y desviación estándar por ciudad usando Google Earth Engine.

Cubre el periodo 2000-2003 (antes del arranque de Sentinel-2/Landsat en el pipeline GTP).
Años 2004+ están cubiertos por Sentinel-2_extract.py.

Estrategia: reduceRegion en GEE sobre MODIS/061/MOD13A3 (NDVI mensual a 500m).
  - Banda: NDVI (escala x 0.0001 para convertir a rango real -1 a 1)
  - Reductores: mean() y stdDev() sobre el ROI tamponado

Output: DatosProcesados/modis_ndvi.csv
Columnas: City, Year, Month, NDVI_Mean, NDVI_Std

Idempotente: reanuda desde donde se quedó si se interrumpe.

Ejecución:
  python modis_ndvi_extract.py
  python modis_ndvi_extract.py --workers 6
"""

import csv
import time
import random
import calendar
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
START_YEAR     = 2000           # MODIS Terra disponible desde febrero 2000
END_YEAR       = 2003           # 2004+ cubierto por Sentinel-2_extract.py
SCALE_M        = 500            # Resolución nativa MOD13A3 (500 m)
MAX_WORKERS    = 8
MAX_RETRIES    = 4
BUFFER_M       = 20_000         # 20 km buffer alrededor del centroide

MODIS_SCALE_FACTOR = 0.0001     # Factor de escala NDVI: DN × 0.0001 → [-1, 1]

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "modis_ndvi.csv"
CSV_COLUMNS = ["City", "Year", "Month", "NDVI_Mean", "NDVI_Std"]

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
    - retry_missing=True: excluye filas sin datos (NDVI_Mean vacío) → se reintentarán.
    """
    if not csv_path.exists():
        return set()
    done = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                if retry_missing and not row.get("NDVI_Mean"):
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
    Calcula NDVI_Mean y NDVI_Std MODIS para una ciudad y un mes.

    MOD13A3 produce una imagen por mes → usamos .mean() para robustez.
    Los valores DN se convierten a NDVI real aplicando el factor de escala 0.0001.

    Retorna:
      - tuple (City, Year, Month, NDVI_Mean, NDVI_Std) si OK
      - str con prefijo [WARN] si no hay datos válidos
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
                ee.ImageCollection("MODIS/061/MOD13A3")
                .filterBounds(roi)
                .filterDate(start, end)
                .select(["NDVI"])
            )

            # MOD13A3: una imagen por mes → .mean() equivalente a .first()
            img = col.mean().multiply(MODIS_SCALE_FACTOR).clip(roi)

            # Calcular mean y stdDev con reduceRegion combinado
            stats_mean = img.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            stats_std = img.reduceRegion(
                reducer=ee.Reducer.stdDev(),
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            ndvi_mean = stats_mean.get("NDVI")
            ndvi_std  = stats_std.get("NDVI")

            if ndvi_mean is None:
                return f"[WARN] {city} {year}-{month:02d}: sin datos MODIS NDVI"

            return (
                city, year, month,
                round(ndvi_mean, 6),
                round(ndvi_std or 0.0, 6),
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
        description="GTP MODIS NDVI — NDVI mensual 2000-2003 vía GEE (MOD13A3)"
    )
    parser.add_argument("--workers",       type=int,  default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    parser.add_argument("--retry-missing", action="store_true",
                        help="Reintentar meses sin datos NDVI del CSV anterior")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN MODIS NDVI (2000-2003)")
    print(f"  Colección:  MODIS/061/MOD13A3")
    print(f"  Escala:     {SCALE_M}m | Buffer: {BUFFER_M//1000}km")
    print(f"  Periodo:    {START_YEAR}-{END_YEAR} (mensual)")
    print(f"  Hilos:      {args.workers}")
    print(f"  Output:     {OUTPUT_CSV}")
    print("=" * 60)

    init_csv(OUTPUT_CSV)
    done = load_done(OUTPUT_CSV, retry_missing=args.retry_missing)
    print(f"  Registros ya procesados: {len(done):,}")
    if args.retry_missing:
        print("  [retry-missing] Se reintentarán meses sin datos previos.")

    tasks = [
        (city, coords, year, month)
        for city, coords in config.EURO_FUAS.items()
        for year in range(START_YEAR, END_YEAR + 1)
        for month in range(1, 13)
        if (city, year, month) not in done
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
                # Sin datos MODIS ese mes → escribir fila vacía para no reintentar
                append_row(OUTPUT_CSV, (city, year, month, "", ""))
                warn += 1
                if warn % 200 == 0:
                    print(result)
            else:
                err += 1
                print(result)

            if i % 500 == 0:
                print(f"  [{i:,}/{len(tasks):,}] OK={ok:,} | WARN={warn:,} | ERR={err:,}")

    print()
    print("=" * 60)
    print(f"  FIN — MODIS NDVI extraído")
    print(f"  Filas escritas:    {ok:,}")
    print(f"  Sin datos:         {warn:,}")
    print(f"  Errores GEE:       {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
