"""
era5_extract.py — GTP (Green Turning Point)
Extrae temperatura media y precipitación total mensual por ciudad usando Google Earth Engine.

Estrategia: reduceRegion en GEE sobre ERA5-Land MONTHLY_AGGR (sin descargas).
Dataset: ECMWF/ERA5_LAND/MONTHLY_AGGR
  - temperature_2m          → temperatura del aire a 2m (K → °C)
  - total_precipitation_sum → precipitación total mensual (m)

Output: DatosProcesados/era5.csv
Columnas: City, Year, Month, Temp_Mean_C, Precip_Total_m

Idempotente: reanuda desde donde se quedó si se interrumpe.

Ejecución:
  python era5_extract.py
  python era5_extract.py --start-year 2018 --workers 6
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
START_YEAR     = 2018           # ERA5-Land disponible desde 1950; usamos 2018 para alinear con S2
SCALE_M        = 11_132         # ~0.1°, resolución nativa ERA5-Land (~9 km)
MAX_WORKERS    = 8
MAX_RETRIES    = 4
BUFFER_M       = 20_000         # 20 km buffer alrededor del centroide

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "era5.csv"
CSV_COLUMNS = ["City", "Year", "Month", "Temp_Mean_C", "Precip_Total_m"]

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
# WORKER — procesa una ciudad×mes
# ==============================================================================
def process_city_month(task: tuple):
    """
    Calcula Temp_Mean_C y Precip_Total_m para una ciudad y un mes.
    ERA5-Land MONTHLY_AGGR tiene exactamente una imagen por mes → usamos .mean()
    para robustez (devuelve lo mismo que .first() en este caso).

    Retorna:
      - tuple (City, Year, Month, Temp_Mean_C, Precip_Total_m) si OK
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
                ee.ImageCollection("ECMWF/ERA5_LAND/MONTHLY_AGGR")
                .filterBounds(roi)
                .filterDate(start, end)
                .select(["temperature_2m", "total_precipitation_sum"])
            )

            # ERA5-Land MONTHLY_AGGR: una imagen por mes → .mean() es equivalente a .first()
            img = col.mean().clip(roi)

            stats = img.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            temp_k   = stats.get("temperature_2m")
            precip_m = stats.get("total_precipitation_sum")

            if temp_k is None:
                return f"[WARN] {city} {year}-{month:02d}: sin datos ERA5"

            temp_c = round(temp_k - 273.15, 4)   # Kelvin → Celsius
            return (city, year, month, temp_c, round(precip_m or 0.0, 6))

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
        description="GTP ERA5-Land — temperatura y precipitación mensual vía GEE"
    )
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help=f"Año de inicio (default: {START_YEAR})")
    parser.add_argument("--workers",    type=int, default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN ERA5-Land (Temperatura + Precipitación)")
    print(f"  Colección:  ECMWF/ERA5_LAND/MONTHLY_AGGR")
    print(f"  Escala:     {SCALE_M}m (~9km) | Buffer: {BUFFER_M//1000}km")
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
    print(f"  FIN — ERA5 extraído")
    print(f"  Filas escritas:    {ok:,}")
    print(f"  Sin datos:         {warn:,}")
    print(f"  Errores GEE:       {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
