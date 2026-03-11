"""
s5p_aerosol_extract.py — GTP (Green Turning Point)
Extrae el Índice de Aerosol UV (UVAI) mensual por ciudad usando Google Earth Engine.

Estrategia: reduceRegion en GEE (sin descargas).
GEE calcula la media mensual del absorbing_aerosol_index de Sentinel-5P.
El UVAI es un proxy de la concentración de aerosoles absorbentes (humo, polvo, ceniza).
  - UVAI > 0 → aerosoles absorbentes (humo, polvo mineral)
  - UVAI < 0 → aerosoles dispersivos (aerosol marino, nube)
  - |UVAI| alto → mayor concentración de partículas en suspensión

Datos:
  Colección: COPERNICUS/S5P/OFFL/L3_AER_AI
  Banda:     absorbing_aerosol_index → renombrada a UVAI
  Disponible: desde 2018

Output: DatosProcesados/s5p_aerosol.csv
Columnas: City, Year, Month, UVAI_Mean, UVAI_Std, pixel_count

Idempotente: reanuda desde donde se quedó si se interrumpe.

Ejecución:
  python s5p_aerosol_extract.py
  python s5p_aerosol_extract.py --start-year 2019 --workers 6
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
START_YEAR     = 2018           # S5P AER_AI disponible desde finales de 2018
SCALE_M        = 1113           # ~1km, resolución nativa S5P (~5.5km) → agrupado a 1km
MAX_WORKERS    = 8
MAX_RETRIES    = 4
BUFFER_M       = 20_000         # 20 km buffer alrededor del centroide

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "s5p_aerosol.csv"
CSV_COLUMNS = ["City", "Year", "Month", "UVAI_Mean", "UVAI_Std", "pixel_count"]

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
    Calcula UVAI_Mean, UVAI_Std y pixel_count para una ciudad y un mes.
    Usa GEE reduceRegion: sin descargar ningún archivo.

    Retorna:
      - tuple (City, Year, Month, UVAI_Mean, UVAI_Std, pixel_count) si OK
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
            col = (
                ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_AER_AI")
                .filterBounds(roi)
                .filterDate(start, end)
                .select("absorbing_aerosol_index")
                .map(lambda img: img.rename("UVAI"))
            )

            img = col.mean().clip(roi)

            reducer = (
                ee.Reducer.mean()
                .combine(ee.Reducer.stdDev(), sharedInputs=True)
                .combine(ee.Reducer.count(),  sharedInputs=True)
            )

            stats = img.reduceRegion(
                reducer=reducer,
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            uvai_mean = stats.get("UVAI_mean")
            uvai_std  = stats.get("UVAI_stdDev", 0.0)
            n_pixels  = stats.get("UVAI_count",  0)

            if uvai_mean is None:
                return f"[WARN] {city} {year}-{month:02d}: sin píxeles válidos"

            return (
                city, year, month,
                round(uvai_mean, 6),
                round(uvai_std or 0.0, 6),
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
        description="GTP S5P Aerosol UVAI — extracción vía GEE reduceRegion"
    )
    parser.add_argument("--start-year", type=int, default=START_YEAR,
                        help=f"Año de inicio (default: {START_YEAR})")
    parser.add_argument("--workers",    type=int, default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN UVAI (Sentinel-5P Aerosol via GEE)")
    print(f"  Colección:  COPERNICUS/S5P/OFFL/L3_AER_AI")
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
    print(f"  FIN — Aerosol UVAI extraído")
    print(f"  Filas escritas:    {ok:,}")
    print(f"  Sin datos:         {warn:,}")
    print(f"  Errores GEE:       {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
