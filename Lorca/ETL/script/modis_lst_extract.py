"""
modis_lst_extract.py — GTP (Green Turning Point)
Extrae Land Surface Temperature (Day + Night) mensual por ciudad usando Google Earth Engine.

Estrategia: reduceRegion en GEE sobre MODIS/061/MOD11A2 (LST composites de 8 días a 1km).
  - Banda LST_Day_1km  : LST diurna  (DN × 0.02 → Kelvin → Celsius)
  - Banda LST_Night_1km: LST nocturna (DN × 0.02 → Kelvin → Celsius)
  - Agrupación mensual: filterDate(inicio_mes, fin_mes).mean() sobre los composites de 8 días

Output: DatosProcesados/modis_lst.csv
Columnas: City, Year, Month, LST_Day_C, LST_Night_C

Idempotente: reanuda desde donde se quedó si se interrumpe.

Ejecución:
  python modis_lst_extract.py
  python modis_lst_extract.py --start-year 2010 --workers 6
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
START_YEAR     = 2000           # MODIS Terra disponible desde marzo 2000
SCALE_M        = 1_000          # Resolución nativa MOD11A2 (1 km)
MAX_WORKERS    = 8
MAX_RETRIES    = 4
BUFFER_M       = 20_000         # 20 km buffer alrededor del centroide

LST_SCALE_FACTOR = 0.02         # Factor de escala LST: DN × 0.02 → Kelvin
KELVIN_OFFSET    = 273.15       # Conversión K → °C

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "modis_lst.csv"
CSV_COLUMNS = ["City", "Year", "Month", "LST_Day_C", "LST_Night_C"]

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
    - retry_missing=True: excluye filas sin datos (LST_Day_C vacío) → se reintentarán.
    """
    if not csv_path.exists():
        return set()
    done = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                if retry_missing and not row.get("LST_Day_C"):
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
    Calcula LST_Day_C y LST_Night_C MODIS para una ciudad y un mes.

    MOD11A2 produce composites de 8 días → se agregan a mensual con .mean().
    Los valores DN se convierten a Kelvin (× 0.02) y luego a Celsius (− 273.15).

    Retorna:
      - tuple (City, Year, Month, LST_Day_C, LST_Night_C) si OK
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
                ee.ImageCollection("MODIS/061/MOD11A2")
                .filterBounds(roi)
                .filterDate(start, end)
                .select(["LST_Day_1km", "LST_Night_1km"])
            )

            # Agregar composites de 8 días a media mensual, aplicar escala y convertir a °C
            img = (
                col.mean()
                   .multiply(LST_SCALE_FACTOR)      # DN → Kelvin
                   .subtract(KELVIN_OFFSET)          # Kelvin → Celsius
                   .clip(roi)
            )

            stats = img.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=roi,
                scale=SCALE_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            lst_day   = stats.get("LST_Day_1km")
            lst_night = stats.get("LST_Night_1km")

            if lst_day is None and lst_night is None:
                return f"[WARN] {city} {year}-{month:02d}: sin datos MODIS LST"

            return (
                city, year, month,
                round(lst_day,   4) if lst_day   is not None else "",
                round(lst_night, 4) if lst_night is not None else "",
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
        description="GTP MODIS LST — Land Surface Temperature mensual vía GEE (MOD11A2)"
    )
    parser.add_argument("--start-year",    type=int,  default=START_YEAR,
                        help=f"Año de inicio (default: {START_YEAR})")
    parser.add_argument("--workers",       type=int,  default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    parser.add_argument("--retry-missing", action="store_true",
                        help="Reintentar meses sin datos LST del CSV anterior")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN MODIS LST (Land Surface Temperature)")
    print(f"  Colección:  MODIS/061/MOD11A2")
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
                # Sin datos LST ese mes → escribir fila vacía para no reintentar
                append_row(OUTPUT_CSV, (city, year, month, "", ""))
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
    print(f"  FIN — MODIS LST extraído")
    print(f"  Filas escritas:    {ok:,}")
    print(f"  Sin datos:         {warn:,}")
    print(f"  Errores GEE:       {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
