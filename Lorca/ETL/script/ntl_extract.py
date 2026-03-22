"""
ntl_extract.py — GTP (Green Turning Point)
Extrae datos de luminosidad nocturna (Nighttime Lights) por ciudad usando Google Earth Engine.

Estrategia dual según año:
  - 2000-2011 : DMSP-OLS (NOAA/DMSP-OLS/NIGHTTIME_LIGHTS) — composites anuales
                Banda: stable_lights (DN 0-63)
                Agrupación: .mean() y .stdDev() sobre el año → una fila por ciudad×año (Month=0)
  - 2012+     : VIIRS DNB (NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG) — composites mensuales
                Banda: avg_rad (nW/cm²/sr), filtrada por cf_cvg > 0 (elimina píxeles con nubes)
                → 12 filas por ciudad×año (Month=1-12)

Output: DatosProcesados/ntl.csv
Columnas: City, Year, Month, NTL_Mean, NTL_Std, NTL_Source

  Month=0  → dato anual DMSP (sin dimensión mensual)
  Month=1-12 → dato mensual VIIRS

Idempotente: reanuda desde donde se quedó si se interrumpe.
  Clave de idempotencia: (City, Year, Month)

Ejecución:
  python ntl_extract.py
  python ntl_extract.py --start-year 2015 --workers 6
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
GOOGLE_PROJECT  = "gtpuem23"
START_YEAR      = 2000           # DMSP disponible desde 1992; GTP desde 2000
DMSP_END_YEAR   = 2011           # Último año con datos DMSP fiables
VIIRS_START     = 2012           # VIIRS disponible desde abril 2012
SCALE_DMSP_M    = 1_000          # Resolución nativa DMSP (~1 km)
SCALE_VIIRS_M   = 500            # Resolución nativa VIIRS (~500 m)
MAX_WORKERS     = 8
MAX_RETRIES     = 4
BUFFER_M        = 20_000         # 20 km buffer alrededor del centroide

OUTPUT_CSV  = config.INPUT_DIR_PROCESSED / "ntl.csv"
CSV_COLUMNS = ["City", "Year", "Month", "NTL_Mean", "NTL_Std", "NTL_Source"]

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
    - retry_missing=True: excluye filas sin datos (NTL_Mean vacío) → se reintentarán.
    """
    if not csv_path.exists():
        return set()
    done = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                if retry_missing and not row.get("NTL_Mean"):
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
# WORKERS
# ==============================================================================
def process_dmsp_year(task: tuple):
    """
    Calcula NTL_Mean y NTL_Std DMSP-OLS para una ciudad y un año completo.
    DMSP produce composites anuales → Month=0 en el CSV (sin dimensión mensual).

    Retorna:
      - tuple (City, Year, 0, NTL_Mean, NTL_Std, 'DMSP') si OK
      - str con prefijo [WARN] si no hay datos válidos
      - str con prefijo [ERROR] si hay fallo GEE
    """
    city, (lat, lon), year = task

    start = f"{year}-01-01"
    end   = f"{year}-12-31"
    roi   = ee.Geometry.Point([lon, lat]).buffer(BUFFER_M).bounds()

    for attempt in range(MAX_RETRIES):
        try:
            col = (
                ee.ImageCollection("NOAA/DMSP-OLS/NIGHTTIME_LIGHTS")
                .filterBounds(roi)
                .filterDate(start, end)
                .select(["stable_lights"])
            )

            img_mean = col.mean().clip(roi)

            stats_mean = img_mean.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=roi,
                scale=SCALE_DMSP_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            stats_std = img_mean.reduceRegion(
                reducer=ee.Reducer.stdDev(),
                geometry=roi,
                scale=SCALE_DMSP_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            ntl_mean = stats_mean.get("stable_lights")
            ntl_std  = stats_std.get("stable_lights")

            if ntl_mean is None:
                return f"[WARN] {city} {year} DMSP: sin datos NTL"

            # Opción A: repetir el valor anual en los 12 meses para
            # mantener granularidad mensual uniforme con VIIRS y el resto de fuentes.
            return [
                (city, year, m, round(ntl_mean, 4), round(ntl_std or 0.0, 4), "DMSP")
                for m in range(1, 13)
            ]

        except ee.EEException as e:
            if attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                time.sleep(wait)
            else:
                return f"[ERROR-GEE] {city} {year} DMSP: {e}"
        except Exception as e:
            return f"[ERROR] {city} {year} DMSP: {e}"

    return f"[ERROR] {city} {year} DMSP: máx reintentos alcanzados"


def process_viirs_month(task: tuple):
    """
    Calcula NTL_Mean y NTL_Std VIIRS DNB para una ciudad y un mes.
    Aplica máscara de cobertura de nubes: cf_cvg > 0 filtra píxeles contaminados.

    Retorna:
      - tuple (City, Year, Month, NTL_Mean, NTL_Std, 'VIIRS') si OK
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
                ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
                .filterBounds(roi)
                .filterDate(start, end)
            )

            def mask_clouds(img):
                """Elimina píxeles con cobertura de nubes (cf_cvg = 0)."""
                cloud_free = img.select("cf_cvg").gt(0)
                return img.select("avg_rad").updateMask(cloud_free)

            col_masked = col.map(mask_clouds)
            img = col_masked.mean().clip(roi)

            stats_mean = img.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=roi,
                scale=SCALE_VIIRS_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            stats_std = img.reduceRegion(
                reducer=ee.Reducer.stdDev(),
                geometry=roi,
                scale=SCALE_VIIRS_M,
                maxPixels=1e8,
                bestEffort=True,
            ).getInfo()

            ntl_mean = stats_mean.get("avg_rad")
            ntl_std  = stats_std.get("avg_rad")

            if ntl_mean is None:
                return f"[WARN] {city} {year}-{month:02d} VIIRS: sin datos NTL"

            return (
                city, year, month,
                round(ntl_mean, 6),
                round(ntl_std or 0.0, 6),
                "VIIRS",
            )

        except ee.EEException as e:
            if attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                time.sleep(wait)
            else:
                return f"[ERROR-GEE] {city} {year}-{month:02d} VIIRS: {e}"
        except Exception as e:
            return f"[ERROR] {city} {year}-{month:02d} VIIRS: {e}"

    return f"[ERROR] {city} {year}-{month:02d} VIIRS: máx reintentos alcanzados"


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP NTL — Luminosidad Nocturna DMSP (2000-2011) + VIIRS (2012+) vía GEE"
    )
    parser.add_argument("--start-year",    type=int,  default=START_YEAR,
                        help=f"Año de inicio (default: {START_YEAR})")
    parser.add_argument("--workers",       type=int,  default=MAX_WORKERS,
                        help=f"Hilos paralelos (default: {MAX_WORKERS})")
    parser.add_argument("--retry-missing", action="store_true",
                        help="Reintentar registros sin datos NTL del CSV anterior")
    args = parser.parse_args()

    print("=" * 60)
    print("  GTP — EXTRACCIÓN NIGHTTIME LIGHTS (DMSP + VIIRS)")
    print(f"  DMSP:   NOAA/DMSP-OLS/NIGHTTIME_LIGHTS ({args.start_year}-{DMSP_END_YEAR})")
    print(f"  VIIRS:  NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG ({VIIRS_START}+)")
    print(f"  Buffer: {BUFFER_M//1000}km | Hilos: {args.workers}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)

    init_csv(OUTPUT_CSV)
    done = load_done(OUTPUT_CSV, retry_missing=args.retry_missing)
    print(f"  Registros ya procesados: {len(done):,}")
    if args.retry_missing:
        print("  [retry-missing] Se reintentarán registros sin datos previos.")

    now = datetime.datetime.now()

    # --- Tareas DMSP (anuales expandidas a 12 meses, Opción A) ---
    # Una tarea por ciudad×año; process_dmsp_year devuelve 12 filas (Month=1-12).
    # Consideramos "done" si month=1 ya existe (el resto del año también se escribió).
    dmsp_tasks = [
        ("DMSP", city, coords, year)
        for city, coords in config.EURO_FUAS.items()
        for year in range(args.start_year, min(DMSP_END_YEAR, now.year) + 1)
        if (city, year, 1) not in done
    ]

    # --- Tareas VIIRS (mensuales, Month=1-12) ---
    viirs_tasks = [
        ("VIIRS", city, coords, year, month)
        for city, coords in config.EURO_FUAS.items()
        for year in range(max(VIIRS_START, args.start_year), now.year + 1)
        for month in range(1, 13)
        if not (year == now.year and month > now.month)
        and (city, year, month) not in done
    ]

    total = len(dmsp_tasks) + len(viirs_tasks)
    print(f"  Pendientes:  {len(dmsp_tasks):,} DMSP (anual) + {len(viirs_tasks):,} VIIRS (mensual) = {total:,}\n")

    ok = warn = err = 0

    def submit_and_collect(executor, tasks):
        nonlocal ok, warn, err

        futures_map = {}
        for t in tasks:
            source = t[0]
            if source == "DMSP":
                _, city, coords, year = t
                fut = executor.submit(process_dmsp_year, (city, coords, year))
                futures_map[fut] = ("DMSP", city, year, None)
            else:
                _, city, coords, year, month = t
                fut = executor.submit(process_viirs_month, (city, coords, year, month))
                futures_map[fut] = ("VIIRS", city, year, month)

        return futures_map

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        all_futures = submit_and_collect(executor, dmsp_tasks + viirs_tasks)
        counter = 0

        for future in as_completed(all_futures):
            result = future.result()
            source, city, year, month = all_futures[future]
            counter += 1

            if isinstance(result, list):
                # DMSP: lista de 12 tuplas (una por mes)
                for row in result:
                    append_row(OUTPUT_CSV, row)
                ok += 1
            elif isinstance(result, tuple):
                # VIIRS: una tupla por mes
                append_row(OUTPUT_CSV, result)
                ok += 1
            elif isinstance(result, str) and "[WARN]" in result:
                # Sin datos: escribir filas vacías para no reintentar
                months = range(1, 13) if source == "DMSP" else [month]
                for m in months:
                    append_row(OUTPUT_CSV, (city, year, m, "", "", source))
                warn += 1
                if warn % 200 == 0:
                    print(result)
            else:
                err += 1
                print(result)

            if counter % 500 == 0:
                print(f"  [{counter:,}/{total:,}] OK={ok:,} | WARN={warn:,} | ERR={err:,}")

    print()
    print("=" * 60)
    print(f"  FIN — Nighttime Lights extraído")
    print(f"  Filas escritas:    {ok:,}")
    print(f"  Sin datos:         {warn:,}")
    print(f"  Errores GEE:       {err:,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
