"""
run_all.py — GTP (Green Turning Point)
Orquestador maestro único: ETL completo → pipeline BBDD + ML.

Fases:
  1. Económicos globales : pib_ine, eurostat, OECD, InvestEU, YFinance
  2. Ambientales globales: Sentinel-2 (NDVI), Sentinel-5P (NO2), HRL + hrl_to_csv
  3. Post-proceso        : merge.py (dataset maestro CSV)
  4. BBDD + ML + Export  : bronze → silver → gold → clustering → ekc →
                           xgboost → prophet → export_to_mariadb
                           (via spark-submit run_pipeline.py en YARN)

Ejecución completa:
  python run_all.py

Solo ETL (sin BBDD):
  python run_all.py --skip-bbdd

Solo BBDD (datos ya descargados):
  python run_all.py --only-bbdd

BBDD parcial (solo modelos ML, sin re-ingestar):
  python run_all.py --only-bbdd --bbdd-args "--only-models"

Dry-run completo:
  python run_all.py --dry-run

Nota sobre los scripts de satélite:
  Sentinel-2_extract.py y Sentinel-5p_extract.py iteran TODAS las ciudades
  internamente (via config.EURO_FUAS). No se ejecutan por ciudad.
  HRL_extract.py descarga GeoTIFFs; hrl_to_csv.py los convierte a CSV.
"""

import subprocess
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# ==============================================================================
# RUTAS
# ==============================================================================
BASE_DIR        = Path(__file__).resolve().parent
ETL_SCRIPTS_DIR = str(BASE_DIR)
BBDD_DIR        = BASE_DIR.parent.parent / "BBDD"       # Lorca/BBDD/
RUN_PIPELINE    = BBDD_DIR / "run_pipeline.py"

sys.path.insert(0, str(BASE_DIR))
from config import EURO_FUAS

# ==============================================================================
# PIPELINE — scripts en orden de ejecución
# ==============================================================================

# FASE 1: Datos económicos y financieros
SCRIPTS_ECONOMICOS = [
    {"file": "pib_ine_extract.py",   "args": []},
    {"file": "eurostat_extract.py",  "args": ["--datasets", "all", "--eager-merge"]},
    {"file": "OECD_extract.py",      "args": ["--pause", "30", "--penalty-429", "60,120,300"]},
    # Post-proceso OECD: raw/ → DatosProcesados/oecd_indicators.csv
    {"file": "oecd_process.py",      "args": []},
    {"file": "InvestEU_extract.py",  "args": ["--what", "all"],
     "env": {
         "INVESTEU_BASE_SLEEP":    "30",
         "INVESTEU_429_PENALTIES": "60,120,300",
         "INVESTEU_TIMEOUT":       "90",
         "INVESTEU_MAX_RETRIES":   "3",
     }},
    # Post-proceso InvestEU: raw/ → DatosProcesados/investeu_summary.csv
    {"file": "investeu_process.py",  "args": []},
    {"file": "YFinance_extract.py",  "args": []},
]

# FASE 2: Datos ambientales satelitales
# Cada script itera todas las ciudades internamente — sin bucle por ciudad.
SCRIPTS_AMBIENTALES = [
    # NDVI mensual via GEE getDownloadURL → ZIPs con TIFs
    {"file": "Sentinel-2_extract.py",    "args": []},
    # Convierte los ZIPs de S2 a sentinel2.csv y los elimina (libera espacio)
    {"file": "sentinel2_to_csv.py",      "args": []},
    # NO2 mensual via GEE getDownloadURL → ZIPs con TIFs
    {"file": "Sentinel-5p_extract.py",   "args": []},
    # Convierte los ZIPs de S5P a s5p.csv y los elimina (libera espacio)
    {"file": "s5p_to_csv.py",            "args": []},
    # Impermeabilización via EEA REST → descarga GeoTIFFs
    {"file": "HRL_extract.py",           "args": []},
    # Convierte los GeoTIFFs de HRL a hrl.csv (requiere rasterio)
    {"file": "hrl_to_csv.py",            "args": []},
    # Temperatura + precipitación mensual via GEE ERA5-Land → era5.csv
    {"file": "era5_extract.py",          "args": []},
    # Índice de aerosol UV (UVAI) mensual via GEE S5P → s5p_aerosol.csv
    {"file": "s5p_aerosol_extract.py",   "args": []},
    # Uso de suelo ESA WorldCover 2020+2021 via GEE → urban_atlas.csv
    {"file": "urban_atlas_extract.py",   "args": []},
    # Emisiones CO2 nacionales EDGAR v8 (descarga automática JRC) → edgar_co2.csv
    {"file": "edgar_co2_extract.py",     "args": []},
]

# FASE 3: Post-proceso (merge → dataset maestro CSV)
SCRIPTS_FINALES = [
    {"file": "merge.py", "args": []},
]


# ==============================================================================
# MOTOR DE EJECUCIÓN
# ==============================================================================
def ejecutar_script(script: dict, dry_run: bool = False) -> bool:
    """
    Ejecuta un script Python como subproceso y hace streaming de su output.
    Soporta {"file": "..."} (busca en ETL_SCRIPTS_DIR) o {"path": "..."} (ruta directa).
    """
    args     = script.get("args", [])
    env_vars = script.get("env", {})

    if "path" in script:
        script_path = os.path.abspath(script["path"])
        file_label  = os.path.basename(script_path)
    else:
        file_label  = script["file"]
        script_path = os.path.join(ETL_SCRIPTS_DIR, file_label)

    if not os.path.exists(script_path):
        print(f"  [WARN] Script no encontrado: {script_path} — saltando")
        return False

    env = {**os.environ, **env_vars}

    print(f"\n  >>> {file_label} {' '.join(args)}")
    if env_vars:
        print(f"      ENV: {list(env_vars.keys())}")

    if dry_run:
        print(f"  [DRY RUN] python {script_path} {' '.join(args)}")
        return True

    process = subprocess.Popen(
        ["python", script_path] + args,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    for line in process.stdout:
        print(f"    {line.rstrip()}")
    process.wait()

    if process.returncode == 0:
        print(f"  [OK] {file_label}")
        return True
    else:
        print(f"  [ERROR] {file_label} salió con código {process.returncode}")
        return False


def ejecutar_bbdd_pipeline(extra_args: list = None, dry_run: bool = False) -> bool:
    """
    Lanza run_pipeline.py via spark-submit.
    En Docker usa SPARK_MASTER=local[4]; en Lorca usa yarn (set via env var).
    extra_args permite pasar flags como --only-models, --skip-bronze, etc.
    """
    if not RUN_PIPELINE.exists():
        print(f"  [ERROR] run_pipeline.py no encontrado en: {RUN_PIPELINE}")
        return False

    spark_master = os.environ.get("SPARK_MASTER", "local[4]")
    is_local     = spark_master.startswith("local")

    cmd = [
        "spark-submit",
        "--master", spark_master,
        "--deploy-mode", "client",
        "--driver-memory", "4g",
    ]
    if not is_local:
        cmd += [
            "--executor-memory", "4g",
            "--executor-cores", "2",
            "--num-executors", "4",
        ]
    cmd += [str(RUN_PIPELINE)] + (extra_args or [])

    print(f"\n  >>> spark-submit run_pipeline.py {' '.join(extra_args or [])}")

    if dry_run:
        print(f"  [DRY RUN] {' '.join(cmd)}")
        return True

    result = subprocess.run(cmd, capture_output=False)
    if result.returncode == 0:
        print(f"  [OK] run_pipeline.py")
        return True
    else:
        print(f"  [ERROR] run_pipeline.py salió con código {result.returncode}")
        return False


def ejecutar_fase(nombre: str, scripts: list, dry_run: bool = False) -> tuple[int, int]:
    """Ejecuta una lista de scripts y devuelve (completados, total)."""
    print(f"\n{'='*60}")
    print(f"  FASE: {nombre}")
    print(f"{'='*60}")
    ok = sum(ejecutar_script(s, dry_run=dry_run) for s in scripts)
    return ok, len(scripts)


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP — Orquestador maestro: ETL completo + pipeline BBDD/ML"
    )
    parser.add_argument("--skip-bbdd",    action="store_true",
                        help="Ejecutar solo el ETL, sin lanzar run_pipeline.py")
    parser.add_argument("--only-bbdd",    action="store_true",
                        help="Saltar todo el ETL, ejecutar solo run_pipeline.py")
    parser.add_argument("--bbdd-args",    default="",
                        help="Argumentos extra para run_pipeline.py (ej: '--only-models')")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Mostrar comandos sin ejecutar nada")
    args = parser.parse_args()

    inicio = datetime.now()
    print(f"{'='*60}")
    print(f"  GTP — PIPELINE MAESTRO (ETL + BBDD + ML)")
    print(f"  Ciudades: {len(EURO_FUAS)} | Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Modo:     {'DRY RUN' if args.dry_run else 'REAL'}")
    if args.only_bbdd:
        print(f"  Alcance:  Solo BBDD/ML (ETL saltado)")
    elif args.skip_bbdd:
        print(f"  Alcance:  Solo ETL (BBDD saltado)")
    else:
        print(f"  Alcance:  ETL completo + BBDD/ML")
    print(f"{'='*60}")

    resultados = {}

    # ── FASES ETL ──────────────────────────────────────────────────────────────
    if not args.only_bbdd:
        resultados["Economicos"]  = ejecutar_fase(
            "DATOS ECONÓMICOS Y FINANCIEROS", SCRIPTS_ECONOMICOS, args.dry_run)
        resultados["Ambientales"] = ejecutar_fase(
            "DATOS AMBIENTALES (SATÉLITE)",   SCRIPTS_AMBIENTALES, args.dry_run)
        resultados["Finales"]     = ejecutar_fase(
            "POST-PROCESO (MERGE)",           SCRIPTS_FINALES, args.dry_run)

    # ── FASE BBDD + ML ─────────────────────────────────────────────────────────
    if not args.skip_bbdd:
        print(f"\n{'='*60}")
        print(f"  FASE: BBDD + ML + EXPORT (spark-submit)")
        print(f"{'='*60}")
        extra = args.bbdd_args.split() if args.bbdd_args else []
        bbdd_ok = ejecutar_bbdd_pipeline(extra_args=extra, dry_run=args.dry_run)
        resultados["BBDD_ML"] = (1 if bbdd_ok else 0, 1)

    # ── RESUMEN ────────────────────────────────────────────────────────────────
    fin      = datetime.now()
    duracion = (fin - inicio).total_seconds() / 60
    total_ok = sum(r[0] for r in resultados.values())
    total    = sum(r[1] for r in resultados.values())

    print(f"\n{'='*60}")
    print(f"  RESUMEN PIPELINE GTP")
    print(f"  Duración total: {duracion:.1f} min")
    print(f"  {'Fase':<22} {'OK':>4} / {'Total':>5}")
    print(f"  {'-'*37}")
    for fase, (ok, tot) in resultados.items():
        estado = "✓" if ok == tot else "✗"
        print(f"  {estado} {fase:<20} {ok:>4} / {tot:>5}")
    print(f"  {'-'*37}")
    print(f"  {'TOTAL':<22} {total_ok:>4} / {total:>5}")
    estado_final = "COMPLETADO SIN ERRORES" if total_ok == total else "COMPLETADO CON ERRORES"
    print(f"  {estado_final}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
