"""
run_pipeline.py — GTP (Green Turning Point) [Docker / local mode]
Orquestador maestro del pipeline BBDD + ML.

Arquitectura de datos:
  Bronze: CSV → HDFS Parquet (spark-submit)
  Silver: Bronze HDFS → MariaDB Silver via Spark + mysql.connector (spark-submit)
  Gold:   Silver MariaDB → Gold MariaDB via python directo
  ML:     Gold MariaDB → modelos sklearn/prophet → actualizan MariaDB (python)

Orden de ejecucion:
  1. Bronze Ingest     → CSV del ETL → HDFS Bronze (Parquet particionado)
  2. Silver Transform  → Bronze HDFS → MariaDB Silver (dims + facts)
  3. Gold Build        → MariaDB Silver → MariaDB Gold (fact_kuznets, city_ranking)
  4. Clustering        → K-Means sobre Silver; actualiza Gold + dim_cluster
  5. EKC Regression    → Panel Regression por cluster; actualiza dim_cluster + fact_kuznets
  6. XGBoost           → Clasificador de fase EKC; actualiza fact_kuznets + model_results
  7. Prophet           → Forecast NDVI mensual; actualiza fact_kuznets + model_results

Ejecucion en Docker:
  docker compose --profile pipeline run --rm pipeline

Ejecucion parcial (ejemplos):
  python BBDD/run_pipeline.py --skip-bronze
  python BBDD/run_pipeline.py --only-models
  python BBDD/run_pipeline.py --only-ingest
  python BBDD/run_pipeline.py --dry-run
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# ==============================================================================
# CONFIGURACION
# ==============================================================================
BBDD_DIR = Path(__file__).resolve().parent

# Scripts que usan spark-submit (leen/escriben Bronze HDFS)
SPARK_SCRIPTS = {
    "bronze": BBDD_DIR / "bronze_ingest.py",
    "silver": BBDD_DIR / "silver_transform.py",
}

# Scripts que se ejecutan directamente con python (leen/escriben MariaDB)
PYTHON_SCRIPTS = {
    "gold":       BBDD_DIR / "gold_build.py",
    "clustering": BBDD_DIR / "models" / "clustering.py",
    "ekc":        BBDD_DIR / "models" / "ekc_regression.py",
    "xgboost":    BBDD_DIR / "models" / "xgboost_classifier.py",
    "prophet":    BBDD_DIR / "models" / "prophet_forecast.py",
}

ALL_SCRIPTS = {**SPARK_SCRIPTS, **PYTHON_SCRIPTS}

SCRIPT_ARGS = {
    "bronze":     ["--mode", "overwrite", "--source", "all"],
    "silver":     [],
    "gold":       ["--table", "all"],
    "clustering": ["--k-min", "2", "--k-max", "8"],
    "ekc":        [],
    "xgboost":    ["--test-years", "2"],
    "prophet":    ["--horizon", "5"],
}

# Descripcion de cada fase para el log
PHASE_DESC = {
    "bronze":     "Bronze Ingest (CSV → HDFS Parquet)",
    "silver":     "Silver Transform (Bronze HDFS → MariaDB Silver)",
    "gold":       "Gold Build (MariaDB Silver → MariaDB Gold)",
    "clustering": "K-Means Clustering (MariaDB → dim_cluster + fact_kuznets)",
    "ekc":        "EKC Panel Regression (MariaDB → dim_cluster + fact_kuznets)",
    "xgboost":    "XGBoost Classifier (MariaDB → fact_kuznets + model_results)",
    "prophet":    "Prophet Forecast (MariaDB → fact_kuznets + model_results)",
}


# ==============================================================================
# UTILIDADES
# ==============================================================================

def timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def print_header(phase: str, step: int, total: int):
    desc = PHASE_DESC.get(phase, phase.upper())
    print()
    print("=" * 65)
    print(f"  [{step}/{total}] {desc}")
    print(f"  Inicio: {timestamp()}")
    print("=" * 65)


def print_result(phase: str, success: bool, elapsed: float):
    status = "OK" if success else "FALLO"
    print(f"\n  [{status}] {phase} — {elapsed:.1f}s")


def run_spark_submit(script_path: Path, extra_args: list) -> bool:
    """
    Ejecuta un script PySpark con spark-submit.
    En Docker usa SPARK_MASTER=local[4]; en Lorca usa yarn (env var).
    """
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

    cmd += [str(script_path)] + extra_args
    print(f"  CMD: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode == 0


def run_python(script_path: Path, extra_args: list) -> bool:
    """
    Ejecuta un script Python directamente (sin spark-submit).
    Usado para los modelos ML y Gold Build que acceden solo a MariaDB.
    """
    cmd = [sys.executable, str(script_path)] + extra_args
    print(f"  CMD: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    return result.returncode == 0


# ==============================================================================
# RUNNER DE FASES
# ==============================================================================

class PipelineRunner:
    def __init__(self, dry_run: bool = False):
        self.dry_run  = dry_run
        self.results  = {}
        self.start_ts = datetime.now(timezone.utc)

    def run_phase(self, phase: str, step: int, total: int, extra_args: list = None):
        print_header(phase, step, total)
        t0 = time.time()

        script = ALL_SCRIPTS.get(phase)
        if script is None:
            print(f"  [ERROR] Script no definido para fase '{phase}'")
            self.results[phase] = (False, 0)
            return False

        if not script.exists():
            print(f"  [ERROR] Archivo no encontrado: {script}")
            self.results[phase] = (False, 0)
            return False

        args = (extra_args or []) + SCRIPT_ARGS.get(phase, [])

        if self.dry_run:
            use_spark = phase in SPARK_SCRIPTS
            runner    = "spark-submit" if use_spark else "python"
            print(f"  [DRY RUN] {runner} {script.name} {' '.join(args)}")
            success = True
        else:
            if phase in SPARK_SCRIPTS:
                success = run_spark_submit(script, args)
            else:
                success = run_python(script, args)

        elapsed = time.time() - t0
        self.results[phase] = (success, elapsed)
        print_result(phase, success, elapsed)

        if not success:
            print(f"  [PIPELINE] Fase '{phase}' fallo. Continuando con fases siguientes.")

        return success

    def print_summary(self):
        total_elapsed = (datetime.now(timezone.utc) - self.start_ts).total_seconds()
        print()
        print("=" * 65)
        print("  RESUMEN DEL PIPELINE GTP")
        print(f"  Fin:          {timestamp()}")
        print(f"  Tiempo total: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
        print("-" * 65)
        print(f"  {'Fase':<20} {'Estado':<12} {'Tiempo':>8}")
        print("-" * 65)
        all_ok = True
        for phase, (success, elapsed) in self.results.items():
            status = "OK" if success else "FALLO"
            print(f"  {phase:<20} {status:<12} {elapsed:>6.1f}s")
            if not success:
                all_ok = False
        print("-" * 65)
        overall = "COMPLETADO SIN ERRORES" if all_ok else "COMPLETADO CON ERRORES"
        print(f"  Estado final: {overall}")
        print("=" * 65)
        return all_ok


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP Pipeline — Orquestador maestro BBDD + ML"
    )
    parser.add_argument("--skip-bronze",     action="store_true", help="Saltar Bronze Ingest")
    parser.add_argument("--skip-silver",     action="store_true", help="Saltar Silver Transform")
    parser.add_argument("--skip-gold",       action="store_true", help="Saltar Gold Build")
    parser.add_argument("--skip-clustering", action="store_true", help="Saltar K-Means")
    parser.add_argument("--skip-ekc",        action="store_true", help="Saltar EKC Regression")
    parser.add_argument("--skip-xgboost",    action="store_true", help="Saltar XGBoost")
    parser.add_argument("--skip-prophet",    action="store_true", help="Saltar Prophet")
    parser.add_argument("--only-models",    action="store_true",
                        help="Ejecutar solo los 4 modelos ML (salta bronze, silver, gold)")
    parser.add_argument("--only-ingest",    action="store_true",
                        help="Ejecutar solo Bronze+Silver+Gold (salta modelos ML)")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Mostrar comandos sin ejecutarlos")
    parser.add_argument("--bronze-source",  default="all",
                        help="Fuente Bronze (default: all)")
    args = parser.parse_args()

    if args.only_models:
        args.skip_bronze = args.skip_silver = args.skip_gold = True
    if args.only_ingest:
        args.skip_clustering = args.skip_ekc = args.skip_xgboost = args.skip_prophet = True

    phases = []
    if not args.skip_bronze:     phases.append("bronze")
    if not args.skip_silver:     phases.append("silver")
    if not args.skip_gold:       phases.append("gold")
    if not args.skip_clustering: phases.append("clustering")
    if not args.skip_ekc:        phases.append("ekc")
    if not args.skip_xgboost:    phases.append("xgboost")
    if not args.skip_prophet:    phases.append("prophet")

    spark_master = os.environ.get("SPARK_MASTER", "local[4]")

    print("=" * 65)
    print("  GTP — PIPELINE MAESTRO (BBDD + ML)")
    print(f"  Inicio:       {timestamp()}")
    print(f"  Spark Master: {spark_master}")
    print(f"  Fases:        {' -> '.join(phases) if phases else '(ninguna)'}")
    print(f"  Dry Run:      {'SI' if args.dry_run else 'NO'}")
    print("=" * 65)

    if not phases:
        print("[WARN] Ninguna fase seleccionada. Usa --help.")
        sys.exit(0)

    runner = PipelineRunner(dry_run=args.dry_run)
    total  = len(phases)

    for step, phase in enumerate(phases, start=1):
        extra = []
        if phase == "bronze" and args.bronze_source != "all":
            extra = ["--source", args.bronze_source]
        runner.run_phase(phase, step, total, extra_args=extra)

    all_ok = runner.print_summary()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
