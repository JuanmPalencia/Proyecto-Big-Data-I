"""
run_pipeline.py — GTP (Green Turning Point)
Orquestador maestro del pipeline BBDD + ML.

Ejecuta las fases en orden correcto, con control de errores
y logging de cada etapa. Cada fase puede omitirse con flags.

Orden de ejecución:
  1. Bronze Ingest     → CSV del ETL → HDFS Bronze (Parquet particionado)
  2. Silver Transform  → Bronze → Star Schema Kimball en HDFS Silver
  3. Gold Build        → Silver → tablas analíticas base en HDFS Gold
  4. Clustering        → K-Means sobre Silver; actualiza Gold con cluster_id
  5. EKC Regression    → Panel Regression por cluster; actualiza Gold con β₁, β₂, Y*
  6. XGBoost           → Clasificador de fase EKC; actualiza Gold con turning_point_phase
  7. Prophet           → Forecast NDVI mensual; actualiza Gold con forecast_1y/3y/5y
  8. Export PG         → Gold HDFS → PostgreSQL (serving layer para dashboard/API)

Ejecución completa en Lorca:
  spark-submit --master yarn run_pipeline.py

Ejecución parcial (desde Silver, saltando Bronze):
  spark-submit --master yarn run_pipeline.py --skip-bronze --skip-silver

Solo modelos ML (los datos ya están en Gold):
  spark-submit --master yarn run_pipeline.py --only-models

Solo exportar a PostgreSQL (pipeline ya corrió antes):
  spark-submit --master yarn run_pipeline.py --only-export
"""

import sys
import time
import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
# Directorio raíz de los scripts BBDD (donde está este archivo)
BBDD_DIR = Path(__file__).resolve().parent

SCRIPTS = {
    "bronze":       BBDD_DIR / "bronze_ingest.py",
    "silver":       BBDD_DIR / "silver_transform.py",
    "gold":         BBDD_DIR / "gold_build.py",
    "clustering":   BBDD_DIR / "models" / "clustering.py",
    "ekc":          BBDD_DIR / "models" / "ekc_regression.py",
    "xgboost":      BBDD_DIR / "models" / "xgboost_classifier.py",
    "prophet":      BBDD_DIR / "models" / "prophet_forecast.py",
    "export":       BBDD_DIR / "export_to_postgres.py",
}

# Argumentos por defecto para cada script
# Para export: las credenciales PG se toman de env vars (PG_HOST, PG_DB, PG_USER, PG_PASSWORD)
SCRIPT_ARGS = {
    "bronze":       ["--mode", "overwrite", "--source", "all"],
    "silver":       [],
    "gold":         ["--table", "all"],
    "clustering":   ["--k-min", "2", "--k-max", "8"],
    "ekc":          [],
    "xgboost":      ["--test-years", "2"],
    "prophet":      ["--horizon", "5"],
    "export":       ["--mode", "spark"],
}

# ==============================================================================
# UTILIDADES
# ==============================================================================

def timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def print_header(phase: str, step: int, total: int):
    print()
    print("=" * 65)
    print(f"  [{step}/{total}] {phase.upper()}")
    print(f"  Inicio: {timestamp()}")
    print("=" * 65)


def print_result(phase: str, success: bool, elapsed: float):
    status = "OK" if success else "FALLÓ"
    print(f"\n  [{status}] {phase} — {elapsed:.1f}s")


def run_spark_submit(script_path: Path, extra_args: list, phase: str = "") -> bool:
    """
    Ejecuta un script PySpark con spark-submit.
    Devuelve True si el proceso terminó con código 0.
    La fase 'export' añade el driver JDBC de PostgreSQL automáticamente.
    """
    cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--driver-memory", "2g",
        "--executor-memory", "4g",
        "--executor-cores", "2",
        "--num-executors", "4",
    ]
    # La exportación a PostgreSQL requiere el driver JDBC
    if phase == "export":
        cmd += ["--packages", "org.postgresql:postgresql:42.7.3"]
    cmd += [str(script_path)] + extra_args

    print(f"  CMD: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=False)
    return result.returncode == 0


# ==============================================================================
# RUNNER DE FASES
# ==============================================================================

class PipelineRunner:
    def __init__(self, dry_run: bool = False):
        self.dry_run   = dry_run
        self.results   = {}   # fase → (success, elapsed_seconds)
        self.start_ts  = datetime.now(timezone.utc)

    def run_phase(self, phase: str, step: int, total: int, extra_args: list = None):
        print_header(phase, step, total)
        t0 = time.time()

        script = SCRIPTS.get(phase)
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
            print(f"  [DRY RUN] Se ejecutaría: spark-submit ... {script.name} {' '.join(args)}")
            success = True
        else:
            success = run_spark_submit(script, args, phase=phase)

        elapsed = time.time() - t0
        self.results[phase] = (success, elapsed)
        print_result(phase, success, elapsed)

        if not success:
            print(f"  [PIPELINE] Fase '{phase}' falló. El pipeline continúa con las fases siguientes.")

        return success

    def print_summary(self):
        total_elapsed = (datetime.now(timezone.utc) - self.start_ts).total_seconds()
        print()
        print("=" * 65)
        print("  RESUMEN DEL PIPELINE GTP")
        print(f"  Fin: {timestamp()}")
        print(f"  Tiempo total: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
        print("-" * 65)
        print(f"  {'Fase':<20} {'Estado':<12} {'Tiempo':>8}")
        print("-" * 65)
        all_ok = True
        for phase, (success, elapsed) in self.results.items():
            status = "✓ OK" if success else "✗ FALLÓ"
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
    # Flags para saltar fases
    parser.add_argument("--skip-bronze",     action="store_true", help="Saltar ingesta Bronze")
    parser.add_argument("--skip-silver",     action="store_true", help="Saltar Silver Transform")
    parser.add_argument("--skip-gold",       action="store_true", help="Saltar Gold Build base")
    parser.add_argument("--skip-clustering", action="store_true", help="Saltar K-Means")
    parser.add_argument("--skip-ekc",        action="store_true", help="Saltar EKC Regression")
    parser.add_argument("--skip-xgboost",    action="store_true", help="Saltar XGBoost")
    parser.add_argument("--skip-prophet",    action="store_true", help="Saltar Prophet")
    parser.add_argument("--skip-export",     action="store_true", help="Saltar Export a PostgreSQL")
    # Shortcuts
    parser.add_argument("--only-models",    action="store_true",
                        help="Ejecutar solo los 4 modelos ML (salta bronze, silver, gold, export)")
    parser.add_argument("--only-ingest",    action="store_true",
                        help="Ejecutar solo Bronze+Silver+Gold (salta modelos ML y export)")
    parser.add_argument("--only-export",    action="store_true",
                        help="Ejecutar solo la exportación a PostgreSQL (pipeline ya corrió)")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Mostrar comandos sin ejecutarlos")
    # Overrides
    parser.add_argument("--bronze-source",  default="all",
                        help="Fuente Bronze (default: all)")
    args = parser.parse_args()

    # Aplicar shortcuts
    if args.only_models:
        args.skip_bronze = args.skip_silver = args.skip_gold = True
        args.skip_export = True
    if args.only_ingest:
        args.skip_clustering = args.skip_ekc = args.skip_xgboost = args.skip_prophet = True
        args.skip_export = True
    if args.only_export:
        args.skip_bronze = args.skip_silver = args.skip_gold = True
        args.skip_clustering = args.skip_ekc = args.skip_xgboost = args.skip_prophet = True

    # Construir plan de ejecución
    phases = []
    if not args.skip_bronze:     phases.append("bronze")
    if not args.skip_silver:     phases.append("silver")
    if not args.skip_gold:       phases.append("gold")
    if not args.skip_clustering: phases.append("clustering")
    if not args.skip_ekc:        phases.append("ekc")
    if not args.skip_xgboost:    phases.append("xgboost")
    if not args.skip_prophet:    phases.append("prophet")
    if not args.skip_export:     phases.append("export")

    print("=" * 65)
    print("  GTP — PIPELINE MAESTRO (BBDD + ML)")
    print(f"  Inicio:   {timestamp()}")
    print(f"  Fases:    {' → '.join(phases) if phases else '(ninguna seleccionada)'}")
    print(f"  Dry Run:  {'SÍ' if args.dry_run else 'NO'}")
    print("=" * 65)

    if not phases:
        print("[WARN] Ninguna fase seleccionada. Usa --help para ver opciones.")
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
