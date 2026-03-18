"""
init_hive_ddl.py — GTP (Green Turning Point)
Inicializa el esquema Hive en el metastore ejecutando los DDL de Bronze, Silver y Gold.

Los DDL usan CREATE TABLE IF NOT EXISTS / CREATE DATABASE IF NOT EXISTS, por lo que
este script es seguro de ejecutar múltiples veces (idempotente).

Cuándo ejecutar:
  - La primera vez que se levanta el stack Docker
  - Después de borrar el volumen hive_metastore_db (docker volume rm ...)
  - NO es necesario en cada pipeline run (pero no hace daño)

Ejecución:
  docker compose run --rm pipeline python BBDD/init_hive_ddl.py
"""

import re
from pathlib import Path
from pyspark.sql import SparkSession

BBDD_DIR  = Path(__file__).resolve().parent
DDL_FILES = [
    BBDD_DIR / "schemas" / "bronze_ddl.sql",
    BBDD_DIR / "schemas" / "silver_ddl.sql",
    BBDD_DIR / "schemas" / "gold_ddl.sql",
]


def run_ddl_file(spark: SparkSession, ddl_path: Path) -> int:
    """
    Lee un archivo DDL multi-sentencia y ejecuta cada sentencia por separado.
    Ignora líneas de comentario puras. Devuelve el número de sentencias ejecutadas.
    """
    print(f"\n  Aplicando: {ddl_path.name}")
    sql_raw = ddl_path.read_text(encoding="utf-8")

    # Eliminar bloques de comentario de línea completa para evitar sentencias vacías
    lines = [l for l in sql_raw.splitlines() if not l.strip().startswith("--")]
    sql_clean = "\n".join(lines)

    # Dividir por ';' (al final de sentencia real)
    statements = [s.strip() for s in re.split(r";", sql_clean)]
    statements = [s for s in statements if s]  # quitar vacíos

    ok = skip = fail = 0
    for stmt in statements:
        # Ignorar si sólo tiene espacios/comentarios
        if not re.sub(r"--[^\n]*", "", stmt).strip():
            skip += 1
            continue
        try:
            spark.sql(stmt)
            ok += 1
        except Exception as e:
            err = str(e)
            # Si ya existe no es un error real
            if "already exists" in err.lower():
                skip += 1
            else:
                print(f"    [WARN] {err[:120]}")
                fail += 1

    print(f"    → {ok} sentencias OK | {skip} ignoradas | {fail} advertencias")
    return ok


def main():
    print("=" * 60)
    print("  GTP — INICIALIZACIÓN DDL HIVE")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("GTP_DDL_Init")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.hive.metastore.client.socket.timeout", "1200")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    total = 0
    for ddl_file in DDL_FILES:
        if not ddl_file.exists():
            print(f"  [SKIP] No encontrado: {ddl_file}")
            continue
        total += run_ddl_file(spark, ddl_file)

    spark.stop()

    print()
    print("=" * 60)
    print(f"  DDL Hive inicializado ({total} sentencias ejecutadas)")
    print("  Bases de datos: gtp_bronze, gtp_silver, gtp_gold")
    print("=" * 60)


if __name__ == "__main__":
    main()
