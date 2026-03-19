"""
export_to_postgres.py — GTP (Green Turning Point)

Exporta las tablas Gold de HDFS (Hive Parquet) a PostgreSQL (capa de servicio).

Propósito:
    PostgreSQL sirve al dashboard (Power BI / Tableau).
    La latencia de Hive/HDFS (~segundos) es inadecuada para dashboards;
    PostgreSQL responde en <10ms con índices adecuados.

Arquitectura:
    HDFS Gold (Hive/Parquet)
        └── export_to_postgres.py
                ├── Modo spark: Spark JDBC (en Lorca, lee HDFS directamente)
                └── Modo local: pandas + sqlalchemy (local, lee CSVs exportados)
                        └── PostgreSQL schema gtp.*
                                └── Power BI / Tableau (conexión directa)

Ejecución en Lorca (modo spark):
    spark-submit --master yarn \\
        --packages org.postgresql:postgresql:42.7.3 \\
        export_to_postgres.py \\
        --pg-host <host> --pg-db gtp --pg-user <user> --pg-password <pw>

Ejecución local (modo local, sin Spark):
    python export_to_postgres.py --mode local \\
        --export-dir Lorca/exports/gold \\
        --pg-host localhost --pg-db gtp --pg-user postgres --pg-password <pw>

Variables de entorno (se prefieren sobre argumentos CLI para no exponer credenciales):
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

Prerequisitos:
    - PostgreSQL con el schema gtp.* creado (postgres_serving_ddl.sql)
    - Modo spark: spark-submit con --packages org.postgresql:postgresql:42.7.3
    - Modo local: pip install pandas sqlalchemy psycopg2-binary
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Cargar .env si existe (Lorca/ o Lorca/BBDD/)
try:
    from dotenv import load_dotenv
    for _env in [
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]:
        if _env.exists():
            load_dotenv(_env)
            break
except ImportError:
    pass  # python-dotenv no instalado, se usan las vars de entorno del shell

# EURO_FUAS para exportar dim_city con coordenadas (necesario para mapas Power BI)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ETL" / "script"))
from config import EURO_FUAS


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
HDFS_GOLD = "hdfs:///user/gtp/gold"

GOLD_TABLES = [
    "fact_kuznets",
    "city_ranking",
    "ekc_parameters",
    "model_results",
]

# Cómo recargar datos en PostgreSQL:
#   "replace" → DROP + CREATE + INSERT (tabla completa, más seguro para batch)
#   "append"  → solo INSERT (útil si solo llegan datos nuevos)
PG_WRITE_MODE = "replace"


def get_pg_config(args) -> dict:
    """
    Construye la config de PostgreSQL priorizando variables de entorno sobre CLI.
    Las env vars evitan que las credenciales aparezcan en `ps aux`.
    """
    return {
        "host":     os.environ.get("PG_HOST",     args.pg_host),
        "port":     os.environ.get("PG_PORT",     str(args.pg_port)),
        "database": os.environ.get("PG_DB",       args.pg_db),
        "user":     os.environ.get("PG_USER",     args.pg_user),
        "password": os.environ.get("PG_PASSWORD", args.pg_password),
    }


def timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_dim_city_df():
    """
    Construye un DataFrame de pandas con dim_city desde EURO_FUAS.
    Formato city_code: 'NombreCiudad_ISO2' → city_name = 'NombreCiudad', country_code = 'ISO2'
    """
    import pandas as pd
    rows = []
    for city_code, (lat, lon) in EURO_FUAS.items():
        parts = city_code.rsplit("_", 1)
        city_name    = parts[0] if len(parts) == 2 else city_code
        country_code = parts[1] if len(parts) == 2 else ""
        rows.append({
            "city_code":    city_code,
            "city_name":    city_name,
            "country_code": country_code,
            "latitude":     lat,
            "longitude":    lon,
        })
    return pd.DataFrame(rows)


def export_dim_city(engine, dry_run: bool) -> tuple:
    """Exporta dim_city (coordenadas) a PostgreSQL. Necesario para mapas Power BI."""
    try:
        df = build_dim_city_df()
        n = len(df)
        if dry_run:
            print(f"    [DRY RUN] Escribiría {n} filas a PostgreSQL gtp.dim_city")
        else:
            df.to_sql(
                name="dim_city", schema=None, con=engine,
                if_exists="replace", index=False, chunksize=500, method="multi",
            )
            print(f"    [OK] {n} filas → PostgreSQL gtp.dim_city")
        return (True, n)
    except Exception as e:
        print(f"    [ERROR] dim_city: {e}")
        return (False, 0)


# ==============================================================================
# MODO SPARK — Lee Parquet de HDFS, escribe vía JDBC
# Para usar en Lorca con: spark-submit --packages org.postgresql:postgresql:42.7.3
# ==============================================================================
def export_spark_mode(pg: dict, tables: list, dry_run: bool) -> dict:
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("GTP_Export_PostgreSQL")
        .config("spark.sql.shuffle.partitions", "50")
        .enableHiveSupport()
        .getOrCreate()
    )

    jdbc_url = f"jdbc:mysql://{pg['host']}:{pg['port']}/{pg['database']}?useSSL=false&allowPublicKeyRetrieval=true"
    jdbc_props = {
        "user":      pg["user"],
        "password":  pg["password"],
        "driver":    "com.mysql.cj.jdbc.Driver",
        "batchsize": "5000",
    }

    results = {}
    for table in tables:
        hdfs_path = f"{HDFS_GOLD}/{table}"
        print(f"\n  [{table}] Leyendo {hdfs_path} ...")
        t0 = time.time()
        try:
            df = spark.read.parquet(hdfs_path)
            n = df.count()
            print(f"    {n:,} filas leídas en {time.time()-t0:.1f}s")

            if dry_run:
                print(f"    [DRY RUN] Escribiría a PostgreSQL gtp.{table}")
            else:
                (
                    df.coalesce(4)  # Limitar conexiones JDBC paralelas
                    .write
                    .jdbc(
                        url=jdbc_url,
                        table=table,
                        mode=PG_WRITE_MODE,
                        properties=jdbc_props,
                    )
                )
                elapsed = time.time() - t0
                print(f"    [OK] {n:,} filas → PostgreSQL gtp.{table} ({elapsed:.1f}s)")

            results[table] = (True, n)

        except Exception as e:
            print(f"    [ERROR] {e}")
            results[table] = (False, 0)

    # dim_city siempre se exporta (coordenadas para Power BI)
    print(f"\n  [dim_city] Exportando coordenadas desde config.EURO_FUAS ...")
    try:
        import pandas as pd
        from sqlalchemy import create_engine
        conn_str = f"mysql+pymysql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"
        engine = create_engine(conn_str)
        results["dim_city"] = export_dim_city(engine, dry_run)
    except Exception as e:
        print(f"    [ERROR] dim_city: {e}")
        results["dim_city"] = (False, 0)

    spark.stop()
    return results


# ==============================================================================
# MODO LOCAL — Lee CSVs exportados, escribe vía pandas + sqlalchemy
# Para usar localmente sin Spark: pip install pandas sqlalchemy psycopg2-binary
# ==============================================================================
def export_local_mode(pg: dict, export_dir: Path, tables: list, dry_run: bool) -> dict:
    try:
        import pandas as pd
        from sqlalchemy import create_engine, text
    except ImportError:
        print("[ERROR] Instala dependencias: pip install pandas sqlalchemy pymysql")
        sys.exit(1)

    conn_str = (
        f"mysql+pymysql://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['database']}"
    )
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"  [OK] Conexión a PostgreSQL: {pg['user']}@{pg['host']}:{pg['port']}/{pg['database']}")
    except Exception as e:
        print(f"  [ERROR] No se puede conectar a PostgreSQL: {e}")
        sys.exit(1)

    # dim_city siempre se exporta primero (coordenadas para Power BI)
    print(f"\n  [dim_city] Exportando coordenadas desde config.EURO_FUAS ...")
    results = {"dim_city": export_dim_city(engine, dry_run)}

    for table in tables:
        # Busca el CSV en export_dir/{table}/*.csv o export_dir/{table}.csv
        candidates = (
            list(export_dir.glob(f"{table}/*.csv")) +
            list(export_dir.glob(f"{table}.csv"))
        )
        if not candidates:
            print(f"\n  [{table}] [WARN] No se encontró CSV en {export_dir}. Ignorando.")
            results[table] = (False, 0)
            continue

        csv_path = candidates[0]
        print(f"\n  [{table}] Leyendo {csv_path.name} ...")
        t0 = time.time()
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            n = len(df)
            print(f"    {n:,} filas leídas, {len(df.columns)} columnas")

            if dry_run:
                print(f"    [DRY RUN] Escribiría a PostgreSQL gtp.{table}")
            else:
                df.to_sql(
                    name=table,
                    schema=None,           # MariaDB no usa schemas, usa la DB directamente
                    con=engine,
                    if_exists="replace",   # Trunca y recarga
                    index=False,
                    chunksize=500,
                    method="multi",
                )
                elapsed = time.time() - t0
                print(f"    [OK] {n:,} filas → PostgreSQL gtp.{table} ({elapsed:.1f}s)")

            results[table] = (True, n)

        except Exception as e:
            print(f"    [ERROR] {e}")
            results[table] = (False, 0)

    return results


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="GTP: Exportar tablas Gold a PostgreSQL (capa de servicio)"
    )
    parser.add_argument(
        "--mode", choices=["spark", "local"], default="spark",
        help=(
            "spark: lee HDFS via PySpark+JDBC (en Lorca) | "
            "local: lee CSVs con pandas (sin Spark) — default: spark"
        )
    )
    parser.add_argument(
        "--tables", nargs="+", default=GOLD_TABLES,
        help="Tablas a exportar (default: todas)"
    )
    parser.add_argument("--pg-host",     default="localhost",  help="Host PostgreSQL")
    parser.add_argument("--pg-port",     default=5432, type=int, help="Puerto PostgreSQL")
    parser.add_argument("--pg-db",       default="gtp",        help="Base de datos PostgreSQL")
    parser.add_argument("--pg-user",     default="postgres",   help="Usuario PostgreSQL")
    parser.add_argument("--pg-password", default="",           help="Contraseña PostgreSQL")
    parser.add_argument(
        "--export-dir", default="exports/gold",
        help="Directorio con CSVs del Gold (solo modo local)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simular sin escribir en PostgreSQL"
    )
    args = parser.parse_args()

    print("=" * 65)
    print("  GTP — EXPORT TO POSTGRESQL (Serving Layer)")
    print(f"  Modo:     {args.mode}")
    print(f"  Tablas:   {', '.join(args.tables)}")
    print(f"  Dry Run:  {'SÍ' if args.dry_run else 'NO'}")
    print(f"  Inicio:   {timestamp()}")
    print("=" * 65)

    pg = get_pg_config(args)
    print(f"  Target:   {pg['user']}@{pg['host']}:{pg['port']}/{pg['database']}")

    if args.mode == "spark":
        results = export_spark_mode(pg, args.tables, args.dry_run)
    else:
        export_dir = Path(args.export_dir)
        if not export_dir.exists():
            print(f"\n[ERROR] El directorio --export-dir no existe: {export_dir}")
            sys.exit(1)
        results = export_local_mode(pg, export_dir, args.tables, args.dry_run)

    # Resumen final
    print()
    print("=" * 65)
    print(f"  {'Tabla':<25} {'Estado':<12} {'Filas':>8}")
    print("-" * 65)
    all_ok = True
    total_rows = 0
    for table, (success, n) in results.items():
        status = "✓ OK" if success else "✗ FALLÓ"
        print(f"  {table:<25} {status:<12} {n:>8,}")
        if not success:
            all_ok = False
        total_rows += n
    print("-" * 65)
    print(f"  {'TOTAL':<25} {'':12} {total_rows:>8,}")
    print(f"  Fin: {timestamp()}")
    print("=" * 65)

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
