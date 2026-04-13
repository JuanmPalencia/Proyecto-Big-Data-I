#!/bin/bash
# =============================================================================
# setup_lorca.sh — GTP (Green Turning Point)
# Configura el entorno completo en el cluster Lorca (UEM).
# Ejecutar UNA SOLA VEZ antes de correr run_all.py.
#
# Uso (desde la carpeta Lorca/):
#   chmod +x setup_lorca.sh
#   ./setup_lorca.sh
# =============================================================================

set -e  # Para si cualquier comando falla

# Situarse siempre en el directorio donde está este script (Lorca/)
cd "$(dirname "$0")"

echo "=============================================="
echo "  GTP — Setup Lorca"
echo "=============================================="

# ==============================================================================
# 1. ENTORNO PYTHON (virtualenv — obligatorio en JupyterHub/Lorca)
# ==============================================================================
echo ""
echo "[1/4] Configurando entorno Python (virtualenv)..."

VENV_DIR="$HOME/gtp_venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "  Creando virtualenv en $VENV_DIR ..."
    python -m venv "$VENV_DIR"
    echo "  [OK] Virtualenv creado"
else
    echo "  Virtualenv ya existe en $VENV_DIR — reutilizando"
fi

# Activar el virtualenv
source "$VENV_DIR/bin/activate"
echo "  [OK] Virtualenv activado: $(which python)"

echo "  Instalando dependencias desde requirements.txt ..."
# Nota: pyspark está en requirements.txt para entornos locales.
# En Lorca, PySpark viene del cluster — si hay conflicto de versión,
# comenta la línea pyspark en requirements.txt y usa el PySpark del sistema.
pip install -r requirements.txt -q
echo "  [OK] Dependencias instaladas"

# ==============================================================================
# 2. DIRECTORIOS HDFS
# ==============================================================================
echo ""
echo "[2/4] Creando directorios en HDFS..."

HDFS_ROOT="${HDFS_ROOT:-/user/gtp}"

hdfs dfs -mkdir -p "$HDFS_ROOT/bronze"
hdfs dfs -mkdir -p "$HDFS_ROOT/silver"
hdfs dfs -mkdir -p "$HDFS_ROOT/gold"
hdfs dfs -mkdir -p "$HDFS_ROOT/models/clustering"
hdfs dfs -mkdir -p "$HDFS_ROOT/models/xgboost"
hdfs dfs -mkdir -p "$HDFS_ROOT/models/prophet"

echo "  [OK] Directorios HDFS creados:"
hdfs dfs -ls "$HDFS_ROOT/"

# ==============================================================================
# 4. SCHEMAS: HIVE (Bronze/Silver/Gold) + MARIADB (serving layer)
# ==============================================================================
echo ""
echo "[3/4] Aplicando DDL schemas..."

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

DB_HOST="${PG_HOST:-your-mariadb-host}"
DB_PORT="${PG_PORT:-3306}"
DB_NAME="${PG_DB:-your_database_name}"
DB_USER="${PG_USER:-your_database_user}"
DB_PASSWORD="${PG_PASSWORD:-replace-with-real-password}"

# Detectar cliente SQL disponible para ejecutar DDL de Hive
if command -v hive >/dev/null 2>&1; then
    HIVE_CMD=(hive)
    HIVE_LABEL="hive"
elif command -v spark-sql >/dev/null 2>&1; then
    HIVE_CMD=(spark-sql)
    HIVE_LABEL="spark-sql"
elif command -v beeline >/dev/null 2>&1; then
    HIVE_CMD=(beeline -u "jdbc:hive2://localhost:10000/default")
    HIVE_LABEL="beeline"
else
    echo "  [ERROR] No se encontró cliente SQL para Hive (hive/spark-sql/beeline)."
    echo "  Pide al administrador del cluster habilitar uno de esos comandos."
    exit 1
fi

echo "  Cliente SQL detectado: $HIVE_LABEL"

# Helper: comprueba si una base de datos Hive ya existe
db_exists() {
    local db_name="$1"
    "${HIVE_CMD[@]}" -e "SHOW DATABASES LIKE '${db_name}';" 2>/dev/null | grep -Eiq "^${db_name}$"
}

if db_exists "gtp_bronze"; then
    echo "  [SKIP] gtp_bronze ya existe — se omite bronze_ddl.sql"
else
    echo "  Aplicando Bronze DDL (12 tablas Hive)..."
    "${HIVE_CMD[@]}" -f BBDD/schemas/bronze_ddl.sql
    echo "  [OK] Bronze DDL aplicado"
fi

if db_exists "gtp_silver"; then
    echo "  [SKIP] gtp_silver ya existe — se omite silver_ddl.sql"
else
    echo "  Aplicando Silver DDL..."
    "${HIVE_CMD[@]}" -f BBDD/schemas/silver_ddl.sql
    echo "  [OK] Silver DDL aplicado"
fi

if db_exists "gtp_gold"; then
    echo "  [SKIP] gtp_gold ya existe — se omite gold_ddl.sql"
else
    echo "  Aplicando Gold DDL..."
    "${HIVE_CMD[@]}" -f BBDD/schemas/gold_ddl.sql
    echo "  [OK] Gold DDL aplicado"
fi

echo "  Aplicando MariaDB serving layer DDL (${DB_NAME})..."
if command -v mysql >/dev/null 2>&1; then
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" \
        < BBDD/schemas/mariadb_serving_ddl.sql
    echo "  [OK] MariaDB DDL aplicado (cliente: mysql)"
elif command -v mariadb >/dev/null 2>&1; then
    mariadb -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" \
        < BBDD/schemas/mariadb_serving_ddl.sql
    echo "  [OK] MariaDB DDL aplicado (cliente: mariadb)"
else
    echo "  mysql/mariadb CLI no disponible. Usando fallback Python (pymysql)..."
    DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" python - <<'PY'
import re
import os
from pathlib import Path

import pymysql

ddl_path = Path("BBDD/schemas/mariadb_serving_ddl.sql")
sql = ddl_path.read_text(encoding="utf-8")

# Elimina comentarios de línea para evitar sentencias vacías al partir por ';'
clean_lines = []
for line in sql.splitlines():
    s = line.strip()
    if s.startswith("--"):
        continue
    clean_lines.append(line)

clean_sql = "\n".join(clean_lines)
statements = [s.strip() for s in clean_sql.split(";") if s.strip()]

conn = pymysql.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ["DB_PORT"]),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    database=os.environ["DB_NAME"],
    autocommit=True,
)

try:
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    print("  [OK] MariaDB DDL aplicado (cliente: pymysql)")
finally:
    conn.close()
PY
fi

# ==============================================================================
# 5. GOOGLE EARTH ENGINE
# ==============================================================================
echo ""
echo "[4/4] Autenticación Google Earth Engine (proyecto: gtpuem23)..."
echo ""
echo "  IMPORTANTE — el cluster es headless (sin navegador)."
echo "  El comando abrirá una URL. Cópiala en tu navegador local, autoriza"
echo "  con la cuenta Google del proyecto gtpuem23 y pega el token aquí."
echo ""
if command -v gcloud >/dev/null 2>&1; then
    earthengine authenticate
else
    echo "  gcloud no disponible; usando auth mode notebook (headless)..."
    earthengine authenticate --auth_mode=notebook
fi
echo "  [OK] GEE autenticado"

# Verificar que GEE funciona
python -c "
import ee
try:
    ee.Initialize(project='gtpuem23')
    print('  [OK] GEE inicializado correctamente con proyecto gtpuem23')
except Exception as e:
    print(f'  [WARN] GEE init falló: {e}')
"

# ==============================================================================
# RESUMEN
# ==============================================================================
echo ""
echo "=============================================="
echo "  Setup completado."
echo ""
echo "  Entorno Python:  $VENV_DIR"
echo "  Para activarlo en sesiones futuras:"
echo "    source ~/gtp_venv/bin/activate"
echo ""
echo "  Credenciales pendientes de rellenar en .env:"
echo "    COPERNICUS_USER / COPERNICUS_PASSWORD  (HRL)"
echo "    WEKEO_USERNAME / WEKEO_PASSWORD        (satélite alternativo)"
echo ""
echo "  Siguiente paso:"
echo "    cd Lorca/"
echo "    source ~/gtp_venv/bin/activate"
echo "    python ETL/script/run_all.py"
echo "=============================================="
