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
echo "[1/5] Configurando entorno Python (virtualenv)..."

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

hdfs dfs -mkdir -p /user/gtp/bronze
hdfs dfs -mkdir -p /user/gtp/silver
hdfs dfs -mkdir -p /user/gtp/gold
hdfs dfs -mkdir -p /user/gtp/models/clustering
hdfs dfs -mkdir -p /user/gtp/models/xgboost
hdfs dfs -mkdir -p /user/gtp/models/prophet

echo "  [OK] Directorios HDFS creados:"
hdfs dfs -ls /user/gtp/

# ==============================================================================
# 4. SCHEMAS: HIVE (Bronze/Silver/Gold) + MARIADB (serving layer)
# ==============================================================================
echo ""
echo "[3/4] Aplicando DDL schemas..."

echo "  Aplicando Bronze DDL (12 tablas Hive)..."
hive -f BBDD/schemas/bronze_ddl.sql
echo "  [OK] Bronze DDL aplicado"

echo "  Aplicando Silver DDL..."
hive -f BBDD/schemas/silver_ddl.sql
echo "  [OK] Silver DDL aplicado"

echo "  Aplicando Gold DDL..."
hive -f BBDD/schemas/gold_ddl.sql
echo "  [OK] Gold DDL aplicado"

echo "  Aplicando MariaDB serving layer DDL (bd_rvm_gtp)..."
mysql -h 10.151.30.2 -P 3306 -u bd_rvm_gtp -pSol2026A bd_rvm_gtp \
    < BBDD/schemas/mariadb_serving_ddl.sql
echo "  [OK] MariaDB DDL aplicado"

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
earthengine authenticate
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
