#!/bin/bash
# =============================================================================
# setup_lorca.sh — GTP (Green Turning Point)
# Configura el entorno completo en el cluster Lorca.
# Ejecutar UNA SOLA VEZ antes de correr run_all.py.
#
# Uso:
#   chmod +x setup_lorca.sh
#   ./setup_lorca.sh
# =============================================================================

set -e  # Para si cualquier comando falla

echo "=============================================="
echo "  GTP — Setup Lorca"
echo "=============================================="

# ==============================================================================
# 1. DEPENDENCIAS PYTHON
# ==============================================================================
echo ""
echo "[1/4] Instalando dependencias Python..."
pip install -r requirements.txt
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
# 3. DATABASES HIVE + MARIADB
# ==============================================================================
echo ""
echo "[3/4] Creando databases y tablas Hive..."

hive -f BBDD/schemas/bronze_ddl.sql
echo "  [OK] Bronze DDL aplicado"

hive -f BBDD/schemas/silver_ddl.sql
echo "  [OK] Silver DDL aplicado"

hive -f BBDD/schemas/gold_ddl.sql
echo "  [OK] Gold DDL aplicado"

echo ""
echo "  Creando tablas en MariaDB (bd_rvm_gtp)..."
mysql -h 10.151.30.2 -P 3306 -u bd_rvm_gtp -pSol2026A bd_rvm_gtp < BBDD/schemas/mariadb_serving_ddl.sql
echo "  [OK] MariaDB DDL aplicado"

# ==============================================================================
# 4. GOOGLE EARTH ENGINE
# ==============================================================================
echo ""
echo "[4/4] Autenticación Google Earth Engine..."
echo "  Abrirá un enlace en el navegador. Autoriza con tu cuenta Google."
echo "  Si estás en modo headless (sin navegador), copia el enlace manualmente."
echo ""
earthengine authenticate
echo "  [OK] GEE autenticado"

# ==============================================================================
# RESUMEN
# ==============================================================================
echo ""
echo "=============================================="
echo "  Setup completado."
echo ""
echo "  Siguiente paso:"
echo "    1. Copia .env.example a .env y rellena las credenciales PostgreSQL"
echo "    2. Ejecuta: python ETL/script/run_all.py"
echo "=============================================="
