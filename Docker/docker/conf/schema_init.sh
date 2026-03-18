#\!/bin/bash
# schema_init.sh -- inicializa el schema Datanucleus de Hive en PostgreSQL.
# Usa validate-then-init para ser idempotente.

echo "=== GTP: Hive Schema Init ==="

if /opt/hive/bin/schematool -dbType postgres -validate > /dev/null 2>&1; then
    echo "Schema Hive ya existe. Nada que hacer."
    exit 0
fi

echo "Schema no encontrado. Inicializando..."
/opt/hive/bin/schematool -dbType postgres -initSchema
echo "=== Schema inicializado correctamente ==="
