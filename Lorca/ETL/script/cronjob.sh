#!/bin/bash

# --- CONFIGURACIÓN ---

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
LOG_FILE="$LOG_DIR/cronjob_$(date +\%Y\%m\%d).log"

mkdir -p "$LOG_DIR"

echo "========================================================" >> "$LOG_FILE"
echo " INICIANDO ACTUALIZACIÓN MENSUAL GTP: $(date)" >> "$LOG_FILE"
echo "========================================================" >> "$LOG_FILE"

# 1. Ir al directorio
cd "$PROJECT_DIR" || { echo "No encuentro el directorio $PROJECT_DIR"; exit 1; }


# Función auxiliar para ejecutar scripts
run_step() {
    script_path=$1
    echo "--- Ejecutando: $script_path ---" >> "$LOG_FILE"
    # Usamos python3
    if python3 -u "$script_path" >> "$LOG_FILE" 2>&1; then
        echo "[OK] $script_path finalizado." >> "$LOG_FILE"
    else
        echo "[ERROR] Fallo en $script_path. Revisa el log." >> "$LOG_FILE"
        exit 1
    fi
}

# --- FASE 1: DESCARGA (EXTRACT) ---
echo ">> [FASE 1] Buscando nuevos datos..." >> "$LOG_FILE"

run_step "Sentinel-5p_extract.py" 
run_step "Sentinel-2_extract.py"      
run_step "eurostat.py"              
run_step "YFinance_extract.py" 
run_step "HRL_extract.py"
run_step "InvestEU_extract.py"
run_step "OECD_extract.py"
run_step "pib_ine_extract.py"


# --- FASE 2: PROCESAMIENTO (LIMPIEZA) ---
echo ">> [FASE 2] Procesando y limpiando datos..." >> "$LOG_FILE"

run_step("../../bbdd/spark.py")

# --- FASE 3: MERGE FINAL ---
echo ">> [FASE 3] Generando Master Dataset actualizado..." >> "$LOG_FILE"

run_step "merge.py"

echo "========================================================" >> "$LOG_FILE"
echo " ACTUALIZACIÓN COMPLETADA: $(date)" >> "$LOG_FILE"
echo "========================================================" >> "$LOG_FILE"