import subprocess
import os
from datetime import datetime

# ======================================================
# Configuración de scripts ETL y parámetros
# ======================================================

SCRIPTS = [
    {
        "file": "pib_ine_extract.py",
        "args": []
    },
    {
        "file": "eurostat_extract.py",
        "args": [
            "--datasets", "all",
            "--filters", "time=2010:2024&geo=ES,PT,FR",
            "--eager-merge"
        ]
    },
    {
        "file": "OECD_extract.py",
        "args": [
            "--pause", "300",                # 5 minutos entre descargas
            "--penalty-429", "300,600,1200"  # Castigos por rate limit
        ]
    },
    {
        "file": "InvestEU_extract.py",
        "args": ["--what", "all"],
        "env": {
            "INVESTEU_BASE_SLEEP": "300",
            "INVESTEU_429_PENALTIES": "300,600,1200",
            "INVESTEU_TIMEOUT": "90",
            "INVESTEU_MAX_RETRIES": "3"
        }
    },
    {
        "file": "HLR_extract.py",
        "args": []
    },
    {
        "file": "Sentinel-2_extract.py",
        "args": []
    }
]

# ======================================================
# Función para ejecutar scripts
# ======================================================

def ejecutar_script(script):
    """Ejecuta un script Python con sus argumentos y variables de entorno."""
    file = script["file"]
    args = script.get("args", [])
    env_vars = script.get("env", {})

    if not os.path.exists(file):
        print(f"⚠️ Script no encontrado: {file}")
        return False

    # Entorno del sistema + específico del script
    env = os.environ.copy()
    env.update(env_vars)

    print(f"\n▶️ Ejecutando {file} {' '.join(args)}")
    if env_vars:
        print(f"   🌐 Variables de entorno: {', '.join(env_vars.keys())}")

    try:
        subprocess.run(["python", file] + args, check=True, env=env)
        print(f"✅ {file} completado correctamente.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al ejecutar {file}: {e}")
        return False


# ======================================================
# Main
# ======================================================

def main():
    inicio = datetime.now()
    print(f"🚀 Inicio del proceso ETL completo — {inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")

    completados = 0
    for script in SCRIPTS:
        if ejecutar_script(script):
            completados += 1

    fin = datetime.now()
    duracion = (fin - inicio).total_seconds() / 60

    print(f"\n🏁 ETL finalizado ({completados}/{len(SCRIPTS)} completados con éxito)")
    print(f"🕒 Duración total: {duracion:.1f} minutos\n")


if __name__ == "__main__":
    main()
