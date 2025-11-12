import subprocess
import os
from datetime import datetime

# ======================================================
# Configuración de scripts ETL y parámetros
# ======================================================

SCRIPTS = [
    #{"file": "pib_ine_extract.py", "args": []},
    {"file": "eurostat_extract.py","args": ["--datasets", "all", "--eager-merge"]},
    #{"file": "OECD_extract.py","args": ["--pause", "300", "--penalty-429", "300,600,1200"]},
    #{"file": "InvestEU_extract.py","args": ["--what", "all"],"env": {"INVESTEU_BASE_SLEEP": "300",
    #"INVESTEU_429_PENALTIES": "300,600,1200","INVESTEU_TIMEOUT": "90","INVESTEU_MAX_RETRIES": "3"}},
    #{
    #"file": "HRL_extract.py",
    #"args": [
    #    "--service", "tree_cover",
    #    "--layer", "density",
    #    "--bbox=-9.4,35.9,3.4,43.8"
    #]
    #},




    #{"file": "Sentinel-2_extract.py", "args": []},
]

# ======================================================
# Función para ejecutar scripts mostrando salida en vivo
# ======================================================

def ejecutar_script(script):
    file = script["file"]
    args = script.get("args", [])
    env_vars = script.get("env", {})

    script_path = os.path.join("ETL", "script", file)
    if not os.path.exists(script_path):
        print(f"⚠️ Script no encontrado: {script_path}")
        return False

    env = os.environ.copy()
    env.update(env_vars)

    print(f"\n▶️ Ejecutando {file} {' '.join(args)}")
    if env_vars:
        print(f"   🌐 Variables de entorno: {', '.join(env_vars.keys())}\n")

    # 🔹 Captura y muestra la salida del proceso en vivo
    process = subprocess.Popen(
        ["python", script_path] + args,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    for line in process.stdout:
        print(f"   {line.strip()}")

    process.wait()

    if process.returncode == 0:
        print(f"✅ {file} completado correctamente.\n")
        return True
    else:
        print(f"❌ Error al ejecutar {file} (código {process.returncode}).\n")
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
