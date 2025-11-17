import subprocess # Permite ejecutar conmandos de terminal desde Python (crea subprocesos)
import os #Permite interactuar con el Sistema Operativo (rutas de archivos, variables de entorno)
from datetime import datetime # Necesaria para medir el rendimiento del proceso ETL

# ======================================================
# BLOQUE 1: Configuración del Pipeline (Flujo de Trobajo)
# ======================================================

# Definimos una lista de diccionarios. Cada elemento es una "tarea" que el orquestador debe ejecutar.
# Esta estructura permite escalabilidad: añadir un nuevo paso es tan facil como agregar una linea aqui.

SCRIPTS = [

    # ---1. Datos Económicos (PIB, Eurostat, OECD, InvestEU, HRL)---
    # # ===========================================================

    # 1. PIB INE: Extrae datos macroeconómicos de España
    {"file": "pib_ine_extract.py", "args": []},

    # 2. Eurostat: Datos a nivel europeo para commparativa
    {"file": "eurostat_extract.py",
    "args": [
        "--datasets", "all",
        "--eager-merge",
        #"--filters", "time=2000:2024&geo=ES,ES300" #ES300 = madrid
        ]
    },

    # 3. OECD: Datos globales de desarrollo
    # Se configuran "penalties" (esperas) para evitar el error HTTP 429 (Too Many Requests)
    {"file": "OECD_extract.py",
    "args": [
        "--pause", "300",
        "--penalty-429", "300,600,1200"
        ]
    },

    # 4. InvestEU: Datos específicos de inversión en proyectos (clave para detectar financiación ambiental)
    # Se invectan variables de entorno (env) para controlar la agresividad de las peticiones
    {"file": "InvestEU_extract.py",
    "args": [
        "--what", "all"
        ],
    "env": {
        "INVESTEU_BASE_SLEEP": "300", # Espera base entre peticiones
        "INVESTEU_429_PENALTIES": "300,600,1200", # Penalización exponencial si nos bloquean
        "INVESTEU_TIMEOUT": "90", # Tiempo máximo de espera por respuesta antes de dar error
        "INVESTEU_MAX_RETRIES": "3" # Número de reintentos permitidos
        }
    },


    # ---2.FUENTES DE OBSERVACIÓN TERRESTRE (Contexto estructural)---
    # # ===========================================================

    # 5. HRL (High Resolution Layers): Producto de copernicus que ya indica densidad arbórea
    # Sirve como "Ground Truth" (verdad terreno) para validar nuestros cálculos de Sentinel
    {
    "file": "HRL_extract.py", 
    "args": [
        "--service", "tree_cover", # Servicio de cobertura arbórea
        "--layer", "density", # Capa de densidad (0-100%)
        "--bbox=-434145,4891968,-367311,4951156", # Bounding Box (Caja geográfica) que cubre la península ibérica
        "--size", "2048" # Tamaño de la imagen resultante (en píxeles)
    ]
    },
    {
    "file": "HRL_extract.py",
    "args": [
        "--service", "impervious",
        "--layer", "density",
        # Misma BBOX en EPSG:3857 (Mercator)
        "--bbox", "-434145,4891968,-367311,4951156",
        "--size", "2048"
    ]
    },

    # --- 3. SENTINEL-2 (Variables deependientes / Eje Y) ---
    # # ===========================================================
    # Ejecutamos el mismo script tres veces, cada vez con diferentes parámetros para obtener distintos productos.

    # 6. Sentinel-2: Capa visual (TCI) 
    # Objetivo: Generar evidencia visual para la memoria del proyecto
    {
        "file": "Sentinel-2_extract.py",
        "args": [
            "--aoi", "madrid", # Área de interés: Madrid
            "--asset", "tci", # True Color Image (Imagen en color real)
            "--days-back", "30", # Ventana temporal: Últumo mes
            "--max-downloads", "50" # Limitamos descargas oara ahorrar espacio/tiempo en prueba
        ]
    },

    # 7. Sentinel-2: Capa de Calidad(SCL)
    # Objetivo: Data Cleaning. Crear una máscara para eliminar píxeles con nubes o nieve
    {
        "file": "Sentinel-2_extract.py",
        "args": [
            "--aoi", "madrid",
            "--asset", "scl",         # Scene Classification Layer (Mapa de clasificacióon de píxeles)
            "--days-back", "365",     # Histírico de un año para ver estacionalidad
            "--max-downloads", "50"
        ]
    },

    # 8. Sentinel-2; Capa Científica (Bandas Espectrales).
    # Objetivo: Cálculo del KPI ambiental. Bandas necesarias para la fórmula NDVI = (NIR - RED) / (NIR + RED)
    {
        "file": "Sentinel-2_extract.py",
        "args": [
            "--aoi", "madrid",
            "--bands", "B04,B08",     # Rojo (B04) + Infrarrojo cercano (B08) = NDVI
            "--days-back", "365",     # Debe coincidir con el periodo del SCL para poder cruzarlos
            "--max-downloads", "50"
        ]
    }
]

# ======================================================
# BLOQUE 2: Motor de ejecución de scripts
# ======================================================

def ejecutar_script(script):
    """
    Ejecuta un script de Python como un subproceso independiente.
    Captura su salida (logs) en tiempo real y gestiona errores.
    """
    file = script["file"]
    args = script.get("args", [])
    env_vars = script.get("env", {}) # Obtiene variables de entorno extra si existen

    # Construcción de la ruta absoluta para evitar errores de "File Not Found"
    script_path = os.path.join("ETL", "script", file)

    # Verificación de seguridad: ¿Existe el archivo antes de lanzarlo?
    if not os.path.exists(script_path):
        print(f"⚠️ Script no encontrado: {script_path}")
        return False

    # Preparación del entorno: Copiamos el entorno actual y añadimos las variables personalizadas
    # Esto aísla la configuración de cada script (sandbox)
    env = os.environ.copy()
    env.update(env_vars)

    print(f"\n▶️ Ejecutando {file} {' '.join(args)}")
    if env_vars:
        print(f"   🌐 Variables de entorno: {', '.join(env_vars.keys())}\n")

    # Subprocess.Popen: Lanza el script.
    # stdout = subprocess.PIPE permite capturar lo que el scropt imprime para mostrarlo aqui
    process = subprocess.Popen(
        ["python", script_path] + args,
        env=env,
        stdout=subprocess.PIPE, # Capturamos la salida estándar (logs)
        stderr=subprocess.STDOUT, # Redirigimos errores a la salida estándar para verlos juntos
        text=True, # Decodifica los bytes a texto legible
    )

    # Bucle de lectura en tiempo real (Streaming de logs):
    # Esto es vital para procesos largos (como descargas) para saber que no se ha colgado.
    for line in process.stdout:
        print(f"   {line.strip()}")

    # Esperamos a que el subproceso termine
    process.wait()

    # Evaluación del resultado (Exit Code 0 significa éxito)
    if process.returncode == 0:
        print(f"✅ {file} completado correctamente.\n")
        return True
    else:
        print(f"❌ Error al ejecutar {file} (código {process.returncode}).\n")
        return False


# ======================================================
# BLOQUE 3: Main (Punto de entrada del script)
# ======================================================

def main():
    # Registro de tiempo incial para métricas de rendimiento
    inicio = datetime.now()
    print(f"🚀 Inicio del proceso ETL completo — {inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
    print("==================================================================")

    completados = 0
    # Iteramos sobre la lista de tareas definida arriba
    for script in SCRIPTS:
        if ejecutar_script(script):
            completados += 1
            # Nota: Aquí podríamos añadir un "else: break" si quisieramos detener todo ante un error.
            # Al ejecutarlo con docker se puede detener el proceso entero con ctrl+c.

    # Cálculo de métricas finales
    fin = datetime.now()
    duracion = (fin - inicio).total_seconds() / 60
    print("==================================================================")
    print("==================================================================")
    print(f"\n🏁 ETL finalizado ({completados}/{len(SCRIPTS)} completados con éxito)")
    print(f"🕒 Duración total: {duracion:.1f} minutos\n")
    print("==================================================================")
    print("==================================================================")

if __name__ == "__main__":
    main()
