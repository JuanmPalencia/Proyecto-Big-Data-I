import os # Para interactuar con el sistema operativo (crear carpetas,unir rutas)
import requests # Para realizar las peticiones HTTP (descargar las imágenes)
import argparse # Para crear la interfaz de línea de comandos (ej: --service tree_cover)
from urllib.parse import urlencode 
from datetime import datetime # Para añadir fechas (timestamps) a los logs

# ======================================================
# CONFIGURACIÓN DE RUTAS Y LOGS
# ======================================================

# Define el directorio base del PROYECTO (subiendo 2 niveles desde /ETL/script)
# Esto es una buena práctica para que funcione en Docker y en local
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

# Define la carpeta de datos específica para HLR (High Resolution Layers)
DATA_DIR = os.path.join(BASE_DIR, "data", "hrl")

# Define una carpeta central de logs para todo el proyecto
LOG_DIR = os.path.join(BASE_DIR, "logs")

# Asegura que la carpeta de logs exista antes de intentar escribir en ella
os.makedirs(LOG_DIR, exist_ok=True)

def log(msg):
    """
    Función de logging centralizada. Escribe un mensaje
    tanto en la consola como en un archivo "etl.log"
    """
    # Genera un timestamp actual para el log
    ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

    # Abre el archivo de log en un modo "append"(a) para añadir al final
    with open(os.path.join(LOG_DIR, "etl.log"), "a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")

    # Imprime el mismo mensaje en la consola para el usuario
    print(msg)

# ======================================================
# CATÁLOGO DE ENDPOINTS (Servidor de Mapas)
# ======================================================

# Este diccionario es el "cerebro" del script
# Mapea nombre amigables (ej: "tree_cover") a las URLs
# de los servidores de mapas de copernicus
HRL_ENDPOINTS = {
    # HLR Cobertura Arbórea (clave para el Eje Y: Medioambiente)
    "tree_cover": {
        "dir": "hrl_tree_cover",
        "layers": {

            # % de densidad de árboles
            "density":
            "https://geoserver.vlcc.geoville.com/geoserver/HRL_TCF/wms",

            # Cambios en la cobertura (2015 vs 2018)
            "change":
            "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_TreeCoverChangeMask_15_18/ImageServer/exportImage",

            # Tipo de hoja (caduca, perenne)
            "leaf_type":
            "https://geoserver.vlcc.geoville.com/geoserver/HRL_TCF/wms",

            # Tipo de bosque
            "forest_type":
            "https://geoserver.vlcc.geoville.com/geoserver/HRL_TCF/wms"
        }
    },

    # HLR Impermeabilidad (Asfalto/Edificios - Eje Y inverso)
    "impervious": {
        "dir": "hrl_impervious",
        "layers": {

            # % de densidad de asfalto 0-100%
            "density":
            "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessDensity_2018/ImageServer/exportImage",

            # Cambios en el asfalto (2015-2018)
            "change":
            "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessChange_15_18/ImageServer/exportImage",

            # Zonas construidas (mapa binario)
            "builtup":
            "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_BuiltUp_2018/ImageServer/exportImage"
        }
    },

    # Atlas Urbano (zonificación detallada de ciudades)
    "urban_atlas": {
        "dir": "hrl_urban_atlas",
        "layers": {
            "urban_2018":
            "https://image.discomap.eea.europa.eu/arcgis/rest/services/UrbanAtlas/UA_UrbanAtlas_2018/MapServer/export"
        }
    },

    # Pequeñas zonas de bosque (setos,etc)
    "small_woody": {
        "dir": "hrl_small_woody",
        "layers": {
            "woody_2018": "https://copernicus.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_SmallWoodyFeatures_2018_005m/ImageServer/exportImage"
        }
    }
}

# ======================================================
# FUNCIÓN DE DESCARGA (Lógica de API)
# ======================================================


def download_layer(service, layer, bbox, size=1024, crs="EPSG:3857"):
    """
    Descarga una capa de mapa HLR dado un servicio, capa y bounding box
    Detecta automáticamente si es un servidor WMS (GeoServer) o ESRI (ArcGis)
    """
    # 1. Validación de entradas
    if service not in HRL_ENDPOINTS:
        raise ValueError(f"Servicio '{service}' no reconocido.")
    if layer not in HRL_ENDPOINTS[service]["layers"]:
        raise ValueError(f"Capa '{layer}' no disponible en {service}.")

    # 2. Preparación de rutas de salida
    out_dir = os.path.join(DATA_DIR, HRL_ENDPOINTS[service]["dir"])
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{layer}.png")

    # 3. Obtiene la URL base del catálogo
    url = HRL_ENDPOINTS[service]["layers"][layer]
    log(f"🛰️ Descargando {service}/{layer} desde {url}")

    # 4. LÓGICA DE DETECCIÓN DE API
    # Dos servidores distintos (GeosServer y ArcGis) requieren
    # parámetros de consulta (query params) totalmente diferentes

    # A) Si es un servidor WMS (Web Map Service) de GeoServer
    if "geoserver" in url:
        params = {
            "service": "WMS",
            "version": "1.1.0",
            "request": "GetMap", # Solicitamos un mapa
            # Lógica para seleccionar el nombre técnico de la capa en GeoServer
            "layers": "HRL_TCF:TCD_S2021" if "tree" in service else "HRL_TCF:FTY_S2021",
            "bbox": bbox, # El Bounding Box (ej:"xmin,ymin,xmax,ymax")
            "width": size, # Ancho de la imagen en píxeles
            "height": size, # Alto de la imagen en píxeles
            "srs": crs, # Sistema de Coordenadas (EPSG:3857=Web Marcator)
            "styles": "",
            "format": "image/png",
            "transparent": "true"
        }

        # B) Si es un servidor ESRI (ArcGIS Image Server)
    else:
        params = {
            "f": "image", # Solicitamos una imagen
            "bbox": bbox, # El Bounding Box
            # El código "102100" es el alias de ESRI para EPSG:3857
            "imageSR": 102100,
            "bboxSR": 102100,
            "size": f"{size},{size}" # Formato ancho/alto
        }

    # 5. Ejecución de la descarga
    # "requests" construirá la URL final (ej:url + ?service=WMS&version=1.1.0...)
    r = requests.get(url, params=params, timeout=180)

    # Si la descarga falla detiene el script
    r.raise_for_status()

    # 6. Guardado del archivo
    # "wb" (write bytes) es OBLIGATORIO para guardar archivos
    # que no son texto, como imágenes, zips o PDFs
    with open(out_file, "wb") as f:
        f.write(r.content)

    log(f"✅ Guardado en {out_file}")
    return out_file

# ======================================================
# MAIN/PUNTO DE ENTRADA
# ======================================================
if __name__ == "__main__":
    # 1. Configuración del Parseador de Argumentos
    ap = argparse.ArgumentParser(description="ETL unificado para HRL Copernicus")
    ap.add_argument("--service", required=True, choices=list(HRL_ENDPOINTS.keys()))
    ap.add_argument("--layer", required=True, help="Nombre de la capa dentro del servicio")
    ap.add_argument("--bbox", required=True, help="Bounding box en EPSG:3857 (xmin,ymin,xmax,ymax)")
    ap.add_argument("--size", type=int, default=1024)

    # 2. Lee los argumentos pasados desde "run_all.py"
    args = ap.parse_args()

    # 3. Redefinición de rutas
    # Estas líneas se definen aquí, de nuevo para asegurar que 
    # las rutas sean correctas, aunque ya estuvieran definidas arriba
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    DATA_DIR = os.path.join(BASE_DIR, "data", "hrl")
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    # Bloque 4. Ejecución principal
    try:

        # LLama a la función de descarga con los argumentos de la consola
        file_path = download_layer(args.service, args.layer, args.bbox, args.size)
        log(f"📦 Proceso completado correctamente → {file_path}")
    except Exception as e:
        
        # Captura cualquier error (ej: fallo de red, validacion)
        log(f"❌ Error: {e}")
