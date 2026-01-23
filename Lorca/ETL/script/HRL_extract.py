import os
import math
import time
import requests
import config  # Importamos la configuración centralizada

# --- CONFIGURACIÓN ---
# Guardamos en data/hrl siguiendo la estructura del proyecto
OUTPUT_DIR = config.DATA_DIR / "hrl"

# Resolución de la imagen solicitada (px). 
# 2048x2048 px para un radio de 25km nos da una resolución aprox de 25m/px,
# suficiente para distinguir parques grandes y zonas industriales.
IMG_SIZE = 2048 
RADIUS_KM = 25   

# Catálogo de servicios ArcGIS (REST endpoints de la EEA)
# Nos centramos en 2018 porque es el año base más completo y validado.
HRL_CATALOG = {
    2018: {
        "Imperviousness": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessDensity_2018/ImageServer/exportImage",
        "Tree_Cover":     "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_TreeCoverDensity_2018/ImageServer/exportImage",
        "Small_Woody":    "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_SmallWoodyFeatures_2018_005m/ImageServer/exportImage"
    }
}

# ==============================================================================
# MATEMÁTICA GEOESPACIAL (PROYECCIONES)
# ==============================================================================

def wgs84_to_web_mercator(lat, lon):
    """
    Convierte coordenadas geográficas (GPS/WGS84) a Web Mercator (EPSG:3857).
    Los servidores de mapas web (Google, Bing, Esri) 'hablan' en Mercator.
    """
    # Límites matemáticos de la proyección Mercator
    if abs(lat) > 85.051129:
        raise ValueError("La latitud excede los límites de la proyección Web Mercator.")

    r_earth = 6378137.0
    x = lon * (math.pi / 180.0) * r_earth
    # La magia logarítmica para aplanar la esfera
    y = math.log(math.tan((90 + lat) * math.pi / 360.0)) * r_earth
    return x, y

def get_bbox_string(lat, lon, radius_km):
    """
    Calcula la caja delimitadora (Bounding Box) necesaria para la API de ArcGIS.
    Define el cuadrado geográfico que queremos descargar.
    """
    center_x, center_y = wgs84_to_web_mercator(lat, lon)
    r_meters = radius_km * 1000
    
    # Formato: minx, miny, maxx, maxy
    return f"{int(center_x - r_meters)},{int(center_y - r_meters)},{int(center_x + r_meters)},{int(center_y + r_meters)}"

# ==============================================================================
# MOTOR DE DESCARGA
# ==============================================================================

def download_image(url, bbox, filepath):
    """
    Pide la imagen al servidor y la guarda.
    Incluye validación robusta para asegurar que no guardamos archivos corruptos.
    """
    # 102100 es el código legacy de Esri para Web Mercator (EPSG:3857)
    params = {
        "f": "image",           # Formato de respuesta binaria
        "bbox": bbox,           # Área
        "imageSR": 102100,      # Proyección de salida
        "bboxSR": 102100,       # Proyección de la caja
        "size": f"{IMG_SIZE},{IMG_SIZE}", # Dimensiones en píxeles
        "format": "tiff"        # Formato de archivo (GeoTIFF)
    }

    try:
        # Timeout un poco largo (45s) porque los servidores de la EEA a veces son lentos
        r = requests.get(url, params=params, timeout=45, stream=True)
        r.raise_for_status()

        # Validación de seguridad:
        # A veces la API falla pero devuelve un código 200 y un JSON con el error.
        # Verificamos que el Content-Type sea realmente una imagen.
        content_type = r.headers.get('Content-Type', '').lower()
        if 'image' not in content_type and 'tiff' not in content_type:
            print(f"[ERROR] El servidor devolvió texto/JSON en lugar de imagen: {os.path.basename(filepath)}")
            return False

        # Escritura en disco por chunks (para no saturar RAM)
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True

    except Exception as e:
        print(f"[ERROR] Fallo de red en {os.path.basename(filepath)}: {e}")
        return False

def main():
    print(f"--- INICIANDO DESCARGA HRL (ARCGIS REST API) ---")
    print(f"Output: {OUTPUT_DIR}")
    
    # Iteramos sobre el catálogo (Año -> Capas)
    for year, layers in HRL_CATALOG.items():
        for layer_name, api_url in layers.items():
            
            # Organizamos carpetas por año y tipo de capa
            target_dir = OUTPUT_DIR / str(year) / layer_name
            target_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"\n>>> Descargando capa: {layer_name} ({year})")
            count = 0

            # Iteramos sobre las ciudades definidas en config.py
            for city, (lat, lon) in config.EURO_FUAS.items():
                
                filename = f"{city}_{layer_name}_{year}.tif"
                filepath = target_dir / filename

                # Idempotencia: Si ya la tenemos, pasamos a la siguiente
                if filepath.exists():
                    continue

                # Calculamos coordenadas
                bbox_str = get_bbox_string(lat, lon, RADIUS_KM)
                
                # Ejecutamos descarga
                success = download_image(api_url, bbox_str, filepath)
                
                if success:
                    print(f" -> [OK] {city}")
                    count += 1
                    # Pausa de cortesía para no ser baneados por la EEA
                    time.sleep(0.5) 
            
            print(f"   (Completados {count} archivos en esta capa)")

    print("\n[FIN] Todas las descargas de HRL finalizadas.")

if __name__ == "__main__":
    main()