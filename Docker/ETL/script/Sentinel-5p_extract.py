import os
import time
import calendar
import datetime
import requests
import ee
from concurrent.futures import ThreadPoolExecutor, as_completed
import config  # Importamos la configuración para no repetir el diccionario de ciudades

# --- CONFIGURACIÓN DE DESCARGA ---
GOOGLE_PROJECT = "gtpuem"  # Tu ID de proyecto en Google Cloud/GEE
START_YEAR = 2018          # Sentinel-5P empezó a operar fiablemente en 2018
OUTPUT_DIR = config.DATA_DIR / "sentinel5p"

# Resolución espacial: 
# Sentinel-5P tiene una resolución nativa de ~3.5x7km. 
# Re-muestreamos a ~1km (1113.2m) para tener píxeles cuadrados y manejables.
SCALE_M = 1113.2 
MAX_WORKERS = 10  # Hilos paralelos (ajustar según tu ancho de banda)

# --- INICIALIZACIÓN GEE ---
try:
    # Iniciamos la conexión con los servidores de Google
    ee.Initialize(project=GOOGLE_PROJECT)
    print("[INFO] Conexión exitosa con Google Earth Engine.")
except Exception as e:
    raise RuntimeError(f"Fallo de autenticación GEE. Ejecuta 'earthengine authenticate' primero.\nError: {e}")

def mask_clouds(image):
    """
    Filtro de Calidad: Las nubes bloquean la visión del sensor troposférico.
    Esta función elimina cualquier píxel con más del 30% de cobertura nubosa
    para evitar falsos positivos en la lectura de NO2.
    """
    cloud_frac = image.select('cloud_fraction')
    return image.updateMask(cloud_frac.lt(0.3))

def get_monthly_composite(lat, lon, year, month, radius_km=20):
    """
    Crea una imagen resumen del mes para una ciudad específica.
    
    Lógica:
    1. Define el área de interés (punto central + radio).
    2. Busca todas las pasadas del satélite en ese mes.
    3. Elimina nubes.
    4. Calcula la MEDIA de los valores válidos (reducción temporal).
    """
    # Calculamos el último día del mes automáticamente
    last_day = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day}"

    # Geometría: Un círculo alrededor del centro de la ciudad
    point = ee.Geometry.Point([lon, lat])
    roi = point.buffer(radius_km * 1000).bounds()

    # Seleccionamos la colección 'OFFL' (Offline).
    # Es preferible a la 'NRTI' (Real Time) para estudios históricos porque
    # tiene mejores calibraciones científicas post-procesamiento.
    collection = (ee.ImageCollection('COPERNICUS/S5P/OFFL/L3_NO2')
                  .filterBounds(roi)
                  .filterDate(start_date, end_date)
                  .map(mask_clouds)
                  .select('tropospheric_NO2_column_number_density'))

    # Si hubo muchas nubes y no hay datos, devolvemos None
    if collection.size().getInfo() == 0:
        return None

    # Devolvemos la media mensual recortada a la zona de interés
    return collection.mean().clip(roi)

def process_task(task_args):
    """
    Worker: Se encarga de pedir la URL a Google y descargar el archivo.
    """
    city, (lat, lon), year, month, out_dir = task_args
    
    filename = f"{city}_NO2_{year}-{month:02d}"
    # Guardamos cada ciudad en su propia carpeta para ser ordenados
    city_dir = out_dir / city
    final_path = city_dir / f"{filename}.zip"
    
    # Idempotencia: Si ya existe, no lo volvemos a descargar (ahorra tiempo y cuota)
    if final_path.exists():
        return None 

    try:
        # 1. Procesamiento en la nube (GEE)
        img = get_monthly_composite(lat, lon, year, month)
        if not img:
            return f"[WARN] {city} {year}-{month}: Cielo cubierto o sin datos."

        # 2. Generar URL de descarga
        # Pedimos a GEE que prepare un ZIP con el GeoTIFF
        url = img.getDownloadURL({
            'name': filename,
            'scale': SCALE_M,
            'crs': 'EPSG:3857', # Proyección Web Mercator (estándar mapas web)
            'filePerBand': False
        })

        # 3. Descarga física al disco duro
        r = requests.get(url, stream=True, timeout=60)
        if r.status_code == 200:
            city_dir.mkdir(parents=True, exist_ok=True)
            with open(final_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            return f"[OK] Descargado: {filename}"
        else:
            return f"[ERROR] HTTP {r.status_code} al descargar {filename}"

    except Exception as e:
        return f"[ERROR] Excepción GEE en {filename}: {e}"

def main():
    print(f"--- INICIANDO DESCARGA MASIVA SENTINEL-5P ---")
    print(f"Proyecto GEE: {GOOGLE_PROJECT}")
    print(f"Hilos paralelos: {MAX_WORKERS}")
    
    tasks = []
    now = datetime.datetime.now()
    
    # Construimos la cola de trabajo:
    # Todas las ciudades x Todos los meses desde 2018 hasta hoy
    for city, coords in config.EURO_FUAS.items():
        for year in range(START_YEAR, now.year + 1):
            for month in range(1, 13):
                # No intentamos descargar meses futuros
                if year == now.year and month > now.month: 
                    break
                
                tasks.append((city, coords, year, month, OUTPUT_DIR))
    
    print(f"Total de imágenes a procesar: {len(tasks)}")
    print("Comenzando ejecución paralela...")

    # Usamos ThreadPoolExecutor para descargar varias imágenes a la vez
    # GEE aguanta bien la concurrencia, el límite suele ser tu red.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_task, t) for t in tasks]
        
        for i, future in enumerate(as_completed(futures)):
            res = future.result()
            if res: 
                print(f"[{i+1}/{len(tasks)}] {res}")

    print("\n[FIN] Todas las descargas completadas.")

if __name__ == "__main__":
    main()