import os
import time
import random
import datetime
import requests
import ee
from concurrent.futures import ThreadPoolExecutor, as_completed
import config  # Importamos configuración centralizada

# --- CONFIGURACIÓN DE DESCARGA ---
GOOGLE_PROJECT = "gtpuem"
START_YEAR = 2018
OUTPUT_DIR = config.DATA_DIR / "sentinel2"

# Sentinel-2 tiene 10m de resolución nativa. 
# Para un estudio continental, 10m es demasiado pesado (TB de datos).
# 60m es un estándar científico aceptable para análisis urbano macro.
SCALE_M = 60 
MAX_WORKERS = 12 

# --- INICIALIZACIÓN GEE ---
try:
    ee.Initialize(project=GOOGLE_PROJECT)
    print("[INFO] GEE inicializado correctamente.")
except Exception as e:
    raise RuntimeError(f"Fallo de autenticación GEE: {e}")

def robust_request(url, max_retries=5):
    """
    Gestión inteligente de reintentos para evitar errores de red o cuota.
    Si Google dice 'espera', esperamos exponencialmente.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True, timeout=60)
            
            if response.status_code == 200:
                return response
            
            # Errores temporales (Too Many Requests, Server Error) -> Reintentamos
            if response.status_code in [429, 500, 502, 503]:
                wait_time = (2 ** attempt) + random.uniform(0.1, 1.0)
                time.sleep(wait_time)
                continue
            
            # Otros errores (404, 403) -> Fallamos inmediatamente
            return None
            
        except requests.exceptions.RequestException:
            time.sleep(2 ** attempt)
            
    return None

def get_masked_ndvi_collection(roi, start_date, end_date):
    """
    Crea una colección de imágenes limpias de nubes y calcula el NDVI.
    Usa la banda QA60 (Quality Assurance) para eliminar píxeles nublados.
    """
    def mask_clouds(image):
        qa = image.select('QA60')
        
        # Bits 10 y 11 son nubes opacas y cirros respectivamente.
        # Queremos píxeles donde ambos sean 0.
        cloud_bit_mask = (1 << 10)
        cirrus_bit_mask = (1 << 11)
        
        mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(
               qa.bitwiseAnd(cirrus_bit_mask).eq(0))
        
        # Escalamos los valores de reflectancia (0-10000 -> 0-1)
        return image.updateMask(mask).divide(10000)

    # Usamos la colección Harmonized para evitar saltos de calibración antiguos
    collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                  .filterBounds(roi)
                  .filterDate(start_date, end_date)
                  # Pre-filtro: descartamos imágenes totalmente cubiertas de nubes
                  .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 60))
                  .map(mask_clouds))
    
    # Añadimos la banda NDVI calculada al vuelo
    # Fórmula: (NIR - RED) / (NIR + RED)
    return collection.map(lambda img: img.addBands(img.normalizedDifference(['B8', 'B4']).rename('NDVI')))

def process_city_date(task_payload):
    """
    Worker: Procesa una ciudad y fecha, genera la imagen mediana y la descarga.
    """
    city, (lat, lon), year, month, output_dir = task_payload
    
    filename = f"{city}_NDVI_{year}-{month:02d}"
    city_dir = output_dir / city
    zip_path = city_dir / f"{filename}.zip"
    
    # Idempotencia
    if zip_path.exists():
        return None

    # Definimos fechas
    start = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end = f"{year}-{month:02d}-{last_day}"

    try:
        # Área de Interés (ROI): Buffer de 20km alrededor del centro
        roi = ee.Geometry.Point([lon, lat]).buffer(20000).bounds()
        
        col = get_masked_ndvi_collection(roi, start, end)
        
        # Reducción: Mediana mensual.
        # La mediana es excelente para eliminar nubes residuales o sombras
        # que hayan escapado a la máscara QA60.
        img_reduced = col.select('NDVI').median().clip(roi)
        
        # Pedimos URL de descarga
        try:
            url = img_reduced.getDownloadURL({
                'name': filename,
                'crs': 'EPSG:3857', # Web Mercator
                'scale': SCALE_M,
                'region': roi,
                'filePerBand': False
            })
        except ee.EEException:
            # Si falla getDownloadURL suele ser porque la imagen está vacía (todo nubes)
            return f"[WARN] Sin datos válidos: {filename}"

        # Descarga física
        resp = robust_request(url)
        if resp:
            city_dir.mkdir(parents=True, exist_ok=True)
            with open(zip_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=1024*1024):
                    f.write(chunk)
            return f"[OK] Descargado: {filename}"
        else:
            return f"[ERROR] Fallo de red: {filename}"

    except Exception as e:
        return f"[ERROR] GEE: {filename} -> {e}"

def main():
    print(f"--- INICIANDO DESCARGA SENTINEL-2 (VEGETACIÓN) ---")
    print(f"Escala: {SCALE_M}m | Hilos: {MAX_WORKERS}")
    
    current_date = datetime.datetime.now()
    tasks = []
    
    # Construir cola de trabajo
    for city, coords in config.EURO_FUAS.items():
        for year in range(START_YEAR, current_date.year + 1):
            for month in range(1, 13):
                if year == current_date.year and month > current_date.month:
                    break
                tasks.append((city, coords, year, month, OUTPUT_DIR))
    
    print(f"Total imágenes: {len(tasks)}")
    print("Ejecutando descargas...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_city_date, task) for task in tasks]
        
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            if result:
                print(f"[{i+1}/{len(tasks)}] {result}")

    print("\n[FIN] Descarga Sentinel-2 completada.")

if __name__ == "__main__":
    import calendar # Import necesario para el worker
    main()