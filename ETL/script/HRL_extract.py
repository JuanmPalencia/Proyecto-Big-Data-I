import os
import requests
import argparse
from urllib.parse import urlencode
from datetime import datetime

# ======================================================
# CONFIGURACIÓN GENERAL
# ======================================================

# Forzar base del proyecto en vez de la carpeta del script
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DATA_DIR = os.path.join(BASE_DIR, "data", "hrl")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

def log(msg):
    ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    with open(os.path.join(LOG_DIR, "etl.log"), "a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")
    print(msg)

# ======================================================
# ENDPOINTS PRINCIPALES
# ======================================================
HRL_ENDPOINTS = {
    "tree_cover": {
        "dir": "hrl_tree_cover",
        "layers": {
            "density": "https://geoserver.vlcc.geoville.com/geoserver/HRL_TCF/wms",
            "change": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_TreeCoverChangeMask_15_18/ImageServer/exportImage",
            "leaf_type": "https://geoserver.vlcc.geoville.com/geoserver/HRL_TCF/wms",
            "forest_type": "https://geoserver.vlcc.geoville.com/geoserver/HRL_TCF/wms"
        }
    },
    "impervious": {
        "dir": "hrl_impervious",
        "layers": {
            "density": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessDensity_2018/ImageServer/exportImage",
            "change": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessChange_15_18/ImageServer/exportImage",
            "builtup": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_BuiltUp_2018/ImageServer/exportImage"
        }
    },
    "urban_atlas": {
        "dir": "hrl_urban_atlas",
        "layers": {
            "urban_2018": "https://image.discomap.eea.europa.eu/arcgis/rest/services/UrbanAtlas/UA_UrbanAtlas_2018/MapServer/export"
        }
    },
    "small_woody": {
        "dir": "hrl_small_woody",
        "layers": {
            "woody_2018": "https://copernicus.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_SmallWoodyFeatures_2018_005m/ImageServer/exportImage"
        }
    }
}

# ======================================================
# FUNCIÓN DE DESCARGA
# ======================================================
def download_layer(service, layer, bbox, size=1024, crs="EPSG:3857"):
    if service not in HRL_ENDPOINTS:
        raise ValueError(f"Servicio '{service}' no reconocido.")
    if layer not in HRL_ENDPOINTS[service]["layers"]:
        raise ValueError(f"Capa '{layer}' no disponible en {service}.")

    out_dir = os.path.join(DATA_DIR, HRL_ENDPOINTS[service]["dir"])
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"{layer}.png")

    url = HRL_ENDPOINTS[service]["layers"][layer]
    log(f"🛰️ Descargando {service}/{layer} desde {url}")

    if "geoserver" in url:
        params = {
            "service": "WMS",
            "version": "1.1.0",
            "request": "GetMap",
            "layers": "HRL_TCF:TCD_S2021" if "tree" in service else "HRL_TCF:FTY_S2021",
            "bbox": bbox,
            "width": size,
            "height": size,
            "srs": crs,
            "styles": "",
            "format": "image/png",
            "transparent": "true"
        }
    else:
        params = {
            "f": "image",
            "bbox": bbox,
            "imageSR": 102100,
            "bboxSR": 102100,
            "size": f"{size},{size}"
        }

    r = requests.get(url, params=params, timeout=180)
    r.raise_for_status()

    with open(out_file, "wb") as f:
        f.write(r.content)

    log(f"✅ Guardado en {out_file}")
    return out_file

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ETL unificado para HRL Copernicus")
    ap.add_argument("--service", required=True, choices=list(HRL_ENDPOINTS.keys()))
    ap.add_argument("--layer", required=True, help="Nombre de la capa dentro del servicio")
    ap.add_argument("--bbox", required=True, help="Bounding box en EPSG:3857 (xmin,ymin,xmax,ymax)")
    ap.add_argument("--size", type=int, default=1024)
    args = ap.parse_args()

    try:
        file_path = download_layer(args.service, args.layer, args.bbox, args.size)
        log(f"📦 Proceso completado correctamente → {file_path}")
    except Exception as e:
        log(f"❌ Error: {e}")
