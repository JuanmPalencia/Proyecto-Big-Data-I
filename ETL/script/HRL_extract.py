import os
import requests
import argparse
from urllib.parse import urlencode
from datetime import datetime
from hashlib import md5  # 🔹 NUEVO: para firmar la solicitud (deduplicado)

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

def _sig_path(out_file: str) -> str:
    """Ruta del archivo de firma .sig junto al PNG."""
    return out_file + ".sig"

def _calc_request_signature(url: str, params: dict) -> str:
    """
    Firma determinística del pedido (URL + params) para evitar descargas repetidas.
    No cambia nombres de archivos; solo guarda/verifica una .sig.
    """
    # Normaliza el orden de los parámetros para que la firma sea estable
    qs = urlencode(sorted(params.items()), doseq=True)
    data = f"{url}?{qs}".encode("utf-8")
    return md5(data).hexdigest()

def _already_downloaded(out_file: str, signature: str) -> bool:
    """Devuelve True si existe el PNG y su .sig coincide con la firma del pedido."""
    if not os.path.exists(out_file):
        return False
    sig_file = _sig_path(out_file)
    if not os.path.exists(sig_file):
        return False
    try:
        with open(sig_file, "r", encoding="utf-8") as f:
            prev = f.read().strip()
        return prev == signature
    except Exception:
        return False

def _write_signature(out_file: str, signature: str) -> None:
    """Escribe/actualiza la firma del pedido junto al PNG."""
    try:
        with open(_sig_path(out_file), "w", encoding="utf-8") as f:
            f.write(signature)
    except Exception as e:
        log(f"⚠️ No se pudo escribir firma: {e}")

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

    # 🔹 NUEVO: deduplicado por firma (mismo pedido → no descarga de nuevo)
    signature = _calc_request_signature(url, params)
    if _already_downloaded(out_file, signature):
        log(f"⏭️  Omitido (ya descargado con misma solicitud) → {out_file}")
        return out_file

    r = requests.get(url, params=params, timeout=180)
    r.raise_for_status()

    with open(out_file, "wb") as f:
        f.write(r.content)

    # 🔹 NUEVO: guarda la firma asociada a este PNG
    _write_signature(out_file, signature)

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

    # 🔧 --- BLOQUE NUEVO: ajustar rutas Docker-friendly ---
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    DATA_DIR = os.path.join(BASE_DIR, "data", "hrl")
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    try:
        file_path = download_layer(args.service, args.layer, args.bbox, args.size)
        log(f"📦 Proceso completado correctamente → {file_path}")
    except Exception as e:
        log(f"❌ Error: {e}")
