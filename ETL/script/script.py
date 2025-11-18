#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Copernicus OData – Probes + Inspección de esquema + Descarga opcional

Requisitos:
  pip install requests pandas

(para quitar el warning de requests)
  pip install charset-normalizer   # o: pip install chardet

Credenciales (solo si vas a descargar):
  export COPERNICUS_USER="tu_usuario"
  export COPERNICUS_PASSWORD="tu_password"

Uso sugerido:
  python script.py
  python script.py --download --max-downloads 2
  python script.py --days-back 7 --aoi madrid --download

Parámetros disponibles:
  --days-back N         ventana temporal [hoy-N, hoy]
  --collection NAME     colección (por defecto SENTINEL-2)
  --aoi {tiny,madrid,custom}   AOI predefinido o custom con --wkt
  --wkt "POLYGON((...))"       WKT si usas --aoi custom
  --top N               límite por página en consultas
  --max-pages N         nº máximo de páginas a traer
  --download            habilita descarga de productos
  --max-downloads N     cuántos productos descargar (por defecto 1)
  --csv PATH            ruta para exportar CSV (por defecto: copernicus_schema_sample.csv)
"""

import os
from dotenv import load_dotenv
import sys
import time
import json
import argparse
from datetime import date, timedelta
from urllib.parse import urlencode

import requests
import pandas as pd
from pandas import json_normalize

load_dotenv()
user = os.getenv("COPERNICUS_USER")
pwd = os.getenv("COPERNICUS_PASSWORD")
BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"


# =========================
# Utilidades generales
# =========================

def iso_day(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def today_and_start(days_back: int):
    today = date.today()
    start = today - timedelta(days=days_back)
    return iso_day(today), iso_day(start)

def pretty_dt(s: str) -> str:
    # Algunos registros tienen datetime ISO; acorta para imprimir
    if not s:
        return s
    return s.replace("T", " ")[:19]

def ensure_env(var: str) -> str:
    v = os.getenv(var)
    if not v:
        raise RuntimeError(f"Falta variable de entorno: {var}")
    return v

# =========================
# Autenticación (para descarga)
# =========================

def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    try:
        r.raise_for_status()
    except Exception:
        try:
            payload = r.json()
        except Exception:
            payload = r.text
        raise RuntimeError(f"Fallo creando token. Respuesta: {payload}")
    js = r.json()
    return js["access_token"]

# =========================
# Consultas OData
# =========================

def make_filter(collection: str, start_iso: str, end_iso: str, wkt: str | None) -> str:
    base_filter = (
        f"Collection/Name eq '{collection}' "
        f"and ContentDate/Start gt {start_iso}T00:00:00.000Z "
        f"and ContentDate/Start lt {end_iso}T00:00:00.000Z"
    )
    if wkt:
        # Intersects espacial
        base_filter += f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
    return base_filter

def fetch_page(params: dict) -> dict:
    r = requests.get(f"{BASE}?{urlencode(params)}", timeout=90)
    r.raise_for_status()
    return r.json()

def fetch_all(collection: str, start_iso: str, end_iso: str, wkt: str | None,
            top: int = 100, max_pages: int = 5, select: str | None = None,
            orderby: str = "ContentDate/Start desc", include_count: bool = True) -> dict:
    """
    Trae varias páginas y devuelve un dict con:
    {"@odata.count": count?, "value": [items...]}
    Nota: El catálogo soporta $skip; $skiptoken puede no estar disponible.
    """
    params = {
        "$filter": make_filter(collection, start_iso, end_iso, wkt),
        "$orderby": orderby,
        "$top": str(top),
    }
    if include_count:
        params["$count"] = "true"
    if select:
        params["$select"] = select

    all_items = []
    count = None
    skip = 0

    for page in range(max_pages):
        page_params = dict(params)
        if skip:
            page_params["$skip"] = str(skip)
        js = fetch_page(page_params)
        if "@odata.count" in js and count is None:
            count = js["@odata.count"]
        items = js.get("value", [])
        all_items.extend(items)
        got = len(items)
        if got < top:
            break
        skip += top
        # Pequeño respiro para ser amables con el servidor
        time.sleep(0.5)

    out = {"value": all_items}
    if count is not None:
        out["@odata.count"] = count
    return out

# =========================
# Inspección de esquema
# =========================

def show_schema(js: dict, max_print: int = 1):
    vals = js.get("value", [])
    total = len(vals)
    print(f"\nTotal devueltos (acumulado): {total}")
    if "@odata.count" in js:
        print("Count (para el filtro):", js["@odata.count"])
    if total == 0:
        return
    first = vals[0]
    print("\n— Keys del primer item —")
    print(sorted(first.keys()))
    print("\n— Tipos por key (primer item) —")
    for k, v in first.items():
        print(f"{k:30s} -> {type(v).__name__}")
    print("\n— Ejemplo compacto (primer item) —")
    preview = {k: (v[:120]+"...") if isinstance(v, str) and len(v) > 120 else v for k, v in first.items()}
    print(json.dumps(preview, indent=2, ensure_ascii=False))

def to_flat_df(js: dict) -> pd.DataFrame:
    df = json_normalize(js.get("value", []))
    if not df.empty:
        first_cols = [c for c in [
            "Id", "Name", "ContentDate.Start", "ContentDate.End",
            "ContentType", "ContentLength", "OriginDate", "GeoFootprint",
            "Checksum.Value", "Checksum.Algorithm"
        ] if c in df.columns]
        other_cols = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + other_cols]
    return df

# =========================
# Descarga de productos
# =========================

def follow_redirects(session: requests.Session, url: str, max_hops: int = 10) -> requests.Response:
    """Sigue redirecciones manualmente (algunos endpoints devuelven 302/303 con Location)."""
    resp = session.get(url, allow_redirects=False, timeout=120)
    hops = 0
    while resp.status_code in (301, 302, 303, 307, 308) and hops < max_hops:
        url = resp.headers.get("Location")
        if not url:
            break
        resp = session.get(url, allow_redirects=False, timeout=120)
        hops += 1
    return resp

def download_product_by_id(session: requests.Session, product_id: str, identifier: str | None = None, out_dir: str = ".") -> str:
    """
    Descarga el zip del producto.
    - session debe llevar Authorization: Bearer <token>
    - identifier se usa para el nombre de archivo si está disponible
    """
    url = f"{BASE}({product_id})/$value"
    resp = follow_redirects(session, url)
    resp.raise_for_status()
    # Nombre de archivo
    if not identifier:
        identifier = product_id
    out_path = os.path.join(out_dir, f"{identifier}.zip")
    with open(out_path, "wb") as f:
        f.write(resp.content)
    return out_path

# =========================
# AOIs predefinidos
# =========================

AOIS = {
    "tiny": "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
    "madrid": "POLYGON((-3.9 40.2, -3.9 40.6, -3.3 40.6, -3.3 40.2, -3.9 40.2))",
}

# =========================
# CLI
# =========================

def parse_args():
    ap = argparse.ArgumentParser(description="Copernicus OData – Probes + Esquema + Descarga opcional")
    ap.add_argument("--days-back", type=int, default=1, help="Ventana temporal [hoy-N, hoy] (default 1)")
    ap.add_argument("--collection", type=str, default="SENTINEL-2", help="Colección (p.ej. SENTINEL-2, SENTINEL-1)")
    ap.add_argument("--aoi", type=str, choices=["tiny", "madrid", "custom"], default="tiny", help="AOI predefinido o custom")
    ap.add_argument("--wkt", type=str, default=None, help="WKT para AOI custom (requiere --aoi custom)")
    ap.add_argument("--top", type=int, default=100, help="Límite por página (default 100)")
    ap.add_argument("--max-pages", type=int, default=3, help="Nº máximo de páginas a traer (default 3)")
    ap.add_argument("--select", type=str, default="Id,Name,ContentDate,ContentType,ContentLength,OriginDate,GeoFootprint",
                    help="Campos a seleccionar (vacío para todos)")
    ap.add_argument("--orderby", type=str, default="ContentDate/Start desc", help="Orden OData (default ContentDate/Start desc)")
    ap.add_argument("--csv", type=str, default="copernicus_schema_sample.csv", help="Ruta de salida CSV")
    ap.add_argument("--download", action="store_true", help="Descargar productos encontrados")
    ap.add_argument("--max-downloads", type=int, default=1, help="Máximo de productos a descargar (default 1)")
    ap.add_argument("--out-dir", type=str, default=".", help="Directorio de descargas")
    return ap.parse_args()

# =========================
# Main
# =========================

def main():
    args = parse_args()

    # AOI
    if args.aoi == "custom":
        if not args.wkt:
            print("ERROR: --aoi custom requiere --wkt 'POLYGON((...))'", file=sys.stderr)
            sys.exit(2)
        wkt = args.wkt
    else:
        wkt = AOIS[args.aoi]

    today_iso, start_iso = today_and_start(args.days_back)

    print("→ Ejecutando consulta OData para inspección de esquema…")
    print("Colección:", args.collection)
    print("Ventana:  ", f"{start_iso} → {today_iso}")
    print("AOI:      ", args.aoi, ("(custom)" if args.aoi == "custom" else ""), "\n")

    select = args.select if args.select else None
    js = fetch_all(
        collection=args.collection,
        start_iso=start_iso,
        end_iso=today_iso,
        wkt=wkt,
        top=args.top,
        max_pages=args.max_pages,
        select=select,
        orderby=args.orderby,
        include_count=True
    )

    # Mostrar conteo y esquema
    if "@odata.count" in js:
        print("Total (count) para este filtro:", js["@odata.count"])
    show_schema(js)

    # DataFrame y CSV
    df = to_flat_df(js)
    if df.empty:
        print("\nNo hay filas para este filtro/AOI. Prueba con más días (--days-back 7) o un AOI más amplio (--aoi madrid).")
    else:
        print("\n— Columnas del DataFrame aplanado —")
        print(list(df.columns))
        print("\n— Primeras filas —")
        print(df.head(10))
        try:
            df.to_csv(args.csv, index=False)
            print(f"\nMuestra guardada en {args.csv} (filas: {len(df)})")
        except Exception as e:
            print(f"Advertencia: no se pudo escribir CSV: {e}")

    # Descarga opcional
    if args.download and not df.empty:
        try:
            user = ensure_env("COPERNICUS_USER")
            pwd = ensure_env("COPERNICUS_PASSWORD")
        except RuntimeError as e:
            print(f"\nDescarga deshabilitada: {e}")
            return

        token = get_keycloak(user, pwd)
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})

        # Elegir candidatos a descargar:
        #  - Si existe Name, usamos identifier = Name.split('.')[0]
        #  - Requiere Id (clave OData)
        downloaded = 0
        for item in js.get("value", []):
            if downloaded >= args.max_downloads:
                break
            product_id = item.get("Id")
            name = item.get("Name")
            identifier = None
            if isinstance(name, str) and name:
                identifier = name.split(".")[0]

            if not product_id:
                continue
            try:
                path = download_product_by_id(session, product_id, identifier=identifier, out_dir=args.out_dir)
                print(f"Descargado: {path}")
                downloaded += 1
                # pausa breve por cortesía
                time.sleep(0.5)
            except Exception as e:
                print(f"Error descargando Id={product_id}: {e}")

        if downloaded == 0:
            print("No se descargó ningún producto (revisa que haya Ids válidos en la respuesta).")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTPError: {e} – contenido: {getattr(e.response, 'text', '')[:200]}...")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)