#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinel-5P — Descarga de datos de Contaminación (NetCDF).
Principalmente NO2 (Dióxido de Nitrógeno) para análisis de calidad del aire.
"""

from __future__ import annotations
import os, sys, time, argparse
from datetime import date, timedelta
from urllib.parse import urlencode
from dotenv import load_dotenv
import requests
import pandas as pd
from pandas import json_normalize

# Configuración Base
CAT_BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

# --- Funciones Auxiliares ---

def iso_day(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def today_and_start(days_back: int):
    today = date.today()
    start = today - timedelta(days=days_back)
    return iso_day(today), iso_day(start)

def ensure_env(var: str) -> str:
    v = os.getenv(var)
    if not v:
        raise RuntimeError(f"Falta variable de entorno: {var}")
    return v

# --- Autenticación ---

def get_keycloak(username: str, password: str) -> str:
    data = {"client_id": "cdse-public", "username": username, "password": password, "grant_type": "password"}
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    try:
        r.raise_for_status()
    except Exception:
        try: payload = r.json()
        except Exception: payload = r.text
        raise RuntimeError(f"Fallo creando token: {payload}")
    return r.json()["access_token"]

def refresh_session(session: requests.Session, user: str, pwd: str):
    """Renueva el token si caduca durante la descarga."""
    print("   🔄 Renovando token de acceso (S5P)...")
    new_token = get_keycloak(user, pwd)
    session.headers.update({"Authorization": f"Bearer {new_token}"})

# --- Búsqueda OData ---

def make_filter(start_iso: str, end_iso: str, wkt: str | None, product_type: str) -> str:
    # Filtro específico para Sentinel-5P
    base = (
        f"Collection/Name eq 'SENTINEL-5P' "
        f"and ContentDate/Start gt {start_iso}T00:00:00.000Z "
        f"and ContentDate/Start lt {end_iso}T00:00:00.000Z "
        f"and contains(Name,'{product_type}')" # Ej: L2__NO2___
    )
    if wkt:
        base += f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
    return base

def fetch_all(start_iso: str, end_iso: str, wkt: str | None,
            top: int, max_pages: int, orderby: str, product_type: str) -> dict:
    params = {
        "$filter": make_filter(start_iso, end_iso, wkt, product_type),
        "$orderby": orderby,
        "$top": str(top),
        "$count": "true"
    }
    
    all_items, count, skip = [], None, 0
    for _ in range(max_pages):
        # Lógica de reintento para búsquedas (Error 504 Gateway Timeout)
        for attempt in range(3):
            try:
                page_params = dict(params)
                if skip: page_params["$skip"] = str(skip)
                
                r = requests.get(f"{CAT_BASE}?{urlencode(page_params)}", timeout=90)
                r.raise_for_status()
                js = r.json()
                break # Éxito
            except Exception as e:
                if attempt == 2: raise e
                time.sleep(2)

        if "@odata.count" in js and count is None: count = js["@odata.count"]
        items = js.get("value", [])
        all_items.extend(items)
        if len(items) < top: break
        skip += top
        time.sleep(0.3)
        
    out = {"value": all_items}
    if count is not None: out["@odata.count"] = count
    return out

def to_flat_df(js: dict) -> pd.DataFrame:
    df = json_normalize(js.get("value", []))
    if not df.empty:
        cols = ["Id", "Name", "ContentDate.Start", "ContentLength"]
        existing = [c for c in cols if c in df.columns]
        df = df[existing]
    return df

# --- Descarga ---

def download_file(session: requests.Session, product_id: str, name: str, out_dir: str, user: str, pwd: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    # Sentinel-5P son archivos .nc (NetCDF)
    if not name.endswith(".nc"):
        name = f"{name}.nc"
        
    out_path = os.path.join(out_dir, name)
    
    # 1. Comprobar si ya existe (Caché)
    if os.path.exists(out_path):
        size_mb = os.path.getsize(out_path) / (1024 * 1024)
        if size_mb > 1: 
            print(f"   ⏭️ Archivo ya existe ({size_mb:.2f} MB), saltando.")
            return out_path
        else:
            try: os.remove(out_path)
            except: pass

    url = f"{CAT_BASE}({product_id})/$value"
    
    # 2. Descarga con reintentos y renovación de token
    max_retries = 2
    for attempt in range(max_retries):
        try:
            # Seguir redirecciones manualmente para detectar 401 en el paso intermedio
            resp = session.get(url, allow_redirects=False, timeout=120)
            while resp.status_code in (301, 302, 303, 307, 308):
                resp = session.get(resp.headers["Location"], allow_redirects=False, timeout=300)

            if resp.status_code == 401:
                if attempt < max_retries - 1:
                    refresh_session(session, user, pwd)
                    continue
                else:
                    resp.raise_for_status()

            resp.raise_for_status()
            
            # 3. Escritura en disco
            with open(out_path, "wb") as f:
                f.write(resp.content)
                f.flush()
                os.fsync(f.fileno())
            
            print(f"   💾 Descargado: {name} ({len(resp.content)/(1024*1024):.2f} MB)")
            break
            
        except Exception as e:
            print(f"   ❌ Error descarga: {e}")
            if attempt == max_retries - 1: return None
            time.sleep(2)

    return out_path

# --- CLI ---

AOIS = {
    "madrid": "POLYGON((-3.9 40.2, -3.9 40.6, -3.3 40.6, -3.3 40.2, -3.9 40.2))",
}

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-back", type=int, default=30)
    ap.add_argument("--aoi", type=str, default="madrid")
    ap.add_argument("--wkt", type=str, default=None)
    # Producto por defecto: Dióxido de Nitrógeno (L2__NO2___)
    ap.add_argument("--product", type=str, default="L2__NO2___", help="Ej: L2__NO2___, L2__CO____, L2__O3____")
    ap.add_argument("--max-downloads", type=int, default=10)
    ap.add_argument("--out-dir", type=str, default="data/s5p")
    ap.add_argument("--download", action="store_true")
    return ap.parse_args()

def main():
    load_dotenv()
    args = parse_args()

    # Rutas
    DATA_DIR = os.path.join(os.getcwd(), "data", "s5p")
    os.makedirs(DATA_DIR, exist_ok=True)
    args.out_dir = DATA_DIR

    # AOI
    if args.aoi == "custom":
        if not args.wkt: sys.exit("Error: --aoi custom requiere --wkt")
        wkt = args.wkt
    else:
        wkt = AOIS.get(args.aoi, AOIS["madrid"])

    # Búsqueda
    today, start = today_and_start(args.days_back)
    print(f"--- Sentinel-5P: {args.product} ---")
    print(f"Consulta: {start} -> {today} | AOI: {args.aoi}")

    try:
        js = fetch_all(start, today, wkt, 50, 5, "ContentDate/Start desc", args.product)
    except Exception as e:
        print(f"Error buscando: {e}")
        return

    if "@odata.count" in js:
        print(f"Encontrados: {js['@odata.count']}")

    df = to_flat_df(js)
    if df.empty:
        print("No se encontraron datos.")
        return
    
    print(df.head(5))

    if not args.download:
        return

    # Auth y Descarga
    try:
        user = ensure_env("COPERNICUS_USER")
        pwd = ensure_env("COPERNICUS_PASSWORD")
        token = get_keycloak(user, pwd)
    except Exception as e:
        print(f"Error Auth: {e}")
        return

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    downloaded = 0
    for item in js.get("value", []):
        if downloaded >= args.max_downloads: break
        
        pid = item.get("Id")
        name = item.get("Name")
        
        if download_file(session, pid, name, args.out_dir, user, pwd):
            downloaded += 1
            time.sleep(0.5)

    print(f"\nListo. Archivos descargados: {downloaded}")

if __name__ == "__main__":
    main()