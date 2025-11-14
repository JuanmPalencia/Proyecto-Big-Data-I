#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinel-2 — Descargar ZIP completo (.SAFE) y extraer solo TCI/Bxx/SCL del interior.
...
"""

from __future__ import annotations
import os, sys, time, json, argparse, fnmatch, zipfile
from datetime import date, timedelta
from urllib.parse import urlencode

from dotenv import load_dotenv
import requests
import pandas as pd
from pandas import json_normalize
from PIL import Image 

Image.MAX_IMAGE_PIXELS = None # Para evitar errores con imágenes gigantes de satélite
CAT_BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

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

def get_keycloak(username: str, password: str) -> str:
    data = {"client_id": "cdse-public", "username": username, "password": password, "grant_type": "password"}
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    try:
        r.raise_for_status()
    except Exception:
        try: payload = r.json()
        except Exception: payload = r.text
        raise RuntimeError(f"Fallo creando token. Respuesta: {payload}")
    return r.json()["access_token"]

def make_filter(collection: str, start_iso: str, end_iso: str, wkt: str | None,
                only_l2a: bool, tile: str | None) -> str:
    base = (
        f"Collection/Name eq '{collection}' "
        f"and ContentDate/Start gt {start_iso}T00:00:00.000Z "
        f"and ContentDate/Start lt {end_iso}T00:00:00.000Z"
    )
    if wkt:
        base += f" and OData.CSC.Intersects(area=geography'SRID=4326;{wkt}')"
    if only_l2a:
        base += " and not contains(Name,'L1C')"
    if tile:
        base += f" and contains(Name,'{tile}')"
    return base

def fetch_page(params: dict) -> dict:
    r = requests.get(f"{CAT_BASE}?{urlencode(params)}", timeout=90)
    r.raise_for_status()
    return r.json()

def fetch_all(collection: str, start_iso: str, end_iso: str, wkt: str | None,
              top: int, max_pages: int, orderby: str, include_count: bool,
              only_l2a: bool, tile: str | None, select: str | None) -> dict:
    params = {
        "$filter": make_filter(collection, start_iso, end_iso, wkt, only_l2a, tile),
        "$orderby": orderby,
        "$top": str(top),
    }
    if include_count: params["$count"] = "true"
    if select: params["$select"] = select

    all_items, count, skip = [], None, 0
    for _ in range(max_pages):
        page_params = dict(params)
        if skip: page_params["$skip"] = str(skip)
        js = fetch_page(page_params)
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
        first_cols = [c for c in [
            "Id", "Name", "ContentDate.Start", "ContentDate.End",
            "ContentType", "ContentLength", "OriginDate", "GeoFootprint"
        ] if c in df.columns]
        other_cols = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + other_cols]
    return df

def follow_redirects(session: requests.Session, url: str, max_hops: int = 10) -> requests.Response:
    resp = session.get(url, allow_redirects=False, timeout=120)
    hops = 0
    while resp.status_code in (301, 302, 303, 307, 308) and hops < max_hops:
        loc = resp.headers.get("Location")
        if not loc: break
        resp = session.get(loc, allow_redirects=False, timeout=300)
        hops += 1
    return resp

# 🔹 NUEVO: si el ZIP ya existe, no lo vuelve a descargar
def download_product_zip(session: requests.Session, product_id: str, identifier: str, out_dir: str) -> str:
    """
    Descarga el .SAFE (ZIP) completo:
      GET https://catalogue.dataspace.copernicus.eu/odata/v1/Products(<GUID>)/$value
    """
    os.makedirs(out_dir, exist_ok=True)
    out_zip = os.path.join(out_dir, f"{identifier}.zip")
    if os.path.exists(out_zip):
        print(f"⏭️  ZIP ya existe, omitiendo descarga: {out_zip}")
        return out_zip

    url = f"{CAT_BASE}({product_id})/$value"
    resp = follow_redirects(session, url)
    resp.raise_for_status()
    with open(out_zip, "wb") as f:
        f.write(resp.content)
    return out_zip

def build_patterns(mode: str, bands: list[str] | None) -> list[str]:
    mode = (mode or "").lower()
    pats: list[str] = []
    if mode == "tci":
        pats = [
            "*IMG_DATA*/R10m/*TCI*.jp2",
            "*IMG_DATA*/R20m/*TCI*.jp2",
            "*IMG_DATA*/R60m/*TCI*.jp2",
            "*IMG_DATA_R10m*/*TCI*.jp2",
            "*IMG_DATA_R20m*/*TCI*.jp2",
            "*IMG_DATA_R60m*/*TCI*.jp2",
            "*IMG_DATA*/*TCI*.jp2",
            "*/*TCI*.jp2",
        ]
    elif mode == "scl":
        pats = [
            "*IMG_DATA*/R20m/*SCL*.jp2",
            "*IMG_DATA_R20m*/*SCL*.jp2",
            "*IMG_DATA*/*SCL*.jp2",
            "*/*SCL*.jp2",
        ]
    elif mode == "bands" and bands:
        b = [x.strip().upper() for x in bands]
        for band in b:
            pats += [
                f"*IMG_DATA*/R10m/*{band}*10m*.jp2",
                f"*IMG_DATA*/R20m/*{band}*20m*.jp2",
                f"*IMG_DATA*/R60m/*{band}*60m*.jp2",
                f"*IMG_DATA_R10m*/*{band}*.jp2",
                f"*IMG_DATA_R20m*/*{band}*.jp2",
                f"*IMG_DATA_R60m*/*{band}*.jp2",
                f"*IMG_DATA*/*{band}*.jp2",
                f"*/*{band}*.jp2",
            ]
    return pats

def extract_selected_from_zip(zip_path: str, mode: str, bands: list[str] | None, out_dir: str) -> list[str]:
    pats = build_patterns(mode, bands)
    extracted: list[str] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()

        if mode.lower() == "tci":
            tci_candidates = [m for m in members if "TCI" in m.upper()]
            if tci_candidates:
                print("🔎 Posibles TCI detectados en el ZIP (muestras):")
                for x in tci_candidates[:20]:
                    print("  -", x)

        to_get = set()
        for pat in pats:
            for m in members:
                if fnmatch.fnmatch(m, pat):
                    to_get.add(m)

        if not to_get:
            print(f"⚠️  No se encontraron archivos que coincidan con {mode} (bandas={bands}) dentro del ZIP.")
            return []

        base_out = os.path.join(os.path.dirname(zip_path), os.path.splitext(os.path.basename(zip_path))[0], "extracted")
        os.makedirs(base_out, exist_ok=True)

        for m in sorted(to_get):
            safe_name = m.replace("/", "_")
            out_file = os.path.join(base_out, safe_name)



            # --- INICIO CAMBIO CONVERSIÓN ---
            with zf.open(m) as src, open(out_file, "wb") as dst:
                dst.write(src.read())
            
            # Si es una imagen .jp2 y queremos convertirla (solo para TCI/Visualización)
            if out_file.lower().endswith(".jp2") and mode == "tci":
                try:
                    png_path = out_file.replace(".jp2", ".png")
                    with Image.open(out_file) as img:
                        print(f"   ↳ Convirtiendo a PNG...")
                        img.save(png_path, "PNG")
                    
                    # Opcional: Borrar el .jp2 original para ahorrar espacio
                    os.remove(out_file)
                    extracted.append(png_path)
                    print(f"✔ Generado: {png_path}")
                except Exception as e:
                    print(f"⚠️ Error convirtiendo imagen: {e}")
                    extracted.append(out_file) # Si falla, nos quedamos con el jp2
            else:
                extracted.append(out_file)
            # --- FIN CAMBIO ---



    return extracted

AOIS = {
    "tiny": "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
    "madrid": "POLYGON((-3.9 40.2, -3.9 40.6, -3.3 40.6, -3.3 40.2, -3.9 40.2))",
}

def parse_args():
    ap = argparse.ArgumentParser(description="Sentinel-2 — descarga ZIP completo y extracción selectiva (TCI/Bxx/SCL)")
    ap.add_argument("--days-back", type=int, default=7)
    ap.add_argument("--collection", type=str, default="SENTINEL-2")
    ap.add_argument("--aoi", type=str, choices=["tiny", "madrid", "custom"], default="madrid")
    ap.add_argument("--wkt", type=str, default=None)
    ap.add_argument("--top", type=int, default=50)
    ap.add_argument("--max-pages", type=int, default=2)
    ap.add_argument("--orderby", type=str, default="ContentDate/Start desc")
    ap.add_argument("--select", type=str, default="Id,Name,ContentDate,ContentType,ContentLength,OriginDate,GeoFootprint")
    ap.add_argument("--only-l2a", dest="only_l2a", action="store_true")
    ap.add_argument("--no-only-l2a", dest="only_l2a", action="store_false")
    ap.set_defaults(only_l2a=True)
    ap.add_argument("--tile", type=str, default=None)
    ap.add_argument("--csv", type=str, default="copernicus_schema_sample.csv")
    ap.add_argument("--download", action="store_true", help="Descargar ZIP .SAFE y extraer")
    ap.add_argument("--asset", type=str, choices=["tci", "scl"], default=None, help="tci | scl")
    ap.add_argument("--bands", type=str, default=None, help="B04,B08,...")
    ap.add_argument("--max-downloads", type=int, default=1)
    ap.add_argument("--out-dir", type=str, default="data/s2")
    return ap.parse_args()

def main():
    load_dotenv()
    args = parse_args()

    DATA_DIR = os.path.join(os.getcwd(), "data", "s2")
    os.makedirs(DATA_DIR, exist_ok=True)
    args.out_dir = DATA_DIR
    args.csv = os.path.join(DATA_DIR, os.path.basename(args.csv))

    if args.aoi == "custom":
        if not args.wkt:
            print("ERROR: --aoi custom requiere --wkt 'POLYGON((...))'", file=sys.stderr)
            sys.exit(2)
        wkt = args.wkt
    else:
        wkt = AOIS[args.aoi]

    today_iso, start_iso = today_and_start(args.days_back)
    print("→ Consulta OData…")
    print("Colección:", args.collection)
    print("Ventana:  ", f"{start_iso} → {today_iso}")
    print("AOI:      ", args.aoi)
    print("Solo L2A: ", args.only_l2a)
    if args.tile:
        print("Tile:     ", args.tile)
    print()

    js = fetch_all(
        collection=args.collection,
        start_iso=start_iso,
        end_iso=today_iso,
        wkt=wkt,
        top=args.top,
        max_pages=args.max_pages,
        orderby=args.orderby,
        include_count=True,
        only_l2a=args.only_l2a,
        tile=args.tile,
        select=args.select
    )
    if "@odata.count" in js:
        print("Total (count) filtro:", js["@odata.count"])

    df = to_flat_df(js)
    if df.empty:
        print("No hay productos para este filtro.")
        return

    print("\n— Primeros productos —")
    cols = [c for c in ["Id", "Name", "ContentDate.Start"] if c in df.columns]
    print(df[cols].head(10))

    try:
        df.to_csv(args.csv, index=False)
        print(f"\nCSV guardado: {args.csv} (filas: {len(df)})")
    except Exception as e:
        print(f"Advertencia al escribir CSV: {e}")

    if not args.download:
        print("\nDescarga desactivada (usa --download).")
        return

    try:
        user = ensure_env("COPERNICUS_USER")
        pwd = ensure_env("COPERNICUS_PASSWORD")
    except RuntimeError as e:
        print(f"Descarga deshabilitada: {e}")
        return

    try:
        token = get_keycloak(user, pwd)
    except Exception as e:
        print(f"Error de autenticación: {e}")
        return

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    bands = None
    mode = "tci"
    if args.bands:
        bands = [s.strip() for s in args.bands.split(",") if s.strip()]
        mode = "bands"
    elif args.asset:
        mode = args.asset.lower()

    downloaded = 0
    for item in js.get("value", []):
        if downloaded >= args.max_downloads:
            break
        pid = item.get("Id")
        name = item.get("Name", "")
        identifier = name.split(".")[0] if isinstance(name, str) and name else pid
        if not pid:
            continue

        try:
            print(f"\n[{identifier}] Descargando ZIP completo…")
            zip_path = download_product_zip(session, pid, identifier, args.out_dir)
            print(f"[{identifier}] ZIP guardado en: {zip_path}")

            print(f"[{identifier}] Extrayendo '{mode}' (bandas={bands})…")
            extracted = extract_selected_from_zip(zip_path, mode, bands, args.out_dir)
            if not extracted:
                print(f"[{identifier}] ⚠️  No se extrajo nada que coincida con el modo.")
            else:
                print(f"[{identifier}] ✅ Extraídos {len(extracted)} archivos.")

            downloaded += 1
            time.sleep(0.3)

        except requests.HTTPError as he:
            print(f"[{identifier}] Error HTTP al descargar/extraer: {he}")
        except Exception as e:
            print(f"[{identifier}] Error: {e}")

    if downloaded == 0:
        print("\nNo se descargaron ZIPs (revisa token, filtros o estado online/offline).")
    else:
        print(f"\n✅ Listo. Productos procesados: {downloaded}")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTPError: {e} – contenido: {getattr(e.response, 'text', '')[:200]}...")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
