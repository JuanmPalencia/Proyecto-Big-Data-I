# -*- coding: utf-8 -*-
"""
Eurostat ETL — Green Turning Point (GTP)
Guarda CSVs en: data/raw/eurostat/

Uso ejemplos:
  py ETL/script/eurostat_extract.py --datasets all
  py ETL/script/eurostat_extract.py --datasets all --filters "time=2010:2024&geo=ES,PT,FR"
  py ETL/script/eurostat_extract.py --datasets gdp_per_capita,ghg_emissions --filters "time=2015:2024"
  # Grupos nuevos (se guardan como nombre__part1.csv, part2.csv, ...)
  py ETL/script/eurostat_extract.py --datasets egss,circular --filters "time=2015:2024"

Opcional:
  --eager-merge  -> fusiona automáticamente las partes de cada grupo en un único CSV (nombre.csv)
"""

import os
import sys
import argparse
import time
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, UTC

# -------- Mapa de datasets "simples" (compatibilidad con tu versión) --------
DATASETS = {
    # ECONÓMICO
    "gdp_per_capita": "nama_10_pc",
    "employment_by_sector": "nama_10_a64_e",   # NACE/ISIC A*64 (empleo por industria)
    "gross_fixed_capital": "nama_10_an6",
    "productivity": "nama_10_lp_ulc",

    # DEMOGRÁFICO
    "population_by_age": "demo_r_pjangrp3",
    "education_level": "edat_lfse_04",
    "population_density": "demo_r_d3dens",
    "migration": "demo_gind",
    "household_size": "ilc_lvph01",

    # AMBIENTAL
    "ghg_emissions": "env_air_emis",
    "renewable_energy": "nrg_ind_ren",
    "energy_consumption": "nrg_bal_c",
    "environmental_expenditure": "env_ac_exp2",

    # SOCIAL / I+D
    "poverty_rate": "ilc_peps01",
    "innovation_rd": "rd_e_gerdtot",
}

# -------- NUEVO: Grupos (varios endpoints bajo un mismo "dataset lógico") ----
DATASET_GROUPS = {
    # EGSS — sector de bienes y servicios ambientales
    "egss": [
        "env_ac_egss1",   # producción/VA/export/taxes
        "env_ac_egss2",   # empleo y número de unidades
    ],
    # BLI proxy (bienestar): satisfacción, salud, ingresos
    "bli_indicators": [
        "ilc_pw01",       # life satisfaction (0–10)
        "hlth_silc_01",   # self-perceived health (%)
        "ilc_di12",       # median equivalised net income (PPS)
    ],
    # Regional Wellbeing proxy (NUTS2): PIB regional, paro, educación terciaria
    "regional_wellbeing": [
        "nama_10r_3gdp",  # regional GDP
        "lfst_r_lfu3rt",  # regional unemployment rate
        "edat_lfse_04",   # tertiary education (25–64)
        # Si está disponible y lo quieres añadir:
        # "demo_r_mlifexp",  # life expectancy (NUTS2)
    ],
    # Circular economy: CMU% + reciclaje municipal (puedes añadir packaging)
    "circular": [
        "cei_srm030",     # Circular Material Use Rate (CMU %)
        "cei_srm020",     # Recycling rate of municipal waste
        # "cei_srm030",   # Packaging waste recycling rate (opcional)
    ],
}

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
DEFAULT_OUT = os.path.join("data", "raw", "eurostat")

# Parámetros de red simples y amables (manteniendo tu estilo original)
TIMEOUT = 90
RETRIES = 3
PAUSE_S = 0.8  # pequeña pausa entre llamadas del mismo grupo para ser amables


def parse_filters(filters_str: str) -> dict:
    """Convierte 'time=2015:2024&geo=ES,PT' a dict {'time':'2015:2024','geo':'ES,PT'}."""
    if not filters_str:
        return {}
    pairs = [p for p in filters_str.replace(" ", "").split("&") if p]
    q = {}
    for p in pairs:
        if "=" in p:
            k, v = p.split("=", 1)
            q[k] = v
    return q


# -------------------- Helpers Eurostat 1.0 (JSON SDMX) -------------------- #

def _build_inv_map(dim_obj: dict) -> dict:
    """
    Devuelve mapa pos->clave para una dimensión.
    - Si existe category.index lo usa.
    - Si no, usa el orden de category.label.
    """
    cat = dim_obj.get("category", {})
    if "index" in cat and isinstance(cat["index"], dict):
        return {pos: key for key, pos in cat["index"].items()}
    labels = list((cat.get("label") or {}).keys())
    return {i: k for i, k in enumerate(labels)}


def _unravel_index(flat_idx: int, sizes: list[int]) -> list[int]:
    """
    Convierte un índice plano en su tupla multi-índice según sizes.
    sizes = [n_dim0, n_dim1, ...] (orden de Eurostat)
    """
    idxs = []
    for size in reversed(sizes):
        idxs.append(flat_idx % size)
        flat_idx //= size
    return list(reversed(idxs))


def fetch_eurostat_table(dataset_code: str, params: dict | None = None) -> pd.DataFrame:
    """
    Descarga una tabla Eurostat (API 1.0) y la normaliza a DataFrame.
    Conserva el comportamiento de tu función original.
    """
    # Intento 1: JSON (tu parser)
    url = BASE_URL + dataset_code
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, params=params or {}, timeout=TIMEOUT)
            r.raise_for_status()
            js = r.json()

            # Orden de dimensiones: usa 'id' si existe; si no, el orden de keys()
            dim_ids = js.get("id") or list(js.get("dimension", {}).keys())
            dims = []
            inv_maps = {}
            sizes = []

            for d in dim_ids:
                dim_obj = js["dimension"][d]
                dims.append(d)
                inv_maps[d] = _build_inv_map(dim_obj)
                if "index" in dim_obj.get("category", {}):
                    sizes.append(len(dim_obj["category"]["index"]))
                elif "label" in dim_obj.get("category", {}):
                    sizes.append(len(dim_obj["category"]["label"]))
                else:
                    sizes.append(1)

            records = []
            values = js.get("value")

            if isinstance(values, dict):
                for key, val in values.items():
                    if key == "":
                        idxs = [0] * len(dims)
                    elif ":" in key:
                        idxs = [int(i) for i in key.split(":")]
                    else:
                        try:
                            idxs = _unravel_index(int(key), sizes)
                        except Exception:
                            idxs = [0] * len(dims)
                    if len(idxs) < len(dims):
                        idxs = idxs + [0] * (len(dims) - len(idxs))
                    rec = {d: inv_maps[d].get(idxs[i], None) for i, d in enumerate(dims)}
                    rec["value"] = val
                    records.append(rec)

            elif isinstance(values, list):
                for flat_i, val in enumerate(values):
                    idxs = _unravel_index(flat_i, sizes)
                    rec = {d: inv_maps[d].get(idxs[i], None) for i, d in enumerate(dims)}
                    rec["value"] = val
                    records.append(rec)
            else:
                return pd.DataFrame()

            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            df["dataset_code"] = dataset_code
            df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")
            df = df[[*dims, "value", "dataset_code", "extraction_date"]]
            return df

        except Exception as e:
            last = e
            if attempt < RETRIES:
                time.sleep(0.7 * attempt)
            else:
                # Intento 2 (final): TSV crudo si el JSON falló (algunos endpoints grandes)
                try:
                    tsv_url = (BASE_URL + dataset_code)
                    if params:
                        # reconstruimos query string
                        extras = "&".join([f"{k}={v}" for k, v in params.items()])
                        tsv_url = f"{tsv_url}?{extras}&format=TSV"
                    else:
                        tsv_url = f"{tsv_url}?format=TSV"
                    r2 = requests.get(tsv_url, timeout=TIMEOUT)
                    r2.raise_for_status()
                    return pd.read_csv(StringIO(r2.text), sep="\t")
                except Exception:
                    raise last


# -------------------- Guardado -------------------- #

def save_csv(df: pd.DataFrame, name: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"✅ {name}.csv → {path} ({len(df):,} filas)")


def save_part(df: pd.DataFrame, group_name: str, part_idx: int, out_dir: str, source_table: str | None = None):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{group_name}__part{part_idx}.csv")
    if source_table:
        df = df.copy()
        if "source_table" not in df.columns:
            df["source_table"] = source_table
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"✅ {group_name} :: part{part_idx} ({source_table or ''}) → {path} ({len(df):,} filas)")


def merge_parts(group_name: str, out_dir: str):
    """Fusiona group__part*.csv en group.csv (manteniendo source_table)."""
    import glob
    pattern = os.path.join(out_dir, f"{group_name}__part*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"⚠️  {group_name}: no hay parts para fusionar.")
        return
    frames = []
    for f in files:
        df = pd.read_csv(f)
        frames.append(df)
    merged = pd.concat(frames, ignore_index=True)
    path = os.path.join(out_dir, f"{group_name}.csv")
    merged.to_csv(path, index=False, encoding="utf-8")
    print(f"🤝 {group_name}.csv (merge) → {path} ({len(merged):,} filas)")


# -------------------- Descarga de grupos -------------------- #

def download_group(group_key: str, params: dict, out_dir: str, eager_merge: bool):
    table_list = DATASET_GROUPS[group_key]
    for idx, table_code in enumerate(table_list, start=1):
        df = fetch_eurostat_table(table_code, params=params)
        if df is None or df.empty:
            print(f"⚠️  {group_key} / {table_code}: sin datos (revisa filtros).")
            continue
        save_part(df, group_key, idx, out_dir, source_table=table_code)
        time.sleep(PAUSE_S)
    if eager_merge:
        merge_parts(group_key, out_dir)


# -------------------- CLI principal -------------------- #

def main():
    ap = argparse.ArgumentParser(description="Eurostat ETL (GTP) con soporte de grupos")
    ap.add_argument("--datasets", required=True,
                    help="Lista separada por comas (ej. gdp_per_capita,ghg_emissions) o 'all'")
    ap.add_argument("--out-dir", default=DEFAULT_OUT, help="Directorio de salida (default data/raw/eurostat)")
    ap.add_argument("--filters", default="",
                    help="Filtros API. Ej: \"time=2010:2024&geo=ES,PT,FR\"")
    ap.add_argument("--eager-merge", action="store_true",
                    help="Fusiona las partes de cada grupo en un único CSV (nombre.csv)")
    args = ap.parse_args()

    # Resolver selección
    if args.datasets.lower() == "all":
        # Mantén compatibilidad: descargamos todos los simples + todos los grupos
        selected_simple = list(DATASETS.keys())
        selected_groups = list(DATASET_GROUPS.keys())
    else:
        req = [s.strip() for s in args.datasets.split(",") if s.strip()]
        selected_simple = [s for s in req if s in DATASETS]
        selected_groups = [s for s in req if s in DATASET_GROUPS]
        unknown = [s for s in req if s not in DATASETS and s not in DATASET_GROUPS]
        if unknown:
            print(f"❌ Dataset(s) no definidos: {unknown}")
            print(f"   Simples: {list(DATASETS.keys())}")
            print(f"   Grupos:  {list(DATASET_GROUPS.keys())}")
            sys.exit(1)

    params = parse_filters(args.filters)

    if not params:
        print("⚠️  Descargando SIN filtros. Puede ser pesado y tardar más o agotar cuota.")
        print("   Recomendado: añade al menos 'time=YYYY:YYYY' y/o 'geo=PAISES'")
        print()

    print("🚀 Eurostat ETL — inicio")
    print(f"   Simples: {selected_simple}")
    print(f"   Grupos:  {selected_groups}")
    print(f"   Filtros: {params if params else '(sin filtros)'}")
    print(f"   Salida:  {args.out_dir}\n")

    # 1) Simples (comportamiento original)
    for name in selected_simple:
        code = DATASETS[name]
        try:
            print(f"🔄 Descargando {name} [{code}] ...")
            df = fetch_eurostat_table(code, params=params)
            if df is None or df.empty:
                print(f"⚠️ {name}: sin datos devueltos (revisa filtros).")
                continue
            save_csv(df, name, args.out_dir)
        except requests.HTTPError as e:
            print(f"❌ HTTP {name}: {e}")
        except requests.ConnectionError as e:
            print(f"❌ Conexión {name}: {e}")
        except Exception as e:
            print(f"❌ Error {name}: {e}")

    # 2) Grupos (nuevos)
    for gname in selected_groups:
        try:
            print(f"🔄 Descargando grupo {gname} …")
            download_group(gname, params=params, out_dir=args.out_dir, eager_merge=args.eager_merge)
        except requests.HTTPError as e:
            print(f"❌ HTTP {gname}: {e}")
        except requests.ConnectionError as e:
            print(f"❌ Conexión {gname}: {e}")
        except Exception as e:
            print(f"❌ Error {gname}: {e}")

    print("\n🕒 Fin:", datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"))


if __name__ == "__main__":
    main()
