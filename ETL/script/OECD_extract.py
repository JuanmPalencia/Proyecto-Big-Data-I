# -*- coding: utf-8 -*-
"""
OECD ETL — SDMX REST (CSV con etiquetas)
Descarga datasets de la OECD y guarda CSV en data/raw/oecd/.

NUEVO:
- Pausa base entre descargas: 5 min (300s).
- Manejo 429 con "castigo" escalonado: 5m → 10m → 20m (por defecto).
- Respeta Retry-After si es mayor que el castigo.

Ejemplos:
  py ETL/script/oecd_extract.py                              # todos (pausas de 5m)
  py ETL/script/oecd_extract.py --only env_tax,gdp           # subset
  py ETL/script/oecd_extract.py --pause 60                   # pausa base 1m
  py ETL/script/oecd_extract.py --penalty-429 300,600,1200   # (por defecto)
"""

from __future__ import annotations
import os, time, argparse
import random
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, UTC
from typing import Dict, List

# -------------------- Config base --------------------
# Detecta la ubicación real del script (tanto en local como en contenedor)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Carpeta de salida → /app/data/raw/oecd (en Docker) o ./data/raw/oecd (en local)
OUT_DIR = os.path.join(BASE_DIR, "..", "..", "data", "raw", "oecd")
os.makedirs(OUT_DIR, exist_ok=True)

# ======================================================
# CONFIGURACIÓN BASE OECD
# ======================================================

BASE = "https://sdmx.oecd.org/public/rest/data"

DEFAULT_START = os.getenv("OECD_START_YEAR", "2010")
DEFAULT_END   = os.getenv("OECD_END_YEAR",   "2023")
DEFAULT_TIMEOUT = int(os.getenv("OECD_TIMEOUT", "90"))
DEFAULT_MAX_RETRIES = int(os.getenv("OECD_MAX_RETRIES", "3"))

# ⚠️ Pausa BASE entre requests (por defecto 5 minutos)
DEFAULT_PAUSE = float(os.getenv("OECD_PAUSE_RETRY", "300"))

# ⚠️ Castigo por 429 (en segundos) — por defecto: 5m, 10m, 20m
DEFAULT_PENALTY_429 = os.getenv("OECD_PENALTY_429", "300,600,1200")

os.makedirs(OUT_DIR, exist_ok=True)

# -------------------- Endpoints (plantillas) --------------------
# Todas las URLs incluyen {start} y {end}. Si el link no trae 'format=',
# se añade &format=csvfilewithlabels automáticamente.

DATA_URLS: Dict[str, str] = {
    # 🌍 Ambiente/energía
    "air_ghg": (
        f"{BASE}/OECD.ENV.EPI,DSD_AIR_GHG@DF_AIR_GHG,1.0/"
        "all?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "env_tax": (
        f"{BASE}/OECD.ENV.EPI,DSD_ERTR@DF_ERTR,1.0/"
        "A..TAXREV._T._T.USD.?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    # renewable_energy → integrarlo desde Eurostat (nrg_ind_ren) en otro script si lo deseas.

    "material_resources": (
        f"{BASE}/OECD.ENV.EPI,DSD_MATERIAL_RESOURCES@DF_MATERIAL_RESOURCES,1.0/"
        "OECD.A.DMC..TOT?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "material_footprint": (
        f"{BASE}/OECD.ENV.EPI,DSD_MATERIAL_RESOURCES@DF_MATERIAL_RESOURCES,1.0/"
        "OECD.A.MF..TOT?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "env_policy": (
        f"{BASE}/OECD.ECO.MAD,DSD_EPS@DF_EPS,1.0/"
        ".A..EPS?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "green_growth": (
        f"{BASE}/OECD.ENV.EPI,DSD_GG@DF_GREEN_GROWTH,1.1/"
        "AUS....?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),  # Cambia 'AUS....?' por 'all' si quieres todos los países.

    # 💶 Sectores ambientales / gasto
    "env_expenditure": (
        f"{BASE}/OECD.ENV.EPI,DSD_EPEA@DF_EPEA,1.0/"
        ".A.NEEP...S1._T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    
    # "circular":  # añadir cuando el flow estable esté publicado.

    # 👩‍🔬 I+D, patentes, VC
    "rd_expenditure": (
        f"{BASE}/OECD.STI.STP,DSD_RDS_GERD@DF_GERD_SEO,1.0/"
        ".A.._T....._T.XDC.?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "patents_ipc": (
        f"{BASE}/OECD.STI.PIE,DSD_PATENTS@DF_PATENTS,1.0/"
        ".A...PRIORITY...INVENTOR..._T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "enviromental_ipc": (
        f"{BASE}/OECD.STI.PIE,DSD_PATENTS@DF_PATENTS_ENVIROMENT,1.0/"
        ".A...PRIORITY...INVENTOR..._T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "vc_investment": (
        f"{BASE}/OECD.SDD.TPS,DSD_VC@DF_VC_INV,1.0/"
        "...USD_EXC.A?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # 📊 Macroeconomía y mercado laboral
    "gdp": (
        f"{BASE}/OECD.SDD.NAD,DSD_NAAG@DF_NAAG_I,1.0/"
        "A.AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LVA+LTU+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA.B1GQ_R_GR.."
        "?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "productivity": (
        f"{BASE}/OECD.SDD.TPS,DSD_PDB@DF_PDB_ISIC4_I4,1.0/"
        ".A.EMP......?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "labour_force": (
        f"{BASE}/OECD.SDD.TPS,DSD_ALFS@DF_SUMTAB,1.0/"
        ".LF.._Z._T....A?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # ❤️ Social / bienestar / salud
    "bli_indicators": (
        f"{BASE}/OECD.SDD.BLI,DSD_BLI@DF_BLI,1.0/"
        "all?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "regional_wellbeing": (
        f"{BASE}/OECD.SDD.RWB,DSD_RWB@DF_REGIONAL_WELLBEING,1.0/"
        "all?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "health_expenditure": (
        f"{BASE}/OECD.ELS.HD,DSD_SHA@DF_SHA,1.0/"
        ".A.EXP_HEALTH.PT_B1GQ._T.._T.._T...?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),

    # Otros ambientales
    "air_pollutants": (
        f"{BASE}/OECD.ENV.EPI,DSD_AIR_EMISSIONS@DF_AIR_EMISSIONS,1.0/"
        ".A.SOX.T_EM_MM.T?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
    "energy_supply": (
        f"{BASE}/OECD.SDD.NAD.SEEA,DSD_PEFA@DF_PEFASUP,1.1/"
        "CRI.A..ATU_HH+ATU+A+B+C+D+E+F+G+H+I+J+K+L+M+N+O+P+Q+R+S+T+U+HH..SUP.?startPeriod={start}&endPeriod={end}&dimensionAtObservation=AllDimensions"
    ),
}

# -------------------- Utilidades --------------------

def _ensure_csv_format(url: str) -> str:
    """Añade &format=csvfilewithlabels si no está presente."""
    if "format=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}format=csvfilewithlabels"

def _with_period(url_template: str, start: str, end: str) -> str:
    return url_template.format(start=start, end=end)

def _sleep_with_jitter(base_seconds: float):
    """
    Pausa base entre descargas con un pequeño jitter (±10%) para evitar
    sincronizarse con otros procesos.
    """
    jitter = base_seconds * 0.1
    time.sleep(max(0.0, base_seconds + random.uniform(-jitter, jitter)))

def _parse_penalties(s: str) -> List[int]:
    """
    Convierte '300,600,1200' -> [300, 600, 1200]
    """
    out: List[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            pass
    return out or [300, 600, 1200]

def robust_get(url: str, timeout: int, max_retries: int, penalties_429: List[int]):
    """
    GET con manejo de 429 y 5xx:
      - Para 429 usa max(Retry-After, penalty[i]).
      - penalty[i] por defecto: 300s (5m), 600s (10m), 1200s (20m).
      - Si hay más de 3 consecutivos, repite el último (20m).
      - Para 5xx aplica backoff lineal: 60s, 120s, 180s… (amigable).
    """
    last_exc = None
    consec_429 = 0

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)

            # 429 — Too Many Requests
            if resp.status_code == 429:
                consec_429 += 1
                ra = resp.headers.get("Retry-After")
                ra_val = int(ra) if ra and ra.isdigit() else 0
                idx = min(consec_429 - 1, len(penalties_429) - 1)
                penalty = penalties_429[idx]
                wait_s = max(ra_val, penalty)
                print(f"   ⏳ 429 rate limit — esperando {wait_s}s (intento {attempt}/{max_retries}, #{consec_429} consecutivo)…")
                time.sleep(wait_s)
                # no contamos este intento como “consumido”: continuamos el loop sin incrementar 'attempt'
                continue

            # 5xx — Server errors: backoff lineal 60s * attempt
            if 500 <= resp.status_code < 600:
                wait_s = 60 * attempt
                print(f"   ⚠️ {resp.status_code} server error — reintento en {wait_s}s…")
                time.sleep(wait_s)
                continue

            resp.raise_for_status()
            return resp

        except requests.HTTPError as e:
            last_exc = e
            # Para códigos distintos de 429/5xx, o si ya falló raise_for_status()
            if attempt == max_retries:
                raise
            # pequeña espera genérica antes del siguiente intento
            time.sleep(5)
        except Exception as e:
            last_exc = e
            if attempt == max_retries:
                raise
            # problemas de red/transitorios: 30s, 60s, 90s…
            wait_s = 30 * attempt
            print(f"   ⚠️ Error de red — reintento en {wait_s}s…")
            time.sleep(wait_s)

    if last_exc:
        raise last_exc

def fetch_csv(url: str, timeout: int, max_retries: int, penalties_429: List[int]) -> pd.DataFrame:
    r = robust_get(url, timeout=timeout, max_retries=max_retries, penalties_429=penalties_429)
    return pd.read_csv(StringIO(r.text), low_memory=False)

def save_df(df: pd.DataFrame, name: str):
    df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")
    path = os.path.join(OUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"✅ {name}.csv  ({len(df):,} filas) -> {path}")

# -------------------- CLI principal --------------------

def main():
    ap = argparse.ArgumentParser(description="OECD ETL — SDMX REST (CSV, 429-friendly)")
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--only", default="all", help="lista separada por comas; 'all' por defecto")
    ap.add_argument("--pause", type=float, default=DEFAULT_PAUSE, help="pausa base entre descargas (segundos)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="timeout por request (segundos)")
    ap.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    ap.add_argument("--penalty-429", default=DEFAULT_PENALTY_429,
                    help="lista de segundos para castigo por 429, ej. '300,600,1200'")
    ap.add_argument("--overwrite", action="store_true", help="si existe el CSV, re-descargarlo")
    args = ap.parse_args()

    penalties_429 = _parse_penalties(args.penalty_429)

    print("🚀 OECD ETL — inicio")
    print(f"   Periodo:  {args.start}–{args.end}")
    print(f"   Carpeta:  {OUT_DIR}")
    print(f"   Timeout:  {args.timeout}s | Retries: {args.max_retries}")
    print(f"   Pausa base entre descargas: {int(args.pause)}s")
    print(f"   Castigos 429 (s): {penalties_429}\n")

    targets = list(DATA_URLS.keys()) if args.only == "all" else [t.strip() for t in args.only.split(",") if t.strip()]

    for key in targets:
        if key not in DATA_URLS:
            print(f"❌ '{key}' no está definido. Opciones: {list(DATA_URLS.keys())}")
            continue

        out_path = os.path.join(OUT_DIR, f"{key}.csv")
        if os.path.exists(out_path) and not args.overwrite:
            print(f"⏭️  {key}: ya existe, usa --overwrite para re-descargar.")
            # pausa amistosa aunque saltemos el dataset
            _sleep_with_jitter(args.pause)
            continue

        url = _with_period(DATA_URLS[key], args.start, args.end)
        if "format=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}format=csvfilewithlabels"

        print(f"🔄 {key} …")
        try:
            df = fetch_csv(url, timeout=args.timeout, max_retries=args.max_retries, penalties_429=penalties_429)
            if df.empty:
                print(f"⚠️  {key}: respuesta vacía.")
            else:
                save_df(df, key)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "HTTP"
            print(f"❌ {key}: {code} {e}")
        except Exception as e:
            print(f"❌ {key}: {e}")

        # Pausa BASE entre datasets (por defecto 5 minutos) con pequeño jitter
        _sleep_with_jitter(args.pause)

    print(f"\n🕒 Fin: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")

if __name__ == "__main__":
    main()
