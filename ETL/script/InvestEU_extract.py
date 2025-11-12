# -*- coding: utf-8 -*-
"""
InvestEU ETL — Green Turning Point (GTP)
----------------------------------------
Descarga RAW desde:
  A) Listado de operaciones (HTML paginado)  -> data/raw/investeu/operations_list.csv
  B) Beneficiarios/Final recipients (PDF EIB) -> data/raw/investeu/final_recipients_YYYY.csv

Uso local:
  py ETL/script/investeu_extract.py --what all
  py ETL/script/investeu_extract.py --what list
  py ETL/script/investeu_extract.py --what recipients --years 2024

Variables de entorno (Docker/CI):
  INVESTEU_BASE_SLEEP=300          # pausa base (seg) entre llamadas
  INVESTEU_429_PENALTIES=300,600,1200
  INVESTEU_TIMEOUT=90
  INVESTEU_MAX_RETRIES=3
  INVESTEU_RECIPIENT_PDFS=URL1,URL2   # opcional: lista explícita de PDFs a parsear
"""

from __future__ import annotations
import os, re, time, argparse, csv, math, sys
import requests
from datetime import datetime, UTC
from urllib.parse import urljoin
from pathlib import Path
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import pdfplumber
import pandas as pd

# ---------------- Config ---------------- #

BASE_DIR = Path(__file__).resolve().parents[2]  # sube hasta la raíz del proyecto (/app)
DATA_DIR = BASE_DIR / "data" / "raw" / "investeu"
LOG_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    """Escribe mensaje en consola y en log/etl_investeu.log"""
    ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{ts} {msg}"
    print(line)
    with open(LOG_DIR / "etl_investeu.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")

# 🔹 NUEVO: helper de deduplicado inocuo
def _drop_duplicates_safe(df: pd.DataFrame, note: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    before = len(df)
    df = df.drop_duplicates(ignore_index=True)
    removed = before - len(df)
    if removed > 0:
        log(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})")
    return df

LIST_URL = "https://investeu.europa.eu/investeu-operations/investeu-operations-list_en"
# Este PDF cambia por año; por defecto incluimos 2024 (puedes añadir más por env)
DEFAULT_RECIPIENT_PDFS = [
    "https://www.eib.org/attachments/general/lists/investeu-final-recipients-beneficiaries-en.pdf"
]

OUT_DIR = DATA_DIR  # forzamos salida a la carpeta Docker-friendly

TIMEOUT = int(os.getenv("INVESTEU_TIMEOUT", "90"))
MAX_RETRIES = int(os.getenv("INVESTEU_MAX_RETRIES", "3"))
BASE_SLEEP = int(os.getenv("INVESTEU_BASE_SLEEP", "300"))  # pausa entre descargas
PENALTIES = [int(x) for x in os.getenv("INVESTEU_429_PENALTIES", "300,600,1200").split(",") if x.strip()]

HEADERS = {
    "User-Agent": "GTP/1.0 (academic, non-commercial; contact: data@gtp.local)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------- Utilidades HTTP con backoff ---------------- #

def get_with_backoff(url: str, params=None, headers=None) -> requests.Response:
    last_err = None
    headers = headers or {}
    penalty_idx = 0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                wait_s = PENALTIES[min(penalty_idx, len(PENALTIES)-1)]
                penalty_idx += 1
                print(f"   ⏳ 429 rate limit — esperando {wait_s}s (intento {attempt}/{MAX_RETRIES})…")
                time.sleep(wait_s)
                continue
            r.raise_for_status()
            return r
        except requests.HTTPError as e:
            last_err = e
            if attempt == MAX_RETRIES:
                raise
            print(f"   ⚠️  HTTP {e.response.status_code} — reintento {attempt}/{MAX_RETRIES} en 30s…")
            time.sleep(30)
        except Exception as e:
            last_err = e
            if attempt == MAX_RETRIES:
                raise
            print(f"   ⚠️  Error conexión — reintento {attempt}/{MAX_RETRIES} en 15s…")
            time.sleep(15)
    raise last_err

# ---------------- A) Scraper listado operaciones ---------------- #

def parse_list_page(html: str) -> Dict[str, List[Dict]]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for a in soup.select('a[href*="/investeu-operations/"]'):
        href = a.get("href") or ""
        if "/investeu-operations/" in href and href.strip().endswith("_en"):
            title = a.get_text(strip=True)
            url = urljoin(LIST_URL, href)
            li = a.find_parent(["article", "div", "li"]) or a
            meta_text = " ".join(li.get_text(" ", strip=True).split())
            m_date = re.search(r"(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})", meta_text)
            date = m_date.group(1) if m_date else None
            tags = []
            for tag_sel in ["span.field--name-field-tags a", ".tags a", "a[href*='/country/']"]:
                for t in li.select(tag_sel):
                    txt = t.get_text(strip=True)
                    if txt and txt not in tags:
                        tags.append(txt)
            items.append({
                "title": title,
                "url": url,
                "date_text": date,
                "tags": ";".join(tags) if tags else None,
            })

    # Paginación (best-effort)
    next_url = None
    for sel in ["a[rel='next']", "a.pager__link--next", "a:contains('Next')", "a:contains('next')"]:
        nxt = soup.select_one(sel)
        if nxt and nxt.get("href"):
            next_url = urljoin(LIST_URL, nxt.get("href"))
            break

    return {"items": items, "next_url": next_url}

def scrape_operations_list(base_url: str = LIST_URL) -> pd.DataFrame:
    print("🔄 Listado de operaciones InvestEU …")
    seen_urls = set()
    all_rows = []
    url = base_url

    while url and url not in seen_urls:
        seen_urls.add(url)
        r = get_with_backoff(url, headers=HEADERS)
        data = parse_list_page(r.text)
        rows = data["items"]
        print(f"   • Página {len(seen_urls)}: {len(rows)} ítems")
        all_rows.extend(rows)
        url = data["next_url"]
        time.sleep(BASE_SLEEP)

    if not all_rows:
        print("⚠️  No se extrajeron operaciones (revisa estructura/paginación).")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"])
    df["source"] = base_url
    df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")
    cols = ["title", "url", "date_text", "tags", "source", "extraction_date"]
    return df[cols]

# ---------------- B) PDF EIB final recipients ---------------- #

def normalise_pdf_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.dropna(how="all", axis=1)
    df = df.rename(columns=lambda c: str(c).strip())
    expected = ["Financial Product", "Operation Name", "Borrower Name", "Borrower Address",
                "Borrower Country", "Financing Form", "Policy Area supported",
                "Amount of financial support received in EUR"]
    rename_map = {}
    for col in df.columns:
        c = col.lower()
        if "financial" in c and "product" in c: rename_map[col] = "Financial Product"
        elif "operation" in c and "name" in c: rename_map[col] = "Operation Name"
        elif "borrower" in c and "name" in c: rename_map[col] = "Borrower Name"
        elif "borrower" in c and "address" in c: rename_map[col] = "Borrower Address"
        elif "country" in c: rename_map[col] = "Borrower Country"
        elif "financing" in c and "form" in c: rename_map[col] = "Financing Form"
        elif "policy" in c and "area" in c: rename_map[col] = "Policy Area supported"
        elif "amount" in c and "eur" in c: rename_map[col] = "Amount of financial support received in EUR"
    df = df.rename(columns=rename_map)
    return df

def extract_final_recipients_from_pdf(pdf_url: str) -> pd.DataFrame:
    print(f"🔄 PDF EIB recipients: {pdf_url}")
    r = get_with_backoff(pdf_url, headers={"User-Agent": HEADERS["User-Agent"], "Accept": "application/pdf"})
    with open(OUT_DIR / "_tmp_investeu.pdf", "wb") as f:
        f.write(r.content)

    tables = []
    with pdfplumber.open(OUT_DIR / "_tmp_investeu.pdf") as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            try:
                tbl = page.extract_table()
                if not tbl or len(tbl) < 2:
                    continue
                header = [str(x).strip() if x is not None else "" for x in tbl[0]]
                rows = tbl[1:]
                df = pd.DataFrame(rows, columns=header)
                df["page"] = i
                tables.append(df)
            except Exception:
                continue

    if not tables:
        print("⚠️  No se extrajeron tablas del PDF (estructura no tabular).")
        return pd.DataFrame()

    df = pd.concat(tables, ignore_index=True)
    df = normalise_pdf_table(df)
    df["pdf_url"] = pdf_url
    df["extraction_date"] = datetime.now(UTC).strftime("%Y-%m-%d")
    # 🔹 NUEVO: dedup genérico por fila completa (no altera columnas)
    df = _drop_duplicates_safe(df, note="final_recipients(pdf)")
    return df

# ---------------- Main ---------------- #

def main():
    ap = argparse.ArgumentParser(description="InvestEU ETL (GTP) — operaciones y beneficiarios")
    ap.add_argument("--what", default="all", choices=["all", "list", "recipients"],
                    help="Qué extraer: list (operaciones), recipients (beneficiarios PDF) o all")
    ap.add_argument("--years", default="", help="Años objetivo p/ recipients (informativo si pasas PDFs explícitos)")
    ap.add_argument("--out-dir", default=str(OUT_DIR), help="Carpeta salida")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    print("🚀 InvestEU ETL — inicio")
    print(f"   Carpeta out: {out}")
    print(f"   Timeout:     {TIMEOUT}s | Retries: {MAX_RETRIES}")
    print(f"   Pausa base:  {BASE_SLEEP}s  | Castigos 429 (s): {PENALTIES}\n")

    if args.what in ("all", "list"):
        try:
            df_list = scrape_operations_list(LIST_URL)
            if not df_list.empty:
                # 🔹 NUEVO: dedup final por si el sitio repite enlaces
                df_list = _drop_duplicates_safe(df_list, note="operations_list.csv")
                p = out / "operations_list.csv"
                df_list.to_csv(p, index=False, encoding="utf-8")
                print(f"✅ operations_list.csv -> {p} ({len(df_list):,} filas)")
        except requests.HTTPError as e:
            print(f"❌ HTTP list: {e}")
        except Exception as e:
            print(f"❌ Error list: {e}")

        time.sleep(BASE_SLEEP)

    if args.what in ("all", "recipients"):
        pdfs_env = os.getenv("INVESTEU_RECIPIENT_PDFS")
        pdf_urls = [u.strip() for u in pdfs_env.split(",")] if pdfs_env else DEFAULT_RECIPIENT_PDFS

        all_pdf_rows = []
        for u in pdf_urls:
            try:
                df_pdf = extract_final_recipients_from_pdf(u)
                if not df_pdf.empty:
                    all_pdf_rows.append(df_pdf)
            except requests.HTTPError as e:
                print(f"❌ HTTP recipients: {e}")
            except Exception as e:
                print(f"❌ Error recipients: {e}")
            time.sleep(BASE_SLEEP)

        if all_pdf_rows:
            df_all = pd.concat(all_pdf_rows, ignore_index=True)
            # 🔹 NUEVO: dedup combinado (por seguridad)
            df_all = _drop_duplicates_safe(df_all, note="final_recipients(all)")
            year_hint = ""
            m = re.search(r"(20\d{2})", " ".join(pdf_urls))
            if m:
                year_hint = f"_{m.group(1)}"
            p = out / f"final_recipients{year_hint}.csv"
            df_all.to_csv(p, index=False, encoding="utf-8")
            print(f"✅ final_recipients{year_hint}.csv -> {p} ({len(df_all):,} filas)")

    print(f"\n🕒 Fin: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")

if __name__ == "__main__":
    main()
