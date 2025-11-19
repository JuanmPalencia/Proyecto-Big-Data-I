# -*- coding: utf-8 -*-
"""
Finance ETL — Yahoo Finance (2018 stats)
...
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import List
from datetime import datetime, UTC

import pandas as pd
import yfinance as yf

# 🔹 NUEVO: helper dedup
def _drop_duplicates_safe(df: pd.DataFrame, subset=None, note: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=subset, ignore_index=True)
    removed = before - len(df)
    if removed > 0:
        print(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})")
    return df

def extract_financial_data_2018(tickers: List[str], out_dir: Path) -> pd.DataFrame:
    out_dir.mkdir(parents=True, exist_ok=True)
    print(" Extrayendo datos financieros para 2018…")
    records = []

    for t in tickers:
        try:
            ticker = yf.Ticker(t)
            info = ticker.info
            hist = ticker.history(start="2018-01-01", end="2018-12-31")
            if hist.empty:
                print(f"   • {t}: sin histórico 2018, se omite.")
                continue
            vol = hist["Close"].pct_change().std() * (252 ** 0.5)
            record = {
                "Ticker": t,
                "Company": info.get("shortName"),
                "Sector": info.get("sector"),
                "MarketCap": info.get("marketCap"),
                "ROE": info.get("returqnOnEquity"),
                "ROI": info.get("returnOnInvestment"),
                "Volatility_2018": vol,
                "Country": info.get("country"),
                "Extraction_Year": 2018,
                "Extraction_Date": datetime.now(UTC).strftime("%Y-%m-%d"),
            }
            records.append(record)
            print(f"   • {t}: {info.get('shortName')} — OK")
            time.sleep(2)
        except Exception as e:
            print(f"Error en {t}: {e}")
            continue

    df_fin = pd.DataFrame(records)
    # 🔹 NUEVO: dedup por Ticker (un registro por ticker)
    df_fin = _drop_duplicates_safe(df_fin, subset=["Ticker"], note="financial_data_2018")
    if not df_fin.empty:
        p = out_dir / "financial_data_2018.csv"
        df_fin.to_csv(p, index=False, encoding="utf-8")
        print(f"financial_data_2018.csv -> {p} ({len(df_fin):,} filas)")
    else:
        print(" No se extrajeron datos financieros (revisa tickers o conexión).")
    return df_fin

def main():
    ap = argparse.ArgumentParser(
        description="InvestEU ETL (GTP) — operaciones, beneficiarios y datos financieros"
    )
    ap.add_argument("--what", default="finance", choices=["all", "finance"], help="Qué extraer: finance o all (equivalente aquí).")
    ap.add_argument("--out-dir", default="data/raw/finance", help="Carpeta de salida para los CSV (default: data/raw/finance)")
    ap.add_argument("--tickers", default="", help="Lista de tickers separados por coma (opcional; si no, se usa el preset).")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    if args.what in ("all", "finance"):
        tickers_energy_eu = [
            "RDSA.L","BP.L","ENI.MI","TOTF.PA","IBE.MC","ORA.PA",
        ]
        user_tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
        tickers = user_tickers if user_tickers else tickers_energy_eu
        extract_financial_data_2018(tickers, out)

    print(f"\n🕒 Fin: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")

if __name__ == "__main__":
    main()
