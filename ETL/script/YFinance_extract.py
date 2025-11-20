# -*- coding: utf-8 -*-
"""
Finance ETL — Yahoo Finance (2018 stats)
...
"""

# Habilita anotaciones de tipo modernas
from __future__ import annotations
# Para leer argumentos de terminal (ej. --what finance)
import argparse
# Para pausas (sleep) entre peticiones
import time
# Para manejar rutas de archivos de forma moderna
from pathlib import Path
# Para especificar que "tickers" es una lista de strings
from typing import List
# Para obtener la fecha actual
from datetime import datetime, UTC
# Para manejar las tablas de datos
import pandas as pd
# Librería NO OFICIAL para conectar con Yahoo Finance
import yfinance as yf

# Funcion auxiliar para eliminar duplicados sin errores
def _drop_duplicates_safe(df: pd.DataFrame, subset=None, note: str = "") -> pd.DataFrame:
    # Si el DataFrame esta vacio, lo devuelve tal cual
    if df is None or df.empty:
        return df
    # Guarda el numero de filas inicial
    before = len(df)
    # Elimina duplicados basandose en las columnas"subset"
    df = df.drop_duplicates(subset=subset, ignore_index=True)
    # Calculas cuantas se borraron
    removed = before - len(df)
    # Si se borro alguna, avisa en la consola
    if removed > 0:
        print(f"🧹 {note} eliminados {removed:,} duplicados (final: {len(df):,})")
    return df # Devuelve el DataFrame limpio

# Funcion principal de extraccion
def extract_financial_data_2018(tickers: List[str], out_dir: Path) -> pd.DataFrame:
    # Crea la carpeta de salida si no existe
    out_dir.mkdir(parents=True, exist_ok=True)
    print(" Extrayendo datos financieros para 2018…")
    records = [] # Lista para guardar los datos de cada empresa

    # Bucle: itera sobre cada "ticker" (codigo de epresa, ej: "IBE.MC" para Iberdrola)
    for t in tickers:
        try:
            # Crea un objeto Ticker de yfinance
            ticker = yf.Ticker(t)
            # Descarga la información general de la empresa
            info = ticker.info

            # Descarga el historial de precios SOLO para el año 2018
            hist = ticker.history(start="2018-01-01", end="2018-12-31")
            # Si no hay datos para 2018, se salta esta empresa
            if hist.empty:
                print(f"   • {t}: sin histórico 2018, se omite.")
                continue

            # Calcula la Volatilidad Anualizada
            # 1. oct_change(): calcula el % de cambio diario
            # 2. std(): Calcula la desviación estándar (cuánto varía)
            # 3. * (252 ** 0.5): Anualiza el dato (hay aprox 252 días de bolsa al año)
            vol = hist["Close"].pct_change().std() * (252 ** 0.5)

            # Crea un diccionario con todos los datos extraidos
            record = {
                "Ticker": t,
                "Company": info.get("shortName"), # Nombre corto (ej. Iberdrola S.A.)
                "Sector": info.get("sector"), # Sector (ej. Utilites)
                "MarketCap": info.get("marketCap"), # Valor en bolsa
                "ROE": info.get("returnOnEquity"), # Rentabilidad sobre capital
                "ROI": info.get("returnOnInvestment"), # Retorno de inversión
                "Volatility_2018": vol, # Nuestra métrica calculada
                "Country": info.get("country"), # Pais
                "Extraction_Year": 2018, # Dato fijo: Año del estudio
                "Extraction_Date": datetime.now(UTC).strftime("%Y-%m-%d"), # Fecha de hoy
            }
            # Añade el registro a la lista
            records.append(record)
            print(f"   • {t}: {info.get('shortName')} — OK")
            # Pausa de 2 segundos para no saturar Yahoo
            time.sleep(2)
        except Exception as e:
            # Si falla una empresa imprime el error pero sigue con la siguiente
            print(f"Error en {t}: {e}")
            continue

    # Cpmvierte la lista de diccionarios en un DataFrame
    df_fin = pd.DataFrame(records)
    # Elimina los duplicados por Ticker
    df_fin = _drop_duplicates_safe(df_fin, subset=["Ticker"], note="financial_data_2018")

    # Si se extrajeron datos, guarda el CSV
    if not df_fin.empty:
        p = out_dir / "financial_data_2018.csv" # Ruta
        df_fin.to_csv(p, index=False, encoding="utf-8")
        print(f"financial_data_2018.csv -> {p} ({len(df_fin):,} filas)")
    else:
        print(" No se extrajeron datos financieros (revisa tickers o conexión).")
    return df_fin

# Función main (CLI)
def main():
    # Configura los argumentos de terminal
    ap = argparse.ArgumentParser(
        description="InvestEU ETL (GTP) — operaciones, beneficiarios y datos financieros"
    )
    # Argumento --what (aunque solo soporta "finance" realmente)
    ap.add_argument("--what", default="finance", choices=["all", "finance"], help="Qué extraer: finance o all (equivalente aquí).")
    # Argumento --out-dir (donde guardar)
    ap.add_argument("--out-dir", default="data/raw/finance", help="Carpeta de salida para los CSV (default: data/raw/finance)")
    # Argumento --tickers (para pasar una lista personalizada de empresas)
    ap.add_argument("--tickers", default="", help="Lista de tickers separados por coma (opcional; si no, se usa el preset).")
    args = ap.parse_args()

    # Convierte la ruta de salida a objeto Path y crea la carpeta
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Si el usuario quiere finanzas
    if args.what in ("all", "finance"):
        # Lista de empresas por defecto (Energeticas Europeas)
        # RDSA.L (Shell), BP.L (BP), ENI.MI (Eni), TOTF.PA (TotalEnergies), IBE.MC (Iberdrola), ORA.PA (Orange?)
        tickers_energy_eu = [
            "RDSA.L","BP.L","ENI.MI","TOTF.PA","IBE.MC","ORA.PA",
        ]
        # Si el usuario paso tickers por terminal usa esos, sino por defecto
        user_tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
        tickers = user_tickers if user_tickers else tickers_energy_eu
        # Llama a la funcion de extraccion
        extract_financial_data_2018(tickers, out)
# Imprime la hora de fin
    print(f"\n🕒 Fin: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}")

if __name__ == "__main__":
    main()
