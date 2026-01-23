import argparse
import time
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf
import numpy as np
import config  # Importamos tu configuración centralizada

# --- CARTERA DE SEGUIMIENTO (GREEN DEAL EUROPE) ---
# Hemos seleccionado empresas líderes en sus sectores (Best-in-Class)
# que son fundamentales para la transición ecológica europea.
GREEN_TICKERS = [
    # --- ESPAÑA (IBEX 35) ---
    "IBE.MC",  # Iberdrola: Gigante global de renovables
    "REP.MC",  # Repsol: Líder en transición y combustibles sintéticos
    "SAN.MC",  # Santander: Financiación de proyectos verdes
    "BBVA.MC", # BBVA: Pionero en bonos verdes
    "ITX.MC",  # Inditex: Logística sostenible (Scope 3 emissions)
    "ANA.MC",  # Acciona: Infraestructuras regenerativas y Agua
    "FER.MC",  # Ferrovial: Movilidad sostenible
    "ELE.MC",  # Endesa: Electrificación
    "ENG.MC",  # Enagás: Hidrógeno verde

    # --- FRANCIA (CAC 40) ---
    "AIR.PA",  # Airbus: Descarbonización aérea (SAF)
    "TTE.PA",  # TotalEnergies: Diversificación energética
    "BNP.PA",  # BNP Paribas: Banca de inversión sostenible
    "SU.PA",   # Schneider Electric: Eficiencia energética (Clave)
    "VIE.PA",  # Veolia: Gestión de residuos y agua
    "DG.PA",   # Vinci: Construcción sostenible

    # --- ALEMANIA (DAX) ---
    "VOW3.DE", # Volkswagen: La apuesta por el vehículo eléctrico (EV)
    "SIE.DE",  # Siemens: Infraestructura inteligente
    "BMW.DE",  # BMW: Economía circular en automoción
    "EOAN.DE", # E.ON: Redes de distribución inteligentes
    "RWE.DE",  # RWE: Transición del carbón a renovables
    "ALV.DE",  # Allianz: Asset Management con criterios ESG

    # --- ITALIA (FTSE MIB) ---
    "ENEL.MI", # Enel: Mayor utility de Europa
    "ISP.MI",  # Intesa Sanpaolo
    "ENI.MI",  # Eni: Biocombustibles

    # --- PAÍSES BAJOS (AEX) ---
    "INGA.AS", # ING Group
    "ASML.AS", # ASML: Eficiencia en semiconductores (vital para Smart Cities)
    "PHIA.AS", # Philips: HealthTech y sostenibilidad

    # --- NÓRDICOS (Líderes ESG) ---
    "ORSTED.CO", # Orsted (DK): El rey de la eólica marina
    "VWS.CO",    # Vestas (DK): Fabricante de turbinas
    "NOVO-B.CO", # Novo Nordisk (DK): Sostenibilidad social
    "EQNR.OL",   # Equinor (NO): Energía eólica flotante
    "NHY.OL",    # Norsk Hydro (NO): Aluminio bajo en carbono
    "VOLV-B.ST", # Volvo (SE): Camiones eléctricos
    "ERIC-B.ST", # Ericsson (SE): Conectividad 5G (Eficiencia)
    "NESTE.HE",  # Neste (FI): Líder mundial en diésel renovable

    # --- REINO UNIDO (Relevancia Geográfica) ---
    "SHEL.L",   # Shell
    "BP.L",     # BP
    "NG.L",     # National Grid: La columna vertebral de la electrificación
    "SSE.L",    # SSE: Renovables
]

def get_safe_info(info_dict, key, default="Unknown"):
    """
    Yahoo Finance a veces devuelve 'None' o cambia las claves del diccionario.
    Esta función protege el script para que no falle por un dato faltante.
    """
    val = info_dict.get(key)
    return val if val is not None else default

def extract_financial_data(tickers, output_path, start_year="2006"):
    """
    Descarga el histórico de precios y calcula métricas anuales de riesgo/retorno.
    """
    # Definimos rango temporal
    end_year = str(datetime.now().year)
    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"
    
    print(f"--- INICIANDO EXTRACCIÓN FINANCIERA (YFINANCE) ---")
    print(f"Rango: {start_date} a {end_date}")
    print(f"Total Tickers: {len(tickers)}")
    
    records_annual = []
    
    for i, t in enumerate(tickers):
        try:
            print(f"[{i+1}/{len(tickers)}] Procesando {t}...", end=" ", flush=True)
            
            # Instanciamos el objeto Ticker
            ticker = yf.Ticker(t)
            
            # 1. Descarga Histórico (OHLCV)
            # auto_adjust=True ajusta por dividendos y splits (Vital para retorno real)
            hist = ticker.history(start=start_date, end=end_date, auto_adjust=True)
            
            if hist.empty:
                print(f"⚠️  Data vacío o ticker deslistado.")
                continue

            # 2. Obtener Metadatos (Sector, Industria, Ratios)
            # Esto es lento porque hace una llamada extra a la API, pero necesario.
            info = ticker.info 
            
            # 3. Ingeniería de Características (Financial Feature Engineering)
            # Calculamos el retorno diario para poder medir la volatilidad
            hist['Daily_Return'] = hist['Close'].pct_change()
            
            # Agrupamos por Año para alinear con los datos macroeconómicos (Kuznets)
            grouped = hist.groupby(hist.index.year)
            
            years_processed = 0
            for year, group in grouped:
                # Necesitamos un mínimo de días cotizados para que el dato sea fiable
                if len(group) < 50: 
                    continue 
                
                # --- Métrica de Riesgo: Volatilidad Anualizada ---
                # Desviación estándar diaria * raíz de 252 (días bursátiles promedio).
                # Esto nos dice cuánto oscila el precio en un año.
                volatility = group['Daily_Return'].std() * np.sqrt(252)
                
                # --- Métrica de Rentabilidad: Retorno Anual ---
                # (Precio Final / Precio Inicial) - 1
                annual_return = (group['Close'].iloc[-1] / group['Close'].iloc[0]) - 1
                
                records_annual.append({
                    "Ticker": t,
                    "Company": get_safe_info(info, "shortName", t),
                    "Sector": get_safe_info(info, "sector"),
                    "Industry": get_safe_info(info, "industry"),
                    "Country": get_safe_info(info, "country"), # Clave para agrupar por país luego
                    "Year": year,
                    "Close_Price_Avg": group['Close'].mean(),
                    "Annual_Return": annual_return,
                    "Volatility": volatility,
                    "Volume_Avg": group['Volume'].mean(),
                    
                    # Ratios de valoración actuales (Snapshot)
                    # Útil para saber si la empresa está 'barata' (P/E bajo) hoy.
                    "Current_PE": get_safe_info(info, "trailingPE", None),
                    "Current_Beta": get_safe_info(info, "beta", None),
                    "Extraction_Date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
                })
                years_processed += 1
            
            print(f"✅ OK ({years_processed} años extraídos)")
            
            # Pausa de cortesía para no saturar la API de Yahoo
            time.sleep(0.3)
            
        except Exception as e:
            print(f"❌ Error crítico en {t}: {e}")

    # --- GUARDADO ---
    if records_annual:
        df = pd.DataFrame(records_annual)
        
        # Limpieza de duplicados por seguridad
        df.drop_duplicates(subset=["Ticker", "Year"], inplace=True)
        df.sort_values(by=["Ticker", "Year"], inplace=True)
        
        # Aseguramos directorio
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_path, index=False)
        print(f"\n[ÉXITO] Dataset Financiero generado.")
        print(f" -> Ruta: {output_path}")
        print(f" -> Dimensiones: {df.shape}")
        print(df[['Ticker', 'Year', 'Annual_Return', 'Volatility']].head())
    else:
        print("\n[WARN] No se generaron datos financieros.")

# --- ENTRY POINT ---
def main():
    # Parseo de argumentos por si queremos ejecutarlo con opciones personalizadas
    parser = argparse.ArgumentParser(description="ETL Financiero para Green Turning Point")
    parser.add_argument("--out", default="finance_2006_2025.csv", help="Nombre del archivo de salida")
    args = parser.parse_args()

    # Usamos config.BASE_DIR para guardar el CSV en la raíz del proyecto (o donde prefieras)
    output_file = config.BASE_DIR / args.out
    
    extract_financial_data(GREEN_TICKERS, output_file)

if __name__ == "__main__":
    main()