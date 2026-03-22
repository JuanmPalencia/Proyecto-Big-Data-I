"""
population_extract.py — GTP (Green Turning Point)
Extrae población de Unidades de Análisis Urbano (FUA) desde Eurostat.

Fuente primaria: dataset `urb_cpop1` (Urban Audit city statistics)
  - Indicador: POP (total population)
  - URL: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/urb_cpop1?format=TSV&compressed=true
  - Códigos Urban Audit: ES001C, DE001C, FR001C … (formato ISO2 + número + 'C')

Fallback: `demo_r_pjanaggr3` (NUTS-3 population by age group) cuando no hay Urban Audit.
  - Usa el código NUTS-3 de config.EUROSTAT_CODES para el mismo país.

Mapeo: EURO_FUAS city_code → Eurostat Urban Audit code
  Incluye los principales ~40 ciudades europeas.
  Las ciudades sin mapeo se escriben como Population=None.

Output: DatosProcesados/population.csv
Columnas: City, Country, Year, Population, Source

  Source: URBAN_AUDIT | NUTS3_ESTIMATE | NONE

Años: 2000-2024 (anual)

Ejecución:
  python population_extract.py
"""

import io
import gzip
import csv
import requests
import config
from pathlib import Path

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
START_YEAR = 2000
END_YEAR   = 2024

URL_URB  = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "urb_cpop1?format=TSV&compressed=true"
)
URL_NUTS3 = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "demo_r_pjanaggr3?format=TSV&compressed=true"
)

RAW_DIR    = config.DATA_DIR / "raw_eurostat"
OUTPUT_CSV = config.INPUT_DIR_PROCESSED / "population.csv"
CSV_COLUMNS = ["City", "Country", "Year", "Population", "Source"]

# ==============================================================================
# MAPEO EURO_FUAS → URBAN AUDIT CODE
# ------------------------------------------------------------------------------
# Formato Urban Audit: ISO2 + número secuencial (3 dígitos, sin padding en GR=EL)
# + sufijo 'C' (core city) o 'K' (FUA) — usamos 'C' para la ciudad núcleo.
# Las ciudades marcadas None no tienen código Urban Audit fiable; se usa NUTS-3.
# ==============================================================================
URBAN_AUDIT_CODES = {
    # --- ESPAÑA ---
    "Madrid_ES":               "ES001C",
    "Barcelona_ES":            "ES002C",
    "Valencia_ES":             "ES003C",
    "Sevilla_ES":              "ES004C",
    "Bilbao_ES":               "ES007C",
    "Malaga_ES":               "ES006C",
    "Zaragoza_ES":             "ES005C",
    "Murcia_ES":               "ES009C",
    "Palma_ES":                "ES010C",
    "LasPalmas_ES":            "ES008C",
    "Alicante_ES":             "ES013C",
    "Cordoba_ES":              "ES014C",
    "Valladolid_ES":           "ES011C",
    "Vigo_ES":                 "ES012C",
    "Gijon_ES":                "ES015C",
    "Coruna_ES":               "ES016C",
    "Vitoria_ES":              "ES020C",
    "Granada_ES":              "ES017C",
    "Oviedo_ES":               "ES015C",
    "SantaCruzTenerife_ES":    "ES018C",
    "Pamplona_ES":             "ES019C",
    "Almeria_ES":              None,
    "SanSebastian_ES":         "ES021C",
    "Santander_ES":            "ES022C",
    "Burgos_ES":               "ES026C",
    "Castellon_ES":            "ES035C",

    # --- PORTUGAL ---
    "Lisbon_PT":               "PT001C",
    "Porto_PT":                "PT002C",
    "Braga_PT":                "PT003C",
    "Coimbra_PT":              "PT004C",
    "Funchal_PT":              "PT008C",
    "Setubal_PT":              None,
    "Faro_PT":                 "PT005C",
    "Aveiro_PT":               "PT006C",
    "Viseu_PT":                "PT007C",
    "Evora_PT":                "PT009C",

    # --- FRANCIA ---
    "Paris_FR":                "FR001C",
    "Lyon_FR":                 "FR002C",
    "Marseille_FR":            "FR003C",
    "Toulouse_FR":             "FR005C",
    "Lille_FR":                "FR004C",
    "Bordeaux_FR":             "FR006C",
    "Nice_FR":                 "FR007C",
    "Nantes_FR":               "FR008C",
    "Strasbourg_FR":           "FR009C",
    "Rennes_FR":               "FR010C",
    "Grenoble_FR":             "FR011C",
    "Rouen_FR":                "FR012C",
    "Montpellier_FR":          "FR014C",
    "Toulon_FR":               "FR013C",
    "Lens_FR":                 "FR015C",
    "Nancy_FR":                "FR017C",
    "Metz_FR":                 "FR019C",
    "Tours_FR":                "FR018C",
    "ClermontFerrand_FR":      "FR020C",
    "Orleans_FR":              "FR022C",
    "Mulhouse_FR":             "FR029C",
    "Caen_FR":                 "FR023C",
    "Angers_FR":               "FR025C",
    "Dijon_FR":                "FR024C",
    "Brest_FR":                "FR030C",
    "LeHavre_FR":              "FR034C",

    # --- ITALIA ---
    "Rome_IT":                 "IT001C",
    "Milan_IT":                "IT002C",
    "Naples_IT":               "IT003C",
    "Turin_IT":                "IT004C",
    "Palermo_IT":              "IT005C",
    "Genoa_IT":                "IT006C",
    "Bologna_IT":              "IT009C",
    "Florence_IT":             "IT007C",
    "Bari_IT":                 "IT008C",
    "Catania_IT":              "IT010C",
    "Venice_IT":               "IT011C",
    "Verona_IT":               "IT012C",
    "Messina_IT":              "IT015C",
    "Padua_IT":                "IT013C",
    "Trieste_IT":              "IT016C",
    "Brescia_IT":              "IT018C",
    "Taranto_IT":              "IT017C",
    "Prato_IT":                None,
    "Modena_IT":               "IT020C",
    "Parma_IT":                "IT021C",
    "Perugia_IT":              "IT023C",
    "Livorno_IT":              "IT022C",
    "Cagliari_IT":             "IT014C",
    "Foggia_IT":               "IT025C",

    # --- ALEMANIA ---
    "Berlin_DE":               "DE001C",
    "Munich_DE":               "DE003C",
    "Hamburg_DE":              "DE002C",
    "Frankfurt_DE":            "DE005C",
    "Cologne_DE":              "DE004C",
    "Stuttgart_DE":            "DE006C",
    "Dusseldorf_DE":           "DE009C",
    "Leipzig_DE":              "DE012C",
    "Dortmund_DE":             "DE008C",
    "Essen_DE":                None,
    "Bremen_DE":               "DE010C",
    "Dresden_DE":              "DE011C",
    "Hanover_DE":              "DE013C",
    "Nuremberg_DE":            "DE014C",
    "Duisburg_DE":             None,
    "Bochum_DE":               None,
    "Wuppertal_DE":            "DE024C",
    "Bielefeld_DE":            "DE015C",
    "Bonn_DE":                 "DE021C",
    "Munster_DE":              "DE030C",
    "Karlsruhe_DE":            "DE017C",
    "Mannheim_DE":             "DE016C",
    "Augsburg_DE":             "DE020C",
    "Wiesbaden_DE":            "DE023C",
    "Gelsenkirchen_DE":        None,
    "Monchengladbach_DE":      "DE028C",
    "Braunschweig_DE":         "DE027C",
    "Chemnitz_DE":             "DE022C",
    "Kiel_DE":                 "DE031C",
    "Aachen_DE":               "DE018C",
    "Halle_DE":                "DE033C",
    "Magdeburg_DE":            "DE032C",
    "Freiburg_DE":             "DE036C",
    "Lubeck_DE":               "DE039C",
    "Erfurt_DE":               "DE034C",
    "Rostock_DE":              "DE038C",
    "Mainz_DE":                "DE035C",
    "Kassel_DE":               "DE037C",

    # --- REINO UNIDO (post-Brexit: cobertura limitada en Eurostat) ---
    "London_UK":               "UK001C",
    "Birmingham_UK":           "UK002C",
    "Manchester_UK":           "UK003C",
    "Glasgow_UK":              "UK004C",
    "Leeds_UK":                "UK005C",
    "Liverpool_UK":            "UK006C",
    "Newcastle_UK":            "UK007C",
    "Sheffield_UK":            "UK008C",
    "Bristol_UK":              "UK009C",
    "Belfast_UK":              "UK013C",
    "Edinburgh_UK":            "UK011C",
    "Cardiff_UK":              "UK010C",
    "Leicester_UK":            "UK014C",
    "Coventry_UK":             "UK016C",
    "Nottingham_UK":           "UK012C",
    "Southampton_UK":          "UK018C",
    "Bradford_UK":             None,
    "Hull_UK":                 "UK017C",
    "Stoke_UK":                "UK020C",
    "Wolverhampton_UK":        None,
    "Plymouth_UK":             "UK023C",
    "Derby_UK":                "UK022C",
    "Aberdeen_UK":             "UK019C",
    "Brighton_UK":             "UK021C",
    "Portsmouth_UK":           None,
    "Norwich_UK":              "UK025C",

    # --- PAÍSES BAJOS ---
    "Amsterdam_NL":            "NL002C",
    "Rotterdam_NL":            "NL003C",
    "TheHague_NL":             "NL001C",
    "Utrecht_NL":              "NL004C",
    "Eindhoven_NL":            "NL005C",
    "Tilburg_NL":              "NL006C",
    "Groningen_NL":            "NL007C",
    "Almere_NL":               None,
    "Breda_NL":                "NL010C",
    "Nijmegen_NL":             "NL009C",
    "Enschede_NL":             "NL008C",
    "Apeldoorn_NL":            "NL011C",
    "Haarlem_NL":              None,
    "Arnhem_NL":               None,

    # --- BÉLGICA ---
    "Brussels_BE":             "BE001C",
    "Antwerp_BE":              "BE002C",
    "Ghent_BE":                "BE003C",
    "Charleroi_BE":            "BE004C",
    "Liege_BE":                "BE005C",
    "Bruges_BE":               "BE006C",
    "Namur_BE":                "BE007C",
    "Mons_BE":                 "BE008C",
    "Aalst_BE":                None,
    "Leuven_BE":               None,

    # --- POLONIA ---
    "Warsaw_PL":               "PL001C",
    "Krakow_PL":               "PL002C",
    "Lodz_PL":                 "PL004C",
    "Wroclaw_PL":              "PL005C",
    "Poznan_PL":               "PL006C",
    "Gdansk_PL":               "PL003C",
    "Szczecin_PL":             "PL007C",
    "Bydgoszcz_PL":            "PL008C",
    "Lublin_PL":               "PL009C",
    "Katowice_PL":             "PL010C",
    "Bialystok_PL":            "PL011C",
    "Radom_PL":                "PL013C",
    "Gdynia_PL":               None,
    "Czestochowa_PL":          "PL012C",
    "Sosnowiec_PL":            None,
    "Torun_PL":                None,
    "Kielce_PL":               "PL014C",
    "Rzeszow_PL":              "PL015C",

    # --- RUMANÍA ---
    "Bucharest_RO":            "RO001C",
    "Cluj_RO":                 "RO002C",
    "Timisoara_RO":            "RO003C",
    "Iasi_RO":                 "RO006C",
    "Constanta_RO":            "RO005C",
    "Craiova_RO":              "RO004C",
    "Brasov_RO":               "RO007C",
    "Galati_RO":               "RO008C",
    "Ploiesti_RO":             "RO009C",
    "Oradea_RO":               "RO010C",

    # --- REPÚBLICA CHECA ---
    "Prague_CZ":               "CZ001C",
    "Brno_CZ":                 "CZ002C",
    "Ostrava_CZ":              "CZ003C",
    "Plzen_CZ":                "CZ004C",
    "Liberec_CZ":              "CZ005C",
    "Olomouc_CZ":              "CZ006C",
    "CeskeBudejovice_CZ":      "CZ008C",
    "HradecKralove_CZ":        "CZ009C",
    "UstiNadLabem_CZ":         "CZ007C",
    "Pardubice_CZ":            None,

    # --- HUNGRÍA ---
    "Budapest_HU":             "HU001C",
    "Debrecen_HU":             "HU004C",
    "Szeged_HU":               "HU005C",
    "Miskolc_HU":              "HU002C",
    "Pecs_HU":                 "HU006C",
    "Gyor_HU":                 "HU003C",
    "Nyiregyhaza_HU":          "HU007C",
    "Kecskemet_HU":            "HU008C",
    "Szekesfehervar_HU":       "HU009C",
    "Szombathely_HU":          "HU010C",

    # --- AUSTRIA ---
    "Vienna_AT":               "AT001C",
    "Graz_AT":                 "AT002C",
    "Linz_AT":                 "AT003C",
    "Salzburg_AT":             "AT004C",
    "Innsbruck_AT":            "AT005C",
    "Klagenfurt_AT":           "AT006C",
    "Villach_AT":              None,
    "Wels_AT":                 None,
    "SanktPolten_AT":          None,
    "Dornbirn_AT":             None,

    # --- SUECIA ---
    "Stockholm_SE":            "SE001C",
    "Gothenburg_SE":           "SE002C",
    "Malmo_SE":                "SE003C",
    "Uppsala_SE":              "SE004C",
    "Vasteras_SE":             "SE005C",
    "Orebro_SE":               "SE006C",
    "Linkoping_SE":            "SE007C",
    "Helsingborg_SE":          None,
    "Jonkoping_SE":            "SE008C",
    "Norrkoping_SE":           None,

    # --- DINAMARCA ---
    "Copenhagen_DK":           "DK001C",
    "Aarhus_DK":               "DK002C",
    "Odense_DK":               "DK003C",
    "Aalborg_DK":              "DK004C",
    "Esbjerg_DK":              "DK005C",
    "Randers_DK":              None,
    "Kolding_DK":              "DK006C",
    "Horsens_DK":              "DK007C",
    "Vejle_DK":                None,
    "Roskilde_DK":             None,

    # --- FINLANDIA ---
    "Helsinki_FI":             "FI001C",
    "Espoo_FI":                None,
    "Tampere_FI":              "FI002C",
    "Vantaa_FI":               None,
    "Oulu_FI":                 "FI004C",
    "Turku_FI":                "FI003C",
    "Jyvaskyla_FI":            "FI005C",
    "Lahti_FI":                "FI006C",
    "Kuopio_FI":               "FI007C",
    "Pori_FI":                 "FI008C",

    # --- IRLANDA ---
    "Dublin_IE":               "IE001C",
    "Cork_IE":                 "IE002C",
    "Limerick_IE":             "IE003C",
    "Galway_IE":               "IE004C",
    "Waterford_IE":            "IE005C",
    "Drogheda_IE":             "IE006C",
    "Dundalk_IE":              "IE007C",
    "Navan_IE":                "IE008C",
    "Swords_IE":               None,
    "Bray_IE":                 None,

    # --- GRECIA (Eurostat usa EL para Grecia) ---
    "Athens_GR":               "EL001C",
    "Thessaloniki_GR":         "EL002C",
    "Patras_GR":               "EL003C",
    "Heraklion_GR":            "EL004C",
    "Larissa_GR":              "EL005C",
    "Volos_GR":                "EL006C",
    "Ioannina_GR":             "EL007C",
    "Trikala_GR":              None,
    "Chalcis_GR":              "EL008C",
    "Serres_GR":               "EL009C",

    # --- SUIZA (no EU/EEA Eurostat): generalmente sin cobertura Urban Audit ---
    "Zurich_CH":               None,
    "Geneva_CH":               None,
    "Basel_CH":                None,
    "Bern_CH":                 None,
    "Lausanne_CH":             None,
    "Lugano_CH":               None,
    "Lucerne_CH":              None,
    "StGallen_CH":             None,
    "Winterthur_CH":           None,
    "Biel_CH":                 None,

    # --- NORUEGA (EEA: cobertura parcial Urban Audit) ---
    "Oslo_NO":                 "NO001C",
    "Bergen_NO":               "NO002C",
    "Trondheim_NO":            "NO004C",
    "Stavanger_NO":            "NO003C",
    "Drammen_NO":              None,
    "Fredrikstad_NO":          "NO005C",
    "Kristiansand_NO":         "NO006C",
    "Sandnes_NO":              None,
    "Tromso_NO":               "NO007C",
    "Sarpsborg_NO":            None,
}


# ==============================================================================
# UTILIDADES DE DESCARGA
# ==============================================================================
def download_eurostat_tsv(url: str, filename: str):
    """
    Descarga y cachea un dataset Eurostat TSV comprimido.
    Devuelve el contenido como texto (str) para parsing manual.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    local_path = RAW_DIR / filename

    if local_path.exists():
        print(f"  [CACHE] {filename}")
        with open(local_path, "rb") as f:
            raw = f.read()
    else:
        print(f"  [WEB]   Descargando {filename} …")
        try:
            r = requests.get(url, timeout=120)
            r.raise_for_status()
            raw = r.content
            with open(local_path, "wb") as f:
                f.write(raw)
        except Exception as e:
            print(f"  [ERROR] Fallo en descarga {filename}: {e}")
            return None

    # Detectar si es gzip
    try:
        with gzip.open(io.BytesIO(raw), "rt", encoding="utf-8") as f:
            return f.read()
    except Exception:
        # No es gzip: texto plano
        return raw.decode("utf-8", errors="replace")


def parse_tsv_to_dict(tsv_text: str, geo_col_alias: str) -> dict:
    """
    Parsea el TSV de Eurostat en formato largo.
    Devuelve: {geo_code: {year: value_float_or_None}}

    El formato de la primera columna es:  'freq,indic_ur,geo\\time'
    Las columnas siguientes son años: '2023 ', '2022 ', …

    geo_col_alias: nombre esperado de la dimensión geográfica
    (ej: 'geo' para demo_r_pjanaggr3, 'cities' para urb_cpop1)
    """
    lines = tsv_text.strip().splitlines()
    if not lines:
        return {}

    header_raw = lines[0].split("\t")
    # Primera celda: "freq,indic_ur,cities\time" o similar
    dim_part   = header_raw[0].split("\\")[0]   # antes del backslash
    dim_names  = [d.strip() for d in dim_part.split(",")]
    year_cols  = [c.strip() for c in header_raw[1:]]

    # Identificar índice de la columna geográfica
    geo_idx = None
    for i, d in enumerate(dim_names):
        if d in (geo_col_alias, "geo", "cities", "metroreg", "nuts"):
            geo_idx = i
            break
    if geo_idx is None:
        # Intentar el último campo de dimensiones como fallback
        geo_idx = len(dim_names) - 1

    result = {}
    for line in lines[1:]:
        parts = line.split("\t")
        if not parts:
            continue
        dim_vals = [v.strip() for v in parts[0].split(",")]
        if len(dim_vals) <= geo_idx:
            continue
        geo_code = dim_vals[geo_idx].strip()
        year_vals = parts[1:]

        year_dict = {}
        for y_str, v_str in zip(year_cols, year_vals):
            try:
                year = int(y_str.strip())
            except ValueError:
                continue
            # Limpiar flags Eurostat: '34000 p' → 34000, ':' → None
            v_clean = v_str.strip().split(" ")[0].replace(":", "")
            if v_clean == "" or v_clean == "b" or v_clean == "e":
                year_dict[year] = None
                continue
            try:
                year_dict[year] = float(v_clean)
            except ValueError:
                year_dict[year] = None

        if geo_code:
            result[geo_code] = year_dict

    return result


# ==============================================================================
# RESOLUCIÓN POR CIUDAD
# ==============================================================================
def resolve_population(city: str, ua_code: str | None, nuts3_code: str | None,
                        urb_data: dict, nuts3_data: dict) -> list[tuple]:
    """
    Intenta obtener la serie de población anual (2000-2024) para una ciudad.

    Jerarquía:
    1. Urban Audit (urb_cpop1) — indic_ur=POP, código UAC
    2. NUTS-3 (demo_r_pjanaggr3) — suma todas las edades (TOTAL)
    3. None si no hay datos

    Devuelve lista de (City, Country, Year, Population, Source).
    """
    country = city.split("_")[-1]
    rows = []

    for year in range(START_YEAR, END_YEAR + 1):
        pop   = None
        src   = "NONE"

        # Nivel 1: Urban Audit
        if ua_code and ua_code in urb_data:
            pop_val = urb_data[ua_code].get(year)
            if pop_val is not None:
                pop = int(pop_val)
                src = "URBAN_AUDIT"

        # Nivel 2: NUTS-3 fallback
        if pop is None and nuts3_code and nuts3_code in nuts3_data:
            pop_val = nuts3_data[nuts3_code].get(year)
            if pop_val is not None:
                pop = int(pop_val)
                src = "NUTS3_ESTIMATE"

        rows.append((city, country, year, pop, src))

    return rows


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    print("=" * 60)
    print("  GTP — EXTRACCIÓN POBLACIÓN FUA (Urban Audit + NUTS-3)")
    print(f"  Fuente 1: urb_cpop1 (Urban Audit POP)")
    print(f"  Fuente 2: demo_r_pjanaggr3 (NUTS-3 fallback)")
    print(f"  Años:     {START_YEAR}-{END_YEAR}")
    print(f"  Ciudades: {len(config.EURO_FUAS)}")
    print(f"  Output:   {OUTPUT_CSV}")
    print("=" * 60)

    # --- 1. Descargar datasets ---
    print("\n[1/3] Descargando datos Eurostat …")
    urb_tsv   = download_eurostat_tsv(URL_URB,   "urb_cpop1.tsv.gz")
    nuts3_tsv = download_eurostat_tsv(URL_NUTS3,  "demo_r_pjanaggr3.tsv.gz")

    # --- 2. Parsear ---
    print("\n[2/3] Parseando TSV …")
    urb_data   = {}
    nuts3_data = {}

    if urb_tsv:
        urb_data = parse_tsv_to_dict(urb_tsv, geo_col_alias="cities")
        print(f"  Urban Audit: {len(urb_data):,} geo-codes cargados")
    else:
        print("  [WARN] Sin datos Urban Audit; se usará solo NUTS-3")

    if nuts3_tsv:
        nuts3_data = parse_tsv_to_dict(nuts3_tsv, geo_col_alias="geo")
        print(f"  NUTS-3:      {len(nuts3_data):,} geo-codes cargados")
    else:
        print("  [WARN] Sin datos NUTS-3")

    # --- 3. Resolver por ciudad y escribir CSV ---
    print(f"\n[3/3] Resolviendo población para {len(config.EURO_FUAS)} ciudades …")
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Construir índice rápido de NUTS-3 desde config.EUROSTAT_CODES
    nuts3_lookup = {
        city: codes[1]
        for city, codes in config.EUROSTAT_CODES.items()
        if len(codes) >= 2
    }

    ok_urban = ok_nuts3 = ok_none = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)

        for city in config.EURO_FUAS:
            ua_code    = URBAN_AUDIT_CODES.get(city)          # puede ser None
            nuts3_code = nuts3_lookup.get(city)                # puede ser None

            rows = resolve_population(
                city, ua_code, nuts3_code, urb_data, nuts3_data
            )

            # Contabilizar fuente dominante (la del primer año con dato)
            dominant_src = next((r[4] for r in rows if r[3] is not None), "NONE")
            if dominant_src == "URBAN_AUDIT":
                ok_urban += 1
            elif dominant_src == "NUTS3_ESTIMATE":
                ok_nuts3 += 1
            else:
                ok_none  += 1
                print(f"  [WARN] Sin datos de población: {city}")

            for row in rows:
                writer.writerow(row)

    total = len(config.EURO_FUAS)
    print()
    print("=" * 60)
    print(f"  FIN — Población extraída")
    print(f"  Ciudades con Urban Audit:   {ok_urban:,}/{total}")
    print(f"  Ciudades con NUTS-3 estim.: {ok_nuts3:,}/{total}")
    print(f"  Ciudades sin datos:         {ok_none:,}/{total}")
    print(f"  Filas totales escritas:     {total * (END_YEAR - START_YEAR + 1):,}")
    print(f"  Output: {OUTPUT_CSV}")
    print("=" * 60)


if __name__ == "__main__":
    main()
