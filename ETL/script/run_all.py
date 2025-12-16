import subprocess # Permite ejecutar conmandos de terminal desde Python (crea subprocesos)
import os #Permite interactuar con el Sistema Operativo (rutas de archivos, variables de entorno)
from datetime import datetime # Necesaria para medir el rendimiento del proceso ETL

# ======================================================
# BLOQUE 1: Configuración del Pipeline (Flujo de Trobajo)
# ======================================================

# Definimos una lista de diccionarios. Cada elemento es una "tarea" que el orquestador debe ejecutar.
# Esta estructura permite escalabilidad: añadir un nuevo paso es tan facil como agregar una linea aqui.

EURO_FUAS = {
    # --- ESPAÑA (ES) ---
    "Madrid_ES": (40.4168, -3.7038), "Barcelona_ES": (41.3851, 2.1734),
    "Valencia_ES": (39.4699, -0.3763), "Sevilla_ES": (37.3891, -5.9845),
    "Bilbao_ES": (43.2630, -2.9350), "Malaga_ES": (36.7212, -4.4217),
    "Zaragoza_ES": (41.6488, -0.8891), "Murcia_ES": (37.9922, -1.1307),
    "Palma_ES": (39.5696, 2.6502), "LasPalmas_ES": (28.1235, -15.4363),
    "Alicante_ES": (38.3452, -0.4810), "Cordoba_ES": (37.8882, -4.7794),
    "Valladolid_ES": (41.6523, -4.7245), "Vigo_ES": (42.2406, -8.7207),
    "Gijon_ES": (43.5357, -5.6615), "Coruna_ES": (43.3623, -8.4115),
    "Vitoria_ES": (42.8467, -2.6716), "Granada_ES": (37.1773, -3.5986),
    "Oviedo_ES": (43.3619, -5.8494), "SantaCruzTenerife_ES": (28.4636, -16.2518),
    "Pamplona_ES": (42.8125, -1.6458), "Almeria_ES": (36.8340, -2.4637),
    "SanSebastian_ES": (43.3183, -1.9812), "Burgos_ES": (42.3440, -3.6969),
    "Santander_ES": (43.4623, -3.8099), "Castellon_ES": (39.9864, -0.0513),

    # --- PORTUGAL (PT) ---
    "Lisbon_PT": (38.7223, -9.1393), "Porto_PT": (41.1579, -8.6291),
    "Braga_PT": (41.5454, -8.4265), "Coimbra_PT": (40.2033, -8.4103),
    "Funchal_PT": (32.6609, -16.9085), "Setubal_PT": (38.5244, -8.8882),
    "Faro_PT": (37.0194, -7.9304), "Viseu_PT": (40.6566, -7.9125),
    "Aveiro_PT": (40.6405, -8.6538), "Evora_PT": (38.5714, -7.9135),

    # --- FRANCIA (FR) ---
    "Paris_FR": (48.8566, 2.3522), "Lyon_FR": (45.7640, 4.8357),
    "Marseille_FR": (43.2965, 5.3698), "Toulouse_FR": (43.6047, 1.4442),
    "Lille_FR": (50.6292, 3.0573), "Bordeaux_FR": (44.8378, -0.5792),
    "Nice_FR": (43.7102, 7.2620), "Nantes_FR": (47.2184, -1.5536),
    "Strasbourg_FR": (48.5734, 7.7521), "Rennes_FR": (48.1173, -1.6778),
    "Grenoble_FR": (45.1885, 5.7245), "Rouen_FR": (49.4432, 1.0999),
    "Montpellier_FR": (43.6108, 3.8767), "Toulon_FR": (43.1242, 5.9280),
    "Lens_FR": (50.4292, 2.8310), "Nancy_FR": (48.6921, 6.1844),
    "Metz_FR": (49.1193, 6.1757), "Tours_FR": (47.3941, 0.6848),
    "ClermontFerrand_FR": (45.7772, 3.0870), "Orleans_FR": (47.9030, 1.9093),
    "Mulhouse_FR": (47.7508, 7.3359), "Caen_FR": (49.1829, -0.3707),
    "Angers_FR": (47.4784, -0.5632), "Dijon_FR": (47.3220, 5.0415),
    "Brest_FR": (48.3904, -4.4861), "LeHavre_FR": (49.4944, 0.1079),

    # --- ITALIA (IT) ---
    "Rome_IT": (41.9028, 12.4964), "Milan_IT": (45.4642, 9.1900),
    "Naples_IT": (40.8518, 14.2681), "Turin_IT": (45.0703, 7.6869),
    "Palermo_IT": (38.1157, 13.3615), "Genoa_IT": (44.4056, 8.9463),
    "Bologna_IT": (44.4949, 11.3426), "Florence_IT": (43.7696, 11.2558),
    "Bari_IT": (41.1171, 16.8719), "Catania_IT": (37.5079, 15.0830),
    "Venice_IT": (45.4408, 12.3155), "Verona_IT": (45.4384, 10.9916),
    "Messina_IT": (38.1938, 15.5540), "Padua_IT": (45.4064, 11.8768),
    "Trieste_IT": (45.6495, 13.7768), "Brescia_IT": (45.5416, 10.2118),
    "Taranto_IT": (40.4644, 17.2470), "Prato_IT": (43.8777, 11.1022),
    "Modena_IT": (44.6471, 10.9252), "Parma_IT": (44.8015, 10.3279),
    "Perugia_IT": (43.1107, 12.3908), "Livorno_IT": (43.5485, 10.3106),
    "Cagliari_IT": (39.2238, 9.1217), "Foggia_IT": (41.4610, 15.5450),

    # --- ALEMANIA (DE) ---
    "Berlin_DE": (52.5200, 13.4050), "Munich_DE": (48.1351, 11.5820),
    "Hamburg_DE": (53.5511, 9.9937), "Frankfurt_DE": (50.1109, 8.6821),
    "Cologne_DE": (50.9375, 6.9603), "Stuttgart_DE": (48.7758, 9.1829),
    "Dusseldorf_DE": (51.2277, 6.7735), "Leipzig_DE": (51.3397, 12.3731),
    "Dortmund_DE": (51.5136, 7.4653), "Essen_DE": (51.4556, 7.0116),
    "Bremen_DE": (53.0793, 8.8017), "Dresden_DE": (51.0504, 13.7372),
    "Hanover_DE": (52.3759, 9.7320), "Nuremberg_DE": (49.4521, 11.0767),
    "Duisburg_DE": (51.4344, 6.7623), "Bochum_DE": (51.4818, 7.2162),
    "Wuppertal_DE": (51.2562, 7.1508), "Bielefeld_DE": (52.0302, 8.5325),
    "Bonn_DE": (50.7374, 7.0982), "Munster_DE": (51.9607, 7.6261),
    "Karlsruhe_DE": (49.0069, 8.4037), "Mannheim_DE": (49.4875, 8.4660),
    "Augsburg_DE": (48.3705, 10.8978), "Wiesbaden_DE": (50.0782, 8.2398),
    "Gelsenkirchen_DE": (51.5175, 7.0857), "Monchengladbach_DE": (51.1854, 6.4417),
    "Braunschweig_DE": (52.2689, 10.5268), "Chemnitz_DE": (50.8278, 12.9214),
    "Kiel_DE": (54.3233, 10.1228), "Aachen_DE": (50.7753, 6.0839),
    "Halle_DE": (51.4828, 11.9730), "Magdeburg_DE": (52.1205, 11.6276),
    "Freiburg_DE": (47.9990, 7.8421), "Lubeck_DE": (53.8655, 10.6866),
    "Erfurt_DE": (50.9848, 11.0299), "Rostock_DE": (54.0924, 12.0991),
    "Mainz_DE": (49.9929, 8.2473), "Kassel_DE": (51.3127, 9.4797),

    # --- REINO UNIDO (UK) ---
    "London_UK": (51.5074, -0.1278), "Birmingham_UK": (52.4862, -1.8904),
    "Manchester_UK": (53.4808, -2.2426), "Glasgow_UK": (55.8642, -4.2518),
    "Leeds_UK": (53.8008, -1.5491), "Liverpool_UK": (53.4084, -2.9916),
    "Newcastle_UK": (54.9783, -1.6178), "Sheffield_UK": (53.3811, -1.4701),
    "Bristol_UK": (51.4545, -2.5879), "Belfast_UK": (54.5973, -5.9301),
    "Edinburgh_UK": (55.9533, -3.1883), "Cardiff_UK": (51.4816, -3.1791),
    "Leicester_UK": (52.6369, -1.1398), "Coventry_UK": (52.4068, -1.5197),
    "Nottingham_UK": (52.9548, -1.1581), "Southampton_UK": (50.9097, -1.4044),
    "Bradford_UK": (53.7960, -1.7594), "Hull_UK": (53.7457, -0.3367),
    "Stoke_UK": (53.0027, -2.1794), "Wolverhampton_UK": (52.5862, -2.1288),
    "Plymouth_UK": (50.3755, -4.1427), "Derby_UK": (52.9225, -1.4746),
    "Aberdeen_UK": (57.1497, -2.0943), "Brighton_UK": (50.8225, -0.1372),
    "Portsmouth_UK": (50.8198, -1.0880), "Norwich_UK": (52.6309, 1.2974),

    # --- PAÍSES BAJOS (NL) ---
    "Amsterdam_NL": (52.3676, 4.9041), "Rotterdam_NL": (51.9244, 4.4777),
    "TheHague_NL": (52.0705, 4.3007), "Utrecht_NL": (52.0907, 5.1214),
    "Eindhoven_NL": (51.4416, 5.4697), "Tilburg_NL": (51.5555, 5.0913),
    "Groningen_NL": (53.2194, 6.5665), "Almere_NL": (52.3508, 5.2647),
    "Breda_NL": (51.5719, 4.7683), "Nijmegen_NL": (51.8126, 5.8372),
    "Enschede_NL": (52.2215, 6.8937), "Apeldoorn_NL": (52.2112, 5.9699),
    "Haarlem_NL": (52.3874, 4.6462), "Arnhem_NL": (51.9851, 5.8987),

    # --- BÉLGICA (BE) ---
    "Brussels_BE": (50.8503, 4.3517), "Antwerp_BE": (51.2194, 4.4025),
    "Ghent_BE": (51.0543, 3.7174), "Charleroi_BE": (50.4101, 4.4446),
    "Liege_BE": (50.6326, 5.5797), "Bruges_BE": (51.2093, 3.2247),
    "Namur_BE": (50.4674, 4.8720), "Leuven_BE": (50.8798, 4.7005),
    "Mons_BE": (50.4542, 3.9567), "Aalst_BE": (50.9383, 4.0392),

    # --- POLONIA (PL) ---
    "Warsaw_PL": (52.2297, 21.0122), "Krakow_PL": (50.0647, 19.9450),
    "Lodz_PL": (51.7592, 19.4560), "Wroclaw_PL": (51.1079, 17.0385),
    "Poznan_PL": (52.4064, 16.9252), "Gdansk_PL": (54.3520, 18.6466),
    "Szczecin_PL": (53.4285, 14.5528), "Bydgoszcz_PL": (53.1235, 18.0084),
    "Lublin_PL": (51.2465, 22.5684), "Katowice_PL": (50.2649, 19.0238),
    "Bialystok_PL": (53.1325, 23.1688), "Gdynia_PL": (54.5189, 18.5305),
    "Czestochowa_PL": (50.8118, 19.1203), "Radom_PL": (51.4027, 21.1471),
    "Sosnowiec_PL": (50.2863, 19.1041), "Torun_PL": (53.0138, 18.5984),
    "Kielce_PL": (50.8661, 20.6286), "Rzeszow_PL": (50.0412, 21.9991),

    # --- RUMANÍA (RO) ---
    "Bucharest_RO": (44.4268, 26.1025), "Cluj_RO": (46.7712, 23.6236),
    "Timisoara_RO": (45.7489, 21.2087), "Iasi_RO": (47.1585, 27.6014),
    "Constanta_RO": (44.1792, 28.6121), "Craiova_RO": (44.3180, 23.7949),
    "Brasov_RO": (45.6427, 25.5887), "Galati_RO": (45.4353, 28.0080),
    "Ploiesti_RO": (44.9367, 26.0129), "Oradea_RO": (47.0465, 21.9189),

    # --- REPÚBLICA CHECA (CZ) ---
    "Prague_CZ": (50.0755, 14.4378), "Brno_CZ": (49.1951, 16.6068),
    "Ostrava_CZ": (49.8209, 18.2625), "Plzen_CZ": (49.7384, 13.3736),
    "Liberec_CZ": (50.7663, 15.0543), "Olomouc_CZ": (49.5938, 17.2509),
    "CeskeBudejovice_CZ": (48.9745, 14.4743), "HradecKralove_CZ": (50.2104, 15.8252),
    "UstiNadLabem_CZ": (50.6611, 14.0524), "Pardubice_CZ": (50.0343, 15.7812),

    # --- HUNGRÍA (HU) ---
    "Budapest_HU": (47.4979, 19.0402), "Debrecen_HU": (47.5316, 21.6273),
    "Szeged_HU": (46.2530, 20.1414), "Miskolc_HU": (48.1000, 20.7833),
    "Pecs_HU": (46.0727, 18.2323), "Gyor_HU": (47.6875, 17.6504),
    "Nyiregyhaza_HU": (47.9554, 21.7167), "Kecskemet_HU": (46.9075, 19.6917),
    "Szekesfehervar_HU": (47.1860, 18.4221), "Szombathely_HU": (47.2307, 16.6218),

    # --- AUSTRIA (AT) ---
    "Vienna_AT": (48.2082, 16.3738), "Graz_AT": (47.0707, 15.4395),
    "Linz_AT": (48.3069, 14.2858), "Salzburg_AT": (47.8095, 13.0550),
    "Innsbruck_AT": (47.2692, 11.4041), "Klagenfurt_AT": (46.6247, 14.3053),
    "Villach_AT": (46.6111, 13.8558), "Wels_AT": (48.1575, 14.0289),
    "SanktPolten_AT": (48.2038, 15.6325), "Dornbirn_AT": (47.4125, 9.7417),

    # --- SUECIA (SE) ---
    "Stockholm_SE": (59.3293, 18.0686), "Gothenburg_SE": (57.7089, 11.9746),
    "Malmo_SE": (55.6050, 13.0038), "Uppsala_SE": (59.8586, 17.6389),
    "Vasteras_SE": (59.6099, 16.5448), "Orebro_SE": (59.2753, 15.2134),
    "Linkoping_SE": (58.4109, 15.6216), "Helsingborg_SE": (56.0465, 12.6945),
    "Jonkoping_SE": (57.7826, 14.1618), "Norrkoping_SE": (58.5877, 16.1924),

    # --- DINAMARCA (DK) ---
    "Copenhagen_DK": (55.6761, 12.5683), "Aarhus_DK": (56.1629, 10.2039),
    "Odense_DK": (55.4038, 10.4024), "Aalborg_DK": (57.0488, 9.9217),
    "Esbjerg_DK": (55.4765, 8.4594), "Randers_DK": (56.4608, 10.0364),
    "Kolding_DK": (55.4959, 9.4731), "Horsens_DK": (55.8581, 9.8476),
    "Vejle_DK": (55.7093, 9.5357), "Roskilde_DK": (55.6415, 12.0804),

    # --- FINLANDIA (FI) ---
    "Helsinki_FI": (60.1699, 24.9384), "Espoo_FI": (60.2055, 24.6559),
    "Tampere_FI": (61.4978, 23.7610), "Vantaa_FI": (60.2934, 25.0378),
    "Oulu_FI": (65.0121, 25.4651), "Turku_FI": (60.4518, 22.2666),
    "Jyvaskyla_FI": (62.2426, 25.7473), "Lahti_FI": (60.9827, 25.6612),
    "Kuopio_FI": (62.8924, 27.6770), "Pori_FI": (61.4851, 21.7974),

    # --- IRLANDA (IE) ---
    "Dublin_IE": (53.3498, -6.2603), "Cork_IE": (51.8985, -8.4756),
    "Limerick_IE": (52.6680, -8.6305), "Galway_IE": (53.2707, -9.0568),
    "Waterford_IE": (52.2593, -7.1101), "Drogheda_IE": (53.7155, -6.3560),
    "Dundalk_IE": (54.0090, -6.4049), "Swords_IE": (53.4597, -6.2181),
    "Bray_IE": (53.2009, -6.1111), "Navan_IE": (53.6528, -6.6814),

    # --- GRECIA (GR) ---
    "Athens_GR": (37.9838, 23.7275), "Thessaloniki_GR": (40.6401, 22.9444),
    "Patras_GR": (38.2466, 21.7346), "Heraklion_GR": (35.3387, 25.1442),
    "Larissa_GR": (39.6390, 22.4191), "Volos_GR": (39.3621, 22.9422),
    "Ioannina_GR": (39.6650, 20.8537), "Trikala_GR": (39.5556, 21.7679),
    "Chalcis_GR": (38.4616, 23.5947), "Serres_GR": (41.0849, 23.5475),

    # --- SUIZA (CH) (No EU, pero relevante) ---
    "Zurich_CH": (47.3769, 8.5417), "Geneva_CH": (46.2044, 6.1432),
    "Basel_CH": (47.5596, 7.5886), "Bern_CH": (46.9480, 7.4474),
    "Lausanne_CH": (46.5197, 6.6323), "Lugano_CH": (46.0037, 8.9511),
    "Lucerne_CH": (47.0502, 8.3093), "StGallen_CH": (47.4245, 9.3767),
    "Winterthur_CH": (47.4999, 8.7287), "Biel_CH": (47.1368, 7.2468),

    # --- NORUEGA (NO) (EEA) ---
    "Oslo_NO": (59.9139, 10.7522), "Bergen_NO": (60.3913, 5.3221),
    "Trondheim_NO": (63.4305, 10.3951), "Stavanger_NO": (58.9690, 5.7331),
    "Drammen_NO": (59.7441, 10.2045), "Fredrikstad_NO": (59.2181, 10.9298),
    "Kristiansand_NO": (58.1599, 8.0182), "Sandnes_NO": (58.8524, 5.7352),
    "Tromso_NO": (69.6492, 18.9553), "Sarpsborg_NO": (59.2840, 11.1096),
}



SCRIPTS_GLOBALES = [

    # ---1. Datos Económicos (PIB, Eurostat, OECD, InvestEU, HRL)---
    # # ===========================================================

    # 1. PIB INE: Extrae datos macroeconómicos de España
    {"file": "pib_ine_extract.py", "args": []},

    # 2. Eurostat: Datos a nivel europeo para commparativa
    {"file": "eurostat_extract.py",
    "args": [
        "--datasets", "all",
        "--eager-merge",
        #"--filters", "time=2000:2024&geo=ES,ES300" #ES300 = madrid
        ]
    },


    # 3. OECD: Datos globales de desarrollo
    # Se configuran "penalties" (esperas) para evitar el error HTTP 429 (Too Many Requests)
    {"file": "OECD_extract.py",
    "args": [
        "--pause", "300",
        "--penalty-429", "300,600,1200"
        ]
    },

    # 4. InvestEU: Datos específicos de inversión en proyectos (clave para detectar financiación ambiental)
    # Se invectan variables de entorno (env) para controlar la agresividad de las peticiones
    {"file": "InvestEU_extract.py",
    "args": [
        "--what", "all"
        ],
    "env": {
        "INVESTEU_BASE_SLEEP": "300", # Espera base entre peticiones
        "INVESTEU_429_PENALTIES": "300,600,1200", # Penalización exponencial si nos bloquean
        "INVESTEU_TIMEOUT": "90", # Tiempo máximo de espera por respuesta antes de dar error
        "INVESTEU_MAX_RETRIES": "3" # Número de reintentos permitidos
        }
    },



    {
        "file": "YFinance_extract.py",
        "args": ["--what", "finance", "--out-dir", "data/finance"]
    },
]


def generar_tareas_ciudad(nombre_ciudad, lat, lon):
    """
    Crea la lista de scripts para descargar satélites de UNA ciudad.
    Calcula automáticamente el Bounding Box y el Polygon (WKT) basado en el punto central.
    """
    
    # 1. Definir el tamaño del área (Buffer)
    # 0.1 grados son aprox 11 km. Esto crea un cuadrado de ~22x22km alrededor del centro.
    delta = 0.1 
    
    # Calcular coordenadas extremas
    min_lon = lon - delta
    max_lon = lon + delta
    min_lat = lat - delta
    max_lat = lat + delta
    
    # A. Formato BBOX para HRL (min_lon, min_lat, max_lon, max_lat)
    bbox_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    
    # B. Formato WKT Polygon para Sentinel (Lon Lat, Lon Lat...)
    # Orden: Bottom-Left -> Top-Left -> Top-Right -> Bottom-Right -> Bottom-Left
    wkt_str = (f"POLYGON(({min_lon} {min_lat}, {min_lon} {max_lat}, "
            f"{max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))")

    # Carpeta de salida específica por ciudad
    out_base = f"data/{nombre_ciudad}"

    # Lista de tareas específicas para esta ciudad
    return [
        # --- 5. HRL (Capas Estáticas) ---
        {
            "file": "HRL_extract.py",
            "args": ["--service=tree_cover", "--layer=density", f"--bbox={bbox_str}", "--size=2048"]
        },
        {
            "file": "HRL_extract.py",
            "args": ["--service=impervious", "--layer=density", f"--bbox={bbox_str}", "--size=2048"]
        },

        # --- 6-8. SENTINEL-2 (Imágenes Visuales y Datos) ---
        {
            "file": "Sentinel-2_extract.py",
            "args": [
                "--aoi", "custom", "--wkt", wkt_str,
                "--out-dir", f"{out_base}/s2",
                "--asset", "tci", "--days-back", "365", "--max-downloads", "10", "--download"
            ]
        },
        {
            "file": "Sentinel-2_extract.py",
            "args": [
                "--aoi", "custom", "--wkt", wkt_str,
                "--out-dir", f"{out_base}/s2",
                "--asset", "scl", "--days-back", "365", "--max-downloads", "10", "--download"
            ]
        },
        {
            "file": "Sentinel-2_extract.py",
            "args": [
                "--aoi", "custom", "--wkt", wkt_str,
                "--out-dir", f"{out_base}/s2",
                "--bands", "B04,B08", "--max-downloads", "10", "--download"
            ]
        },

        # --- 8. SENTINEL-5P (Contaminación NO2) ---
        {
            "file": "Sentinel-5P_extract.py", # ¡Atención a la P mayúscula!
            "args": [
                "--aoi", "custom", "--wkt", wkt_str,
                "--out-dir", f"{out_base}/s5p",
                "--product", "L2__NO2___", "--days-back", "365", "--max-downloads", "10", "--download"
            ]
        }
    ]



# ======================================================
# BLOQUE 2: Motor de ejecución de scripts
# ======================================================

def ejecutar_script(script):
    """
    Ejecuta un script de Python como un subproceso independiente.
    Captura su salida (logs) en tiempo real y gestiona errores.
    """
    file = script["file"]
    args = script.get("args", [])
    env_vars = script.get("env", {}) # Obtiene variables de entorno extra si existen

    # Construcción de la ruta absoluta para evitar errores de "File Not Found"
    script_path = os.path.join("ETL", "script", file)

    # Verificación de seguridad: ¿Existe el archivo antes de lanzarlo?
    if not os.path.exists(script_path):
        print(f"⚠️ Script no encontrado: {script_path}")
        return False

    # Preparación del entorno: Copiamos el entorno actual y añadimos las variables personalizadas
    # Esto aísla la configuración de cada script (sandbox)
    env = os.environ.copy()
    env.update(env_vars)

    print(f"\n▶️ Ejecutando {file} {' '.join(args)}")
    if env_vars:
        print(f"   🌐 Variables de entorno: {', '.join(env_vars.keys())}\n")

    # Subprocess.Popen: Lanza el script.
    # stdout = subprocess.PIPE permite capturar lo que el scropt imprime para mostrarlo aqui
    process = subprocess.Popen(
        ["python", script_path] + args,
        env=env,
        stdout=subprocess.PIPE, # Capturamos la salida estándar (logs)
        stderr=subprocess.STDOUT, # Redirigimos errores a la salida estándar para verlos juntos
        text=True, # Decodifica los bytes a texto legible
    )

    # Bucle de lectura en tiempo real (Streaming de logs):
    # Esto es vital para procesos largos (como descargas) para saber que no se ha colgado.
    for line in process.stdout:
        print(f"   {line.strip()}")

    # Esperamos a que el subproceso termine
    process.wait()

    # Evaluación del resultado (Exit Code 0 significa éxito)
    if process.returncode == 0:
        print(f"✅ {file} completado correctamente.\n")
        return True
    else:
        print(f"❌ Error al ejecutar {file} (código {process.returncode}).\n")
        return False


# ======================================================
# BLOQUE 3: Main (Punto de entrada del script)
# ======================================================

def main():
    inicio = datetime.now()
    num_ciudades = len(EURO_FUAS)
    print(f"🚀 Inicio ETL GTP ({num_ciudades} Ciudades Europeas) — {inicio.strftime('%Y-%m-%d %H:%M:%S')}\n")
    print("==================================================================")

    completados = 0
    total_tareas = len(SCRIPTS_GLOBALES) + (num_ciudades * 6) # 6 scripts por ciudad aprox

    # 1. EJECUTAR TAREAS GLOBALES
    print("🌍 FASE 1: DATOS MACROECONÓMICOS (GLOBAL)")
    for script in SCRIPTS_GLOBALES:
        if ejecutar_script(script):
            completados += 1

    # 2. EJECUTAR TAREAS POR CIUDAD
    print("\n🛰️ FASE 2: OBSERVACIÓN TERRESTRE (POR CIUDAD)")
    
    # Iteramos sobre el diccionario EURO_FUAS
    for nombre_ciudad, coordenadas in EURO_FUAS.items():
        lat, lon = coordenadas
        print(f"\n---------------------------------------------------")
        print(f"🏙️ PROCESANDO: {nombre_ciudad} ({lat}, {lon})")
        print(f"---------------------------------------------------")
        
        # Generamos dinámicamente las tareas para esta ciudad
        tareas_ciudad = generar_tareas_ciudad(nombre_ciudad, lat, lon)
        
        for script in tareas_ciudad:
            if ejecutar_script(script):
                completados += 1

    # Cálculo de métricas finales
    fin = datetime.now()
    duracion = (fin - inicio).total_seconds() / 60
    
    print("==================================================================")
    print(f"\n🏁 ETL Finalizado: {completados} tareas completadas.")
    print(f"🕒 Duración total: {duracion:.1f} minutos\n")
    print("==================================================================")

if __name__ == "__main__":
    main()