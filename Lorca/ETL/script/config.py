from pathlib import Path

# --- CONFIGURACIÓN DE RUTAS ---
# BASE_DIR = Lorca/ETL/script/ (donde vive este archivo)
# DATA_DIR = Lorca/data/       (carpeta de datos unificada bajo Lorca/)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent.parent / "data"

# Rutas específicas de entrada y salida
INPUT_DIR_PROCESSED = DATA_DIR / "DatosProcesados"
OUTPUT_FILE_MASTER = DATA_DIR / "merge.csv"
INPUT_FILE_FINANCE = DATA_DIR / "finance_monthly_2006_2025.csv"

# --- DICCIONARIO MAESTRO DE CIUDADES (EURO_FUAS) ---
# Centralizamos aquí las coordenadas para no tener copias dispersas en varios scripts.
# Formato: "Nombre_Ciudad_Pais": (Latitud, Longitud)
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

# ==============================================================================
# DICCIONARIO EUROSTAT (CÓDIGOS METRO Y NUTS3)
# ==============================================================================
# Mapeo: Ciudad -> [Código Área Metropolitana, Código Región NUTS3]
# Usamos esto para buscar el dato más preciso disponible.
EUROSTAT_CODES = {
    # --- ESPAÑA ---
    "Madrid_ES": ["ES001", "ES300"], "Barcelona_ES": ["ES002", "ES511"], 
    "Valencia_ES": ["ES003", "ES523"], "Sevilla_ES": ["ES004", "ES618"], 
    "Zaragoza_ES": ["ES005", "ES243"], "Malaga_ES": ["ES006", "ES617"], 
    "Bilbao_ES": ["ES007", "ES213"], "Murcia_ES": ["ES009", "ES620"], 
    "Palma_ES": ["ES010", "ES532"], "LasPalmas_ES": ["ES008", "ES701"], 
    "Alicante_ES": ["ES013", "ES521"], "Cordoba_ES": ["ES014", "ES613"], 
    "Valladolid_ES": ["ES011", "ES418"], "Vigo_ES": ["ES012", "ES114"], 
    "Gijon_ES": ["ES015", "ES120"], "Coruna_ES": ["ES016", "ES111"], 
    "Vitoria_ES": ["ES020", "ES211"], "Granada_ES": ["ES017", "ES614"],
    "Oviedo_ES": ["ES015", "ES120"], "SantaCruzTenerife_ES": ["ES018", "ES702"], 
    "Pamplona_ES": ["ES019", "ES220"], "Almeria_ES": ["ES503", "ES611"], 
    "SanSebastian_ES": ["ES021", "ES212"], "Santander_ES": ["ES022", "ES130"], 
    "Burgos_ES": ["ES026", "ES412"], "Castellon_ES": ["ES035", "ES522"], 

    # --- PORTUGAL ---
    "Lisbon_PT": ["PT001", "PT170"], "Porto_PT": ["PT002", "PT11A"], 
    "Braga_PT": ["PT003", "PT112"], "Coimbra_PT": ["PT004", "PT16E"], 
    "Funchal_PT": ["PT008", "PT300"], "Setubal_PT": ["PT001", "PT170"], 
    "Faro_PT": ["PT005", "PT150"], "Aveiro_PT": ["PT006", "PT16D"], 
    "Viseu_PT": ["PT007", "PT16G"], "Evora_PT": ["PT009", "PT187"],

    # --- FRANCIA ---
    "Paris_FR": ["FR001", "FR101"], "Lyon_FR": ["FR002", "FRK26"], 
    "Marseille_FR": ["FR003", "FRL04"], "Toulouse_FR": ["FR005", "FRJ23"], 
    "Lille_FR": ["FR004", "FRE11"], "Bordeaux_FR": ["FR006", "FRI12"], 
    "Nice_FR": ["FR007", "FRL03"], "Nantes_FR": ["FR008", "FRG01"], 
    "Strasbourg_FR": ["FR009", "FRF11"], "Rennes_FR": ["FR010", "FRH03"], 
    "Grenoble_FR": ["FR011", "FRK24"], "Rouen_FR": ["FR012", "FRD22"], 
    "Montpellier_FR": ["FR014", "FRJ13"], "Toulon_FR": ["FR013", "FRL05"], 
    "Lens_FR": ["FR015", "FRE12"], "Nancy_FR": ["FR017", "FRF31"], 
    "Metz_FR": ["FR019", "FRF33"], "Tours_FR": ["FR018", "FRB04"],
    "ClermontFerrand_FR": ["FR020", "FRK14"], "Orleans_FR": ["FR022", "FRB06"], 
    "Mulhouse_FR": ["FR029", "FRF12"], "Caen_FR": ["FR023", "FRD11"], 
    "Angers_FR": ["FR025", "FRG02"], "Dijon_FR": ["FR024", "FRC11"], 
    "Brest_FR": ["FR030", "FRH02"], "LeHavre_FR": ["FR034", "FRD22"], 

    # --- ITALIA ---
    "Rome_IT": ["IT001", "ITI43"], "Milan_IT": ["IT002", "ITC4C"], 
    "Naples_IT": ["IT003", "ITF33"], "Turin_IT": ["IT004", "ITC11"], 
    "Palermo_IT": ["IT005", "ITG12"], "Genoa_IT": ["IT006", "ITC33"], 
    "Bologna_IT": ["IT009", "ITH55"], "Florence_IT": ["IT007", "ITI14"], 
    "Bari_IT": ["IT008", "ITF47"], "Catania_IT": ["IT010", "ITG17"], 
    "Venice_IT": ["IT011", "ITH35"], "Verona_IT": ["IT012", "ITH31"], 
    "Messina_IT": ["IT015", "ITG13"], "Padua_IT": ["IT013", "ITH36"], 
    "Trieste_IT": ["IT016", "ITH44"], "Brescia_IT": ["IT018", "ITC47"], 
    "Taranto_IT": ["IT017", "ITF43"], "Prato_IT": ["IT007", "ITI15"], 
    "Modena_IT": ["IT020", "ITH54"], "Parma_IT": ["IT021", "ITH52"], 
    "Perugia_IT": ["IT023", "ITI21"], "Livorno_IT": ["IT022", "ITI16"], 
    "Cagliari_IT": ["IT014", "ITG27"], "Foggia_IT": ["IT025", "ITF46"], 

    # --- ALEMANIA ---
    "Berlin_DE": ["DE001", "DE300"], "Hamburg_DE": ["DE002", "DE600"], 
    "Munich_DE": ["DE003", "DE212"], "Frankfurt_DE": ["DE005", "DE712"], 
    "Cologne_DE": ["DE004", "DEA23"], "Stuttgart_DE": ["DE006", "DE111"], 
    "Dusseldorf_DE": ["DE009", "DEA11"], "Leipzig_DE": ["DE012", "DED51"], 
    "Dortmund_DE": ["DE008", "DEA52"], "Essen_DE": ["DE008", "DEA13"], 
    "Bremen_DE": ["DE010", "DE501"], "Dresden_DE": ["DE011", "DED21"], 
    "Hanover_DE": ["DE013", "DE92"], "Nuremberg_DE": ["DE014", "DE254"], 
    "Duisburg_DE": ["DE008", "DEA12"], "Bochum_DE": ["DE008", "DEA51"], 
    "Wuppertal_DE": ["DE024", "DEA1A"], "Bielefeld_DE": ["DE015", "DEA41"], 
    "Bonn_DE": ["DE021", "DEA22"], "Munster_DE": ["DE030", "DEA33"], 
    "Karlsruhe_DE": ["DE017", "DE122"], "Mannheim_DE": ["DE016", "DE126"], 
    "Augsburg_DE": ["DE020", "DE271"], "Wiesbaden_DE": ["DE023", "DE714"], 
    "Gelsenkirchen_DE": ["DE008", "DEA32"], "Monchengladbach_DE": ["DE028", "DEA15"], 
    "Braunschweig_DE": ["DE027", "DE911"], "Chemnitz_DE": ["DE022", "DED41"], 
    "Kiel_DE": ["DE031", "DEF02"], "Aachen_DE": ["DE018", "DEA21"], 
    "Halle_DE": ["DE033", "DEE02"], "Magdeburg_DE": ["DE032", "DEE03"], 
    "Freiburg_DE": ["DE036", "DE131"], "Lubeck_DE": ["DE039", "DEF03"], 
    "Erfurt_DE": ["DE034", "DEG01"], "Rostock_DE": ["DE038", "DE803"], 
    "Mainz_DE": ["DE035", "DEB35"], "Kassel_DE": ["DE037", "DE731"], 

    # --- UK (Datos Históricos) ---
    "London_UK": ["UK001", "UKI"], "Birmingham_UK": ["UK002", "UKG31"], 
    "Manchester_UK": ["UK003", "UKD3"], "Glasgow_UK": ["UK004", "UKM82"], 
    "Leeds_UK": ["UK005", "UKE42"], "Liverpool_UK": ["UK006", "UKD72"], 
    "Newcastle_UK": ["UK007", "UKC22"], "Sheffield_UK": ["UK008", "UKE32"], 
    "Bristol_UK": ["UK009", "UKK11"], "Belfast_UK": ["UK013", "UKN01"], 
    "Edinburgh_UK": ["UK011", "UKM75"], "Cardiff_UK": ["UK010", "UKL22"], 
    "Leicester_UK": ["UK014", "UKF21"], "Coventry_UK": ["UK016", "UKG33"], 
    "Nottingham_UK": ["UK012", "UKF14"], "Southampton_UK": ["UK018", "UKJ32"], 
    "Bradford_UK": ["UK005", "UKE41"], "Hull_UK": ["UK017", "UKE11"], 
    "Stoke_UK": ["UK020", "UKG23"], "Wolverhampton_UK": ["UK002", "UKG34"], 
    "Plymouth_UK": ["UK023", "UKK41"], "Derby_UK": ["UK022", "UKF11"], 
    "Aberdeen_UK": ["UK019", "UKM50"], "Brighton_UK": ["UK021", "UKJ21"], 
    "Portsmouth_UK": ["UK018", "UKJ31"], "Norwich_UK": ["UK025", "UKH13"], 

    # --- PAÍSES BAJOS ---
    "Amsterdam_NL": ["NL002", "NL329"], "Rotterdam_NL": ["NL003", "NL33C"], 
    "TheHague_NL": ["NL001", "NL332"], "Utrecht_NL": ["NL004", "NL310"], 
    "Eindhoven_NL": ["NL005", "NL414"], "Tilburg_NL": ["NL006", "NL412"], 
    "Groningen_NL": ["NL007", "NL124"], "Enschede_NL": ["NL008", "NL213"], 
    "Apeldoorn_NL": ["NL011", "NL221"], "Haarlem_NL": ["NL002", "NL327"], 
    "Arnhem_NL": ["NL009", "NL226"], "Breda_NL": ["NL010", "NL411"], 
    "Almere_NL": ["NL002", "NL230"], "Nijmegen_NL": ["NL009", "NL226"], 

    # --- BÉLGICA ---
    "Brussels_BE": ["BE001", "BE100"], "Antwerp_BE": ["BE002", "BE211"], 
    "Ghent_BE": ["BE003", "BE234"], "Charleroi_BE": ["BE004", "BE323"], 
    "Liege_BE": ["BE005", "BE332"], "Bruges_BE": ["BE006", "BE251"], 
    "Namur_BE": ["BE007", "BE352"], "Mons_BE": ["BE008", "BE324"], 
    "Aalst_BE": ["BE001", "BE231"], "Leuven_BE": ["BE001", "BE242"], 

    # --- POLONIA ---
    "Warsaw_PL": ["PL001", "PL911"], "Krakow_PL": ["PL002", "PL213"], 
    "Lodz_PL": ["PL004", "PL711"], "Wroclaw_PL": ["PL005", "PL514"], 
    "Poznan_PL": ["PL006", "PL415"], "Gdansk_PL": ["PL003", "PL633"], 
    "Szczecin_PL": ["PL007", "PL424"], "Bydgoszcz_PL": ["PL008", "PL613"], 
    "Lublin_PL": ["PL009", "PL814"], "Katowice_PL": ["PL010", "PL22A"], 
    "Bialystok_PL": ["PL011", "PL841"], "Radom_PL": ["PL013", "PL921"], 
    "Gdynia_PL": ["PL003", "PL633"], "Czestochowa_PL": ["PL012", "PL224"], 
    "Sosnowiec_PL": ["PL010", "PL22B"], "Torun_PL": ["PL008", "PL613"], 
    "Kielce_PL": ["PL014", "PL721"], "Rzeszow_PL": ["PL015", "PL823"], 

    # --- RUMANÍA ---
    "Bucharest_RO": ["RO001", "RO321"], "Cluj_RO": ["RO002", "RO113"], 
    "Timisoara_RO": ["RO003", "RO424"], "Craiova_RO": ["RO004", "RO412"], 
    "Constanta_RO": ["RO005", "RO223"], "Iasi_RO": ["RO006", "RO213"], 
    "Brasov_RO": ["RO007", "RO122"], "Galati_RO": ["RO008", "RO224"], 
    "Ploiesti_RO": ["RO009", "RO316"], "Oradea_RO": ["RO010", "RO111"], 

    # --- REP. CHECA ---
    "Prague_CZ": ["CZ001", "CZ010"], "Brno_CZ": ["CZ002", "CZ064"], 
    "Ostrava_CZ": ["CZ003", "CZ080"], "Plzen_CZ": ["CZ004", "CZ032"], 
    "Liberec_CZ": ["CZ005", "CZ051"], "Olomouc_CZ": ["CZ006", "CZ071"], 
    "CeskeBudejovice_CZ": ["CZ008", "CZ031"], "HradecKralove_CZ": ["CZ009", "CZ052"], 
    "UstiNadLabem_CZ": ["CZ007", "CZ042"], "Pardubice_CZ": ["CZ009", "CZ053"], 

    # --- HUNGRÍA ---
    "Budapest_HU": ["HU001", "HU110"], "Debrecen_HU": ["HU004", "HU321"], 
    "Miskolc_HU": ["HU002", "HU311"], "Szeged_HU": ["HU005", "HU333"], 
    "Pecs_HU": ["HU006", "HU231"], "Gyor_HU": ["HU003", "HU221"], 
    "Nyiregyhaza_HU": ["HU007", "HU323"], "Kecskemet_HU": ["HU008", "HU331"], 
    "Szekesfehervar_HU": ["HU009", "HU211"], "Szombathely_HU": ["HU010", "HU222"], 

    # --- AUSTRIA ---
    "Vienna_AT": ["AT001", "AT130"], "Graz_AT": ["AT002", "AT221"], 
    "Linz_AT": ["AT003", "AT312"], "Salzburg_AT": ["AT004", "AT323"], 
    "Innsbruck_AT": ["AT005", "AT332"], "Klagenfurt_AT": ["AT006", "AT211"], 
    "Villach_AT": ["AT006", "AT212"], "Wels_AT": ["AT003", "AT312"], 
    "SanktPolten_AT": ["AT001", "AT123"], "Dornbirn_AT": ["AT005", "AT342"], 

    # --- SUECIA ---
    "Stockholm_SE": ["SE001", "SE110"], "Gothenburg_SE": ["SE002", "SE232"], 
    "Malmo_SE": ["SE003", "SE224"], "Uppsala_SE": ["SE004", "SE121"], 
    "Vasteras_SE": ["SE005", "SE125"], "Orebro_SE": ["SE006", "SE124"], 
    "Linkoping_SE": ["SE007", "SE123"], "Helsingborg_SE": ["SE003", "SE224"], 
    "Jonkoping_SE": ["SE008", "SE211"], "Norrkoping_SE": ["SE007", "SE123"], 

    # --- DINAMARCA ---
    "Copenhagen_DK": ["DK001", "DK011"], "Aarhus_DK": ["DK002", "DK042"], 
    "Odense_DK": ["DK003", "DK031"], "Aalborg_DK": ["DK004", "DK050"], 
    "Esbjerg_DK": ["DK005", "DK032"], "Randers_DK": ["DK002", "DK042"], 
    "Kolding_DK": ["DK006", "DK032"], "Horsens_DK": ["DK007", "DK042"], 
    "Vejle_DK": ["DK006", "DK032"], "Roskilde_DK": ["DK001", "DK021"], 

    # --- FINLANDIA ---
    "Helsinki_FI": ["FI001", "FI1B1"], "Tampere_FI": ["FI002", "FI197"], 
    "Turku_FI": ["FI003", "FI1C1"], "Oulu_FI": ["FI004", "FI1D9"], 
    "Espoo_FI": ["FI001", "FI1B1"], "Vantaa_FI": ["FI001", "FI1B1"], 
    "Jyvaskyla_FI": ["FI005", "FI193"], "Lahti_FI": ["FI006", "FI1C3"], 
    "Kuopio_FI": ["FI007", "FI1D2"], "Pori_FI": ["FI008", "FI196"], 

    # --- IRLANDA ---
    "Dublin_IE": ["IE001", "IE061"], "Cork_IE": ["IE002", "IE053"], 
    "Limerick_IE": ["IE003", "IE051"], "Galway_IE": ["IE004", "IE042"], 
    "Waterford_IE": ["IE005", "IE052"], "Drogheda_IE": ["IE006", "IE062"], 
    "Dundalk_IE": ["IE007", "IE062"], "Navan_IE": ["IE008", "IE062"], 
    "Swords_IE": ["IE001", "IE061"], "Bray_IE": ["IE001", "IE062"],

    # --- GRECIA ---
    "Athens_GR": ["EL001", "EL303"], "Thessaloniki_GR": ["EL002", "EL522"], 
    "Patras_GR": ["EL003", "EL632"], "Heraklion_GR": ["EL004", "EL431"], 
    "Larissa_GR": ["EL005", "EL612"], "Volos_GR": ["EL006", "EL613"], 
    "Ioannina_GR": ["EL007", "EL543"], "Trikala_GR": ["EL005", "EL611"], 
    "Chalcis_GR": ["EL008", "EL642"], "Serres_GR": ["EL009", "EL526"], 

    # --- SUIZA ---
    "Zurich_CH": ["CH001", "CH040"], "Geneva_CH": ["CH002", "CH013"], 
    "Basel_CH": ["CH003", "CH032"], "Bern_CH": ["CH004", "CH021"], 
    "Lausanne_CH": ["CH005", "CH011"], "Lugano_CH": ["CH006", "CH070"], 
    "Lucerne_CH": ["CH007", "CH061"], "StGallen_CH": ["CH008", "CH055"], 
    "Winterthur_CH": ["CH001", "CH040"], "Biel_CH": ["CH004", "CH021"], 

    # --- NORUEGA ---
    "Oslo_NO": ["NO001", "NO081"], "Bergen_NO": ["NO002", "NO0A2"], 
    "Trondheim_NO": ["NO004", "NO0A3"], "Stavanger_NO": ["NO003", "NO0A1"], 
    "Drammen_NO": ["NO001", "NO030"], "Fredrikstad_NO": ["NO005", "NO030"], 
    "Kristiansand_NO": ["NO006", "NO0A2"], "Tromso_NO": ["NO007", "NO074"], 
    "Sarpsborg_NO": ["NO005", "NO030"], "Sandnes_NO": ["NO003", "NO0A1"], 
}