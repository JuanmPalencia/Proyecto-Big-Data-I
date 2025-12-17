import os
import requests
import math
import time

# ==============================================================================
# 1. CONFIGURACIÓN DE RUTAS & URLS
# ==============================================================================

BASE_OUTPUT_DIR = "data/hrl_full_europe"

HRL_CATALOG = {
    2006: {
        # MapServer
        "Imperviousness": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessDensity_2006/MapServer/export"
    },
    2009: {
        # MapServer
        "Imperviousness": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessDensity_2009/MapServer/export"
    },
    2012: {
        # MapServer
        "Imperviousness": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_Imperviousness_Density_2012/MapServer/export",
        "Tree_Cover":     "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_Tree_Cover_Density_2012/MapServer/export"
    },
    2015: {
        # MapServer
        "Imperviousness": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessDensity_2015/MapServer/export",
        # MapServer
        "Tree_Cover":     "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_TreeCoverDensity_2015/MapServer/export",
        # ImageServer
        "Small_Woody":    "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_SmallWoodyFeatures_2015_005m/ImageServer/exportImage"
    },
    2018: {
        # ImageServer
        "Imperviousness": "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_ImperviousnessDensity_2018/ImageServer/exportImage",
        # ImageServer
        "Tree_Cover":     "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_TreeCoverDensity_2018/ImageServer/exportImage",
        "Small_Woody":    "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_SmallWoodyFeatures_2018_005m/ImageServer/exportImage"
    },
    2021: {
        # ImageServer
        "Small_Woody":    "https://image.discomap.eea.europa.eu/arcgis/rest/services/GioLandPublic/HRL_SmallWoodyFeatures_2021_005m/ImageServer/exportImage"
    }
}

# ==============================================================================
# 2. BBOX (Calculadora de Coordenadas)
# ==============================================================================
def get_bbox_from_centroid(lat, lon, radius_km=25):
    """
    Calcula el 'Bounding Box' (recuadro) en metros (EPSG:3857).
    """
    R = 6378137.0
    x_center = lon * (math.pi / 180.0) * R
    y_center = math.log(math.tan((90 + lat) * math.pi / 360.0)) * R
    r_meters = radius_km * 1000
    return f"{int(x_center - r_meters)},{int(y_center - r_meters)},{int(x_center + r_meters)},{int(y_center + r_meters)}"

# ==============================================================================
# 3. Diccionario EURO_FUAS (Todas las ciudades)
# ==============================================================================
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
# 4. FUNCIÓN DE DESCARGA
# ==============================================================================
def download_image(url, bbox, size=2048, filename=None, out_dir=None):
    """
    Descarga la imagen usando parámetros estándar de ArcGIS REST.
    """
    params = {
        "f": "image",
        "bbox": bbox,
        "imageSR": 102100, 
        "bboxSR": 102100,
        "size": f"{size},{size}",
        "format": "png"
    }

    try:
        response = requests.get(url, params=params, timeout=60, stream=True)
        response.raise_for_status()

        # Validación de contenido
        if b"error" in response.content[:100].lower() or b"html" in response.content[:100].lower():
            print(f"   [ERROR API] Contenido inválido en {filename}: {response.content[:100]}")
            return False

        full_path = os.path.join(out_dir, filename)
        with open(full_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"   [GUARDADO] {filename}")
        return True

    except Exception as e:
        print(f"   [ERROR RED] {e}")
        return False

# ==============================================================================
# 5. BUCLE PRINCIPAL
# ==============================================================================
def main():
    print("🚀 Iniciando Extracción HRL (Modo Densidad - Todos los años)...")
    
    os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

    for city_name, coords in EURO_FUAS.items():
        lat, lon = coords
        bbox = get_bbox_from_centroid(lat, lon)
        
        for year, layers in HRL_CATALOG.items():
            for layer_name, base_url in layers.items():
                
                # Estructura: data/hrl_full_europe/2012/Tree_Cover/
                layer_dir = os.path.join(BASE_OUTPUT_DIR, str(year), layer_name)
                os.makedirs(layer_dir, exist_ok=True)
                
                filename = f"{city_name}_{layer_name}_{year}.png"
                
                if os.path.exists(os.path.join(layer_dir, filename)):
                    print(f"   [SALTAR] Ya existe: {filename}")
                    continue

                print(f"⬇️  {city_name} | {year} | {layer_name}")
                
                success = download_image(base_url, bbox, filename=filename, out_dir=layer_dir)
                
                if success:
                    time.sleep(0.5) # Pausa breve para no saturar

    print("\n🏁 Proceso finalizado.")

if __name__ == "__main__":
    main()