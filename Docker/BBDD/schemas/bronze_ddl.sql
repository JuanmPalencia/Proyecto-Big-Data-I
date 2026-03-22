-- =============================================================================
-- GTP (Green Turning Point) — BRONZE LAYER DDL
-- Capa de datos brutos: append-only, sin transformaciones, con metadata de ingesta.
-- Motor: Apache Hive sobre HDFS (Lorca cluster UEM)
-- Formato de almacenamiento: Parquet (compresión Snappy por defecto)
-- Convención HDFS: hdfs:///user/gtp/bronze/<tabla>/year=YYYY/country=XX/
-- =============================================================================
-- NOTA: Ejecutar con: beeline -u "jdbc:hive2://lorca:10000" -f bronze_ddl.sql
--       o desde PySpark: spark.sql(open("bronze_ddl.sql").read())
-- =============================================================================

CREATE DATABASE IF NOT EXISTS gtp_bronze
  COMMENT 'Capa Bronze GTP: datos brutos de ingesta, append-only'
  LOCATION 'hdfs:///user/gtp/bronze';

USE gtp_bronze;

-- =============================================================================
-- TABLA 1: Sentinel-2 — Índice de Vegetación (NDVI)
-- Fuente: Google Earth Engine, colección COPERNICUS/S2_SR_HARMONIZED
-- Granularidad: ciudad × mes
-- Ingesta: Sentinel-2_extract.py vía GEE
-- =============================================================================
-- Fuente: Sentinel-2_extract.py vía GEE reduceRegion (sin descarga de GeoTIFFs)
-- Columnas alineadas con el CSV real que produce el ETL
CREATE TABLE IF NOT EXISTS bronze_sentinel2_raw (
    city                STRING    COMMENT 'Código ciudad FUA (ej: Madrid_ES)',
    lat                 DOUBLE    COMMENT 'Latitud central (null en Bronze, se une en Silver desde config.py)',
    lon                 DOUBLE    COMMENT 'Longitud central (null en Bronze)',
    month               INT       COMMENT 'Mes (1–12)',
    ndvi_mean           DOUBLE    COMMENT 'Media de NDVI en el ROI de la ciudad (reduceRegion GEE)',
    ndvi_std            DOUBLE    COMMENT 'Desviación estándar del NDVI (heterogeneidad espacial)',
    ndvi_valid_pixels   INT       COMMENT 'Número de píxeles válidos (pixel_count en CSV fuente)',
    -- Metadata de ingesta (columnas de auditoria, NUNCA modificar en Silver)
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta (YYYY-MM-DD)',
    _source_system      STRING    COMMENT 'Sistema fuente: GEE_S2',
    _file_name          STRING    COMMENT 'Nombre del CSV fuente (sentinel2.csv)'
)
COMMENT 'Datos brutos de vegetación Sentinel-2 por ciudad y mes'
PARTITIONED BY (
    year    INT     COMMENT 'Año de la observación',
    country STRING  COMMENT 'Código ISO-2 del país (ej: ES, FR, DE)'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/sentinel2_raw'
TBLPROPERTIES (
    'parquet.compression'       = 'SNAPPY',
    'transactional'             = 'false',
    'immutable'                 = 'true',
    'created_by'                = 'GTP_bronze_ingest.py',
    'data_retention_policy'     = 'PERMANENT'
);

-- =============================================================================
-- TABLA 2: Sentinel-5P — Dióxido de Nitrógeno en Troposfera (NO2)
-- Fuente: Google Earth Engine, colección COPERNICUS/S5P/OFFL/L3_NO2
-- Granularidad: ciudad × mes
-- Ingesta: Sentinel-5p_extract.py vía GEE
-- =============================================================================
-- Fuente: Sentinel-5p_extract.py vía GEE reduceRegion (sin descarga de GeoTIFFs)
CREATE TABLE IF NOT EXISTS bronze_sentinel5p_raw (
    city                STRING    COMMENT 'Código ciudad FUA',
    lat                 DOUBLE    COMMENT 'Latitud central (null en Bronze)',
    lon                 DOUBLE    COMMENT 'Longitud central (null en Bronze)',
    month               INT       COMMENT 'Mes (1–12)',
    no2_mean            DOUBLE    COMMENT 'Densidad media NO2 troposférica (mol/m²), reduceRegion GEE',
    no2_std             DOUBLE    COMMENT 'Desviación estándar espacial del NO2',
    no2_valid_pixels    INT       COMMENT 'Píxeles válidos (pixel_count en CSV fuente)',
    -- Metadata
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system      STRING    COMMENT 'Sistema fuente: GEE_S5P',
    _file_name          STRING    COMMENT 'Nombre del CSV fuente (s5p.csv)'
)
COMMENT 'Datos brutos de calidad del aire Sentinel-5P por ciudad y mes'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/sentinel5p_raw'
TBLPROPERTIES (
    'parquet.compression'       = 'SNAPPY',
    'transactional'             = 'false',
    'immutable'                 = 'true',
    'created_by'                = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 3: HRL (High Resolution Layer) — Impermeabilización del suelo
-- Fuente: Copernicus Land Service via HRL_extract.py
-- Granularidad: ciudad × año (dato estructural, no varía mensualmente)
-- =============================================================================
-- Fuente: HRL_extract.py (descarga GeoTIFFs EEA) + hrl_to_csv.py (agrega a CSV)
-- Columnas alineadas con el CSV real que produce hrl_to_csv.py
CREATE TABLE IF NOT EXISTS bronze_hrl_raw (
    city                    STRING    COMMENT 'Código ciudad FUA',
    lat                     DOUBLE    COMMENT 'Latitud central (null en Bronze)',
    lon                     DOUBLE    COMMENT 'Longitud central (null en Bronze)',
    imperviousness_mean     DOUBLE    COMMENT 'Impermeabilización media del área urbana (0–100%)',
    tree_cover_pct          DOUBLE    COMMENT 'Porcentaje de cobertura arbórea — HRL Tree Cover Density',
    small_woody_mean        DOUBLE    COMMENT 'Porcentaje de elementos leñosos pequeños — HRL Small Woody Features',
    -- Metadata
    _ingestion_date         STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system          STRING    COMMENT 'Sistema fuente: Copernicus_HRL',
    _file_name              STRING    COMMENT 'Nombre del CSV fuente (hrl.csv)'
)
COMMENT 'Datos brutos de impermeabilización del suelo HRL por ciudad y año'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/hrl_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 4: YFinance — Datos financieros de empresas verdes
-- Fuente: Yahoo Finance API vía yfinance (YFinance_extract.py)
-- Granularidad: empresa (ticker) × año × mes  [MENSUAL — estándar Fama-French]
-- Volatilidad mensual: std(daily_return) * sqrt(21)  [días bursátiles/mes]
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_finance_raw (
    ticker              STRING    COMMENT 'Símbolo bursátil (ej: IBE.MC, ORSTED.CO)',
    company_name        STRING    COMMENT 'Nombre corto de la empresa',
    sector              STRING    COMMENT 'Sector GICS de Yahoo Finance',
    industry            STRING    COMMENT 'Industria Yahoo Finance',
    country             STRING    COMMENT 'País de cotización (nombre completo, ej: Spain)',
    fua_country_code    STRING    COMMENT 'Código ISO-2 para cruzar con EURO_FUAS (ej: ES)',
    month               INT       COMMENT 'Mes (1–12) — granularidad mensual',
    close_price         DOUBLE    COMMENT 'Precio de cierre promedio mensual ajustado (dividendos+splits)',
    monthly_return      DOUBLE    COMMENT 'Retorno mensual: (P_final/P_inicial)−1',
    monthly_volatility  DOUBLE    COMMENT 'Volatilidad mensual: std(daily_return)*sqrt(21)',
    volume_avg          DOUBLE    COMMENT 'Volumen medio diario negociado en el mes',
    current_pe          DOUBLE    COMMENT 'PER trailing (snapshot de extracción, no histórico)',
    current_beta        DOUBLE    COMMENT 'Beta de mercado (snapshot)',
    extraction_date     STRING    COMMENT 'Fecha UTC de extracción del snapshot',
    -- Metadata
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta en HDFS',
    _source_system      STRING    COMMENT 'Sistema fuente: YFinance',
    _file_name          STRING    COMMENT 'CSV fuente: finance_monthly_2006_2025.csv'
)
COMMENT 'Datos financieros mensuales de empresas del portafolio Green Deal Europe'
PARTITIONED BY (
    year    INT,
    country_code STRING  COMMENT 'Código ISO-2 (igual que fua_country_code, duplicado para partición)'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/finance_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 5: Eurostat — GDP per cápita
-- Fuente: Eurostat API (nama_10r_3gdp o nama_10_gdp)
-- Granularidad: país (NUTS-0) × año
-- Nota: Usamos GDP per cápita PPS (Purchasing Power Standard) para comparabilidad
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_eurostat_gdp_raw (
    geo_code            STRING    COMMENT 'Código geográfico Eurostat (NUTS-0: ES, FR...)',
    geo_label           STRING    COMMENT 'Nombre del país',
    unit                STRING    COMMENT 'Unidad de medida Eurostat (PPS_HAB, EUR_HAB, MIO_EUR...)',
    gdp_value           DOUBLE    COMMENT 'Valor del GDP en la unidad indicada',
    obs_flag            STRING    COMMENT 'Flag de calidad Eurostat (e=estimado, p=provisional, b=ruptura)',
    -- Metadata
    _ingestion_date     STRING,
    _source_system      STRING    COMMENT 'Sistema fuente: Eurostat_API',
    _dataset_code       STRING    COMMENT 'Código del dataset Eurostat (ej: nama_10r_3gdp)'
)
COMMENT 'GDP per cápita bruto de Eurostat por país y año'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/eurostat_gdp_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 6: Eurostat — Población urbana
-- Fuente: Eurostat API (urb_cpop1 o demo_r_pjanaggr3)
-- Granularidad: ciudad FUA × año
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_eurostat_population_raw (
    city_code           STRING    COMMENT 'Código Eurostat de la ciudad (ej: ES001C — Madrid)',
    city_name           STRING    COMMENT 'Nombre de la ciudad',
    geo_code            STRING    COMMENT 'Código NUTS-0 del país',
    indic_ur            STRING    COMMENT 'Indicador Eurostat (ej: POP — Total Population)',
    population          BIGINT    COMMENT 'Población de la FUA en el año',
    obs_flag            STRING    COMMENT 'Flag de calidad Eurostat',
    -- Metadata
    _ingestion_date     STRING,
    _source_system      STRING    COMMENT 'Sistema fuente: Eurostat_API',
    _dataset_code       STRING
)
COMMENT 'Población urbana bruta de Eurostat por ciudad FUA y año'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/eurostat_population_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 7: ERA5-Land — Temperatura y Precipitación
-- Fuente: Google Earth Engine, colección ECMWF/ERA5_LAND/MONTHLY_AGGR
-- Granularidad: ciudad × mes
-- Ingesta: era5_extract.py vía GEE reduceRegion
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_era5_raw (
    city                STRING    COMMENT 'Código ciudad FUA (ej: Madrid_ES)',
    month               INT       COMMENT 'Mes (1–12)',
    temp_mean_c         DOUBLE    COMMENT 'Temperatura media mensual del aire a 2m (°C)',
    precip_total_m      DOUBLE    COMMENT 'Precipitación total mensual (m)',
    -- Metadata
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system      STRING    COMMENT 'Sistema fuente: GEE_ERA5',
    _file_name          STRING    COMMENT 'Nombre del CSV fuente (era5.csv)'
)
COMMENT 'Datos climáticos ERA5-Land por ciudad y mes — temperatura y precipitación'
PARTITIONED BY (
    year    INT     COMMENT 'Año de la observación',
    country STRING  COMMENT 'Código ISO-2 del país'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/era5_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 8: S5P Aerosol — Índice de Aerosol UV (UVAI)
-- Fuente: Google Earth Engine, colección COPERNICUS/S5P/OFFL/L3_AER_AI
-- Granularidad: ciudad × mes
-- Ingesta: s5p_aerosol_extract.py vía GEE reduceRegion
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_s5p_aerosol_raw (
    city                STRING    COMMENT 'Código ciudad FUA',
    month               INT       COMMENT 'Mes (1–12)',
    uvai_mean           DOUBLE    COMMENT 'Índice UV de aerosoles absorbentes (adimensional)',
    uvai_std            DOUBLE    COMMENT 'Desviación estándar espacial del UVAI',
    uvai_valid_pixels   INT       COMMENT 'Número de píxeles válidos',
    -- Metadata
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system      STRING    COMMENT 'Sistema fuente: GEE_S5P_AER',
    _file_name          STRING    COMMENT 'Nombre del CSV fuente (s5p_aerosol.csv)'
)
COMMENT 'Índice de aerosol UV Sentinel-5P por ciudad y mes (proxy calidad del aire)'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/s5p_aerosol_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 9: Urban Atlas (ESA WorldCover) — Uso de suelo
-- Fuente: Google Earth Engine, ESA/WorldCover/v100 (2020) y v200 (2021)
-- Granularidad: ciudad × año (solo 2020 y 2021 disponibles)
-- Ingesta: urban_atlas_extract.py vía GEE frequencyHistogram
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_urban_atlas_raw (
    city                STRING    COMMENT 'Código ciudad FUA',
    wc_tree_pct         DOUBLE    COMMENT '% píxeles con cobertura arbórea (clase 10)',
    wc_shrub_pct        DOUBLE    COMMENT '% matorral (clase 20)',
    wc_grass_pct        DOUBLE    COMMENT '% pradera/herbazal (clase 30)',
    wc_crop_pct         DOUBLE    COMMENT '% cultivos (clase 40)',
    wc_built_pct        DOUBLE    COMMENT '% urbanizado/construido (clase 50)',
    wc_bare_pct         DOUBLE    COMMENT '% suelo desnudo (clase 60)',
    wc_water_pct        DOUBLE    COMMENT '% aguas permanentes (clase 80)',
    -- Metadata
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system      STRING    COMMENT 'Sistema fuente: GEE_WorldCover',
    _file_name          STRING    COMMENT 'Nombre del CSV fuente (urban_atlas.csv)'
)
COMMENT 'Distribución de uso de suelo ESA WorldCover por ciudad y año (2020–2021)'
PARTITIONED BY (
    year    INT     COMMENT 'Año del mapa WorldCover (2020 o 2021)',
    country STRING  COMMENT 'Código ISO-2 del país'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/urban_atlas_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 10: EDGAR CO2 — Emisiones nacionales de CO2 fósil
-- Fuente: EDGAR v8.0 FT2022 (JRC/EU Commission), descarga automática HTTP
-- Granularidad: ciudad × año (expandido desde país × año)
-- Ingesta: edgar_co2_extract.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_edgar_co2_raw (
    city                STRING    COMMENT 'Código ciudad FUA (valor heredado del país)',
    co2_kt              DOUBLE    COMMENT 'Emisiones CO2 fósil totales del país (kt CO2/año)',
    -- Metadata
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system      STRING    COMMENT 'Sistema fuente: EDGAR_CO2',
    _file_name          STRING    COMMENT 'Nombre del CSV fuente (edgar_co2.csv)'
)
COMMENT 'Emisiones CO2 fósil EDGAR por ciudad/país y año (1970–2022)'
PARTITIONED BY (
    year         INT     COMMENT 'Año de la observación',
    country_code STRING  COMMENT 'Código ISO-2 del país'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/edgar_co2_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 11: OECD — Indicadores de política ambiental y emisiones
-- Fuente: OECD SDMX REST API (env_policy, env_tax, env_expenditure, air_ghg)
-- Procesado: oecd_process.py (expande país → ciudades)
-- Granularidad: ciudad × año (valor de país aplicado a todas las ciudades)
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_oecd_raw (
    city                STRING    COMMENT 'Código ciudad FUA (valor heredado del país)',
    eps_index           DOUBLE    COMMENT 'Environmental Policy Stringency Index (OECD)',
    env_tax_usd         DOUBLE    COMMENT 'Recaudación impuestos ambientales (millones USD)',
    env_expenditure     DOUBLE    COMMENT 'Gasto público protección ambiental NEEP (% PIB)',
    ghg_total_kt        DOUBLE    COMMENT 'Emisiones GHG totales del país (kt CO2eq/año)',
    -- Metadata
    _ingestion_date     STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system      STRING    COMMENT 'Sistema fuente: OECD_API',
    _file_name          STRING    COMMENT 'Nombre del CSV fuente (oecd_indicators.csv)'
)
COMMENT 'Indicadores de política ambiental OECD por ciudad/país y año'
PARTITIONED BY (
    year    INT     COMMENT 'Año de la observación',
    country STRING  COMMENT 'Código ISO-2 del país'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/oecd_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 12: InvestEU — Inversión verde EIB (beneficiarios finales)
-- Fuente: EIB InvestEU Final Recipients PDFs (investeu_process.py)
-- Granularidad: ciudad × año (expandida desde país × año)
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_investeu_raw (
    city                    STRING    COMMENT 'Código ciudad FUA (valor heredado del país)',
    investeu_ops_count      INT       COMMENT 'Nº de operaciones InvestEU en el país y año',
    investeu_total_eur      DOUBLE    COMMENT 'Importe total de financiación InvestEU (EUR)',
    -- Metadata
    _ingestion_date         STRING    COMMENT 'Fecha UTC de ingesta',
    _source_system          STRING    COMMENT 'Sistema fuente: InvestEU_EIB',
    _file_name              STRING    COMMENT 'Nombre del CSV fuente (investeu_summary.csv)'
)
COMMENT 'Inversión InvestEU/EIB por ciudad/país y año (beneficiarios finales)'
PARTITIONED BY (
    year    INT     COMMENT 'Año de la observación',
    country STRING  COMMENT 'Código ISO-2 del país'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/investeu_raw'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'transactional'         = 'false',
    'immutable'             = 'true',
    'created_by'            = 'GTP_bronze_ingest.py'
);

-- =============================================================================
-- TABLA 13: MODIS NDVI — Vegetación 2000-2003 (gap-fill pre-Landsat)
-- Fuente: Google Earth Engine, colección MODIS/006/MOD13A3
-- Granularidad: ciudad × mes
-- Ingesta: modis_ndvi_extract.py vía GEE
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_modis_ndvi_raw (
    city STRING, month INT,
    ndvi_mean DOUBLE COMMENT 'NDVI medio mensual MODIS (MOD13A3, escala 0.0001)',
    ndvi_std DOUBLE COMMENT 'Desviación estándar espacial NDVI MODIS',
    _ingestion_date STRING, _source_system STRING COMMENT 'GEE_MODIS_NDVI', _file_name STRING
) COMMENT 'NDVI mensual MODIS 2000-2003 para cubrir gap pre-Landsat'
PARTITIONED BY (year INT, country STRING)
STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/modis_ndvi_raw'
TBLPROPERTIES ('parquet.compression'='SNAPPY','transactional'='false','immutable'='true','created_by'='GTP_bronze_ingest.py');

-- =============================================================================
-- TABLA 14: MODIS LST — Urban Heat Island mensual
-- Fuente: Google Earth Engine, colección MODIS/006/MOD11A2 agregada mensualmente
-- Granularidad: ciudad × mes
-- Ingesta: modis_lst_extract.py vía GEE
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_modis_lst_raw (
    city STRING, month INT,
    lst_day_c DOUBLE COMMENT 'Temperatura superficial diurna mensual (°C)',
    lst_night_c DOUBLE COMMENT 'Temperatura superficial nocturna mensual (°C)',
    _ingestion_date STRING, _source_system STRING COMMENT 'GEE_MODIS_LST', _file_name STRING
) COMMENT 'Land Surface Temperature MODIS mensual 2000+ (Urban Heat Island)'
PARTITIONED BY (year INT, country STRING)
STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/modis_lst_raw'
TBLPROPERTIES ('parquet.compression'='SNAPPY','transactional'='false','immutable'='true','created_by'='GTP_bronze_ingest.py');

-- =============================================================================
-- TABLA 15: Nighttime Lights — DMSP anual (2000-2011) + VIIRS mensual (2012+)
-- Fuente: Google Earth Engine (NOAA/DMSP-OLS + NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG)
-- Granularidad: ciudad × año (DMSP) / ciudad × mes (VIIRS)
-- Ingesta: ntl_extract.py vía GEE
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_ntl_raw (
    city STRING,
    month INT COMMENT 'Mes 1-12 para VIIRS; 0 para DMSP (anual)',
    ntl_mean DOUBLE COMMENT 'Luminosidad media (DN 0-63 DMSP / nW/cm2/sr VIIRS)',
    ntl_std DOUBLE,
    ntl_source STRING COMMENT 'Sensor: DMSP o VIIRS',
    _ingestion_date STRING, _source_system STRING COMMENT 'GEE_NTL', _file_name STRING
) COMMENT 'Luces nocturnas DMSP-OLS anual (2000-2011) + VIIRS mensual (2012+)'
PARTITIONED BY (year INT, country STRING)
STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/ntl_raw'
TBLPROPERTIES ('parquet.compression'='SNAPPY','transactional'='false','immutable'='true','created_by'='GTP_bronze_ingest.py');

-- =============================================================================
-- TABLA 16: Renovables — % electricidad renovable mensual por país
-- Fuente: Eurostat nrg_cb_pem (mensual) / nrg_ind_ren (anual fallback)
-- Granularidad: país × mes
-- Ingesta: renewables_extract.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_renewables_raw (
    country STRING,
    month INT,
    renewables_pct DOUBLE COMMENT '% electricidad generada por renovables (Eurostat nrg_cb_pem)',
    renewables_source STRING COMMENT 'MONTHLY_NRG o ANNUAL_FILL',
    _ingestion_date STRING, _source_system STRING COMMENT 'Eurostat_nrg', _file_name STRING
) COMMENT '% renovables mensuales por país 2000+ (Eurostat nrg_cb_pem / nrg_ind_ren fallback)'
PARTITIONED BY (year INT)
STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/renewables_raw'
TBLPROPERTIES ('parquet.compression'='SNAPPY','transactional'='false','immutable'='true','created_by'='GTP_bronze_ingest.py');

-- =============================================================================
-- TABLA 17: Turismo — pernoctaciones mensuales por país
-- Fuente: Eurostat tour_occ_nim
-- Granularidad: ciudad × mes (expandido desde país × mes)
-- Ingesta: tourism_extract.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_tourism_raw (
    city STRING, country STRING, month INT,
    tourism_nights DOUBLE COMMENT 'Pernoctaciones mensuales en alojamiento turístico (Eurostat tour_occ_nim)',
    tourism_source STRING,
    _ingestion_date STRING, _source_system STRING COMMENT 'Eurostat_tourism', _file_name STRING
) COMMENT 'Pernoctaciones turísticas mensuales por ciudad/país 2000+ (Eurostat tour_occ_nim)'
PARTITIONED BY (year INT, country STRING)
STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/tourism_raw'
TBLPROPERTIES ('parquet.compression'='SNAPPY','transactional'='false','immutable'='true','created_by'='GTP_bronze_ingest.py');

-- =============================================================================
-- TABLA 18: EU ETS — Precio del carbono mensual
-- Fuente: Ember Climate (mensual) / fallback anual
-- Granularidad: año × mes (serie temporal global, sin ciudad ni país)
-- Ingesta: ets_extract.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_ets_raw (
    month INT,
    ets_price_eur DOUBLE COMMENT 'Precio EUA EU ETS (€/tonelada CO2). NULL 2000-2004.',
    ets_source STRING COMMENT 'EMBER_MONTHLY o FALLBACK_ANNUAL o NO_MARKET',
    _ingestion_date STRING, _source_system STRING COMMENT 'ETS_EUA', _file_name STRING
) COMMENT 'Precio EU ETS (€/tCO2) mensual. Fase 1 desde ene-2005; NULL 2000-2004.'
PARTITIONED BY (year INT)
STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/ets_raw'
TBLPROPERTIES ('parquet.compression'='SNAPPY','transactional'='false','immutable'='true','created_by'='GTP_bronze_ingest.py');

-- =============================================================================
-- TABLA 19: EEA Air Quality — PM2.5 y PM10 anuales (expandidos mensualmente)
-- Fuente: Eurostat sdg_11_50 (datos anuales por ciudad, repetidos mensualmente)
-- Granularidad: ciudad × mes
-- Ingesta: eea_aq_extract.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS bronze_eea_aq_raw (
    city STRING, country STRING, month INT,
    pm25_mean DOUBLE COMMENT 'PM2.5 medio anual (μg/m³), repetido mensualmente (Eurostat sdg_11_50)',
    pm10_mean DOUBLE COMMENT 'PM10 medio anual (μg/m³), repetido mensualmente',
    aq_source STRING COMMENT 'EUROSTAT_ANNUAL_REPEAT',
    _ingestion_date STRING, _source_system STRING COMMENT 'EEA_AQ', _file_name STRING
) COMMENT 'PM2.5 y PM10 por ciudad/país anuales expandidos mensualmente (Eurostat sdg_11_50)'
PARTITIONED BY (year INT, country STRING)
STORED AS PARQUET LOCATION 'hdfs:///user/gtp/bronze/eea_aq_raw'
TBLPROPERTIES ('parquet.compression'='SNAPPY','transactional'='false','immutable'='true','created_by'='GTP_bronze_ingest.py');

-- =============================================================================
-- TABLA 20 (nota): Población urbana FUA — relleno de bronze_eurostat_population_raw
-- La tabla bronze_eurostat_population_raw (TABLA 6) ya existe en el DDL.
-- Se llena vía population_extract.py → population.csv → bronze_ingest.py (source_sk=20).
-- No se crea tabla separada; se documenta aquí para completitud del catálogo.
-- =============================================================================

-- =============================================================================
-- FIN BRONZE DDL
-- Total tablas: 20
-- Convención de particionado: year=YYYY/country=XX (o country_code=XX para finance/edgar)
-- Para registrar particiones manualmente tras carga Parquet directa:
--   MSCK REPAIR TABLE gtp_bronze.bronze_sentinel2_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_era5_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_s5p_aerosol_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_urban_atlas_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_edgar_co2_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_oecd_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_investeu_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_modis_ndvi_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_modis_lst_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_ntl_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_renewables_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_tourism_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_ets_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_eea_aq_raw;
--   MSCK REPAIR TABLE gtp_bronze.bronze_eurostat_population_raw;
-- =============================================================================
