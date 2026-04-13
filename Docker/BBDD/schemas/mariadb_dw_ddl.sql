-- =============================================================================
-- GTP (Green Turning Point) — MariaDB Data Warehouse DDL
-- Silver (Star Schema Kimball) + Gold (Analítico)
-- =============================================================================
-- Motor:       InnoDB
-- Partición:   RANGE por year (Tipo 1) en todas las fact tables
-- Granularity: Mensual (2004–2024 = 252 meses)
-- Dims SCD:    dim_company → Tipo 2 | dim_cluster → Tipo 2 | resto → Tipo 1
-- Ejecución:
--   mysql -h <host> -P <port> -u <user> -p<password> <database> < mariadb_dw_ddl.sql
-- =============================================================================

USE bd_rvm_gtp;

-- Desactivar FK checks para poder hacer DROP en cualquier orden
SET FOREIGN_KEY_CHECKS = 0;

-- =============================================================================
-- DROP — limpiar schema anterior
-- =============================================================================
DROP TABLE IF EXISTS model_results;
DROP TABLE IF EXISTS city_ranking;
DROP TABLE IF EXISTS fact_kuznets;
DROP TABLE IF EXISTS fact_policy;
DROP TABLE IF EXISTS fact_financial;
DROP TABLE IF EXISTS fact_economic;
DROP TABLE IF EXISTS fact_environmental;
DROP TABLE IF EXISTS dim_cluster;
DROP TABLE IF EXISTS dim_company;
DROP TABLE IF EXISTS dim_source;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_city;
DROP TABLE IF EXISTS ekc_parameters;   -- tabla legacy del schema anterior

SET FOREIGN_KEY_CHECKS = 1;

-- =============================================================================
-- SILVER — DIMENSIONES
-- =============================================================================

-- ----------------------------------------------------------------------------
-- DIM_CITY — SCD Tipo 1 (sobreescribir)
-- Catálogo de 237 FUAs europeas con coordenadas geográficas.
-- Atributos estables: coordenadas y país no cambian.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_city (
    city_sk             INT             NOT NULL AUTO_INCREMENT,
    city_code           VARCHAR(100)    NOT NULL  COMMENT 'Clave negocio: NombreCiudad_ISO2',
    city_name           VARCHAR(255),
    country_code        VARCHAR(10)     NOT NULL,
    country_name        VARCHAR(100),
    nuts_code           VARCHAR(20),
    eurostat_city_code  VARCHAR(20),
    lat                 DOUBLE          NOT NULL,
    lon                 DOUBLE          NOT NULL,
    fua_area_km2        DOUBLE,
    _last_updated       DATE,
    PRIMARY KEY (city_sk),
    UNIQUE  KEY uq_city_code   (city_code),
    INDEX   idx_dc_country     (country_code)
) ENGINE=InnoDB COMMENT='Dimensión Ciudad — SCD Tipo 1 — 237 FUAs europeas';

-- ----------------------------------------------------------------------------
-- DIM_DATE — Estática, pre-generada 2004–2024 a nivel mensual
-- 252 filas (21 años × 12 meses). Inmutable.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_sk             INT     NOT NULL  COMMENT 'Formato YYYYMM (ej: 202003)',
    year                INT     NOT NULL,
    month               INT     NOT NULL,
    month_name          VARCHAR(20),
    quarter             INT,
    semester            INT,
    is_leap_year        TINYINT(1),
    is_satellite_era    TINYINT(1)        COMMENT 'TRUE si year >= 2018 (S5P operativo)',
    is_omi_era          TINYINT(1)        COMMENT 'TRUE si year >= 2004 (OMI NO2 disponible)',
    PRIMARY KEY (date_sk),
    INDEX idx_dd_year  (year),
    INDEX idx_dd_ym    (year, month)
) ENGINE=InnoDB COMMENT='Dimensión Fecha — estática mensual 2004-2024';

-- ----------------------------------------------------------------------------
-- DIM_SOURCE — SCD Tipo 1
-- Catálogo de las 12 fuentes de datos del pipeline.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_source (
    source_sk           INT             NOT NULL AUTO_INCREMENT,
    source_code         VARCHAR(50)     NOT NULL  COMMENT 'Clave negocio (ej: GEE_S2, OMI_NO2)',
    source_name         VARCHAR(255),
    source_type         VARCHAR(50)               COMMENT 'API_REST | GEE_COLLECTION | CSV_DOWNLOAD',
    provider            VARCHAR(100),
    url_reference       VARCHAR(500),
    temporal_coverage   VARCHAR(100),
    spatial_coverage    VARCHAR(100),
    update_frequency    VARCHAR(50),
    license             VARCHAR(100),
    _last_updated       DATE,
    PRIMARY KEY (source_sk),
    UNIQUE KEY uq_source_code (source_code)
) ENGINE=InnoDB COMMENT='Dimensión Fuente — SCD Tipo 1 — 20 fuentes del pipeline (sk 1-20)';

-- ----------------------------------------------------------------------------
-- DIM_COMPANY — SCD Tipo 2 (historial de versiones)
-- Los sectores de las empresas cambian (ej: Repsol Oil→Energy).
-- valid_from / valid_to delimitan la vigencia de cada versión.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_company (
    company_sk          BIGINT          NOT NULL AUTO_INCREMENT,
    ticker              VARCHAR(20)     NOT NULL  COMMENT 'Clave negocio: símbolo bursátil',
    company_name        VARCHAR(255),
    sector              VARCHAR(100),
    industry            VARCHAR(100),
    country_code        VARCHAR(10),
    country_name        VARCHAR(100),
    stock_exchange      VARCHAR(20),
    esg_classification  VARCHAR(20)               COMMENT 'GREEN | TRANSITION | MONITOR',
    -- SCD-2 control
    valid_from          DATE            NOT NULL,
    valid_to            DATE            NOT NULL  DEFAULT '9999-12-31',
    is_current          TINYINT(1)      NOT NULL  DEFAULT 1,
    PRIMARY KEY (company_sk),
    INDEX idx_comp_ticker   (ticker),
    INDEX idx_comp_current  (ticker, is_current)
) ENGINE=InnoDB COMMENT='Dimensión Empresa — SCD Tipo 2 — portafolio Green Deal Europe';

-- ----------------------------------------------------------------------------
-- DIM_CLUSTER — SCD Tipo 2 (historial de versiones)
-- Los clusters K-Means se recomputan en cada ejecución del pipeline.
-- Una ciudad que cambia de cluster = señal de inversión relevante.
-- Registrar cuándo ocurrió ese cambio tiene valor analítico directo.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_cluster (
    cluster_sk              BIGINT          NOT NULL AUTO_INCREMENT,
    cluster_id              INT             NOT NULL  COMMENT 'ID del cluster (0-based, de K-Means)',
    cluster_label           VARCHAR(100)              COMMENT 'Etiqueta interpretable (ej: High-Income-Recovering)',
    model_run_id            VARCHAR(50)               COMMENT 'ID de la ejecución del pipeline que generó este cluster',
    -- Parámetros EKC del cluster (Panel Regression)
    beta1                   DOUBLE                    COMMENT 'Coef. lineal β₁ de la EKC',
    beta2                   DOUBLE                    COMMENT 'Coef. cuadrático β₂ (< 0 para U invertida)',
    alpha                   DOUBLE,
    turning_point_y_star    DOUBLE                    COMMENT 'Y* = exp(−β₁/2β₂) en PPS',
    r_squared_adj           DOUBLE,
    f_pvalue                DOUBLE,
    ekc_shape               VARCHAR(50)               COMMENT 'INVERTED_U | MONOTONIC_DOWN | MONOTONIC_UP | UNCLEAR',
    ekc_hypothesis_supported TINYINT(1),
    n_cities                INT                       COMMENT 'Ciudades en el cluster en esta versión',
    -- SCD-2 control
    valid_from              DATE            NOT NULL,
    valid_to                DATE            NOT NULL  DEFAULT '9999-12-31',
    is_current              TINYINT(1)      NOT NULL  DEFAULT 1,
    PRIMARY KEY (cluster_sk),
    INDEX idx_cl_id         (cluster_id),
    INDEX idx_cl_current    (cluster_id, is_current),
    INDEX idx_cl_run        (model_run_id)
) ENGINE=InnoDB COMMENT='Dimensión Cluster — SCD Tipo 2 — parámetros EKC por cluster con historial';

-- =============================================================================
-- SILVER — FACT TABLES (granularidad mensual, RANGE partition by year)
-- NOTA: Las tablas particionadas en MariaDB/InnoDB no soportan FK constraints.
--       Las relaciones se garantizan a nivel aplicación (silver_transform.py).
-- =============================================================================

-- ----------------------------------------------------------------------------
-- FACT_ENVIRONMENTAL — ciudad × año × mes
-- Fuentes: Sentinel-2/Landsat (NDVI), S5P/OMI (NO2), ERA5, HRL, WorldCover, UVAI
-- 237 ciudades × 252 meses = 59.724 filas
-- ----------------------------------------------------------------------------
CREATE TABLE fact_environmental (
    city_sk                 INT             NOT NULL  COMMENT 'FK → dim_city',
    date_sk                 INT             NOT NULL  COMMENT 'FK → dim_date (YYYYMM)',
    year                    INT             NOT NULL,
    month                   INT             NOT NULL,
    -- Fuentes usadas
    source_sk_ndvi          INT                       COMMENT 'FK → dim_source (GEE_S2 o Landsat)',
    source_sk_no2           INT                       COMMENT 'FK → dim_source (GEE_S5P o OMI)',
    source_sk_era5          INT                       COMMENT 'FK → dim_source (ERA5)',
    -- NDVI (Sentinel-2 2017+ | Landsat 2004-2016)
    ndvi_mean               DOUBLE                    COMMENT 'Media NDVI mensual de la ciudad',
    ndvi_std                DOUBLE                    COMMENT 'Desviación estándar NDVI mensual',
    -- NO2 (Sentinel-5P 2018+ | OMI 2004-2017)
    no2_mean                DOUBLE                    COMMENT 'Columna troposférica NO2 media (mol/m²)',
    no2_std                 DOUBLE,
    -- Aerosol UVAI (S5P 2018+, NULL antes)
    uvai_mean               DOUBLE,
    -- ERA5 Climate (disponible desde 1950, cobertura completa)
    temp_mean_c             DOUBLE                    COMMENT 'Temperatura media mensual (°C)',
    precip_sum_m            DOUBLE                    COMMENT 'Precipitación mensual acumulada (m)',
    -- HRL (actualización cada 3 años — valor repetido mensualmente)
    imperviousness_mean     DOUBLE                    COMMENT 'Impermeabilización media suelo (%)',
    tree_cover_pct          DOUBLE,
    built_up_area_pct       DOUBLE,
    -- WorldCover ESA (solo 2020-2021 — valor repetido resto de años)
    wc_tree_pct             DOUBLE,
    wc_built_pct            DOUBLE,
    wc_crop_pct             DOUBLE,
    wc_natural_pct          DOUBLE,
    -- Indicadores derivados
    green_index             DOUBLE                    COMMENT 'Índice verde compuesto GTP ∈ [0,1]',
    ndvi_yoy_change         DOUBLE                    COMMENT 'Cambio NDVI vs mismo mes año anterior',
    -- MODIS LST (Land Surface Temperature) — isla de calor urbano 2000+
    lst_day_c               DOUBLE                    COMMENT 'LST diurna mensual MODIS (°C)',
    lst_night_c             DOUBLE                    COMMENT 'LST nocturna mensual MODIS (°C)',
    -- Nighttime Lights — DMSP (norm.) 2000-2011 + VIIRS 2012+
    ntl_mean                DOUBLE                    COMMENT 'Luces nocturnas normalizadas (nW/cm2/sr equiv.)',
    -- EEA Air Quality — PM2.5/PM10 2000+
    pm25_mean               DOUBLE                    COMMENT 'PM2.5 medio mensual (µg/m³)',
    pm10_mean               DOUBLE                    COMMENT 'PM10 medio mensual (µg/m³)',
    _silver_load_date       DATE,
    PRIMARY KEY (city_sk, year, month),
    INDEX idx_fe_date       (date_sk),
    INDEX idx_fe_city_year  (city_sk, year),
    INDEX idx_fe_year_month (year, month)
) ENGINE=InnoDB
  COMMENT='Hechos ambientales mensuales por ciudad — NDVI + NO2 + ERA5 + HRL + LST + NTL + AQ'
PARTITION BY RANGE (year) (
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- ----------------------------------------------------------------------------
-- FACT_ECONOMIC — ciudad × año × mes
-- Fuentes: Eurostat GDP + Población (anuales interpolados a mensual)
-- 237 ciudades × 252 meses = 59.724 filas
-- ----------------------------------------------------------------------------
CREATE TABLE fact_economic (
    city_sk                 INT             NOT NULL  COMMENT 'FK → dim_city',
    date_sk                 INT             NOT NULL  COMMENT 'FK → dim_date',
    year                    INT             NOT NULL,
    month                   INT             NOT NULL,
    source_sk_gdp           INT                       COMMENT 'FK → dim_source',
    source_sk_pop           INT                       COMMENT 'FK → dim_source',
    -- GDP (interpolación lineal mensual entre valores anuales Eurostat)
    gdp_pps_per_capita      DOUBLE                    COMMENT 'GDP per cápita PPS interpolado',
    gdp_eur_per_capita      DOUBLE,
    gdp_pps_index           DOUBLE                    COMMENT 'Índice GDP PPS (EU=100)',
    ln_gdp_pps              DOUBLE                    COMMENT 'ln(gdp_pps_per_capita) — variable X de la EKC',
    ln_gdp_pps_sq           DOUBLE                    COMMENT '[ln(gdp_pps)]² — término cuadrático EKC',
    gdp_growth_rate         DOUBLE                    COMMENT 'Tasa crecimiento YoY (%)',
    -- Población (interpolación lineal mensual — ahora poblada desde bronze sk=20)
    fua_population          BIGINT                    COMMENT 'Población FUA Eurostat (sk=20)',
    population_density      DOUBLE                    COMMENT 'hab/km² (si fua_area_km2 disponible)',
    population_yoy_growth   DOUBLE,
    -- EDGAR CO2 nacional anual (repetido mensualmente)
    co2_country_kt          DOUBLE                    COMMENT 'Emisiones CO2 país (kt CO2) — EDGAR v8',
    -- Electricidad renovable mensual por país (Eurostat sk=16)
    renewables_pct          DOUBLE                    COMMENT '% electricidad renovable mensual (Eurostat)',
    -- Pernoctaciones turísticas mensuales (Eurostat sk=17)
    tourism_nights          DOUBLE                    COMMENT 'Pernoctaciones turísticas mensuales',
    -- Precio EU ETS mensual (NULL 2000-2004: mercado no existía hasta ene-2005)
    ets_price_eur           DOUBLE                    COMMENT 'Precio EU ETS (€/tCO2), NULL 2000-2004',
    _silver_load_date       DATE,
    PRIMARY KEY (city_sk, year, month),
    INDEX idx_eco_date      (date_sk),
    INDEX idx_eco_city_year (city_sk, year),
    INDEX idx_eco_year      (year)
) ENGINE=InnoDB
  COMMENT='Hechos macroeconómicos mensuales por ciudad — GDP + Población + Renovables + Turismo + ETS'
PARTITION BY RANGE (year) (
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- ----------------------------------------------------------------------------
-- FACT_FINANCIAL — empresa × año × mes
-- Fuente: YFinance (datos mensuales nativos)
-- ~100 tickers × 252 meses = 25.200 filas
-- ----------------------------------------------------------------------------
CREATE TABLE fact_financial (
    company_sk              BIGINT          NOT NULL  COMMENT 'FK → dim_company (versión vigente del mes)',
    date_sk                 INT             NOT NULL  COMMENT 'FK → dim_date',
    year                    INT             NOT NULL,
    month                   INT             NOT NULL,
    source_sk               INT                       COMMENT 'FK → dim_source (YFinance)',
    -- Precio y retorno
    close_price             DOUBLE                    COMMENT 'Precio cierre medio del mes (EUR ajustado)',
    monthly_return          DOUBLE                    COMMENT 'Retorno mensual: (P_fin/P_ini)−1',
    -- Riesgo
    monthly_volatility      DOUBLE                    COMMENT 'Volatilidad mensual de retornos diarios',
    current_beta            DOUBLE                    COMMENT 'Beta de mercado (snapshot del mes)',
    -- Valoración
    volume_avg              DOUBLE                    COMMENT 'Volumen diario medio negociado en el mes',
    current_pe              DOUBLE                    COMMENT 'PER trailing del mes',
    _silver_load_date       DATE,
    PRIMARY KEY (company_sk, year, month),
    INDEX idx_ff_date       (date_sk),
    INDEX idx_ff_year       (year)
) ENGINE=InnoDB
  COMMENT='Hechos financieros mensuales por empresa — YFinance (datos nativos mensuales)'
PARTITION BY RANGE (year) (
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- ----------------------------------------------------------------------------
-- FACT_POLICY — país × año × mes
-- Fuentes: OECD (anual repetido) + EDGAR CO2 (interpolado) + InvestEU (anual)
-- ~30 países EU × 252 meses = 7.560 filas
-- ----------------------------------------------------------------------------
CREATE TABLE fact_policy (
    country_code            VARCHAR(10)     NOT NULL  COMMENT 'Clave negocio: ISO-2 del país',
    date_sk                 INT             NOT NULL  COMMENT 'FK → dim_date',
    year                    INT             NOT NULL,
    month                   INT             NOT NULL,
    source_sk_oecd          INT                       COMMENT 'FK → dim_source (OECD)',
    source_sk_edgar         INT                       COMMENT 'FK → dim_source (EDGAR)',
    source_sk_investeu      INT                       COMMENT 'FK → dim_source (InvestEU)',
    -- OECD (anual — mismo valor repetido los 12 meses del año)
    eps_index               DOUBLE                    COMMENT 'Environmental Policy Stringency (0–6)',
    env_tax_usd             DOUBLE                    COMMENT 'Recaudación impuestos ambientales (mill. USD)',
    env_expenditure         DOUBLE                    COMMENT 'Gasto protección ambiental (% PIB)',
    ghg_total_kt            DOUBLE                    COMMENT 'Emisiones GHG totales del país (kt CO2eq)',
    -- EDGAR CO2 (interpolación lineal mensual entre años)
    co2_country_kt          DOUBLE                    COMMENT 'Emisiones CO2 mensuales estimadas (kt)',
    -- InvestEU (anual — mismo valor repetido los 12 meses)
    investeu_ops_count      INT                       COMMENT 'Nº operaciones InvestEU en el año',
    investeu_total_eur      DOUBLE                    COMMENT 'Importe total financiación InvestEU (EUR)',
    _silver_load_date       DATE,
    PRIMARY KEY (country_code, year, month),
    INDEX idx_fp_date       (date_sk),
    INDEX idx_fp_year       (year)
) ENGINE=InnoDB
  COMMENT='Hechos de política ambiental mensuales por país — OECD + EDGAR + InvestEU'
PARTITION BY RANGE (year) (
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- =============================================================================
-- GOLD — FACT TABLES (granularidad mensual, RANGE partition by year)
-- Tablas desnormalizadas listas para consumo directo (API Flask, Power BI)
-- =============================================================================

-- ----------------------------------------------------------------------------
-- FACT_KUZNETS — tabla central EKC (ciudad × año × mes)
-- Integra Silver + resultados de los 4 modelos ML en una sola fila.
-- 237 ciudades × 252 meses = 59.724 filas
-- ----------------------------------------------------------------------------
CREATE TABLE fact_kuznets (
    city_sk                     INT             NOT NULL  COMMENT 'FK → dim_city',
    cluster_sk                  BIGINT          NOT NULL  DEFAULT 0  COMMENT 'FK → dim_cluster (SCD-2)',
    date_sk                     INT             NOT NULL  COMMENT 'FK → dim_date',
    year                        INT             NOT NULL,
    month                       INT             NOT NULL,
    -- Identifiers (desnormalizados para evitar joins en el dashboard)
    city_code                   VARCHAR(100),
    city_name                   VARCHAR(255),
    country_code                VARCHAR(10),
    country_name                VARCHAR(100),
    -- Indicadores ambientales
    ndvi_mean                   DOUBLE,
    ndvi_trend_slope            DOUBLE          COMMENT 'Pendiente OLS del NDVI en la ventana histórica',
    ndvi_yoy_change             DOUBLE,
    no2_mean                    DOUBLE,
    imperviousness_mean         DOUBLE,
    green_index                 DOUBLE,
    temp_mean_c                 DOUBLE,
    co2_country_kt              DOUBLE,
    -- MODIS LST, NTL, Air Quality (from Silver)
    lst_day_c               DOUBLE                    COMMENT 'LST diurna mensual MODIS (°C)',
    lst_night_c             DOUBLE                    COMMENT 'LST nocturna mensual MODIS (°C)',
    ntl_mean                DOUBLE                    COMMENT 'Luces nocturnas normalizadas (nW/cm2/sr equiv.)',
    pm25_mean               DOUBLE                    COMMENT 'PM2.5 medio mensual (µg/m³)',
    pm10_mean               DOUBLE                    COMMENT 'PM10 medio mensual (µg/m³)',
    -- Indicadores económicos
    gdp_pps_per_capita          DOUBLE,
    ln_gdp_pps                  DOUBLE,
    ln_gdp_pps_sq               DOUBLE,
    gdp_growth_rate             DOUBLE,
    fua_population              BIGINT,
    renewables_pct          DOUBLE                    COMMENT '% electricidad renovable mensual (Eurostat)',
    tourism_nights          DOUBLE                    COMMENT 'Pernoctaciones turísticas mensuales',
    ets_price_eur           DOUBLE                    COMMENT 'Precio EU ETS (€/tCO2), NULL 2000-2004',
    -- Clustering (K-Means)
    cluster_id                  INT,
    cluster_label               VARCHAR(100),
    cluster_silhouette          DOUBLE,
    distance_to_centroid        DOUBLE,
    -- EKC Panel Regression (parámetros del cluster)
    ekc_beta1                   DOUBLE,
    ekc_beta2                   DOUBLE,
    ekc_alpha                   DOUBLE,
    ekc_turning_point_y         DOUBLE,
    ekc_r_squared               DOUBLE,
    ekc_shape                   VARCHAR(50),
    -- XGBoost Classifier
    turning_point_phase         VARCHAR(50)     COMMENT 'DEGRADANDO | TURNING | RECUPERANDO',
    phase_confidence            DOUBLE,
    gdp_gap_to_turning          DOUBLE,
    -- Prophet Time Series
    prophet_ndvi_forecast_1y    DOUBLE,
    prophet_ndvi_forecast_3y    DOUBLE,
    prophet_ndvi_forecast_5y    DOUBLE,
    prophet_turning_year        INT,
    prophet_forecast_lower_95   DOUBLE,
    prophet_forecast_upper_95   DOUBLE,
    -- Política ambiental (de fact_policy)
    eps_index                   DOUBLE,
    env_tax_usd                 DOUBLE,
    investeu_total_eur          DOUBLE,
    -- Financiero (agregado por país)
    fin_tickers                 TEXT,
    fin_close_price_avg         DOUBLE,
    fin_annual_volatility       DOUBLE,
    -- Score de inversión compuesto
    investment_score            DOUBLE          COMMENT 'Score 0–100: green_index + phase_confidence + fin_volatility inversa',
    investment_recommendation   VARCHAR(20)     COMMENT 'STRONG_BUY | BUY | HOLD | AVOID',
    _gold_load_date             DATE,
    PRIMARY KEY (city_sk, year, month),
    INDEX idx_fk_cluster        (cluster_sk),
    INDEX idx_fk_date           (date_sk),
    INDEX idx_fk_phase_year     (turning_point_phase, year),
    INDEX idx_fk_score          (investment_score),
    INDEX idx_fk_country_year   (country_code, year),
    INDEX idx_fk_city_code      (city_code)
) ENGINE=InnoDB
  COMMENT='Tabla central EKC — ciudad × mes con ambiental + económico + ML + financiero'
PARTITION BY RANGE (year) (
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- ----------------------------------------------------------------------------
-- CITY_RANKING — ranking europeo mensual de ciudades
-- 237 ciudades × 252 meses = 59.724 filas
-- ----------------------------------------------------------------------------
CREATE TABLE city_ranking (
    city_sk                     INT             NOT NULL  COMMENT 'FK → dim_city',
    date_sk                     INT             NOT NULL  COMMENT 'FK → dim_date',
    year                        INT             NOT NULL,
    month                       INT             NOT NULL,
    rank_position               INT             COMMENT 'Posición en el ranking europeo ese mes',
    rank_within_country         INT             COMMENT 'Posición dentro del país ese mes',
    city_code                   VARCHAR(100),
    city_name                   VARCHAR(255),
    country_code                VARCHAR(10),
    country_name                VARCHAR(100),
    investment_score            DOUBLE,
    investment_recommendation   VARCHAR(20),
    green_index                 DOUBLE,
    turning_point_phase         VARCHAR(50),
    prophet_turning_year        INT,
    rank_change                 INT             COMMENT 'Cambio de posición vs mes anterior (positivo = mejora)',
    score_change                DOUBLE          COMMENT 'Cambio absoluto de investment_score MoM',
    top_ticker                  VARCHAR(20),
    top_company                 VARCHAR(255),
    _gold_load_date             DATE,
    PRIMARY KEY (city_sk, year, month),
    INDEX idx_cr_date           (date_sk),
    INDEX idx_cr_rank           (year, month, rank_position),
    INDEX idx_cr_country        (country_code, year, month),
    INDEX idx_cr_phase          (turning_point_phase, year)
) ENGINE=InnoDB
  COMMENT='Ranking europeo mensual de ciudades por investment score'
PARTITION BY RANGE (year) (
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- ----------------------------------------------------------------------------
-- MODEL_RESULTS — outputs detallados de los 4 modelos ML (ciudad × año × mes)
-- 237 ciudades × 252 meses = 59.724 filas
-- ----------------------------------------------------------------------------
CREATE TABLE model_results (
    city_sk                     INT             NOT NULL  COMMENT 'FK → dim_city',
    cluster_sk                  BIGINT          NOT NULL  DEFAULT 0  COMMENT 'FK → dim_cluster (SCD-2)',
    date_sk                     INT             NOT NULL  DEFAULT 0  COMMENT 'FK → dim_date',
    year                        INT             NOT NULL,
    month                       INT             NOT NULL,
    city_code                   VARCHAR(100),
    country_code                VARCHAR(10),
    -- K-Means Clustering
    kmeans_cluster_id           INT,
    kmeans_cluster_label        VARCHAR(100),
    kmeans_silhouette           DOUBLE,
    kmeans_distance_centroid    DOUBLE,
    kmeans_k_selected           INT,
    -- EKC Panel Regression
    ekc_cluster_id              INT,
    ekc_fitted_ndvi             DOUBLE,
    ekc_residual                DOUBLE,
    ekc_turning_point_y         DOUBLE,
    ekc_distance_to_turning     DOUBLE          COMMENT 'GDP_actual − Y* (>0 = superó turning)',
    -- XGBoost Classifier
    xgb_predicted_phase         VARCHAR(50),
    xgb_prob_degradando         DOUBLE,
    xgb_prob_turning            DOUBLE,
    xgb_prob_recuperando        DOUBLE,
    xgb_confidence              DOUBLE,
    xgb_feature_importance      TEXT            COMMENT 'JSON: top-5 features más importantes',
    -- Prophet Time Series
    prophet_ndvi_actual         DOUBLE,
    prophet_ndvi_fitted         DOUBLE,
    prophet_trend               DOUBLE,
    prophet_seasonality         DOUBLE,
    prophet_forecast_1y         DOUBLE,
    prophet_forecast_3y         DOUBLE,
    prophet_forecast_5y         DOUBLE,
    prophet_lower_95_5y         DOUBLE,
    prophet_upper_95_5y         DOUBLE,
    prophet_turning_year        INT,
    prophet_mape                DOUBLE,
    _model_run_date             DATE,
    _pipeline_version           VARCHAR(50),
    PRIMARY KEY (city_sk, year, month),
    INDEX idx_mr_cluster        (cluster_sk),
    INDEX idx_mr_date           (date_sk),
    INDEX idx_mr_country_year   (country_code, year)
) ENGINE=InnoDB
  COMMENT='Outputs detallados de los 4 modelos ML por ciudad y mes'
PARTITION BY RANGE (year) (
    PARTITION p2004 VALUES LESS THAN (2005),
    PARTITION p2005 VALUES LESS THAN (2006),
    PARTITION p2006 VALUES LESS THAN (2007),
    PARTITION p2007 VALUES LESS THAN (2008),
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- =============================================================================
-- VISTAS — optimizadas para Power BI / Flask API
-- =============================================================================

-- Mapa europeo: último mes disponible con coordenadas
CREATE OR REPLACE VIEW v_powerbi_map AS
SELECT
    fk.city_code,
    fk.city_name,
    fk.country_code,
    fk.country_name,
    dc.lat,
    dc.lon,
    fk.year,
    fk.month,
    fk.investment_score,
    fk.investment_recommendation,
    fk.turning_point_phase,
    fk.green_index,
    fk.ndvi_mean,
    fk.no2_mean,
    fk.gdp_pps_per_capita,
    fk.prophet_turning_year,
    fk.cluster_label,
    fk.temp_mean_c,
    fk.co2_country_kt,
    fk.eps_index,
    fk.investeu_total_eur
FROM fact_kuznets fk
JOIN dim_city dc ON fk.city_sk = dc.city_sk
WHERE fk.year  = (SELECT MAX(year)  FROM fact_kuznets)
  AND fk.month = (SELECT MAX(month) FROM fact_kuznets
                   WHERE year = (SELECT MAX(year) FROM fact_kuznets));

-- Ciudades en fase TURNING (oportunidades de inversión inmediatas)
CREATE OR REPLACE VIEW v_turning_opportunities AS
SELECT
    fk.city_name,
    fk.country_code,
    fk.country_name,
    fk.year,
    fk.month,
    fk.investment_score,
    fk.green_index,
    fk.gdp_pps_per_capita,
    fk.ekc_turning_point_y,
    fk.gdp_gap_to_turning,
    fk.prophet_turning_year,
    fk.phase_confidence,
    fk.fin_tickers,
    fk.investment_recommendation
FROM fact_kuznets fk
WHERE fk.turning_point_phase = 'TURNING'
  AND fk.year  = (SELECT MAX(year)  FROM fact_kuznets)
  AND fk.month = (SELECT MAX(month) FROM fact_kuznets
                   WHERE year = (SELECT MAX(year) FROM fact_kuznets))
ORDER BY fk.investment_score DESC;

-- Top 20 ciudades del último mes
CREATE OR REPLACE VIEW v_top_20_cities AS
SELECT
    cr.rank_position,
    cr.city_code,
    cr.city_name,
    cr.country_code,
    cr.country_name,
    cr.investment_score,
    cr.investment_recommendation,
    cr.green_index,
    cr.turning_point_phase,
    cr.prophet_turning_year,
    cr.rank_change,
    cr.top_ticker,
    cr.top_company
FROM city_ranking cr
WHERE cr.year  = (SELECT MAX(year)  FROM city_ranking)
  AND cr.month = (SELECT MAX(month) FROM city_ranking
                   WHERE year = (SELECT MAX(year) FROM city_ranking))
ORDER BY cr.rank_position ASC
LIMIT 20;

-- Resumen de clusters vigentes (SCD-2: solo versiones is_current = 1)
CREATE OR REPLACE VIEW v_cluster_summary AS
SELECT
    cluster_id,
    cluster_label,
    beta1,
    beta2,
    turning_point_y_star,
    r_squared_adj,
    ekc_shape,
    ekc_hypothesis_supported,
    n_cities,
    valid_from
FROM dim_cluster
WHERE is_current = 1
ORDER BY cluster_id;

-- Distribución de fases por país y mes
CREATE OR REPLACE VIEW v_phase_distribution AS
SELECT
    year,
    month,
    country_code,
    country_name,
    turning_point_phase,
    COUNT(*)                        AS n_cities,
    ROUND(AVG(investment_score), 2) AS avg_score,
    ROUND(AVG(green_index),      4) AS avg_green_index
FROM fact_kuznets
WHERE turning_point_phase IS NOT NULL
GROUP BY year, month, country_code, country_name, turning_point_phase
ORDER BY year DESC, month DESC, country_code, turning_point_phase;

-- Historial de cambios de cluster por ciudad (usando SCD-2 de dim_cluster)
CREATE OR REPLACE VIEW v_cluster_transitions AS
SELECT
    dc.city_code,
    dc.city_name,
    dc.country_code,
    fk.cluster_id,
    fk.cluster_label,
    cl.valid_from   AS cluster_version_from,
    cl.valid_to     AS cluster_version_to,
    cl.ekc_shape,
    cl.turning_point_y_star
FROM fact_kuznets fk
JOIN dim_city    dc ON fk.city_sk    = dc.city_sk
JOIN dim_cluster cl ON fk.cluster_sk = cl.cluster_sk
ORDER BY dc.city_code, cl.valid_from;

-- =============================================================================
-- FIN DW DDL
-- Tablas Silver : 5 dims + 4 facts = 9 tablas
-- Tablas Gold   : 3 facts = 3 tablas
-- Vistas        : 6
-- Volumetría    : ~332.000 filas totales (2004-2024, granularidad mensual)
-- SCD-2         : dim_company, dim_cluster
-- SCD-1         : dim_city, dim_source
-- Estática      : dim_date
-- Partición     : RANGE por year (Tipo 1) en las 7 fact tables
-- =============================================================================
