-- =============================================================================
-- GTP (Green Turning Point) — MariaDB Serving Layer DDL
-- =============================================================================
-- Propósito: Capa de servicio para el cuadro de mando (Power BI / Tableau).
--            Se alimenta desde el pipeline Spark en Lorca vía export_to_postgres.py.
--
-- Arquitectura:
--   HDFS Gold (Hive/Parquet)
--       └── export_to_postgres.py (Spark JDBC o pandas local)
--               └── MariaDB bd_rvm_gtp.*
--                       └── Power BI / Tableau (conexión directa MySQL/MariaDB)
--
-- Tablas: 5 (dim_city + 4 espejo del Gold)
-- Vistas: 5 (optimizadas para los visuales más comunes de Power BI)
--
-- Ejecución:
--   mysql -h 10.151.30.2 -P 3306 -u bd_rvm_gtp -pSol2026A bd_rvm_gtp < mariadb_serving_ddl.sql
-- =============================================================================

USE bd_rvm_gtp;

-- =============================================================================
-- TABLA 0: DIM_CITY — Dimensión ciudad con coordenadas (para mapas Power BI)
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_city (
    city_code       VARCHAR(100)    NOT NULL PRIMARY KEY,
    city_name       VARCHAR(255),
    country_code    VARCHAR(10)     NOT NULL,
    latitude        DOUBLE          NOT NULL,
    longitude       DOUBLE          NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dc_country ON dim_city (country_code);


-- =============================================================================
-- TABLA 1: FACT_KUZNETS — Tabla central EKC (ciudad × año)
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_kuznets (
    city_code               VARCHAR(100)    NOT NULL,
    year                    INTEGER         NOT NULL,

    city_name               VARCHAR(255),
    country_code            VARCHAR(10)     NOT NULL,
    country_name            VARCHAR(100),

    -- Indicadores ambientales
    ndvi_mean               DOUBLE,
    ndvi_trend_slope        DOUBLE,
    ndvi_yoy_change         DOUBLE,
    no2_mean                DOUBLE,
    imperviousness_mean     DOUBLE,
    green_index             DOUBLE,

    -- Indicadores económicos
    gdp_pps_per_capita      DOUBLE,
    ln_gdp_pps              DOUBLE,
    ln_gdp_pps_sq           DOUBLE,
    gdp_growth_rate         DOUBLE,
    fua_population          BIGINT,

    -- Clustering
    cluster_id              INTEGER,
    cluster_label           VARCHAR(100),
    cluster_silhouette      DOUBLE,
    distance_to_centroid    DOUBLE,

    -- EKC Panel Regression
    ekc_beta1               DOUBLE,
    ekc_beta2               DOUBLE,
    ekc_alpha               DOUBLE,
    ekc_turning_point_y     DOUBLE,
    ekc_r_squared           DOUBLE,
    ekc_p_value_beta1       DOUBLE,
    ekc_p_value_beta2       DOUBLE,
    ekc_shape               VARCHAR(50),

    -- XGBoost Classifier
    turning_point_phase     VARCHAR(50),
    phase_confidence        DOUBLE,
    gdp_gap_to_turning      DOUBLE,

    -- Prophet Time Series
    prophet_ndvi_forecast_1y    DOUBLE,
    prophet_ndvi_forecast_3y    DOUBLE,
    prophet_ndvi_forecast_5y    DOUBLE,
    prophet_ndvi_forecast_10y   DOUBLE,
    prophet_turning_year        INTEGER,
    prophet_forecast_lower_95   DOUBLE,
    prophet_forecast_upper_95   DOUBLE,

    -- Climático (ERA5-Land)
    temp_annual_mean_c      DOUBLE,
    precip_annual_sum_m     DOUBLE,

    -- Calidad del aire adicional (Sentinel-5P Aerosol UVAI)
    uvai_annual_mean        DOUBLE,

    -- Emisiones CO2 nacionales (EDGAR v8)
    co2_country_kt          DOUBLE,

    -- Uso de suelo (ESA WorldCover — solo años 2020 y 2021)
    wc_tree_pct             DOUBLE,
    wc_built_pct            DOUBLE,
    wc_crop_pct             DOUBLE,
    wc_natural_pct          DOUBLE,

    -- Política ambiental (OECD)
    eps_index               DOUBLE,     -- Environmental Policy Stringency (0–6)
    env_tax_usd             DOUBLE,     -- Recaudación impuestos ambientales (mill. USD)
    env_expenditure         DOUBLE,     -- Gasto protección ambiental NEEP (% PIB)
    ghg_total_kt            DOUBLE,     -- Emisiones GHG totales del país (kt CO2eq)

    -- Inversión verde EIB/InvestEU
    investeu_ops_count      INTEGER,    -- Nº operaciones InvestEU en el país y año
    investeu_total_eur      DOUBLE,     -- Importe total financiación InvestEU (EUR)

    -- Nuevas fuentes (Bronze sk 13-20) — media/suma mensual por ciudad
    lst_day_c               DOUBLE      COMMENT 'LST diurna mensual MODIS (°C)',
    lst_night_c             DOUBLE      COMMENT 'LST nocturna mensual MODIS (°C)',
    ntl_mean                DOUBLE      COMMENT 'Luces nocturnas normalizadas (DMSP+VIIRS)',
    pm25_mean               DOUBLE      COMMENT 'PM2.5 mensual (µg/m³) — EEA',
    pm10_mean               DOUBLE      COMMENT 'PM10 mensual (µg/m³) — EEA',
    renewables_pct          DOUBLE      COMMENT '% electricidad renovable mensual (país)',
    tourism_nights          DOUBLE      COMMENT 'Pernoctaciones turísticas mensuales',
    ets_price_eur           DOUBLE      COMMENT 'Precio EU ETS mensual (€/tCO2), NULL 2000-2004'

    -- Financiero
    fin_tickers             TEXT,
    fin_companies           TEXT,
    fin_close_price_avg     DOUBLE,
    fin_annual_volatility   DOUBLE,

    -- Score de inversión
    investment_score        DOUBLE,
    investment_recommendation VARCHAR(20),

    _gold_load_date         VARCHAR(30),

    PRIMARY KEY (city_code, year)
);

CREATE INDEX IF NOT EXISTS idx_fk_country_year  ON fact_kuznets (country_code, year);
CREATE INDEX IF NOT EXISTS idx_fk_phase_year    ON fact_kuznets (turning_point_phase, year);
CREATE INDEX IF NOT EXISTS idx_fk_score         ON fact_kuznets (investment_score);
CREATE INDEX IF NOT EXISTS idx_fk_year          ON fact_kuznets (year);
CREATE INDEX IF NOT EXISTS idx_fk_cluster       ON fact_kuznets (cluster_id, year);


-- =============================================================================
-- TABLA 2: CITY_RANKING
-- =============================================================================
CREATE TABLE IF NOT EXISTS city_ranking (
    year                    INTEGER         NOT NULL,
    rank_position           INTEGER         NOT NULL,
    rank_within_country     INTEGER,
    city_code               VARCHAR(100)    NOT NULL,
    city_name               VARCHAR(255),
    country_code            VARCHAR(10),
    country_name            VARCHAR(100),
    investment_score        DOUBLE,
    investment_recommendation VARCHAR(20),
    green_index             DOUBLE,
    turning_point_phase     VARCHAR(50),
    prophet_turning_year    INTEGER,
    rank_change             INTEGER,
    score_change            DOUBLE,
    top_ticker              VARCHAR(20),
    top_company             VARCHAR(255),
    _gold_load_date         VARCHAR(30),

    PRIMARY KEY (year, city_code)
);

CREATE INDEX IF NOT EXISTS idx_cr_year_rank ON city_ranking (year, rank_position);
CREATE INDEX IF NOT EXISTS idx_cr_country   ON city_ranking (country_code, year);


-- =============================================================================
-- TABLA 3: EKC_PARAMETERS
-- =============================================================================
CREATE TABLE IF NOT EXISTS ekc_parameters (
    cluster_id              INTEGER         NOT NULL,
    estimation_year         INTEGER         NOT NULL,
    cluster_label           VARCHAR(100),
    n_cities                INTEGER,
    n_observations          INTEGER,
    city_codes              TEXT,
    countries_represented   TEXT,

    gdp_pps_mean            DOUBLE,
    gdp_pps_std             DOUBLE,
    ndvi_mean               DOUBLE,

    alpha                   DOUBLE,
    beta1                   DOUBLE,
    beta2                   DOUBLE,
    beta1_se                DOUBLE,
    beta2_se                DOUBLE,
    beta1_pvalue            DOUBLE,
    beta2_pvalue            DOUBLE,

    turning_point_y_star    DOUBLE,
    turning_point_ln_y      DOUBLE,

    r_squared               DOUBLE,
    r_squared_adj           DOUBLE,
    f_statistic             DOUBLE,
    f_pvalue                DOUBLE,
    aic                     DOUBLE,
    bic                     DOUBLE,

    ekc_hypothesis_supported TINYINT(1),
    ekc_shape               VARCHAR(50),
    _estimation_timestamp   VARCHAR(30),
    _model_version          VARCHAR(50),

    PRIMARY KEY (cluster_id, estimation_year)
);


-- =============================================================================
-- TABLA 4: MODEL_RESULTS
-- =============================================================================
CREATE TABLE IF NOT EXISTS model_results (
    city_code               VARCHAR(100)    NOT NULL,
    year                    INTEGER         NOT NULL,
    city_name               VARCHAR(255),
    country_code            VARCHAR(10),

    kmeans_cluster_id       INTEGER,
    kmeans_cluster_label    VARCHAR(100),
    kmeans_silhouette       DOUBLE,
    kmeans_distance_centroid DOUBLE,
    kmeans_k_selected       INTEGER,

    ekc_cluster_id          INTEGER,
    ekc_fitted_ndvi         DOUBLE,
    ekc_residual            DOUBLE,
    ekc_turning_point_y     DOUBLE,
    ekc_distance_to_turning DOUBLE,

    xgb_predicted_phase     VARCHAR(50),
    xgb_prob_degradando     DOUBLE,
    xgb_prob_turning        DOUBLE,
    xgb_prob_recuperando    DOUBLE,
    xgb_confidence          DOUBLE,
    xgb_feature_importance  TEXT,

    prophet_ndvi_actual     DOUBLE,
    prophet_ndvi_fitted     DOUBLE,
    prophet_trend           DOUBLE,
    prophet_seasonality     DOUBLE,
    prophet_forecast_1y     DOUBLE,
    prophet_forecast_3y     DOUBLE,
    prophet_forecast_5y     DOUBLE,
    prophet_forecast_10y    DOUBLE,
    prophet_lower_95_5y     DOUBLE,
    prophet_upper_95_5y     DOUBLE,
    prophet_lower_95_10y    DOUBLE,
    prophet_upper_95_10y    DOUBLE,
    prophet_turning_year    INTEGER,
    prophet_mape            DOUBLE,

    _model_run_date         VARCHAR(30),
    _pipeline_version       VARCHAR(50),

    PRIMARY KEY (city_code, year)
);

CREATE INDEX IF NOT EXISTS idx_mr_country_year ON model_results (country_code, year);


-- =============================================================================
-- VISTAS PARA POWER BI
-- =============================================================================

-- Mapa europeo (último año) — usar en visual Mapa de Power BI
CREATE OR REPLACE VIEW v_powerbi_map AS
SELECT
    fk.city_code,
    fk.city_name,
    fk.country_code,
    fk.country_name,
    dc.latitude,
    dc.longitude,
    fk.year,
    fk.investment_score,
    fk.investment_recommendation,
    fk.turning_point_phase,
    fk.green_index,
    fk.ndvi_mean,
    fk.gdp_pps_per_capita,
    fk.prophet_turning_year,
    fk.cluster_label,
    fk.temp_annual_mean_c,
    fk.co2_country_kt,
    fk.wc_natural_pct,
    fk.uvai_annual_mean,
    fk.eps_index,
    fk.investeu_total_eur
FROM fact_kuznets fk
LEFT JOIN dim_city dc ON fk.city_code = dc.city_code
WHERE fk.year = (SELECT MAX(year) FROM fact_kuznets);


-- Ciudades en fase TURNING (oportunidades de inversión)
CREATE OR REPLACE VIEW v_turning_opportunities AS
SELECT
    fk.city_name,
    fk.country_code,
    fk.country_name,
    fk.year,
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
  AND fk.year = (SELECT MAX(year) FROM fact_kuznets)
ORDER BY fk.investment_score IS NULL ASC, fk.investment_score DESC;


-- Resumen EKC por cluster
CREATE OR REPLACE VIEW v_ekc_summary AS
SELECT
    cluster_id,
    cluster_label,
    n_cities,
    n_observations,
    beta1,
    beta2,
    turning_point_y_star,
    r_squared_adj,
    ekc_hypothesis_supported,
    ekc_shape
FROM ekc_parameters
WHERE estimation_year = (SELECT MAX(estimation_year) FROM ekc_parameters);


-- Top 20 ciudades del último año
CREATE OR REPLACE VIEW v_top_20_cities AS
SELECT
    rank_position,
    city_code,
    city_name,
    country_code,
    country_name,
    investment_score,
    investment_recommendation,
    green_index,
    turning_point_phase,
    prophet_turning_year,
    top_ticker,
    top_company
FROM city_ranking
WHERE year = (SELECT MAX(year) FROM city_ranking)
ORDER BY rank_position ASC
LIMIT 20;


-- Distribución de fases por país y año
CREATE OR REPLACE VIEW v_phase_distribution AS
SELECT
    year,
    country_code,
    country_name,
    turning_point_phase,
    COUNT(*)                        AS n_cities,
    ROUND(AVG(investment_score), 2) AS avg_score,
    ROUND(AVG(green_index), 4)      AS avg_green_index
FROM fact_kuznets
WHERE turning_point_phase IS NOT NULL
GROUP BY year, country_code, country_name, turning_point_phase
ORDER BY year, country_code, turning_point_phase;

-- =============================================================================
-- FIN MariaDB SERVING DDL
-- Conexión Power BI:
--   Inicio > Obtener datos > Base de datos > MySQL
--   Servidor: 10.151.30.2:3306   Base de datos: bd_rvm_gtp
-- =============================================================================
