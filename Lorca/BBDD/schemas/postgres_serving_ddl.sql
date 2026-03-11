-- =============================================================================
-- GTP (Green Turning Point) — PostgreSQL Serving Layer DDL
-- =============================================================================
-- Propósito: Capa de servicio para el cuadro de mando y la API Flask.
--            Se alimenta desde el pipeline Spark en Lorca vía export_to_postgres.py.
--
-- Arquitectura:
--   HDFS Gold (Hive/Parquet)
--       └── export_to_postgres.py (Spark JDBC o pandas local)
--               └── PostgreSQL schema gtp.*
--                       └── Flask API / Power BI / Tableau / Grafana
--
-- Por qué PostgreSQL además de Hive/HDFS:
--   - Hive/HDFS: latencia ~segundos, ideal para cómputo batch Spark
--   - PostgreSQL: latencia <10ms con índices, ideal para dashboards y APIs REST
--   - Volumen tiny (237 ciudades × ~10 años = ~2.370 filas en fact_kuznets)
--
-- Ejecución:
--   psql -U postgres -d gtp -f postgres_serving_ddl.sql
--   O desde Python: pd.read_sql / sqlalchemy / psycopg2
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gtp;

-- =============================================================================
-- TABLA 1: FACT_KUZNETS — Tabla central EKC (ciudad × año)
-- Fuente: gtp_gold.fact_kuznets (Hive/HDFS)
-- Sirve: /api/city/<city_code>, /api/opportunities, /api/ranking
-- Granularidad: 1 fila por ciudad × año
-- =============================================================================
CREATE TABLE IF NOT EXISTS gtp.fact_kuznets (
    -- Clave primaria compuesta
    city_code               TEXT            NOT NULL,
    year                    INTEGER         NOT NULL,

    -- Identificadores legibles
    city_name               TEXT,
    country_code            TEXT            NOT NULL,
    country_name            TEXT,

    -- Indicadores ambientales (de Silver/fact_environmental)
    ndvi_mean               DOUBLE PRECISION,
    ndvi_trend_slope        DOUBLE PRECISION,   -- positivo = ciudad reverdeciendo
    ndvi_yoy_change         DOUBLE PRECISION,
    no2_mean                DOUBLE PRECISION,   -- mol/m², rango EU: 5e-5 a 2e-4
    imperviousness_mean     DOUBLE PRECISION,   -- % suelo sellado [0-100]
    green_index             DOUBLE PRECISION,   -- índice compuesto GTP ∈ [0,1]

    -- Indicadores económicos (de Silver/fact_economic)
    gdp_pps_per_capita      DOUBLE PRECISION,   -- GDP en PPS (EUR/hab)
    ln_gdp_pps              DOUBLE PRECISION,   -- ln(GDP) — variable X de la curva EKC
    ln_gdp_pps_sq           DOUBLE PRECISION,   -- [ln(GDP)]² — término cuadrático EKC
    gdp_growth_rate         DOUBLE PRECISION,   -- crecimiento real YoY (%)
    fua_population          BIGINT,

    -- Clustering — Modelo 1: K-Means
    cluster_id              INTEGER,
    cluster_label           TEXT,               -- ej: "High-Income-Greening"
    cluster_silhouette      DOUBLE PRECISION,   -- ∈ [-1,1]
    distance_to_centroid    DOUBLE PRECISION,

    -- EKC Panel Regression — Modelo 2
    -- Ecuación: ln(NDVI_it) = α + β₁·ln(GDP_it) + β₂·[ln(GDP_it)]² + μᵢ + λₜ
    ekc_beta1               DOUBLE PRECISION,   -- debe ser > 0 para curva EKC
    ekc_beta2               DOUBLE PRECISION,   -- debe ser < 0 para curva EKC
    ekc_alpha               DOUBLE PRECISION,
    ekc_turning_point_y     DOUBLE PRECISION,   -- Y* = exp(-β₁/2β₂) en PPS
    ekc_r_squared           DOUBLE PRECISION,
    ekc_p_value_beta1       DOUBLE PRECISION,
    ekc_p_value_beta2       DOUBLE PRECISION,
    ekc_shape               TEXT,               -- INVERTED_U / MONOTONIC_DOWN / ...

    -- XGBoost Classifier — Modelo 3
    turning_point_phase     TEXT,               -- DEGRADANDO / TURNING / RECUPERANDO
    phase_confidence        DOUBLE PRECISION,   -- probabilidad de la clase predicha ∈ [0,1]
    gdp_gap_to_turning      DOUBLE PRECISION,   -- GDP_actual - Y* (positivo = ya superó turning)

    -- Prophet Time Series — Modelo 4
    prophet_ndvi_forecast_1y    DOUBLE PRECISION,
    prophet_ndvi_forecast_3y    DOUBLE PRECISION,
    prophet_ndvi_forecast_5y    DOUBLE PRECISION,
    prophet_turning_year        INTEGER,         -- año estimado en que la ciudad alcanza turning
    prophet_forecast_lower_95   DOUBLE PRECISION,
    prophet_forecast_upper_95   DOUBLE PRECISION,

    -- Indicadores financieros del país
    fin_tickers             TEXT,               -- JSON array como texto: ["IBE.MC","REP.MC"]
    fin_companies           TEXT,               -- JSON array de nombres
    fin_close_price_avg     DOUBLE PRECISION,
    fin_annual_volatility   DOUBLE PRECISION,

    -- Score de inversión compuesto GTP
    -- investment_score = 50·green_index + 30·phase_confidence + bonus_fase
    investment_score        DOUBLE PRECISION,   -- [0, 100]
    investment_recommendation TEXT,             -- STRONG_BUY / BUY / HOLD / AVOID

    -- Metadata de carga
    _gold_load_date         TEXT,

    PRIMARY KEY (city_code, year)
);

CREATE INDEX IF NOT EXISTS idx_fk_country_year  ON gtp.fact_kuznets (country_code, year);
CREATE INDEX IF NOT EXISTS idx_fk_phase_year    ON gtp.fact_kuznets (turning_point_phase, year);
CREATE INDEX IF NOT EXISTS idx_fk_score         ON gtp.fact_kuznets (investment_score DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_fk_year          ON gtp.fact_kuznets (year);
CREATE INDEX IF NOT EXISTS idx_fk_cluster       ON gtp.fact_kuznets (cluster_id, year);


-- =============================================================================
-- TABLA 2: CITY_RANKING — Ranking europeo snapshot anual
-- Fuente: gtp_gold.city_ranking (Hive/HDFS)
-- Sirve: /api/ranking
-- =============================================================================
CREATE TABLE IF NOT EXISTS gtp.city_ranking (
    year                    INTEGER         NOT NULL,
    rank_position           INTEGER         NOT NULL,
    rank_within_country     INTEGER,
    city_code               TEXT            NOT NULL,
    city_name               TEXT,
    country_code            TEXT,
    country_name            TEXT,
    investment_score        DOUBLE PRECISION,
    investment_recommendation TEXT,
    green_index             DOUBLE PRECISION,
    turning_point_phase     TEXT,
    prophet_turning_year    INTEGER,
    rank_change             INTEGER,        -- positivo = mejora de posición YoY
    score_change            DOUBLE PRECISION,
    top_ticker              TEXT,
    top_company             TEXT,
    _gold_load_date         TEXT,

    PRIMARY KEY (year, city_code)
);

CREATE INDEX IF NOT EXISTS idx_cr_year_rank ON gtp.city_ranking (year, rank_position ASC);
CREATE INDEX IF NOT EXISTS idx_cr_country   ON gtp.city_ranking (country_code, year);


-- =============================================================================
-- TABLA 3: EKC_PARAMETERS — Parámetros EKC estimados por cluster
-- Fuente: gtp_gold.ekc_parameters (Hive/HDFS)
-- Sirve: /api/clusters
-- =============================================================================
CREATE TABLE IF NOT EXISTS gtp.ekc_parameters (
    cluster_id              INTEGER         NOT NULL,
    estimation_year         INTEGER         NOT NULL,
    cluster_label           TEXT,
    n_cities                INTEGER,
    n_observations          INTEGER,
    city_codes              TEXT,           -- JSON array de city_codes en el cluster
    countries_represented   TEXT,           -- JSON array de países

    -- Estadísticos del cluster
    gdp_pps_mean            DOUBLE PRECISION,
    gdp_pps_std             DOUBLE PRECISION,
    ndvi_mean               DOUBLE PRECISION,

    -- Parámetros estimados del panel EKC
    alpha                   DOUBLE PRECISION,
    beta1                   DOUBLE PRECISION,   -- > 0 para EKC confirmada
    beta2                   DOUBLE PRECISION,   -- < 0 para EKC confirmada
    beta1_se                DOUBLE PRECISION,
    beta2_se                DOUBLE PRECISION,
    beta1_pvalue            DOUBLE PRECISION,
    beta2_pvalue            DOUBLE PRECISION,

    -- Turning point Y* = exp(-β₁ / 2β₂)
    turning_point_y_star    DOUBLE PRECISION,   -- en PPS (EUR/hab)
    turning_point_ln_y      DOUBLE PRECISION,

    -- Bondad de ajuste
    r_squared               DOUBLE PRECISION,
    r_squared_adj           DOUBLE PRECISION,
    f_statistic             DOUBLE PRECISION,
    f_pvalue                DOUBLE PRECISION,
    aic                     DOUBLE PRECISION,
    bic                     DOUBLE PRECISION,

    -- Diagnóstico
    ekc_hypothesis_supported BOOLEAN,
    ekc_shape               TEXT,           -- INVERTED_U / MONOTONIC_DOWN / ...
    _estimation_timestamp   TEXT,
    _model_version          TEXT,

    PRIMARY KEY (cluster_id, estimation_year)
);


-- =============================================================================
-- TABLA 4: MODEL_RESULTS — Outputs detallados de los 4 modelos ML
-- Fuente: gtp_gold.model_results (Hive/HDFS)
-- Sirve: /api/city/<city_code> (detalle ML completo)
-- =============================================================================
CREATE TABLE IF NOT EXISTS gtp.model_results (
    city_code               TEXT            NOT NULL,
    year                    INTEGER         NOT NULL,
    city_name               TEXT,
    country_code            TEXT,

    -- K-Means
    kmeans_cluster_id       INTEGER,
    kmeans_cluster_label    TEXT,
    kmeans_silhouette       DOUBLE PRECISION,
    kmeans_distance_centroid DOUBLE PRECISION,
    kmeans_k_selected       INTEGER,

    -- EKC Regression
    ekc_cluster_id          INTEGER,
    ekc_fitted_ndvi         DOUBLE PRECISION,
    ekc_residual            DOUBLE PRECISION,
    ekc_turning_point_y     DOUBLE PRECISION,
    ekc_distance_to_turning DOUBLE PRECISION,

    -- XGBoost
    xgb_predicted_phase     TEXT,
    xgb_prob_degradando     DOUBLE PRECISION,
    xgb_prob_turning        DOUBLE PRECISION,
    xgb_prob_recuperando    DOUBLE PRECISION,
    xgb_confidence          DOUBLE PRECISION,
    xgb_feature_importance  TEXT,           -- JSON top-5 features

    -- Prophet
    prophet_ndvi_actual     DOUBLE PRECISION,
    prophet_ndvi_fitted     DOUBLE PRECISION,
    prophet_trend           DOUBLE PRECISION,
    prophet_seasonality     DOUBLE PRECISION,
    prophet_forecast_1y     DOUBLE PRECISION,
    prophet_forecast_3y     DOUBLE PRECISION,
    prophet_forecast_5y     DOUBLE PRECISION,
    prophet_lower_95_5y     DOUBLE PRECISION,
    prophet_upper_95_5y     DOUBLE PRECISION,
    prophet_turning_year    INTEGER,
    prophet_mape            DOUBLE PRECISION,   -- error in-sample (%) — <20% es bueno

    _model_run_date         TEXT,
    _pipeline_version       TEXT,

    PRIMARY KEY (city_code, year)
);

CREATE INDEX IF NOT EXISTS idx_mr_country_year ON gtp.model_results (country_code, year);


-- =============================================================================
-- VISTAS PARA LA API FLASK
-- Queries pre-construidas para los endpoints más frecuentes.
-- =============================================================================

-- Vista: ciudades en fase TURNING del último año disponible (oportunidades inmediatas)
CREATE OR REPLACE VIEW gtp.v_turning_opportunities AS
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
FROM gtp.fact_kuznets fk
WHERE fk.turning_point_phase = 'TURNING'
  AND fk.year = (SELECT MAX(year) FROM gtp.fact_kuznets)
ORDER BY fk.investment_score DESC NULLS LAST;


-- Vista: Resumen EKC por cluster (última estimación)
CREATE OR REPLACE VIEW gtp.v_ekc_summary AS
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
FROM gtp.ekc_parameters
WHERE estimation_year = (SELECT MAX(estimation_year) FROM gtp.ekc_parameters);


-- Vista: Top 20 ciudades del último año por investment_score
CREATE OR REPLACE VIEW gtp.v_top_20_cities AS
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
FROM gtp.city_ranking
WHERE year = (SELECT MAX(year) FROM gtp.city_ranking)
ORDER BY rank_position ASC
LIMIT 20;


-- Vista: Distribución de fases por país y año (para gráficos de tarta/barras)
CREATE OR REPLACE VIEW gtp.v_phase_distribution AS
SELECT
    year,
    country_code,
    country_name,
    turning_point_phase,
    COUNT(*)                        AS n_cities,
    ROUND(AVG(investment_score)::numeric, 2)    AS avg_score,
    ROUND(AVG(green_index)::numeric, 4)         AS avg_green_index
FROM gtp.fact_kuznets
WHERE turning_point_phase IS NOT NULL
GROUP BY year, country_code, country_name, turning_point_phase
ORDER BY year, country_code, turning_point_phase;

-- =============================================================================
-- FIN PostgreSQL SERVING DDL
-- Total tablas: 4 (espejo del Gold HDFS)
-- Total vistas: 4
-- Frecuencia de refresco: Batch (tras pipeline Lorca → export_to_postgres.py)
-- =============================================================================
