-- =============================================================================
-- GTP (Green Turning Point) — GOLD LAYER DDL
-- Capa analítica final: tablas pre-agregadas listas para dashboard, API y modelos.
-- No hay claves foráneas explícitas (desnormalizadas para máxima velocidad de lectura).
-- Motor: Apache Hive sobre HDFS (Lorca cluster UEM)
-- Formato: Parquet con compresión Snappy
-- Convención HDFS: hdfs:///user/gtp/gold/<tabla>/
-- =============================================================================
-- Flujo de carga: Silver → gold_build.py → Gold
-- Frecuencia de refresco: Batch anual (cuando llegan datos Eurostat del año anterior)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS gtp_gold
  COMMENT 'Capa Gold GTP: tablas analíticas finales, desnormalizadas, listas para consumo'
    LOCATION 'hdfs:///user/gtp/gold';

USE gtp_gold;

-- =============================================================================
-- TABLA 1: FACT_KUZNETS — Tabla central EKC
-- La tabla más importante del proyecto. Integra todas las dimensiones del análisis
-- ambiental-económico en una sola fila por ciudad × año.
-- Incluye el turning point Y* calculado para el cluster al que pertenece la ciudad.
-- Granularidad: ciudad × año
-- Particionado: year/country
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_kuznets (
    -- Identificadores
    city_code               STRING    COMMENT 'Código FUA de la ciudad (ej: Madrid_ES)',
    city_name               STRING    COMMENT 'Nombre legible de la ciudad',
    country_code            STRING    COMMENT 'Código ISO-2 del país',
    country_name            STRING    COMMENT 'Nombre completo del país',

    -- Indicadores ambientales (de Silver/fact_environmental)
    ndvi_mean               DOUBLE    COMMENT 'NDVI medio anual de la ciudad',
    ndvi_trend_slope        DOUBLE    COMMENT 'Pendiente histórica del NDVI (positivo=ciudad reverdeciendo)',
    ndvi_yoy_change         DOUBLE    COMMENT 'Cambio anual absoluto del NDVI',
    no2_mean                DOUBLE    COMMENT 'NO2 troposférico medio anual (mol/m²)',
    imperviousness_mean     DOUBLE    COMMENT 'Impermeabilización media del suelo (%)',
    green_index             DOUBLE    COMMENT 'Índice verde compuesto GTP ∈ [0,1]',

    -- Indicadores económicos (de Silver/fact_economic)
    gdp_pps_per_capita      DOUBLE    COMMENT 'GDP per cápita en PPS del país',
    ln_gdp_pps              DOUBLE    COMMENT 'ln(GDP per cápita PPS) — variable X de la curva EKC',
    ln_gdp_pps_sq           DOUBLE    COMMENT '[ln(GDP)]² — término cuadrático de la curva EKC',
    gdp_growth_rate         DOUBLE    COMMENT 'Tasa de crecimiento real del GDP (%)',
    fua_population          BIGINT    COMMENT 'Población de la FUA',

    -- Clustering (Modelo 1: K-Means)
    cluster_id              INT       COMMENT 'Cluster asignado por K-Means (0..K−1)',
    cluster_label           STRING    COMMENT 'Etiqueta interpretable del cluster (ej: High-Income-Greening)',
    cluster_silhouette      DOUBLE    COMMENT 'Índice Silhouette del punto en su cluster ∈ [−1,1]',
    distance_to_centroid    DOUBLE    COMMENT 'Distancia euclidiana del punto al centroide del cluster',

    -- Parámetros EKC del cluster (Modelo 2: Panel Regression)
    -- Ecuación: ln(NDVI_it) = α + β₁·ln(GDP_it) + β₂·[ln(GDP_it)]² + μᵢ + λₜ
    ekc_beta1               DOUBLE    COMMENT 'Coeficiente lineal β₁ de la EKC para el cluster',
    ekc_beta2               DOUBLE    COMMENT 'Coeficiente cuadrático β₂ de la EKC (debe ser <0 para curva invertida)',
    ekc_alpha               DOUBLE    COMMENT 'Intercepto α del panel EKC',
    ekc_turning_point_y     DOUBLE    COMMENT 'Turning point Y* = exp(−β₁ / 2β₂) en unidades PPS',
    ekc_r_squared           DOUBLE    COMMENT 'R² ajustado de la regresión de panel del cluster',
    ekc_p_value_beta1       DOUBLE    COMMENT 'p-valor del coeficiente β₁',
    ekc_p_value_beta2       DOUBLE    COMMENT 'p-valor del coeficiente β₂',
    ekc_shape               STRING    COMMENT 'Forma de la curva: INVERTED_U / MONOTONIC_DOWN / MONOTONIC_UP / UNCLEAR',

    -- Fase actual de la ciudad (Modelo 3: XGBoost Classifier)
    turning_point_phase     STRING    COMMENT 'Fase EKC: DEGRADANDO / TURNING / RECUPERANDO / UNKNOWN',
    phase_confidence        DOUBLE    COMMENT 'Probabilidad de la clase predicha por XGBoost ∈ [0,1]',
    gdp_gap_to_turning      DOUBLE    COMMENT 'Diferencia GDP_actual − Y* (positivo=ya superó turning point)',

    -- Proyección temporal (Modelo 4: Prophet Time Series)
    prophet_ndvi_forecast_1y    DOUBLE  COMMENT 'Predicción NDVI dentro de 1 año (median forecast)',
    prophet_ndvi_forecast_3y    DOUBLE  COMMENT 'Predicción NDVI dentro de 3 años',
    prophet_ndvi_forecast_5y    DOUBLE  COMMENT 'Predicción NDVI dentro de 5 años',
    prophet_turning_year        INT     COMMENT 'Año estimado en que la ciudad alcanzará el Turning Point (NDVI estabilizado)',
    prophet_forecast_lower_95   DOUBLE  COMMENT 'Límite inferior intervalo 95% de la proyección 5Y',
    prophet_forecast_upper_95   DOUBLE  COMMENT 'Límite superior intervalo 95% de la proyección 5Y',

    -- Indicadores financieros del país (de Silver/fact_financial)
    fin_tickers             STRING    COMMENT 'Lista de tickers del país en el año (JSON array como STRING)',
    fin_companies           STRING    COMMENT 'Lista de nombres de empresa del país (JSON array)',
    fin_close_price_avg     DOUBLE    COMMENT 'Precio medio del sector verde del país',
    fin_annual_volatility   DOUBLE    COMMENT 'Volatilidad media del sector verde del país',

    -- Score de inversión compuesto GTP
    investment_score        DOUBLE    COMMENT 'Score 0–100: combina green_index + phase_confidence + fin_volatility inversa',
    investment_recommendation STRING  COMMENT 'Recomendación: STRONG_BUY / BUY / HOLD / AVOID',

    -- Metadata de carga
    _gold_load_date         STRING    COMMENT 'Fecha de carga en la capa Gold'
)
COMMENT 'Tabla central EKC de GTP: ciudad × año con todos los indicadores ambientales, económicos, de modelos ML y financieros'
PARTITIONED BY (
    year    INT     COMMENT 'Año de la observación',
    country STRING  COMMENT 'Código ISO-2 del país'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/gold/fact_kuznets'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'primary_key_business' = 'city_code,year,country'
);

-- =============================================================================
-- TABLA 2: CITY_RANKING — Ranking europeo de ciudades por índice verde
-- Granularidad: ciudad × año, ordenada por investment_score (snapshot anual)
-- Uso: Dashboard principal, exportación a API Flask
-- Particionado: solo por year (la tabla completa se lee siempre para el ranking)
-- =============================================================================
CREATE TABLE IF NOT EXISTS city_ranking (
    rank_position           INT       COMMENT 'Posición en el ranking europeo ese año (1 = mejor)',
    rank_within_country     INT       COMMENT 'Posición dentro del país ese año',
    city_code               STRING    COMMENT 'Código FUA de la ciudad',
    city_name               STRING    COMMENT 'Nombre de la ciudad',
    country_code            STRING    COMMENT 'Código ISO-2 del país',
    country_name            STRING    COMMENT 'Nombre del país',
    -- KPIs del ranking
    investment_score        DOUBLE    COMMENT 'Score compuesto 0–100 (mismo que en fact_kuznets)',
    investment_recommendation STRING  COMMENT 'Recomendación GTP',
    green_index             DOUBLE    COMMENT 'Índice verde compuesto ∈ [0,1]',
    turning_point_phase     STRING    COMMENT 'Fase EKC actual de la ciudad',
    prophet_turning_year    INT       COMMENT 'Año estimado de turning point',
    -- Variación respecto al año anterior
    rank_change             INT       COMMENT 'Cambio de posición YoY (positivo = mejora)',
    score_change            DOUBLE    COMMENT 'Cambio absoluto de investment_score YoY',
    -- Top tickers del país
    top_ticker              STRING    COMMENT 'Ticker principal recomendado del país',
    top_company             STRING    COMMENT 'Nombre de la empresa principal recomendada',
    -- Metadata
    _gold_load_date         STRING
)
COMMENT 'Ranking europeo de ciudades por score de inversión verde — snapshot anual'
PARTITIONED BY (
    year INT
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/gold/city_ranking'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =============================================================================
-- TABLA 3: EKC_PARAMETERS — Parámetros EKC estimados por cluster
-- Granularidad: cluster × año de estimación
-- Sin partición: muy pequeña (K clusters × pocos años de re-estimación)
-- Uso: Interpretación académica, documentación de modelos, explicabilidad
-- =============================================================================
CREATE TABLE IF NOT EXISTS ekc_parameters (
    cluster_id              INT       COMMENT 'ID del cluster K-Means',
    cluster_label           STRING    COMMENT 'Etiqueta interpretable del cluster',
    estimation_year         INT       COMMENT 'Año en que se re-estimó el modelo de panel',
    -- Descripción del cluster
    n_cities                INT       COMMENT 'Número de ciudades en el cluster',
    n_observations          INT       COMMENT 'Total de observaciones ciudad×año usadas en la regresión',
    city_codes              STRING    COMMENT 'Lista JSON de city_codes en el cluster',
    countries_represented   STRING    COMMENT 'Lista JSON de países con al menos 1 ciudad en el cluster',
    -- Estadísticos descriptivos del cluster
    gdp_pps_mean            DOUBLE    COMMENT 'GDP PPS medio del cluster (para interpretar posición en EKC)',
    gdp_pps_std             DOUBLE    COMMENT 'Desviación estándar del GDP en el cluster',
    ndvi_mean               DOUBLE    COMMENT 'NDVI medio del cluster',
    -- Parámetros EKC estimados (Panel Regression via linearmodels/statsmodels)
    -- Ecuación: ln(NDVI_it) = α + β₁·ln(GDP_it) + β₂·[ln(GDP_it)]² + μᵢ + λₜ
    alpha                   DOUBLE    COMMENT 'Intercepto del modelo de panel',
    beta1                   DOUBLE    COMMENT 'Coeficiente lineal β₁ (debe ser >0 para curva tipo EKC)',
    beta2                   DOUBLE    COMMENT 'Coeficiente cuadrático β₂ (debe ser <0 para curva EKC invertida)',
    beta1_se                DOUBLE    COMMENT 'Error estándar de β₁',
    beta2_se                DOUBLE    COMMENT 'Error estándar de β₂',
    beta1_pvalue            DOUBLE    COMMENT 'p-valor β₁ (< 0.05 = estadísticamente significativo)',
    beta2_pvalue            DOUBLE    COMMENT 'p-valor β₂',
    -- Turning Point
    -- Derivado de EKC cuadrática: Y* = exp(-β₁ / 2β₂)
    turning_point_y_star    DOUBLE    COMMENT 'GDP per cápita PPS en el turning point: exp(−β₁/2β₂)',
    turning_point_ln_y      DOUBLE    COMMENT 'ln(Y*) — punto de inflexión en escala logarítmica',
    -- Bondad de ajuste
    r_squared               DOUBLE    COMMENT 'R² del modelo de panel',
    r_squared_adj           DOUBLE    COMMENT 'R² ajustado (penaliza complejidad)',
    f_statistic             DOUBLE    COMMENT 'Estadístico F del modelo conjunto',
    f_pvalue                DOUBLE    COMMENT 'p-valor del estadístico F',
    aic                     DOUBLE    COMMENT 'Akaike Information Criterion (menor = mejor)',
    bic                     DOUBLE    COMMENT 'Bayesian Information Criterion',
    -- Evaluación cualitativa
    ekc_hypothesis_supported BOOLEAN  COMMENT 'TRUE si β₁>0, β₂<0, ambos significativos al 5%',
    ekc_shape               STRING    COMMENT 'Forma: INVERTED_U / MONOTONIC_DOWN / MONOTONIC_UP / UNCLEAR',
    -- Metadata
    _estimation_timestamp   STRING    COMMENT 'Timestamp UTC de la estimación del modelo',
    _model_version          STRING    COMMENT 'Versión del script de regresión (para reproducibilidad)'
)
COMMENT 'Parámetros EKC estimados por cluster: β₁, β₂, Y*, R² y diagnósticos del panel regression'
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/gold/ekc_parameters'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =============================================================================
-- TABLA 4: MODEL_RESULTS — Predicciones detalladas de todos los modelos ML
-- Granularidad: ciudad × año
-- Une en una sola tabla los outputs de los 4 modelos para facilitar comparación
-- y alimentar la API Flask con respuesta completa.
-- Particionado: year/country
-- =============================================================================
CREATE TABLE IF NOT EXISTS model_results (
    city_code               STRING    COMMENT 'Código FUA de la ciudad',
    city_name               STRING    COMMENT 'Nombre de la ciudad',
    country_code            STRING    COMMENT 'Código ISO-2 del país',

    -- Modelo 1: K-Means Clustering
    kmeans_cluster_id       INT       COMMENT 'Cluster asignado',
    kmeans_cluster_label    STRING    COMMENT 'Etiqueta del cluster',
    kmeans_silhouette       DOUBLE    COMMENT 'Índice Silhouette del punto ∈ [−1,1]',
    kmeans_distance_centroid DOUBLE   COMMENT 'Distancia euclidiana al centroide',
    kmeans_k_selected       INT       COMMENT 'Número K de clusters elegido (por mayor silhouette)',

    -- Modelo 2: EKC Regression
    ekc_cluster_id          INT       COMMENT 'Cluster usado para la regresión de panel',
    ekc_fitted_ndvi         DOUBLE    COMMENT 'Valor NDVI ajustado por el modelo EKC en este año',
    ekc_residual            DOUBLE    COMMENT 'Residual: NDVI_real − NDVI_fitted',
    ekc_turning_point_y     DOUBLE    COMMENT 'Y* del cluster (el mismo para todas las ciudades del cluster)',
    ekc_distance_to_turning DOUBLE    COMMENT 'GDP_actual − Y* (positivo=pasó turning, negativo=aún no)',

    -- Modelo 3: XGBoost Classifier
    xgb_predicted_phase     STRING    COMMENT 'Fase predicha: DEGRADANDO / TURNING / RECUPERANDO',
    xgb_prob_degradando     DOUBLE    COMMENT 'Probabilidad clase DEGRADANDO',
    xgb_prob_turning        DOUBLE    COMMENT 'Probabilidad clase TURNING',
    xgb_prob_recuperando    DOUBLE    COMMENT 'Probabilidad clase RECUPERANDO',
    xgb_confidence          DOUBLE    COMMENT 'Probabilidad de la clase predicha (max de las 3)',
    xgb_feature_importance  STRING    COMMENT 'JSON: top-5 features más importantes para esta predicción',

    -- Modelo 4: Prophet Time Series
    prophet_ndvi_actual     DOUBLE    COMMENT 'NDVI real del año (para validar el modelo)',
    prophet_ndvi_fitted     DOUBLE    COMMENT 'NDVI ajustado in-sample por Prophet',
    prophet_trend           DOUBLE    COMMENT 'Componente de tendencia de Prophet para este año',
    prophet_seasonality     DOUBLE    COMMENT 'Componente estacional anual de Prophet',
    prophet_forecast_1y     DOUBLE    COMMENT 'Predicción NDVI en year+1 (mediana)',
    prophet_forecast_3y     DOUBLE    COMMENT 'Predicción NDVI en year+3 (mediana)',
    prophet_forecast_5y     DOUBLE    COMMENT 'Predicción NDVI en year+5 (mediana)',
    prophet_lower_95_5y     DOUBLE    COMMENT 'Límite inferior IC 95% de forecast a 5 años',
    prophet_upper_95_5y     DOUBLE    COMMENT 'Límite superior IC 95% de forecast a 5 años',
    prophet_turning_year    INT       COMMENT 'Año proyectado en que el NDVI se estabiliza (turning estimado)',
    prophet_mape            DOUBLE    COMMENT 'MAPE in-sample del modelo Prophet (%)',

    -- Metadata de producción
    _model_run_date         STRING    COMMENT 'Fecha UTC en que se ejecutaron los modelos',
    _pipeline_version       STRING    COMMENT 'Versión del pipeline ML (para reproducibilidad)'
)
COMMENT 'Resultados detallados de los 4 modelos ML por ciudad y año — K-Means + EKC + XGBoost + Prophet'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/gold/model_results'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =============================================================================
-- VISTAS DE UTILIDAD GOLD
-- Para la API Flask y dashboards — queries frecuentes pre-construidas
-- =============================================================================

-- Vista: Top 20 ciudades europeas con mejor investment score del último año disponible
CREATE OR REPLACE VIEW v_top_20_cities AS
SELECT
    rank_position,
    city_name,
    country_name,
    investment_score,
    investment_recommendation,
    green_index,
    turning_point_phase,
    prophet_turning_year,
    top_ticker
FROM city_ranking
WHERE year = (SELECT MAX(year) FROM city_ranking)
ORDER BY rank_position ASC
LIMIT 20;

-- Vista: Ciudades en fase TURNING del año más reciente (oportunidades inmediatas)
CREATE OR REPLACE VIEW v_turning_opportunities AS
SELECT
    fk.city_name,
    fk.country_name,
    fk.year,
    fk.investment_score,
    fk.green_index,
    fk.gdp_pps_per_capita,
    fk.ekc_turning_point_y,
    fk.prophet_turning_year,
    fk.fin_tickers,
    fk.investment_recommendation
FROM fact_kuznets fk
WHERE fk.turning_point_phase = 'TURNING'
  AND fk.year = (SELECT MAX(year) FROM fact_kuznets)
ORDER BY fk.investment_score DESC;

-- Vista: Resumen de parámetros EKC por cluster (última estimación)
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

-- =============================================================================
-- FIN GOLD DDL
-- Total tablas: 4
-- Total vistas: 3
-- Tabla principal: fact_kuznets (ciudad × año con EKC completo + ML + finanzas)
-- =============================================================================
-- FLUJO COMPLETO:
--   bronze_ingest.py → gtp_bronze (6 tablas)
--   silver_transform.py → gtp_silver (4 dims + 3 facts)
--   gold_build.py → gtp_gold (4 tablas)
--   Los 4 modelos ML escriben resultados en model_results y fact_kuznets
-- =============================================================================
