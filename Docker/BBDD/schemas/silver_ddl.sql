-- =============================================================================
-- GTP (Green Turning Point) — SILVER LAYER DDL
-- Capa de datos limpios: Star Schema Kimball sobre datos Bronze curados.
-- Incluye dimensiones con SCD y tablas de hechos (facts) normalizadas.
-- Motor: Apache Hive sobre HDFS (Lorca cluster UEM)
-- Formato: Parquet con compresión Snappy
-- Convención HDFS: hdfs:///user/gtp/silver/<tabla>/
-- =============================================================================
-- NOTA SCD:
--   Tipo 1 (Overwrite): dim_city, dim_source — atributos estables, no necesitan historial
--   Tipo 2 (Versioning): dim_company — sectores cambian (ej: Repsol: Oil→Energy)
--   Estática: dim_date — pre-generada, no cambia
-- =============================================================================

CREATE DATABASE IF NOT EXISTS gtp_silver
  COMMENT 'Capa Silver GTP: Star Schema Kimball, datos curados y normalizados'
  LOCATION 'hdfs:///user/gtp/silver';

USE gtp_silver;

-- =============================================================================
-- DIMENSIÓN 1: DIM_CITY — SCD Tipo 1 (Overwrite)
-- Catálogo de ciudades FUA europeas con coordenadas y metadatos geográficos.
-- SCD-1: Las coordenadas y el país no cambian. Si hay corrección, se sobreescribe.
-- Clave de negocio: city_code (ej: Madrid_ES)
-- Particionado: por country (evita small files problem en NameNode)
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_city (
    city_sk             BIGINT    COMMENT 'Surrogate Key — clave técnica autoincremental',
    city_code           STRING    COMMENT 'Clave de negocio: NombreCiudad_PaísISO (ej: Madrid_ES)',
    city_name           STRING    COMMENT 'Nombre legible de la ciudad (ej: Madrid)',
    country_code        STRING    COMMENT 'Código ISO-2 del país (ej: ES)',
    country_name        STRING    COMMENT 'Nombre completo del país (ej: Spain)',
    nuts_code           STRING    COMMENT 'Código NUTS-3 si disponible (ej: ES300 — Madrid)',
    eurostat_city_code  STRING    COMMENT 'Código Eurostat de la FUA (ej: ES001C)',
    lat                 DOUBLE    COMMENT 'Latitud del centroide de la FUA',
    lon                 DOUBLE    COMMENT 'Longitud del centroide de la FUA',
    fua_area_km2        DOUBLE    COMMENT 'Superficie de la Functional Urban Area en km²',
    -- Metadata SCD-1
    _last_updated       STRING    COMMENT 'Última fecha de actualización del registro'
)
COMMENT 'Dimensión Ciudad — SCD Tipo 1 — catálogo de FUAs europeas'
PARTITIONED BY (
    country STRING  COMMENT 'Código ISO-2 del país (partición física)'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/silver/dim_city'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'scd_type'            = '1'
);

-- =============================================================================
-- DIMENSIÓN 2: DIM_COMPANY — SCD Tipo 2 (Versioning)
-- Catálogo de empresas del portafolio Green Deal Europe.
-- SCD-2: Los sectores de las empresas cambian históricamente.
-- Ej: Repsol pasó de "Oil & Gas" a "Energy" en la taxonomía ESG.
-- Clave de negocio: ticker
-- valid_from/valid_to delimitan el período de validez de cada versión.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_company (
    company_sk          BIGINT    COMMENT 'Surrogate Key — único por versión',
    ticker              STRING    COMMENT 'Clave de negocio: símbolo bursátil (ej: IBE.MC)',
    company_name        STRING    COMMENT 'Nombre corto de la empresa',
    sector              STRING    COMMENT 'Sector GICS (ej: Energy, Utilities)',
    industry            STRING    COMMENT 'Industria GICS (ej: Renewable Electricity)',
    country_name        STRING    COMMENT 'País de cotización (nombre completo)',
    stock_exchange      STRING    COMMENT 'Bolsa donde cotiza (ej: MC, PA, DE, L)',
    esg_classification  STRING    COMMENT 'Clasificación ESG interna GTP (GREEN/TRANSITION/MONITOR)',
    -- SCD-2 Control Columns
    valid_from          STRING    COMMENT 'Fecha desde la que esta versión es válida (YYYY-MM-DD)',
    valid_to            STRING    COMMENT 'Fecha hasta la que esta versión es válida (9999-12-31 si activa)',
    is_current          BOOLEAN   COMMENT 'TRUE si es la versión vigente del registro'
)
COMMENT 'Dimensión Empresa — SCD Tipo 2 — portafolio Green Deal Europe con historial de sectores'
PARTITIONED BY (
    country_code STRING  COMMENT 'Código ISO-2 del país de cotización'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/silver/dim_company'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'scd_type'            = '2'
);

-- =============================================================================
-- DIMENSIÓN 3: DIM_DATE — Estática, pre-generada 2000–2030
-- Tabla de calendario para facilitar análisis temporales sin cálculos en SQL.
-- No tiene partición: es pequeña (~11.000 filas) y se lee completamente siempre.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk             INT       COMMENT 'Surrogate Key — formato YYYYMMDD (ej: 20230615)',
    full_date           STRING    COMMENT 'Fecha completa (YYYY-MM-DD)',
    year                INT       COMMENT 'Año (ej: 2023)',
    quarter             INT       COMMENT 'Trimestre (1–4)',
    month               INT       COMMENT 'Mes (1–12)',
    month_name          STRING    COMMENT 'Nombre del mes (January...December)',
    week_of_year        INT       COMMENT 'Semana del año ISO-8601 (1–53)',
    day_of_month        INT       COMMENT 'Día del mes (1–31)',
    day_of_week         INT       COMMENT 'Día de la semana ISO (1=Lunes, 7=Domingo)',
    day_name            STRING    COMMENT 'Nombre del día (Monday...Sunday)',
    is_weekend          BOOLEAN   COMMENT 'TRUE si es sábado o domingo',
    is_leap_year        BOOLEAN   COMMENT 'TRUE si el año es bisiesto',
    fiscal_year         INT       COMMENT 'Año fiscal europeo (igual al año calendario para GTP)',
    semester            INT       COMMENT 'Semestre (1 o 2)',
    -- Rangos utiles para filtros rápidos
    is_satellite_era    BOOLEAN   COMMENT 'TRUE si year >= 2018 (Sentinel-5P operativo)'
)
COMMENT 'Dimensión Fecha — estática, pre-generada 2000–2030'
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/silver/dim_date'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'immutable'           = 'true'
);

-- =============================================================================
-- DIMENSIÓN 4: DIM_SOURCE — SCD Tipo 1
-- Catálogo de fuentes de datos. Documenta cada sistema fuente con sus metadatos.
-- =============================================================================
CREATE TABLE IF NOT EXISTS dim_source (
    source_sk           INT       COMMENT 'Surrogate Key',
    source_code         STRING    COMMENT 'Clave de negocio (ej: GEE_S2, GEE_S5P, Eurostat_GDP)',
    source_name         STRING    COMMENT 'Nombre descriptivo de la fuente',
    source_type         STRING    COMMENT 'Tipo: API_REST, GEE_COLLECTION, CSV_DOWNLOAD, MANUAL',
    provider            STRING    COMMENT 'Organización proveedora (ej: ESA, Eurostat, Yahoo)',
    url_reference       STRING    COMMENT 'URL de documentación oficial de la fuente',
    temporal_coverage   STRING    COMMENT 'Rango temporal disponible (ej: 2018–present)',
    spatial_coverage    STRING    COMMENT 'Cobertura espacial (ej: EU27, Global)',
    update_frequency    STRING    COMMENT 'Frecuencia de actualización (Monthly, Annual, Static)',
    license             STRING    COMMENT 'Licencia de uso (ej: CC BY 4.0, Open Data)',
    -- Metadata SCD-1
    _last_updated       STRING
)
COMMENT 'Dimensión Fuente — SCD Tipo 1 — catálogo de sistemas fuente del pipeline'
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/silver/dim_source'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'scd_type'            = '1'
);

-- =============================================================================
-- FACT TABLE 1: FACT_ENVIRONMENTAL — Indicadores ambientales ciudad × año
-- Granularidad: 1 fila por ciudad × año (datos satelitales anualizados)
-- Deriva de: bronze_sentinel2_raw, bronze_sentinel5p_raw, bronze_hrl_raw
-- Claves foráneas: city_sk, date_sk (year-level), source_sk
-- Particionado: year/country (alinea con Bronze, facilita joins)
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_environmental (
    env_fact_sk             BIGINT    COMMENT 'Surrogate Key del hecho',
    city_sk                 BIGINT    COMMENT 'FK → dim_city.city_sk',
    date_sk                 INT       COMMENT 'FK → dim_date.date_sk (1 enero del año, YYYYMMDD)',
    source_sk_s2            INT       COMMENT 'FK → dim_source (GEE_S2)',
    source_sk_s5p           INT       COMMENT 'FK → dim_source (GEE_S5P)',
    source_sk_hrl           INT       COMMENT 'FK → dim_source (Copernicus_HRL)',
    -- Métricas Sentinel-2 (NDVI anualizado)
    ndvi_annual_mean        DOUBLE    COMMENT 'Media anual del NDVI mensual de la ciudad',
    ndvi_annual_std         DOUBLE    COMMENT 'Variabilidad intra-anual del NDVI',
    ndvi_spring_mean        DOUBLE    COMMENT 'Media NDVI meses de primavera (mar, abr, may)',
    ndvi_summer_mean        DOUBLE    COMMENT 'Media NDVI meses de verano (jun, jul, ago)',
    ndvi_autumn_mean        DOUBLE    COMMENT 'Media NDVI otoño (sep, oct, nov)',
    ndvi_winter_mean        DOUBLE    COMMENT 'Media NDVI invierno (dic, ene, feb)',
    ndvi_valid_months       INT       COMMENT 'Número de meses con datos válidos en el año (max 12)',
    -- Métricas Sentinel-5P (NO2 anualizado)
    no2_annual_mean         DOUBLE    COMMENT 'Media anual de densidad NO2 troposférica (mol/m²)',
    no2_annual_std          DOUBLE    COMMENT 'Variabilidad intra-anual del NO2',
    no2_valid_months        INT       COMMENT 'Número de meses con datos válidos',
    -- Métricas HRL (Impermeabilización del suelo)
    imperviousness_mean     DOUBLE    COMMENT 'Impermeabilización media del área urbana (%)',
    built_up_area_pct       DOUBLE    COMMENT 'Porcentaje de área construida',
    tree_cover_pct          DOUBLE    COMMENT 'Porcentaje de cobertura arbórea',
    -- Indicadores derivados (calculados en silver_transform.py)
    ndvi_yoy_change         DOUBLE    COMMENT 'Cambio absoluto NDVI vs año anterior',
    ndvi_trend_slope        DOUBLE    COMMENT 'Pendiente de regresión NDVI sobre serie completa (β de tiempo)',
    green_index             DOUBLE    COMMENT 'Índice verde compuesto GTP: f(NDVI, NO2, imperviousness) ∈ [0,1]',
    -- Metadata de carga
    _silver_load_date       STRING    COMMENT 'Fecha de carga en Silver'
)
COMMENT 'Hechos ambientales anuales por ciudad — NDVI + NO2 + HRL'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/silver/fact_environmental'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =============================================================================
-- FACT TABLE 2: FACT_ECONOMIC — Indicadores macroeconómicos ciudad × año
-- Granularidad: 1 fila por ciudad × año
-- Deriva de: bronze_eurostat_gdp_raw, bronze_eurostat_population_raw
-- Nota: GDP es a nivel país (NUTS-0); población es a nivel FUA donde disponible
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_economic (
    econ_fact_sk            BIGINT    COMMENT 'Surrogate Key del hecho',
    city_sk                 BIGINT    COMMENT 'FK → dim_city.city_sk',
    date_sk                 INT       COMMENT 'FK → dim_date.date_sk',
    source_sk_gdp           INT       COMMENT 'FK → dim_source (Eurostat_GDP)',
    source_sk_pop           INT       COMMENT 'FK → dim_source (Eurostat_POP)',
    -- GDP (a nivel país NUTS-0, aplicado a todas las ciudades del país)
    gdp_pps_per_capita      DOUBLE    COMMENT 'GDP per cápita en PPS (Purchasing Power Standard)',
    gdp_eur_per_capita      DOUBLE    COMMENT 'GDP per cápita en EUR corrientes',
    gdp_pps_index           DOUBLE    COMMENT 'Índice GDP PPS relativo a la media EU=100',
    ln_gdp_pps              DOUBLE    COMMENT 'ln(gdp_pps_per_capita) — variable clave en EKC',
    ln_gdp_pps_sq           DOUBLE    COMMENT '[ln(gdp_pps_per_capita)]² — término cuadrático EKC',
    gdp_growth_rate         DOUBLE    COMMENT 'Tasa de crecimiento real del GDP YoY (%)',
    -- Población
    fua_population          BIGINT    COMMENT 'Población total de la FUA (de Eurostat si disponible)',
    population_density      DOUBLE    COMMENT 'Densidad de población (hab/km²) si fua_area_km2 conocida',
    population_yoy_growth   DOUBLE    COMMENT 'Crecimiento poblacional YoY (%)',
    -- Metadata
    _silver_load_date       STRING
)
COMMENT 'Hechos macroeconómicos anuales por ciudad — GDP + Población de Eurostat'
PARTITIONED BY (
    year    INT,
    country STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/silver/fact_economic'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =============================================================================
-- FACT TABLE 3: FACT_FINANCIAL — Métricas financieras empresa × año
-- Granularidad: 1 fila por empresa (ticker) × año
-- Deriva de: bronze_finance_raw
-- Nota: No tiene city_sk directo; el join con ciudades se hace vía country_code en Gold
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_financial (
    fin_fact_sk             BIGINT    COMMENT 'Surrogate Key del hecho',
    company_sk              BIGINT    COMMENT 'FK → dim_company.company_sk (versión vigente del año)',
    date_sk                 INT       COMMENT 'FK → dim_date.date_sk',
    source_sk               INT       COMMENT 'FK → dim_source (YFinance)',
    -- Métricas de precio y retorno
    close_price_avg         DOUBLE    COMMENT 'Precio de cierre medio anual ajustado (EUR)',
    annual_return           DOUBLE    COMMENT 'Retorno anual total (%): (P_fin/P_ini)−1',
    -- Métricas de riesgo
    annual_volatility       DOUBLE    COMMENT 'Volatilidad anualizada: σ_diaria × √252',
    current_beta            DOUBLE    COMMENT 'Beta de mercado (snapshot extracción)',
    -- Métricas de valoración
    volume_avg              DOUBLE    COMMENT 'Volumen medio diario negociado',
    current_pe              DOUBLE    COMMENT 'PER trailing (precio/beneficio por acción)',
    -- Metadata
    _silver_load_date       STRING
)
COMMENT 'Hechos financieros anuales por empresa del portafolio Green Deal Europe'
PARTITIONED BY (
    year         INT,
    country_code STRING  COMMENT 'Código ISO-2 del país de cotización'
)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/silver/fact_financial'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- =============================================================================
-- VISTAS DE UTILIDAD SILVER
-- Facilitan queries sin necesidad de recordar los SK internos.
-- =============================================================================

-- Vista: Detalle ambiental con nombre de ciudad y año legible
CREATE OR REPLACE VIEW v_environmental_detail AS
SELECT
    dc.city_name,
    dc.country_name,
    dc.country_code,
    dd.year,
    fe.ndvi_annual_mean,
    fe.no2_annual_mean,
    fe.imperviousness_mean,
    fe.green_index,
    fe.ndvi_trend_slope,
    fe.ndvi_yoy_change
FROM fact_environmental fe
JOIN dim_city dc    ON fe.city_sk  = dc.city_sk
JOIN dim_date dd    ON fe.date_sk  = dd.date_sk;

-- Vista: empresa activa (SCD-2 — solo versiones vigentes)
CREATE OR REPLACE VIEW v_company_current AS
SELECT *
FROM dim_company
WHERE is_current = TRUE;

-- =============================================================================
-- FIN SILVER DDL
-- Total tablas: 7 (4 dimensiones + 3 fact tables)
-- Total vistas: 2
-- SCD-1: dim_city, dim_source
-- SCD-2: dim_company (valid_from/valid_to/is_current)
-- Estática: dim_date
-- =============================================================================
