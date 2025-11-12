-- Usar MySQL 8+, motor InnoDB y UTF8MB4
CREATE DATABASE IF NOT EXISTS gtp DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE gtp;

-- ========================
-- DIMENSIONES BÁSICAS
-- ========================

CREATE TABLE dim_country (
  country_id      INT AUTO_INCREMENT PRIMARY KEY,
  iso3            CHAR(3) NOT NULL UNIQUE,
  iso2            CHAR(2) UNIQUE,
  country_name    VARCHAR(100),
  CONSTRAINT chk_iso3_format CHECK (iso3 REGEXP '^[A-Z]{3}$')
) ENGINE=InnoDB;

CREATE TABLE dim_geo (
  geo_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  geo_level       ENUM('country','NUTS','FUA','region','city') NOT NULL,
  geo_code        VARCHAR(64),      -- ej: ES001L2, NUTS3 code, etc.
  geo_name        VARCHAR(200),
  country_id      INT,
  UNIQUE KEY uk_geo (geo_level, geo_code),
  KEY fk_geo_country (country_id),
  CONSTRAINT fk_geo_country FOREIGN KEY (country_id) REFERENCES dim_country(country_id)
) ENGINE=InnoDB;

CREATE TABLE dim_time (
  time_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  freq            ENUM('A','Q','M','D') NOT NULL, -- Annual/Quarterly/Monthly/Daily
  year            SMALLINT,                       -- 2006..2025
  quarter         TINYINT,                        -- 1..4
  month           TINYINT,                        -- 1..12
  day             TINYINT,                        -- 1..31
  date_utc        DATETIME,                       -- normalizado a UTC (sin TZ en MySQL)
  UNIQUE KEY uk_time (freq, year, quarter, month, day)
) ENGINE=InnoDB;

CREATE TABLE dim_unit (
  unit_id         INT AUTO_INCREMENT PRIMARY KEY,
  unit_code       VARCHAR(32) NOT NULL UNIQUE,    -- 'km2','ug/m3','ratio','t','EUR','C', etc.
  unit_name       VARCHAR(100),
  base_unit_code  VARCHAR(32),                    -- para familias (ej. km2 base de área)
  multiplier_to_base DECIMAL(20,10) DEFAULT 1.0   -- para reescalados (si aplica)
) ENGINE=InnoDB;

CREATE TABLE dim_measure (
  measure_id      INT AUTO_INCREMENT PRIMARY KEY,
  measure_code    VARCHAR(64) NOT NULL UNIQUE,    -- snake_case: pm25, ghg_total, gdp_pc_pps, life_satisfaction
  measure_name    VARCHAR(200),
  description     TEXT
) ENGINE=InnoDB;

CREATE TABLE dim_dataset (
  dataset_id      INT AUTO_INCREMENT PRIMARY KEY,
  dataset_code    VARCHAR(64) NOT NULL UNIQUE,    -- ej: EUROSTAT_BLI_1, EEA_AIR, COPERNICUS_TCD
  source_name     VARCHAR(200),                   -- Eurostat, EEA, Copernicus, etc.
  source_url      VARCHAR(500),
  license         VARCHAR(200),
  notes           TEXT
) ENGINE=InnoDB;

-- Demografía / Atributos opcionales (para BLI, educación, etc.)
CREATE TABLE dim_demography (
  demog_id        INT AUTO_INCREMENT PRIMARY KEY,
  sex             ENUM('male','female','other') NULL,
  age_group       VARCHAR(50) NULL,     -- ej. '15-24','25-34', etc.
  work_status     VARCHAR(50) NULL,     -- employed/unemployed/student/etc.
  education_iscED VARCHAR(50) NULL,     -- ISCED-11: '0-2','3-4','5-8', etc.
  other_attrs     JSON NULL,
  UNIQUE KEY uk_demog (sex, age_group, work_status, education_iscED)
) ENGINE=InnoDB;

-- Clasificaciones opcionales (NACE, SIEC, SEEA/separema/ceparema, etc.)
CREATE TABLE dim_code (
  code_id         INT AUTO_INCREMENT PRIMARY KEY,
  code_system     VARCHAR(50) NOT NULL,  -- 'NACE','SIEC','SEEA','CEPA','NUTS', etc.
  code_value      VARCHAR(50) NOT NULL,
  code_label      VARCHAR(200),
  UNIQUE KEY uk_code (code_system, code_value)
) ENGINE=InnoDB;

-- ========================
-- HECHOS (OBSERVACIONES)
-- ========================
CREATE TABLE fact_observation (
  obs_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  dataset_id      INT NOT NULL,
  geo_id          BIGINT NULL,
  country_id      INT NULL,
  time_id         BIGINT NOT NULL,
  measure_id      INT NOT NULL,
  unit_id         INT NOT NULL,
  value_numeric   DOUBLE NULL,
  extraction_dt   DATETIME NULL,        -- fecha de extracción (UTC)
  source_table    VARCHAR(200) NULL,    -- nombre original en la fuente
  -- desgloses opcionales
  demog_id        INT NULL,
  code_id         INT NULL,             -- p.ej. NACE/SIEC/etc.
  -- claves foráneas
  CONSTRAINT fk_obs_dataset FOREIGN KEY (dataset_id) REFERENCES dim_dataset(dataset_id),
  CONSTRAINT fk_obs_geo     FOREIGN KEY (geo_id)     REFERENCES dim_geo(geo_id),
  CONSTRAINT fk_obs_cty     FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
  CONSTRAINT fk_obs_time    FOREIGN KEY (time_id)    REFERENCES dim_time(time_id),
  CONSTRAINT fk_obs_measure FOREIGN KEY (measure_id) REFERENCES dim_measure(measure_id),
  CONSTRAINT fk_obs_unit    FOREIGN KEY (unit_id)    REFERENCES dim_unit(unit_id),
  CONSTRAINT fk_obs_demog   FOREIGN KEY (demog_id)   REFERENCES dim_demography(demog_id),
  CONSTRAINT fk_obs_code    FOREIGN KEY (code_id)    REFERENCES dim_code(code_id),
  KEY ix_obs_q1 (dataset_id, measure_id, time_id),
  KEY ix_obs_q2 (geo_id, time_id),
  KEY ix_obs_q3 (country_id, time_id)
) ENGINE=InnoDB;

-- ========================
-- SEED BÁSICO
-- ========================
INSERT INTO dim_unit (unit_code, unit_name) VALUES
('ratio','Proporción 0-1'),
('ug/m3','Microgramos por metro cúbico'),
('km2','Kilómetros cuadrados'),
('t','Toneladas'),
('EUR','Euros'),
('C','Grados Celsius');

-- ejemplo medidas más comunes
INSERT INTO dim_measure (measure_code, measure_name) VALUES
('pm25','PM2.5 concentración'),
('ghg_total','Emisiones GEI totales'),
('gdp_pc_pps','PIB per cápita (PPS)'),
('life_satisfaction','Satisfacción con la vida'),
('tcd_pct','Tree Cover Density (%)'),
('ndvi','Índice de Vegetación NDVI');

-- ejemplo dataset
INSERT INTO dim_dataset (dataset_code, source_name) VALUES
('EUROSTAT_BLI_1','Eurostat'),
('EEA_AIR','EEA'),
('COPERNICUS_TCD','Copernicus');

