-- ================================
-- GTP - Modelo E/R (OLTP, 3NF)
-- MySQL 8+ / InnoDB / utf8mb4
-- ================================

CREATE DATABASE IF NOT EXISTS gtp_er
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE gtp_er;

-- 1) País
CREATE TABLE country (
  country_id    INT AUTO_INCREMENT PRIMARY KEY,
  iso3          CHAR(3) NOT NULL UNIQUE,
  iso2          CHAR(2) UNIQUE,
  country_name  VARCHAR(100) NOT NULL,
  CONSTRAINT chk_iso3_format CHECK (iso3 REGEXP '^[A-Z]{3}$')
) ENGINE=InnoDB;

-- 2) Área geográfica (país/region/NUTS/FUA/ciudad)
CREATE TABLE geo_area (
  geo_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
  geo_level   ENUM('country','region','NUTS','FUA','city') NOT NULL,
  geo_code    VARCHAR(64) NOT NULL,
  geo_name    VARCHAR(200) NOT NULL,
  country_id  INT NOT NULL,
  UNIQUE KEY uk_geo (geo_level, geo_code),
  KEY fk_geo_country (country_id),
  CONSTRAINT fk_geo_country FOREIGN KEY (country_id) REFERENCES country(country_id)
) ENGINE=InnoDB;

-- 3) Fuente / institución
CREATE TABLE source (
  source_id    INT AUTO_INCREMENT PRIMARY KEY,
  source_name  VARCHAR(200) NOT NULL,
  source_url   VARCHAR(500),
  license      VARCHAR(200),
  notes        TEXT,
  UNIQUE KEY uk_source_name (source_name)
) ENGINE=InnoDB;

-- 4) Dataset (conjunto de datos de una fuente)
CREATE TABLE dataset (
  dataset_id    INT AUTO_INCREMENT PRIMARY KEY,
  dataset_code  VARCHAR(100) NOT NULL,
  title         VARCHAR(300),
  theme         ENUM('economic','environmental','social','innovation','energy','finance') DEFAULT 'economic',
  source_id     INT NOT NULL,
  UNIQUE KEY uk_dataset_code (dataset_code),
  KEY fk_dataset_source (source_id),
  CONSTRAINT fk_dataset_source FOREIGN KEY (source_id) REFERENCES source(source_id)
) ENGINE=InnoDB;

-- 5) Unidad de medida
CREATE TABLE unit (
  unit_id            INT AUTO_INCREMENT PRIMARY KEY,
  unit_code          VARCHAR(32) NOT NULL,
  unit_name          VARCHAR(100),
  base_unit_code     VARCHAR(32),
  multiplier_to_base DECIMAL(20,10) DEFAULT 1.0,
  UNIQUE KEY uk_unit_code (unit_code)
) ENGINE=InnoDB;

-- 6) Variable / indicador (definición semántica)
CREATE TABLE variable (
  variable_id     INT AUTO_INCREMENT PRIMARY KEY,
  variable_code   VARCHAR(100) NOT NULL,   -- snake_case (p.ej. gdp_pc_pps, ghg_total, pm25)
  variable_name   VARCHAR(300) NOT NULL,
  description     TEXT,
  default_unit_id INT,
  dataset_id      INT,                     -- si la variable es específica de un dataset (opcional)
  UNIQUE KEY uk_variable_code (variable_code),
  KEY fk_var_unit (default_unit_id),
  KEY fk_var_dataset (dataset_id),
  CONSTRAINT fk_var_unit    FOREIGN KEY (default_unit_id) REFERENCES unit(unit_id),
  CONSTRAINT fk_var_dataset FOREIGN KEY (dataset_id)      REFERENCES dataset(dataset_id)
) ENGINE=InnoDB;

-- 7) Sistemas de clasificación (NACE, SIEC, CEPA/SEEA, etc.)
CREATE TABLE classification_system (
  classification_system_id INT AUTO_INCREMENT PRIMARY KEY,
  system_code  VARCHAR(50) NOT NULL,
  system_name  VARCHAR(200) NOT NULL,
  UNIQUE KEY uk_system_code (system_code)
) ENGINE=InnoDB;

-- 8) Códigos de clasificación (valores de cada sistema)
CREATE TABLE classification_code (
  classification_code_id   INT AUTO_INCREMENT PRIMARY KEY,
  classification_system_id INT NOT NULL,
  code_value   VARCHAR(50) NOT NULL,
  code_label   VARCHAR(200),
  UNIQUE KEY uk_code_per_system (classification_system_id, code_value),
  KEY fk_cc_sys (classification_system_id),
  CONSTRAINT fk_cc_sys FOREIGN KEY (classification_system_id)
    REFERENCES classification_system(classification_system_id)
) ENGINE=InnoDB;

-- 9) Puente Variable–Clasificación (M:N)
CREATE TABLE variable_classification (
  variable_id              INT NOT NULL,
  classification_code_id   INT NOT NULL,
  PRIMARY KEY (variable_id, classification_code_id),
  KEY fk_vc_var  (variable_id),
  KEY fk_vc_code (classification_code_id),
  CONSTRAINT fk_vc_var  FOREIGN KEY (variable_id)            REFERENCES variable(variable_id),
  CONSTRAINT fk_vc_code FOREIGN KEY (classification_code_id) REFERENCES classification_code(classification_code_id)
) ENGINE=InnoDB;

-- 10) Perfil demográfico (opcional)
CREATE TABLE demographic_profile (
  demog_id        INT AUTO_INCREMENT PRIMARY KEY,
  sex             ENUM('male','female','other') NULL,
  age_group       VARCHAR(50) NULL,       -- '15-24','25-34', etc.
  work_status     VARCHAR(50) NULL,       -- employed, unemployed, student, etc.
  education_iscED VARCHAR(50) NULL,       -- ISCED-11: '0-2','3-4','5-8', etc.
  other_attrs     JSON NULL,
  -- Unicidad robusta con columna generada (evita funciones en índices)
  concat_key      VARCHAR(255)
    GENERATED ALWAYS AS (CONCAT_WS('|',
      COALESCE(sex,''), COALESCE(age_group,''), COALESCE(work_status,''), COALESCE(education_iscED,''))) STORED,
  UNIQUE KEY uk_demog (concat_key)
) ENGINE=InnoDB;

-- 11) Periodo temporal (normalizado)
CREATE TABLE period (
  period_id  BIGINT AUTO_INCREMENT PRIMARY KEY,
  freq       ENUM('A','Q','M','D') NOT NULL, -- Annual/Quarterly/Monthly/Daily
  year       SMALLINT,
  quarter    TINYINT,
  month      TINYINT,
  day        TINYINT,
  timestamp_utc DATETIME,                    -- primer día del periodo en UTC
  UNIQUE KEY uk_period (freq, year, quarter, month, day)
) ENGINE=InnoDB;

-- 12) Observación (registro operacional único)
CREATE TABLE observation (
  observation_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  dataset_id     INT NOT NULL,
  variable_id    INT NOT NULL,
  unit_id        INT NOT NULL,
  geo_id         BIGINT NOT NULL,
  country_id     INT NOT NULL,
  period_id      BIGINT NOT NULL,
  value_numeric  DOUBLE,
  value_status   VARCHAR(50),          -- 'estimated','provisional','final', etc. (opcional)
  source_table   VARCHAR(200),         -- nombre tabla/cubo origen (opcional)
  source_file    VARCHAR(300),         -- archivo original (opcional)
  extraction_dt  DATETIME,             -- fecha/hora de extracción/carga
  demog_id       INT NULL,             -- opcional
  classification_code_id INT NULL,     -- opcional (p.ej. NACE/SIEC)
  -- Índices y FKs
  KEY ix_obs_q1 (dataset_id, variable_id, period_id),
  KEY ix_obs_q2 (geo_id, period_id),
  KEY ix_obs_q3 (country_id, period_id),
  KEY fk_obs_unit  (unit_id),
  KEY fk_obs_demog (demog_id),
  KEY fk_obs_code  (classification_code_id),
  CONSTRAINT fk_obs_dataset  FOREIGN KEY (dataset_id)  REFERENCES dataset(dataset_id),
  CONSTRAINT fk_obs_variable FOREIGN KEY (variable_id) REFERENCES variable(variable_id),
  CONSTRAINT fk_obs_unit     FOREIGN KEY (unit_id)     REFERENCES unit(unit_id),
  CONSTRAINT fk_obs_geo      FOREIGN KEY (geo_id)      REFERENCES geo_area(geo_id),
  CONSTRAINT fk_obs_country  FOREIGN KEY (country_id)  REFERENCES country(country_id),
  CONSTRAINT fk_obs_period   FOREIGN KEY (period_id)   REFERENCES period(period_id),
  CONSTRAINT fk_obs_demog    FOREIGN KEY (demog_id)    REFERENCES demographic_profile(demog_id),
  CONSTRAINT fk_obs_code     FOREIGN KEY (classification_code_id) REFERENCES classification_code(classification_code_id)
) ENGINE=InnoDB;
