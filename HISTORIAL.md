# Historial de Cambios y Decisiones — GTP (Green Turning Point)

> Registro cronológico de cambios técnicos, decisiones de arquitectura y planteamientos.
> Cada entrada incluye: fecha, tipo (CAMBIO / DECISIÓN / PLANTEAMIENTO / DESCARTADO), afectados y detalle.
> **Norma:** toda decisión, cambio o descarte relevante se registra aquí el mismo día con contexto suficiente para entenderse sin leer el código.

---

## [2026-03-09] DECISIÓN — Stack tecnológico confirmado: Hive + Parquet sobre HDFS (Lorca)

**Contexto:** Se debatió si usar una base de datos relacional clásica (PostgreSQL) o una arquitectura de Data Lakehouse sobre HDFS disponible en el cluster Lorca de la UEM.

**Decisión:** Usar **Apache Hive + Parquet sobre HDFS** como capa de almacenamiento principal en Lorca, con Spark SQL como motor de consulta. PostgreSQL se mantiene únicamente en el pipeline Docker (local/dev).

**Razón:**

- El cluster Lorca ya tiene HDFS y Spark. Añadir PostgreSQL sería redundante y menos escalable.
- Hive da interfaz SQL (HiveQL) sobre ficheros Parquet en HDFS, sin necesidad de un servidor de base de datos separado.
- Las operaciones del proyecto son analíticas (batch), no transaccionales. No se necesita ACID row-level.
- Las "actualizaciones" se hacen sobrescribiendo particiones enteras, lo cual es el paradigma estándar en Big Data.

**Implicación:** `spark.py` se reescribirá para escribir a HDFS con tablas Hive en lugar de exportar solo a CSV local.

---

## [2026-03-09] DECISIÓN — Arquitectura Medallion (Bronze/Silver/Gold) aprobada

**Contexto:** Discusión sobre cómo organizar el Data Lakehouse en HDFS.

**Decisión:** Implementar tres capas sobre HDFS:

- **Bronze:** datos brutos exactos, append-only, con metadata de ingesta.
- **Silver:** datos limpios + modelo dimensional Star Schema (Kimball).
- **Gold:** tablas analíticas finales (fact_kuznets, rankings, parámetros EKC).

**Particionado acordado:** `year=YYYY/country=XX/` para todos los fact tables. Dimensiones por `country=XX/`.

**SCD:**

- DIM_CITY → Tipo 1 (overwrite, coordenadas estables)
- DIM_COMPANY → Tipo 2 (append con valid_from/valid_to, sectores cambian)
- DIM_DATE → Estática, pre-generada 2000–2030
- DIM_SOURCE → Tipo 1

**Pendiente de implementar:** `Lorca/BBDD/spark.py` + nuevo script `Lorca/BBDD/bronze_ingest.py`.

---

## [2026-03-09] DECISIÓN — Stack de modelos ML/analíticos acordado

**Contexto:** Se evaluaron múltiples vías de modelado considerando la heterogeneidad de la curva EKC entre ciudades.

**Decisión final:** 4 modelos en pipeline secuencial:

| Orden | Modelo                             | Librería                     | Viabilidad | Rol                                                         |
| ----- | ---------------------------------- | ----------------------------- | ---------- | ----------------------------------------------------------- |
| 1     | K-Means Clustering                 | pyspark.ml                    | Alta       | Segmentar ciudades antes de cualquier regresión            |
| 2     | EKC Panel Regression (por cluster) | statsmodels / pandas-on-spark | Media      | Estimar β₁, β₂, Y* por segmento                         |
| 3     | XGBoost Clasificación             | xgboost                       | Alta       | Predecir fase de la ciudad (degradando/turning/recuperando) |
| 4     | Prophet Time Series                | prophet (pandas UDF)          | Media      | Proyectar cuándo una ciudad alcanzará el turning point    |

**Descartado:** LSTM — datos insuficientes (≈96 puntos por ciudad), complejidad injustificada para el alcance universitario.

**Pendiente de implementar:** `Lorca/BBDD/models/` (nuevos scripts por modelo).

---

## [2026-03-09] CAMBIO — Corrección de 9 bugs críticos en pipeline Lorca

**Archivos afectados:**

- `Lorca/ETL/script/run_all.py`
- `Lorca/ETL/script/Sentinel-2_extract.py`
- `Lorca/ETL/script/Sentinel-5p_extract.py`
- `Lorca/ETL/script/merge.py`
- `Lorca/ETL/script/YFinance_extract.py`

**Cambios aplicados:**

| # | Bug                                                                                                                                                                                                                                                              | Fix                                                                                                 |
| - | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| 1 | `ETL_SCRIPTS_DIR` apuntaba a ruta inexistente (`ETL/script/ETL/script/`) → ningún script global se ejecutaba                                                                                                                                               | `ETL_SCRIPTS_DIR = BASE_DIR`                                                                      |
| 2 | `EURO_FUAS` duplicado íntegro en `run_all.py` (190 líneas) → riesgo de desincronización con `config.py`                                                                                                                                                | Eliminado →`from config import EURO_FUAS`                                                        |
| 3 | `YFinance_extract.py` recibía `--what finance --out-dir data/finance` pero solo acepta `--out` → `argparse` error                                                                                                                                      | Eliminados args incorrectos                                                                         |
| 4 | `HRL_extract.py` se llamaba 740 veces (por ciudad) pero internamente ya itera todas las ciudades solo → 740 descargas completas redundantes                                                                                                                   | HRL movido a `SCRIPTS_GLOBALES` (1 ejecución total)                                              |
| 5 | Referencia a `Sentinel-5P_extract.py` (P mayúscula) pero el archivo es `Sentinel-5p_extract.py` → error en Linux/Lorca                                                                                                                                     | Corregido a `Sentinel-5p_extract.py`                                                              |
| 6 | `import calendar` dentro de `if __name__ == "__main__":` pero se usa en `process_city_date()` en threads → `NameError`                                                                                                                                  | `import calendar` movido al inicio del archivo                                                    |
| 7 | `df.groupby("City").apply(get_slope)` deprecado en pandas ≥ 2.0                                                                                                                                                                                               | `groupby("City")[["Year","NDVI_Mean"]].apply(get_slope)`                                          |
| 8 | Columnas `Company`, `Close_Price_Avg`, `Volatility`, `Country` en YFinance no coincidían con lo que espera `merge.py` (`Company_Name`, `Close_Price`, `Annual_Volatility`, `FUA_Country_Code`) → merge financiero producía DataFrame vacío | Renombradas columnas + añadido `COUNTRY_TO_CODE` dict + columna `FUA_Country_Code`             |
| 9 | `collection.size().getInfo()` bloqueante dentro de 10 workers paralelos → saturación de cuota GEE                                                                                                                                                            | Eliminado `getInfo()`, colección vacía manejada vía `ee.EEException` en `getDownloadURL()` |

**Impacto:** Pipeline completamente no funcional antes de estas correcciones. Ahora el flujo de ejecución es correcto de principio a fin.

---

## [2026-03-09] CAMBIO — Creación de documentación técnica completa

**Archivos creados:**

- `GTP_DOCUMENTACION_TECNICA.md` (raíz del proyecto)
- `memory/MEMORY.md` (memoria persistente del asistente)
- `memory/PROJECT_REFERENCE.md` (referencia de arquitectura para el asistente)

**Contenido de `GTP_DOCUMENTACION_TECNICA.md`:**

- Teoría EKC con fórmula matemática exacta: `Y* = exp(-β₁ / 2β₂)`
- Cómo GTP operacionaliza Kuznets con los datos reales
- Estructura de directorios exacta con todas las rutas
- Detalle técnico de cada fuente de datos (API, parámetros, output)
- Descripción algorítmica de cada script
- Variables del dataset maestro
- Reglas de imputación
- Tabla completa de bugs corregidos

---

## [2026-03-09] PLANTEAMIENTO — Arquitectura Medallion HDFS + Data Warehouse

**Estado:** Aprobado en la misma sesión. Ver entradas DECISIÓN de la misma fecha.

---

## [2026-03-09] DECISIÓN — Orden de implementación acordado

**Contexto:** Con el stack tecnológico y los modelos ya definidos, se acordó el orden de implementación.

**Orden:**

1. **Schemas DDL (Hive)** — Definir formalmente todas las tablas de Bronze, Silver y Gold con sus DDL SQL. Esto es el contrato de datos que guía toda la implementación posterior.
2. **Bronze ingest** (`bronze_ingest.py`) — Leer los CSVs/GeoTIFFs del ETL y escribirlos a HDFS en formato Parquet particionado, con columnas de metadata de ingesta.
3. **Silver transform** (`silver_transform.py`) — Leer Bronze, limpiar, construir Star Schema Kimball (dimensiones + facts), implementar SCD.
4. **Gold build** (`gold_build.py`) — Agregar Silver a tablas analíticas finales (fact_kuznets, rankings, parámetros EKC).
5. **ML models** (`models/clustering.py`, `ekc_regression.py`, `xgboost_classifier.py`, `prophet_forecast.py`) — Pipeline secuencial K-Means → EKC → XGBoost → Prophet.

**Razón:** Los schemas DDL son el contrato de datos. Sin ellos, cualquier código que lea o escriba datos puede desalinearse. Se definen primero para que Bronze, Silver y Gold tengan una especificación formal de referencia.

**Estructura de carpetas acordada para `Lorca/BBDD/`:**

```
Lorca/BBDD/
├── schemas/
│   ├── bronze_ddl.sql
│   ├── silver_ddl.sql
│   └── gold_ddl.sql
├── bronze_ingest.py
├── silver_transform.py
├── gold_build.py
├── models/
│   ├── clustering.py
│   ├── ekc_regression.py
│   ├── xgboost_classifier.py
│   └── prophet_forecast.py
├── spark.py              (a refactorizar → sustituido por los scripts anteriores)
└── exports.py            (existente)
```

---

## [2026-03-09] CAMBIO — Creación de schemas DDL formales (Bronze / Silver / Gold)

**Archivos creados:**

- `Lorca/BBDD/schemas/bronze_ddl.sql`
- `Lorca/BBDD/schemas/silver_ddl.sql`
- `Lorca/BBDD/schemas/gold_ddl.sql`

**Resumen de tablas definidas:**

**Bronze (6 tablas — datos brutos):**

- `bronze_sentinel2_raw` — NDVI mensual ciudad×mes, particionado `year/country`
- `bronze_sentinel5p_raw` — NO2 mensual, particionado `year/country`
- `bronze_hrl_raw` — impermeabilización del suelo, particionado `year/country`
- `bronze_finance_raw` — cotizaciones y métricas financieras anuales, particionado `year/country`
- `bronze_eurostat_gdp_raw` — GDP per cápita Eurostat, particionado `year/country`
- `bronze_eurostat_population_raw` — población urbana Eurostat, particionado `year/country`

**Silver (7 tablas — Star Schema Kimball):**

- `dim_city` — SCD Tipo 1 (coordenadas estables; overwrite)
- `dim_company` — SCD Tipo 2 (`valid_from`/`valid_to`/`is_current`; sectores cambian)
- `dim_date` — Estática, pre-generada 2000–2030
- `dim_source` — SCD Tipo 1
- `fact_environmental` — NDVI + NO2 + HRL ciudad×año, FK→dim_city + dim_date + dim_source
- `fact_economic` — GDP + población ciudad×año, FK→dim_city + dim_date + dim_source
- `fact_financial` — métricas de empresa×año, FK→dim_company + dim_date

**Gold (4 tablas — analíticas finales):**

- `fact_kuznets` — tabla central EKC con turning point calculado, particionado `year/country`
- `city_ranking` — ranking europeo de ciudades por índice verde, particionado `year`
- `ekc_parameters` — parámetros estimados por cluster (β₁, β₂, Y*, R²), sin partición
- `model_results` — predicciones XGBoost + Prophet ciudad×año, particionado `year/country`

---

## [2026-03-09 22:35] TO-DO LIST — Implementación pendiente completa del proyecto GTP

> Registro del estado de tareas a la fecha. Actualizar este bloque marcando [x] conforme se completen.
> **Norma:** nunca borrar entradas completadas, solo marcarlas.

### FASE 1 — Base de Datos (Lorca/BBDD/)

#### 1.1 Schemas DDL

- [X] `Lorca/BBDD/schemas/bronze_ddl.sql` — 6 tablas Hive Bronze con particionado y metadata
- [X] `Lorca/BBDD/schemas/silver_ddl.sql` — 4 dims (SCD1/2) + 3 facts + 2 vistas
- [X] `Lorca/BBDD/schemas/gold_ddl.sql` — 4 tablas analíticas + 3 vistas

#### 1.2 Pipeline de ingesta y transformación

- [ ] `Lorca/BBDD/bronze_ingest.py` — Lee CSVs del ETL, escribe a HDFS Bronze en Parquet particionado con metadata `_ingestion_date / _source_system / _file_name`
- [ ] `Lorca/BBDD/silver_transform.py` — Lee Bronze, limpia, construye Star Schema, aplica SCD-1 y SCD-2, escribe a HDFS Silver
- [ ] `Lorca/BBDD/gold_build.py` — Lee Silver, construye `fact_kuznets`, `city_ranking`, `ekc_parameters`, `model_results`; calcula `green_index` e `investment_score`

### FASE 2 — Modelos ML (Lorca/BBDD/models/)

- [ ] `models/clustering.py` — K-Means PySpark con selección automática de K por índice Silhouette; guarda asignaciones en Silver/Gold
- [ ] `models/ekc_regression.py` — Panel Regression EKC por cluster con `linearmodels`/`statsmodels`; estima β₁, β₂, Y* = exp(−β₁/2β₂); guarda en `ekc_parameters`
- [ ] `models/xgboost_classifier.py` — Clasificador XGBoost de fase EKC (DEGRADANDO/TURNING/RECUPERANDO); train/test split temporal; guarda probabilidades en `model_results`
- [ ] `models/prophet_forecast.py` — Forecast NDVI por ciudad con Prophet (pandas UDF sobre Spark); proyecta 1Y/3Y/5Y; estima `prophet_turning_year`; guarda en `model_results`

### FASE 3 — Orquestación y utilidades

- [ ] `Lorca/BBDD/run_pipeline.py` — Script maestro que ejecuta las fases en orden: bronze → silver → gold → modelos ML
- [ ] Actualizar `GTP_DOCUMENTACION_TECNICA.md` con DDL schemas, pipeline implementado y resultados de modelos

### FASE 4 — ETL (Lorca/ETL/script/) — Estado actual

- [X] Bug 1–9 corregidos en sesión anterior (ver entrada [2026-03-09] CAMBIO)
- [X] `config.py` — configuración centralizada, sin cambios pendientes
- [X] `run_all.py` — pipeline ETL corregido
- [X] `Sentinel-2_extract.py` — corregido
- [X] `Sentinel-5p_extract.py` — corregido
- [X] `YFinance_extract.py` — corregido
- [X] `merge.py` — corregido
- [ ] `HRL_extract.py` — pendiente revisión de compatibilidad con nuevo pipeline Bronze
- [ ] `Eurostat_extract.py` — pendiente verificar que genera CSVs compatibles con `bronze_ingest.py`

### PRIORIDAD INMEDIATA (próxima sesión)

1. `bronze_ingest.py`
2. `silver_transform.py`
3. `gold_build.py`
4. `models/clustering.py`
5. `models/ekc_regression.py`
6. `models/xgboost_classifier.py`
7. `models/prophet_forecast.py`
8. `run_pipeline.py`

---

## [2026-03-09 22:39] CAMBIO — Creación de CLAUDE.md (contexto persistente del asistente)

**Archivo creado:** `CLAUDE.md` (raíz del proyecto)

**Propósito:** Instrucciones de contexto que Claude Code carga automáticamente al inicio de cada sesión para no perder el hilo del proyecto entre conversaciones.

**Contenido:**
- Orden de lectura obligatoria al iniciar sesión: HISTORIAL.md (últimas 100 líneas) → GTP_DOCUMENTACION_TECNICA.md → DDL schemas → config.py
- Reglas inamovibles: HISTORIAL append-only con fecha+hora, foco en Lorca, stack fijo (Hive+Parquet+HDFS), no UPDATE row-level, fuente de verdad única en config.py
- Arquitectura resumida con árbol de directorios y estado de cada componente
- Fórmula EKC y turning point Y* = exp(−β₁/2β₂)
- Pipeline ML: K-Means → EKC Panel Regression → XGBoost → Prophet
- Rutas HDFS del cluster Lorca
- Convenciones de código (city_code, particionado, columnas metadata, SCD-2)
- Tabla de estado rápido (completado/pendiente) para retomar sin leer todo

---

## [2026-03-09 22:53] CAMBIO — Implementación completa del pipeline BBDD + modelos ML

**Archivos creados:**
- `Lorca/BBDD/bronze_ingest.py`
- `Lorca/BBDD/silver_transform.py`
- `Lorca/BBDD/gold_build.py`
- `Lorca/BBDD/models/clustering.py`
- `Lorca/BBDD/models/ekc_regression.py`
- `Lorca/BBDD/models/xgboost_classifier.py`
- `Lorca/BBDD/models/prophet_forecast.py`
- `Lorca/BBDD/run_pipeline.py`

**Detalle por archivo:**

**`bronze_ingest.py`** — Lee 5 fuentes CSV del ETL y las escribe a HDFS Bronze en Parquet particionado por `year/country`. Añade columnas de auditoría (`_ingestion_date`, `_source_system`, `_file_name`). Ejecuta `MSCK REPAIR TABLE` tras cada escritura para registrar particiones en Hive. Acepta `--source all|s2|s5p|hrl|finance|eurostat` y `--mode overwrite|append`.

**`silver_transform.py`** — Construye el Star Schema Kimball completo:
- `dim_city` (SCD-1): cargada desde `EURO_FUAS` de `config.py` directamente
- `dim_date` (estática): pre-generada 2000–2030 con todos los atributos de calendario
- `dim_source` (SCD-1): catálogo de 6 fuentes hardcodeado
- `dim_company` (SCD-2): detecta cambios de sector/industria en cada ejecución y versiona con `valid_from`/`valid_to`/`is_current`
- `fact_environmental`: NDVI estacional + NO2 anual + HRL, calcula `green_index` compuesto y `ndvi_trend_slope` por OLS ventana
- `fact_economic`: GDP + variables EKC (`ln_gdp_pps`, `ln_gdp_pps_sq`, `gdp_growth_rate`)
- `fact_financial`: métricas empresa×año unidas con `company_sk` de `dim_company`

**`gold_build.py`** — Construye tablas Gold:
- `fact_kuznets`: join de env+eco+fin con columnas de modelos ML como placeholder (se rellenan con cada modelo)
- `city_ranking`: ranking europeo con `rank_position`, `rank_within_country`, `rank_change` YoY
- `ekc_parameters`: placeholder vacío (se rellena con `ekc_regression.py`)
- `model_results`: placeholder vacío (se rellena con los 4 modelos)

**`models/clustering.py`** — K-Means PySpark MLlib:
- Feature matrix: 6 features por ciudad (promedios históricos de NDVI, NO2, GDP, etc.)
- StandardScaler antes del clustering
- Selección automática de K (2–8) por índice Silhouette
- Etiquetado interpretable de clusters por análisis de centroides (High-Income-Greening, Industrializing-Polluted, etc.)
- Actualiza `fact_kuznets` y `model_results` en Gold

**`models/ekc_regression.py`** — Panel Regression EKC con `linearmodels.PanelOLS`:
- Modelo: `ln(NDVI_it) = α + β₁·ln(GDP_it) + β₂·[ln(GDP_it)]² + EntityEffects + TimeEffects`
- Errores estándar cluster-robust por ciudad
- Calcula Y* = exp(−β₁/2β₂) y clasifica forma de curva (INVERTED_U/MONOTONIC_DOWN/etc.)
- Escribe en `ekc_parameters` y actualiza `fact_kuznets` con β₁, β₂, Y*, `gdp_gap_to_turning`

**`models/xgboost_classifier.py`** — Clasificador XGBoost:
- Etiquetado automático de fases (DEGRADANDO/TURNING/RECUPERANDO) por reglas sobre `ndvi_trend_slope`, `gdp_gap_to_turning`
- Split temporal: años ≤ max-2 para train, resto para test (sin data leakage)
- Pesos de clase para balanceo (TURNING es clase minoritaria)
- Actualiza `fact_kuznets` con `turning_point_phase`, `phase_confidence`
- Recalcula `investment_score` (50·green_index + 30·phase_confidence + 20·fase_bonus)
- Recalcula `investment_recommendation` (STRONG_BUY/BUY/HOLD/AVOID)

**`models/prophet_forecast.py`** — Forecast Prophet via pandas UDF (Grouped Map):
- Una función Python por ciudad, paralelizada entre workers Spark
- Estima Turning Year: primer año futuro donde la tendencia cambia de negativa a positiva
- Genera forecast 1Y/3Y/5Y con intervalos de confianza 95%
- Calcula MAPE in-sample por ciudad
- Actualiza `fact_kuznets` y `model_results`

**`run_pipeline.py`** — Orquestador maestro:
- 7 fases configurables con flags `--skip-*` y shortcuts `--only-models`, `--only-ingest`
- `--dry-run` para previsualizar sin ejecutar
- Resumen final con estado (✓/✗) y tiempo de cada fase

**TO-DO LIST actualizada:**

### FASE 1 — Base de Datos
- [x] `bronze_ddl.sql`
- [x] `silver_ddl.sql`
- [x] `gold_ddl.sql`
- [x] `bronze_ingest.py`
- [x] `silver_transform.py`
- [x] `gold_build.py`

### FASE 2 — Modelos ML
- [x] `models/clustering.py`
- [x] `models/ekc_regression.py`
- [x] `models/xgboost_classifier.py`
- [x] `models/prophet_forecast.py`

### FASE 3 — Orquestación
- [x] `run_pipeline.py`
- [ ] Actualizar `GTP_DOCUMENTACION_TECNICA.md` con nueva arquitectura implementada

### Pendiente (próximas sesiones)
- [ ] Actualizar `GTP_DOCUMENTACION_TECNICA.md` con todos los cambios de esta sesión
- [ ] Probar pipeline en Lorca y corregir errores de entorno (rutas, dependencias)
- [ ] Revisar `HRL_extract.py` para compatibilidad con `bronze_ingest.py`
- [ ] Verificar que `eurostat_extract.py` genera CSV compatible con bronze_ingest
- [ ] Actualizar tabla de estado en `CLAUDE.md`

---

## [2026-03-09 23:15] TO-DO LIST — Detallado, estricto y exhaustivo: próximos pasos reales del proyecto GTP

> Esta lista es la hoja de ruta operativa del proyecto. Cada tarea incluye criterio de aceptación, archivo afectado, y dependencias.
> **Norma:** NO se marca una tarea como completada hasta que se ha probado en Lorca y produce el output esperado. No basta con que el código exista.

---

### BLOQUE 0 — BRECHA CRÍTICA BLOQUEANTE (resolver antes que cualquier otra cosa)

> **Problema:** Los scripts de ETL (Sentinel-2, Sentinel-5P, HRL) producen GeoTIFFs. El `bronze_ingest.py` espera CSVs con columnas específicas. **No existe ningún script que convierta GeoTIFFs → CSV.** El pipeline entero está bloqueado sin esto.

#### 0.1 Crear `Lorca/ETL/script/sentinel2_to_csv.py`

- **Qué hace:** Lee los GeoTIFFs descargados por `Sentinel-2_extract.py` desde `DatosProcesados/Sentinel2/`, extrae la estadística media de píxeles dentro de cada polígono FUA, y escribe `DatosProcesados/sentinel2.csv` con columnas:
  ```
  City, Year, Month, NDVI_Mean, NDVI_Std, NDVI_Min, NDVI_Max, pixel_count
  ```
- **Librería necesaria:** `rasterio` + `numpy` (alternativa: `gdal`)
- **Criterio de aceptación:** El CSV resultante tiene ≥1 fila por ciudad×mes, sin nulls en `NDVI_Mean`, y el rango de valores es [−1, 1]
- **Dependencia de:** `Sentinel-2_extract.py` completado (GeoTIFFs existentes en HDFS o local)
- **Tarea concreta en Lorca:** `pip install rasterio` y verificar que el cluster tiene acceso al directorio de GeoTIFFs

#### 0.2 Crear `Lorca/ETL/script/sentinel5p_to_csv.py`

- **Qué hace:** Lee los NetCDF/GeoTIFF de Sentinel-5P desde `DatosProcesados/Sentinel5P/`, extrae media de NO2 por ciudad×mes, y escribe `DatosProcesados/s5p.csv` con columnas:
  ```
  City, Year, Month, NO2_Mean, NO2_Std, qa_value_mean
  ```
- **Criterio de aceptación:** Valores NO2 en unidades mol/m² (rango típico: 1e-5 a 1e-4), sin NaN para ciudades con datos disponibles
- **Dependencia de:** `Sentinel-5p_extract.py` completado

#### 0.3 Crear `Lorca/ETL/script/hrl_to_csv.py`

- **Qué hace:** Lee los GeoTIFFs de HRL (Imperviousness Surface + Tree Cover Density) desde `DatosProcesados/HRL/`, calcula media de píxeles por ciudad, y escribe `DatosProcesados/hrl.csv` con columnas:
  ```
  City, Year, Imperviousness_Mean, Tree_Cover_Pct
  ```
- **Criterio de aceptación:** `Imperviousness_Mean` ∈ [0, 100], `Tree_Cover_Pct` ∈ [0, 100], una fila por ciudad×año disponible (HRL se publica cada 3 años: 2006, 2009, 2012, 2015, 2018)
- **Nota:** Los años entre publicaciones HRL deben interpolarse o dejarse como null (decisión pendiente)

#### 0.4 Verificar `eurostat_extract.py` output real

- **Qué verificar:** Ejecutar en Lorca y confirmar que el CSV generado `DatosProcesados/eurostat_PIB.csv` tiene exactamente las columnas: `City, Year, GDP_Per_Capita, Source`
- **Columna `City`:** debe usar el formato `NombreCiudad_ISO2` (ej: `Madrid_ES`) — si usa otro formato, añadir paso de normalización
- **Criterio de aceptación:** El JOIN en `bronze_ingest.py` entre Eurostat y la columna `city_code` de `EURO_FUAS` no produce filas vacías

---

### BLOQUE 1 — VERIFICACIÓN DE ENTORNO EN LORCA (antes de ejecutar cualquier script)

#### 1.1 Verificar acceso a HDFS

```bash
hdfs dfs -ls /user/gtp/
hdfs dfs -mkdir -p /user/gtp/bronze
hdfs dfs -mkdir -p /user/gtp/silver
hdfs dfs -mkdir -p /user/gtp/gold
hdfs dfs -mkdir -p /user/gtp/models/clustering
hdfs dfs -mkdir -p /user/gtp/models/xgboost
hdfs dfs -mkdir -p /user/gtp/models/prophet
```

- **Criterio de aceptación:** Todos los directorios existen y el usuario `gtp` (o el usuario del cluster) tiene permisos de escritura

#### 1.2 Verificar versión Spark y YARN disponibles

```bash
spark-submit --version
yarn node -list
```

- **Criterio de aceptación:** Spark ≥ 3.0, YARN activo con al menos 4 nodos disponibles
- **Si Spark < 3.0:** Revisar compatibilidad de `applyInPandas` en Prophet (requiere Spark 3.x)

#### 1.3 Instalar dependencias Python en todos los nodos (o vía `--py-files`)

```bash
pip install linearmodels xgboost scikit-learn prophet joblib rasterio
```

- **Alternativa si no hay pip en workers:** Crear `.zip` con dependencias y usar `spark-submit --py-files deps.zip`
- **Verificar para cada librería:**
  - `python -c "import linearmodels; print(linearmodels.__version__)"` — necesario ≥ 4.x
  - `python -c "import xgboost; print(xgboost.__version__)"` — necesario ≥ 1.5
  - `python -c "from prophet import Prophet; print('ok')"` — requiere pystan o cmdstanpy como backend
  - `python -c "import rasterio; print(rasterio.__version__)"` — necesario para Bloque 0

#### 1.4 Verificar acceso a Hive Metastore desde Spark

```bash
spark-submit --master yarn --deploy-mode client \
  --conf spark.sql.catalogImplementation=hive \
  -e "SHOW DATABASES;"
```

- **Criterio de aceptación:** Lista de databases visible sin error de conexión al metastore
- **Si falla:** Verificar `hive-site.xml` en `$SPARK_HOME/conf/` o pedir credenciales al administrador Lorca

#### 1.5 Crear databases Hive si no existen

```bash
beeline -u jdbc:hive2://localhost:10000 -e "
CREATE DATABASE IF NOT EXISTS gtp_bronze LOCATION 'hdfs:///user/gtp/bronze';
CREATE DATABASE IF NOT EXISTS gtp_silver LOCATION 'hdfs:///user/gtp/silver';
CREATE DATABASE IF NOT EXISTS gtp_gold   LOCATION 'hdfs:///user/gtp/gold';
"
```

- **Alternativa:** Ejecutar los DDL SQL directamente: `hive -f Lorca/BBDD/schemas/bronze_ddl.sql`

---

### BLOQUE 2 — PRUEBA PASO A PASO DE CADA SCRIPT

> Cada script debe probarse en orden. No pasar al siguiente hasta que el anterior produzca el output correcto.

#### 2.1 Prueba `bronze_ingest.py` — Fuente Eurostat (la más simple)

```bash
spark-submit --master yarn --deploy-mode client \
  Lorca/BBDD/bronze_ingest.py --source eurostat --mode overwrite
```

- **Verificaciones post-ejecución:**
  ```bash
  hdfs dfs -ls /user/gtp/bronze/eurostat_gdp/
  # debe mostrar directorios year=YYYY/country=XX/
  spark-submit -e "SELECT COUNT(*) FROM gtp_bronze.bronze_eurostat_gdp_raw;"
  # debe devolver número > 0
  ```
- **Errores comunes a anticipar:**
  - `FileNotFoundError` en la ruta del CSV → verificar `INPUT_FILE_MASTER` en `config.py`
  - Columna `country` null → el split de `city_code` por `_` no funciona para nombres de ciudad con guión (ej: `Clermont-Ferrand_FR` → ISO2 = `FR` correcto)
  - `MSCK REPAIR TABLE` falla → ejecutar manualmente en Hive

#### 2.2 Prueba `bronze_ingest.py` — Fuente Finance

```bash
spark-submit --master yarn bronze_ingest.py --source finance --mode overwrite
```

- **Verificar:** Que el CSV de finanzas tiene exactamente los tickers en `EURO_FUAS`, sin tickers desaparecidos
- **Columnas esperadas en Bronze:** `city_code, company_name, ticker, year, close_price_avg, volatility, volume_avg, pe_ratio, market_cap, dividend_yield`

#### 2.3 Prueba `bronze_ingest.py` — Fuente S2 (solo tras crear `sentinel2_to_csv.py`)

```bash
spark-submit --master yarn bronze_ingest.py --source s2 --mode overwrite
```

- **Verificar:** Partición `year=2018/country=ES/` existe y tiene datos de ciudades españolas

#### 2.4 Prueba `silver_transform.py` — Solo dimensiones

```bash
spark-submit --master yarn silver_transform.py --skip-facts
```

- **Verificaciones:**
  ```bash
  spark-submit -e "SELECT COUNT(*) FROM gtp_silver.dim_city;"
  # debe devolver 237 (número de ciudades en EURO_FUAS)
  spark-submit -e "SELECT COUNT(*) FROM gtp_silver.dim_date WHERE year = 2020;"
  # debe devolver 366 (año bisiesto)
  spark-submit -e "SELECT * FROM gtp_silver.dim_company WHERE is_current = true LIMIT 5;"
  # debe devolver registros válidos con valid_to = '9999-12-31'
  ```
- **Errores comunes:**
  - `ImportError: cannot import name EURO_FUAS from config` → verificar que `sys.path.insert` apunta correctamente a `Lorca/ETL/script/`
  - `dim_city` con 0 filas → EURO_FUAS vacío o import fallido

#### 2.5 Prueba `silver_transform.py` — Facts completos

```bash
spark-submit --master yarn silver_transform.py
```

- **Verificaciones post-ejecución:**
  ```bash
  spark-submit -e "SELECT city_name, year, ndvi_mean, green_index FROM gtp_silver.fact_environmental LIMIT 10;"
  # green_index debe estar en [0, 1]
  spark-submit -e "SELECT city_name, year, ln_gdp_pps, ln_gdp_pps_sq FROM gtp_silver.fact_economic LIMIT 10;"
  # ln_gdp_pps_sq debe ser exactamente ln_gdp_pps^2
  ```
- **Error crítico a verificar:** Si `fact_environmental` tiene 0 filas → Bronze Sentinel2 no existe aún (Bloque 0 pendiente)

#### 2.6 Prueba `gold_build.py`

```bash
spark-submit --master yarn gold_build.py --table all
```

- **Verificaciones:**
  ```bash
  spark-submit -e "SELECT COUNT(DISTINCT city_code) FROM gtp_gold.fact_kuznets;"
  # debe ser ≤ 237
  spark-submit -e "SELECT city_name, investment_score FROM gtp_gold.city_ranking WHERE year = 2020 ORDER BY rank_position LIMIT 20;"
  # debe mostrar ranking coherente con green_index * 100
  spark-submit -e "SELECT * FROM gtp_gold.fact_kuznets WHERE cluster_id IS NOT NULL LIMIT 1;"
  # debe devolver null (placeholder hasta que corra clustering)
  ```

#### 2.7 Prueba `models/clustering.py`

```bash
spark-submit --master yarn models/clustering.py --k-min 2 --k-max 6
```

- **Verificaciones:**
  ```bash
  spark-submit -e "SELECT cluster_id, COUNT(*) as n_cities FROM gtp_gold.fact_kuznets WHERE year = 2020 GROUP BY cluster_id;"
  # debe mostrar 2–6 clusters con distribución razonable
  spark-submit -e "SELECT cluster_id, cluster_label FROM gtp_gold.fact_kuznets GROUP BY cluster_id, cluster_label;"
  # cluster_label debe ser descriptivo (ej: High-Income-Greening)
  ```
- **Errores comunes:**
  - Feature matrix con todos NaN → Silver no tiene datos → Bloque 2.5 no completado
  - `ClusteringEvaluator` falla con K=2 → pocas ciudades con datos completos

#### 2.8 Prueba `models/ekc_regression.py`

```bash
spark-submit --master yarn models/ekc_regression.py
```

- **Verificaciones:**
  ```bash
  spark-submit -e "SELECT cluster_id, beta1, beta2, turning_point_y, ekc_shape FROM gtp_gold.ekc_parameters;"
  # turning_point_y debe ser un valor positivo (GDP en euros), típicamente entre 20000 y 60000
  # ekc_shape debe ser predominantemente INVERTED_U para ciudades europeas
  ```
- **Verificación de validez estadística:** `β₂ < 0` es condición necesaria para curva EKC en forma de U invertida. Si β₂ ≥ 0 para todos los clusters, los datos pueden no mostrar el patrón EKC.
- **Errores comunes:**
  - `MIN_OBS_PER_CLUSTER = 30` no alcanzado → fusionar clusters pequeños o reducir umbral temporalmente a 20
  - `linearmodels.PanelOLS` falla con datos sin varianza suficiente → verificar que hay ≥3 años por ciudad en el cluster

#### 2.9 Prueba `models/xgboost_classifier.py`

```bash
spark-submit --master yarn models/xgboost_classifier.py --test-years 2
```

- **Verificaciones:**
  - **F1 Macro > 0.5:** Si F1 Macro < 0.5, el etiquetado automático puede estar mal calibrado (revisar umbrales de `label_phase`)
  - **Distribución de clases:** Si DEGRADANDO > 90%, el turning point Y* puede ser muy alto → todos los GDP están lejos
  - ```bash
    spark-submit -e "SELECT turning_point_phase, COUNT(*) FROM gtp_gold.fact_kuznets WHERE year = 2022 GROUP BY turning_point_phase;"
    # Esperado: mayoría DEGRADANDO, algunas TURNING, pocas RECUPERANDO
    ```
- **Verificar `investment_score`:** Rango esperado [0, 100]. Si todos los scores son < 30, revisar formula de `green_index` → si está todo en 0 es porque Bronze S2 no existe

#### 2.10 Prueba `models/prophet_forecast.py`

```bash
spark-submit --master yarn models/prophet_forecast.py --horizon 5
```

- **Verificaciones:**
  ```bash
  spark-submit -e "SELECT city_name, forecast_ndvi_1y, forecast_ndvi_3y, forecast_ndvi_5y, prophet_mape FROM gtp_gold.fact_kuznets WHERE forecast_ndvi_1y IS NOT NULL LIMIT 20;"
  # MAPE debe ser < 30% para ciudades con datos continuos
  # forecast_ndvi_1y debe estar en rango similar a ndvi_mean (entre 0.0 y 0.8)
  ```
- **Error crítico esperado:** Si Bronze S2 no tiene datos mensuales, Prophet no puede entrenar. Este modelo es el último en ejecutarse y depende de todos los anteriores.

---

### BLOQUE 3 — PRUEBA DEL ORQUESTADOR

#### 3.1 Prueba en modo dry-run

```bash
spark-submit --master yarn run_pipeline.py --dry-run
```

- **Criterio de aceptación:** Muestra los 7 pasos en orden con rutas correctas, sin ejecutar ninguno

#### 3.2 Prueba solo ingesta

```bash
spark-submit --master yarn run_pipeline.py --only-ingest --bronze-source eurostat
```

- **Criterio de aceptación:** Bronze (Eurostat) + Silver + Gold base se ejecutan en secuencia, resumen final muestra ✓ para las 3 fases

#### 3.3 Prueba pipeline completo

```bash
spark-submit --master yarn run_pipeline.py
```

- **Criterio de aceptación:**
  - Las 7 fases terminan (algunas pueden fallar si Bloque 0 no está completo)
  - El resumen final muestra tiempo de ejecución por fase
  - `fact_kuznets` en Gold tiene columnas no-null para: `cluster_id`, `beta1`, `beta2`, `turning_point_phase`, `forecast_ndvi_1y`

---

### BLOQUE 4 — CORRECCIONES DE COMPATIBILIDAD ETL ↔ BBDD

#### 4.1 Estandarizar nombres de ciudad en todos los CSVs

- **Problema potencial:** `Sentinel-2_extract.py` y `eurostat_extract.py` pueden usar nombres distintos para la misma ciudad (ej: `Vienna` vs `Wien_AT` vs `Wien`)
- **Acción:** Crear función `normalize_city_code(name)` en `config.py` que mapee variantes → código canónico `NombreCiudad_ISO2`
- **Dónde aplicar:** En todos los scripts to_csv (Bloque 0) y en `bronze_ingest.py` como paso de limpieza previo al write

#### 4.2 Revisar `HRL_extract.py` — Cobertura de años

- **Leer el archivo completo y verificar:**
  - ¿Descarga solo el año 2018 o múltiples años?
  - ¿Los GeoTIFFs están georeferenciados con CRS EPSG:3035 (LAEA Europe)?
  - ¿Existe paso de recorte por FUA boundary?
- **Acción si no hay recorte:** `hrl_to_csv.py` (Bloque 0.3) debe usar el shapefile FUA de Copernicus para hacer clip antes de calcular media de píxeles

#### 4.3 Verificar columna `Source` en Eurostat CSV

- **`bronze_ingest.py` espera:** columna `source` con valor `"eurostat_gdp"`
- **`eurostat_extract.py` genera:** columna `Source` con valor `"Eurostat"` (mayúscula, valor diferente)
- **Acción:** En `bronze_ingest.py`, función `ingest_eurostat_gdp()`, renombrar y normalizar antes de write:
  ```python
  df = df.withColumnRenamed("Source", "source").withColumn("source", F.lit("eurostat_gdp"))
  ```

---

### BLOQUE 5 — VALIDACIÓN DE MODELOS (criterios estadísticos mínimos)

> Si los modelos no alcanzan estos umbrales, hay un problema de datos o de implementación, no de sintonía.

#### 5.1 Clustering — Validación

| Métrica | Umbral mínimo | Acción si no se cumple |
|---------|---------------|------------------------|
| Silhouette Score | > 0.30 | Revisar features: puede haber features con escala muy diferente (normalizar mejor) |
| N ciudades por cluster | ≥ 20 | Si cluster < 20 ciudades, fusionar con el más cercano |
| Clusters con label coherente | 100% | Si todos los centroides son similares, K óptimo puede ser 2 |

#### 5.2 EKC Panel Regression — Validación

| Métrica | Umbral | Acción |
|---------|--------|--------|
| β₂ < 0 (al menos en 1 cluster) | Requerido | Si β₂ > 0 en todos → los datos no muestran EKC → reportar como hallazgo, no como error |
| t-stat β₁ y β₂ | abs(t) > 1.96 (p < 0.05) | Si no significativo: aumentar umbral `MIN_OBS_PER_CLUSTER` a 50 agrupando ciudades vecinas |
| R² within | > 0.10 | Si R² < 0.1: variación del NDVI no explicada por GDP → añadir variable de control (precipitation, población) |
| Y* en rango [15000, 80000] EUR | Esperado | Si Y* < 5000 o Y* > 200000: β estimados inverosímiles → revisar escala de variables |

#### 5.3 XGBoost — Validación

| Métrica | Umbral | Acción |
|---------|--------|--------|
| F1 Macro | > 0.50 | Si < 0.50: revisar distribución de clases — si DEGRADANDO > 95%, el modelo colapsa |
| Precisión clase TURNING | > 0.40 | Clase rara; aumentar pesos de clase o submuestrear DEGRADANDO |
| Top feature | Debe ser `gdp_gap_to_turning` o `ndvi_trend_slope` | Si top feature es `year` o `cluster_id`, el modelo aprende artefactos |

#### 5.4 Prophet — Validación

| Métrica | Umbral | Acción |
|---------|--------|--------|
| MAPE in-sample | < 20% | Si MAPE > 30%: pocos meses de datos (< 36), revisar rango de `bronze_sentinel2_raw` |
| Ciudades con forecast | ≥ 100 (de 237) | Si < 50: datos de Sentinel-2 insuficientes → Bloque 0 no completado |
| `prophet_turning_year` en [2025, 2050] | Esperado | Si fuera de rango: serie temporal demasiado corta o sin tendencia clara |

---

### BLOQUE 6 — DOCUMENTACIÓN (actualizar tras cada prueba exitosa)

#### 6.1 Actualizar `CLAUDE.md` — Tabla de estado

- **Cambiar de ⏳ a ✅:** Cada componente al ser probado exitosamente en Lorca
- **Añadir columna "Probado en Lorca":** boolean para distinguir "código existe" de "funciona en producción"

#### 6.2 Actualizar `GTP_DOCUMENTACION_TECNICA.md` — Sección de resultados

- **Tras clustering:** Añadir tabla de clusters encontrados (K, silhouette, label, N ciudades, GDP medio, NDVI medio)
- **Tras EKC:** Añadir tabla de parámetros por cluster (β₁, β₂, Y*, R², forma de curva)
- **Tras XGBoost:** Añadir distribución de ciudades por fase (DEGRADANDO/TURNING/RECUPERANDO) y top-10 ciudades TURNING
- **Tras Prophet:** Añadir distribución de `prophet_turning_year` (histograma de años predichos)

#### 6.3 Añadir sección de resultados al informe final del proyecto

- **Pendiente de decidir:** ¿El informe del proyecto incluye las tablas Gold como evidencia? Si sí, exportar a CSV para incluir en el repo:
  ```bash
  spark-submit -e "SELECT * FROM gtp_gold.city_ranking WHERE year = 2022 ORDER BY rank_position" \
    | head -50 > resultados/city_ranking_2022.csv
  ```

---

### BLOQUE 7 — PENDIENTES OPCIONALES / MEJORAS FUTURAS

> Estas tareas NO son bloqueantes pero mejorarían la calidad del trabajo

- [ ] **Interpolación HRL:** Los años sin dato HRL (2007, 2008, 2010, 2011, etc.) deben interpolarse linealmente entre publicaciones → añadir paso en `silver_transform.py`
- [ ] **Validación cruzada del XGBoost:** Usar `TimeSeriesSplit` con 3 folds en lugar de único split para estimación más robusta del F1
- [ ] **SHAP values para XGBoost:** Añadir `shap.TreeExplainer` para interpretabilidad por ciudad individual (útil para el informe)
- [ ] **Mapa interactivo:** `city_ranking` exportada + `folium` para visualización geográfica del ranking europeo
- [ ] **Test de Prophet con `cross_validation`:** Usar `prophet.diagnostics.cross_validation` para MAPE más robusto que el in-sample
- [ ] **Benchmark EKC vs XGBoost:** Comparar `turning_point_phase` del XGBoost con la predicción implícita del EKC (`gdp_gap_to_turning < 0` → turning) para ver consistencia

---

### ORDEN DE EJECUCIÓN ESTRICTO PARA LA PRÓXIMA SESIÓN

```
1. BLOQUE 0.4 → verificar eurostat_extract.py output           [30 min]
2. BLOQUE 1   → verificar entorno Lorca completo               [45 min]
3. BLOQUE 2.1 → bronze Eurostat (el más simple, sin GeoTIFF)   [20 min]
4. BLOQUE 2.4 → silver dims (sin facts)                        [20 min]
5. BLOQUE 0.1 → crear sentinel2_to_csv.py                      [60 min] ← bloqueante para el resto
6. BLOQUE 0.3 → crear hrl_to_csv.py                            [60 min] ← bloqueante para HRL
7. BLOQUE 2   → completar todos los scripts                    [120 min]
8. BLOQUE 3   → orquestador pipeline completo                  [30 min]
9. BLOQUE 6   → actualizar documentación                       [30 min]
```

**TOTAL ESTIMADO:** ~7h de trabajo en el cluster Lorca. Planificar sesión larga o dividir en 2 sesiones.

---

## [2026-03-09 23:45] CAMBIO — Reescritura de Sentinel-2, Sentinel-5p y creación de hrl_to_csv.py

**Problema resuelto:** Los scripts ETL de GEE descargaban GeoTIFFs completos pero ningún script los convertía a CSV. El `merge.py` ya esperaba `sentinel2.csv`, `s5p.csv` y `hrl.csv` — la brecha bloqueaba el pipeline completo.

**Decisión de arquitectura:** En lugar de descargar GeoTIFFs y leerlos con `rasterio`, se usa `reduceRegion` de GEE para obtener los estadísticos directamente en los servidores de Google. Resultado: cero descargas de imágenes para S2 y S5P.

---

### Archivos modificados

**`Lorca/ETL/script/Sentinel-2_extract.py`** — REESCRITO COMPLETO

- **Antes:** `getDownloadURL` → descarga `.zip` con GeoTIFF por ciudad×mes (TB de datos)
- **Ahora:** `reduceRegion(mean + stdDev + count, scale=60m, bestEffort=True)` → devuelve `{'NDVI_mean': X, 'NDVI_stdDev': Y, 'NDVI_count': Z}` directamente
- Mantiene: máscara de nubes QA60, mediana mensual, buffer 20km, colección `S2_SR_HARMONIZED`
- Añade: idempotencia (carga CSV existente al inicio, salta registros ya procesados), reintentos con backoff exponencial, escritura CSV thread-safe con `threading.Lock()`
- Output: `DatosProcesados/sentinel2.csv` — columnas: `City, Year, Month, NDVI_Mean, NDVI_Std, pixel_count`
- Args: `--start-year` (default 2018), `--workers` (default 8)

**`Lorca/ETL/script/Sentinel-5p_extract.py`** — REESCRITO COMPLETO

- **Antes:** `getDownloadURL` → descarga `.zip` con GeoTIFF NO2 por ciudad×mes
- **Ahora:** `reduceRegion(mean + stdDev + count, scale=1113m)` → devuelve `{'NO2_mean': X, ...}`
- Banda: `tropospheric_NO2_column_number_density` → renombrada a `NO2` antes de reducir (simplifica los keys)
- Mantiene: filtro `cloud_fraction < 0.3`, media mensual (no mediana, apropiada para NO2 continuo), colección `COPERNICUS/S5P/OFFL/L3_NO2`
- Añade: misma idempotencia, reintentos, thread-safety que S2
- Output: `DatosProcesados/s5p.csv` — columnas: `City, Year, Month, NO2_Mean, NO2_Std, pixel_count`
- Unidades: mol/m² (rango típico EU: 5×10⁻⁵ a 2×10⁻⁴)

**`Lorca/ETL/script/hrl_to_csv.py`** — NUEVO

- Lee los GeoTIFFs descargados por `HRL_extract.py` de `data/hrl/{año}/{capa}/{Ciudad}_{capa}_{año}.tif`
- Abre cada `.tif` con `rasterio`, convierte a `float`, enmascara nodata (255) y valores > 100
- Calcula `np.nanmean()` de píxeles válidos
- Output: `DatosProcesados/hrl.csv` — columnas: `City, Year, Imperviousness_Mean, Tree_Cover_Pct, Small_Woody_Mean`
- Requiere: `pip install rasterio numpy`
- HRL es estático (solo 2018) y pequeño (~700MB total) → descarga única, conversión única

**`Lorca/ETL/script/run_all.py`** — REFACTORIZADO

- Eliminado el bucle por ciudad y la función `generar_tareas_ciudad` (ya no necesarios)
- Los 3 scripts de satélite iteran todas las ciudades internamente
- Nueva estructura: `SCRIPTS_ECONOMICOS` + `SCRIPTS_AMBIENTALES` + `SCRIPTS_FINALES`
- `SCRIPTS_AMBIENTALES` = `[Sentinel-2_extract.py, Sentinel-5p_extract.py, HRL_extract.py, hrl_to_csv.py]`
- Resumen final con tabla de fases y conteo OK/total

---

### Documentación actualizada

- `GTP_DOCUMENTACION_TECNICA.md` — Sección 7 (flujo ETL) reescrita con nueva arquitectura; Secciones 8.5 y 8.6 actualizadas con proceso `reduceRegion`; Nueva sección 8.6b para `hrl_to_csv.py`
- `CLAUDE.md` — Árbol de directorios actualizado (ETL con ✅ en scripts listos)
- `HISTORIAL.md` — Esta entrada

---

### TO-DO actualizado (brecha crítica resuelta)

- [x] ~~`sentinel2_to_csv.py`~~ → resuelto: `Sentinel-2_extract.py` ahora produce `sentinel2.csv` directamente
- [x] ~~`sentinel5p_to_csv.py`~~ → resuelto: `Sentinel-5p_extract.py` ahora produce `s5p.csv` directamente
- [x] `hrl_to_csv.py` creado
- [ ] Verificar autenticación GEE en Lorca: `earthengine authenticate`
- [ ] Instalar `rasterio` en Lorca: `pip install rasterio`
- [ ] Probar `Sentinel-2_extract.py --workers 2` en Lorca con subset (3-5 ciudades) antes del run completo
- [ ] Probar `hrl_to_csv.py` tras ejecutar `HRL_extract.py`
- [ ] Verificar que `merge.py` produce dataset maestro correcto con los 3 CSVs

---

## [2026-03-10 00:00] CORRECCIÓN — Columns erróneas de Bronze Finance en Bloque 2.2 del HISTORIAL

**Error en:** entrada `[2026-03-09 23:15]`, sección Bloque 2.2, campo "Columnas esperadas en Bronze".

**Lo que decía (incorrecto):**
```
city_code, company_name, ticker, year, close_price_avg, volatility, volume_avg, pe_ratio, market_cap, dividend_yield
```

**Lo correcto** (alineado con `bronze_ddl.sql` y `bronze_ingest.py`):
```
ticker, company_name, sector, industry, country, fua_country_code,
year, close_price, annual_return, annual_volatility, volume_avg,
current_pe, current_beta, extraction_date
+ columna de partición: country_code (= fua_country_code)
```

**Columnas que NO existen** en el DDL ni en el CSV de YFinance: `city_code`, `close_price_avg`, `volatility`, `market_cap`, `dividend_yield`.

**Afectado:** Solo la documentación de verificación. El código (`bronze_ingest.py`, `bronze_ddl.sql`, `YFinance_extract.py`) era correcto y está alineado entre sí. No hubo impacto en el código.

---

## [2026-03-10 00:00] DECISIÓN — Añadir PostgreSQL como Serving Layer para cuadro de mando

**Contexto:** Se debate si HDFS/Hive es suficiente para el dashboard o si se necesita una base de datos relacional adicional.

**Decisión:** Añadir **PostgreSQL como capa de servicio (Serving Layer)**, paralela al Gold HDFS.

**Razón:**
- Hive/HDFS: latencia ~segundos/minutos. Válido para cómputo batch Spark. Inadecuado para dashboards.
- PostgreSQL: latencia <10ms con índices. Ideal para Flask API, Power BI, Tableau, Grafana.
- El volumen es tiny (~2.370 filas en `fact_kuznets`): PostgreSQL lo maneja trivialmente.
- El stack Lorca (HDFS + Hive) se mantiene intacto. PostgreSQL es adicional, no sustitutivo.

**Arquitectura resultante:**
```
ETL → HDFS Bronze → HDFS Silver → HDFS Gold (Hive/Parquet)
                                         │
                                export_to_postgres.py (fase 8)
                                         │
                                PostgreSQL schema gtp.*
                                         │
                            Flask API /api/* + Power BI / Tableau
```

**Implicación:** HDFS es la fuente de verdad analítica. PostgreSQL es un snapshot del Gold para servicio rápido. Si el pipeline corre de nuevo, se sobreescribe PostgreSQL (`if_exists="replace"`).

---

## [2026-03-10 00:00] CAMBIO — Nuevos archivos: PostgreSQL Serving Layer

**Archivos creados:**

- `Lorca/BBDD/schemas/postgres_serving_ddl.sql` — DDL PostgreSQL del serving layer. 4 tablas (espejo del Gold) + 4 vistas para la API Flask. Indexes en columnas frecuentes: `(city_code, year)`, `(country_code, year)`, `(turning_point_phase, year)`, `(year, rank_position)`.

- `Lorca/BBDD/export_to_postgres.py` — Script de exportación Gold → PostgreSQL. Dos modos:
  - `--mode spark`: Lee Parquet HDFS con PySpark, escribe vía JDBC (para Lorca). Requiere `--packages org.postgresql:postgresql:42.7.3`.
  - `--mode local`: Lee CSVs exportados con pandas, escribe vía sqlalchemy (para local, sin Spark). Requiere `pip install pandas sqlalchemy psycopg2-binary`.
  - Credenciales via env vars: `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASSWORD`.

**Archivos modificados:**

- `Lorca/BBDD/run_pipeline.py` — Añadida **fase 8: export** (`export_to_postgres.py`). Nuevos flags: `--skip-export`, `--only-export`. Spark-submit para export incluye automáticamente `--packages org.postgresql:postgresql:42.7.3`.

- `Lorca/Web/app.py` — Añadidos **7 endpoints de datos** que consultan PostgreSQL:
  - `GET /api/ranking` — ranking europeo (filtrable por year, country, limit)
  - `GET /api/city/<city_code>` — serie temporal completa + ML detail
  - `GET /api/opportunities` — ciudades en fase TURNING
  - `GET /api/clusters` — parámetros EKC por cluster
  - `GET /api/years` — años disponibles
  - `GET /api/countries` — países con n_cities
  - `GET /api/summary` — stats agregadas del último año
  - Los endpoints de auth/newsletter siguen funcionando sin PostgreSQL (degradación elegante con 503).

---

## [2026-03-10 00:00] TO-DO — Estado actualizado tras sesión 2026-03-10

### Completado en esta sesión
- [x] Corrección documentación Bronze Finance columns
- [x] `postgres_serving_ddl.sql`
- [x] `export_to_postgres.py`
- [x] `run_pipeline.py` + fase 8
- [x] `app.py` con endpoints de datos

### Pendiente (requiere Lorca)
- [ ] Verificar autenticación GEE: `earthengine authenticate`
- [ ] Instalar dependencias: `pip install rasterio linearmodels xgboost scikit-learn prophet`
- [ ] Instalar driver JDBC en Lorca (o dejar que spark-submit lo descargue con `--packages`)
- [ ] Crear directorios HDFS: `hdfs dfs -mkdir -p /user/gtp/{bronze,silver,gold}`
- [ ] Ejecutar DDL Hive: `hive -f Lorca/BBDD/schemas/bronze_ddl.sql` (etc.)
- [ ] Prueba bronze → silver → gold → export paso a paso (ver Bloque 2 del HISTORIAL)
- [ ] Probar pipeline completo: `spark-submit --master yarn run_pipeline.py`

### Pendiente (no requiere Lorca)
- [ ] Actualizar `GTP_DOCUMENTACION_TECNICA.md` — sección nueva sobre PostgreSQL serving layer
- [ ] Crear `Lorca/BBDD/schemas/postgres_serving_ddl.sql` en la DB real (una sola vez)
- [ ] Decidir si usar Supabase / PostgreSQL propio / RDS para el dashboard

---

## [2026-03-11 00:00] CAMBIO — Corrección de 5 bugs críticos en el pipeline BBDD + modelos ML

**Contexto:** Revisión exhaustiva de los 6 scripts del pipeline Lorca (`bronze_ingest.py`, `silver_transform.py`, `gold_build.py`, `clustering.py`, `ekc_regression.py`, `xgboost_classifier.py`, `prophet_forecast.py`) para detectar bugs que impedirían la ejecución en el cluster.

**Bugs corregidos:**

### Bug 1 — `bronze_ingest.py:33` — Ruta absoluta hardcodeada (CRÍTICO)
- **Problema:** `ETL_BASE = Path("/home/223B3336juan/Big Data I/...")` — falla si el script se ejecuta desde otro usuario o ruta.
- **Fix:** `ETL_BASE = Path(__file__).resolve().parent.parent / "ETL" / "script"` — ruta derivada del propio archivo, portátil en cualquier entorno Lorca.

### Bug 2 — `silver_transform.py:127` — Ruta absoluta hardcodeada en `build_dim_city` (CRÍTICO)
- **Problema:** `etl_script_path = str(Path("/home/223B3336juan/Big Data I/..."))` — si la ruta no existe, `import config` falla silenciosamente, `EURO_FUAS` queda vacío y `dim_city` se crea con 0 filas. Todos los joins posteriores devuelven NULL en `city_sk`.
- **Fix:** `etl_script_path = str(Path(__file__).resolve().parent.parent / "ETL" / "script")`

### Bug 3 — `prophet_forecast.py:241` — Type mismatch en schema del pandas UDF (CRÍTICO)
- **Problema:** `PROPHET_OUTPUT_SCHEMA` declaraba `"ds"` como `StringType()`. Spark convierte la columna `ds` (resultado de `F.to_date()`) a `datetime.date` en pandas. Al devolver el resultado del UDF, PySpark no puede serializar `datetime.date` → `StringType` y lanza un error de tipo en runtime.
- **Fix:** Cambiar a `DateType()` y añadir `DateType` a los imports. Spark convierte `datetime.date` → `DateType` sin problema.

### Bug 4 — `clustering.py:289-293` — Ternary write confuso (MENOR)
- **Problema:** `(writer.partitionBy(...) if cond else writer).parquet(path)` — válido Python pero confuso; no verificaba si `"country"` también existe antes de `partitionBy`.
- **Fix:** Reemplazado por `if/else` explícito con verificación de ambas columnas (`year` AND `country`).

### Bug 5 — `xgboost_classifier.py:335-339` — Mismo patrón ternary (MENOR)
- **Fix:** Igual que Bug 4.

**Bugs descartados (el código era correcto):**
- `silver_transform.py` columnas Bronze: Bronze normaliza a minúsculas en `withColumnRenamed` → columnas `ndvi_mean`, `no2_mean`, etc. son correctas.
- `clustering.py:96` `ndvi_annual_mean`: Silver `fact_environmental` sí tiene esa columna (creada por `.alias("ndvi_annual_mean")` en el aggregation).
- `gold_build.py` `.drop(fin_agg["country_code"])`: PySpark acepta Column objects en `drop()` para desambiguar columnas duplicadas tras join con expresión booleana.
- `ekc_regression.py` AIC formula: `result.loglik * (-2) + 2 * len(params)` es idéntico a `-2 * loglik + 2k` — correcto.

**Archivos modificados:** `bronze_ingest.py`, `silver_transform.py`, `models/prophet_forecast.py`, `models/clustering.py`, `models/xgboost_classifier.py`

---

## [2026-03-11 00:00] TO-DO — Estado actualizado

### Completado
- [x] 5 bugs corregidos en pipeline BBDD/modelos
- [x] `postgres_serving_ddl.sql`, `export_to_postgres.py`, `app.py` (endpoints)
- [x] Pipeline completo escrito: bronze → silver → gold → clustering → ekc → xgboost → prophet → export

### Pendiente (requiere Lorca)
- [ ] Verificar autenticación GEE: `earthengine authenticate`
- [ ] Instalar dependencias: `pip install rasterio linearmodels xgboost scikit-learn prophet`
- [ ] Crear directorios HDFS y ejecutar DDL Hive (ver Bloque 1 del HISTORIAL)
- [ ] Prueba paso a paso: bronze → silver → gold → modelos (ver Bloque 2 del HISTORIAL)
- [ ] Ejecutar pipeline completo: `spark-submit --master yarn run_pipeline.py`

### Pendiente (no requiere Lorca)
- [ ] Actualizar `GTP_DOCUMENTACION_TECNICA.md` — sección PostgreSQL serving layer + pipeline completo

---

## [2026-03-11 00:00] CAMBIO — `run_all.py` integra el pipeline BBDD (orquestador maestro único)

**Problema:** Existían dos orquestadores separados (`run_all.py` para ETL, `run_pipeline.py` para BBDD). Había que ejecutarlos a mano en secuencia.

**Cambio:** `run_all.py` es ahora el **único punto de entrada** de todo el proyecto. Al finalizar el ETL lanza automáticamente `run_pipeline.py` via `spark-submit` en YARN.

**Nuevos flags:**
- `python run_all.py` — pipeline completo (ETL + BBDD + ML + export)
- `python run_all.py --skip-bbdd` — solo ETL
- `python run_all.py --only-bbdd` — solo BBDD/ML (datos ya descargados)
- `python run_all.py --only-bbdd --bbdd-args "--only-models"` — solo reentrenar modelos
- `python run_all.py --dry-run` — muestra comandos sin ejecutar

**`run_pipeline.py` sigue existiendo** y se puede ejecutar independientemente con `spark-submit` cuando se quiera controlar solo la parte Spark.

**Crons sugeridos:**
```bash
# Actualización mensual (satélite + BBDD parcial)
0 6 1 * *  python run_all.py --only-bbdd --bbdd-args "--skip-bronze --skip-silver"
# Actualización anual completa
0 9 2 1 *  python run_all.py
```



---

## [2026-03-11 01:00] CAMBIO — YFinance migrado de granularidad anual a mensual

**Problema:** `YFinance_extract.py` producía un registro por Ticker×Año. Esto reducía la riqueza de la señal financiera y no alineaba con estándares académicos de análisis de riesgo/retorno.

**Decisión:** Cambiar a granularidad mensual — estándar Fama-French. Justificación académica más sólida, ~1.1M filas vs 13M diario (manejable), permite detectar tendencias intra-anuales.

**Cambios en 4 archivos:**

### `Lorca/ETL/script/YFinance_extract.py`
- `groupby([year, month])` en lugar de `groupby(year)`
- Volatilidad mensual: `std * sqrt(21)` en lugar de `std * sqrt(252)` anual
- Umbral mínimo días: 15 (antes 50)
- Renombrado: `Annual_Return` → `Monthly_Return`, `Annual_Volatility` → `Monthly_Volatility`
- Añadida columna `Month`
- Deduplicación por `["Ticker", "Year", "Month"]`
- Output: `finance_monthly_2006_2025.csv` (antes `finance_2006_2025.csv`)

### `Lorca/BBDD/bronze_ingest.py`
- `FINANCE_CSV` apunta a `finance_monthly_2006_2025.csv`
- Schema añade `Month: IntegerType()`
- Renombradas columnas a `monthly_return`, `monthly_volatility`

### `Lorca/BBDD/silver_transform.py`
- `build_fact_financial()`: usa `monthly_return`, `monthly_volatility`; añade `month` a `final_cols`
- `date_sk` calculado como `year * 10000 + month * 100 + 1` (antes solo año)
- `dim_source` SOURCE_CATALOG: YFinance `update_frequency` = `"Monthly"` (antes `"Annual"`)

### `Lorca/BBDD/gold_build.py`
- Agregación financiera mensual → anual: `avg(monthly_volatility) * sqrt(12)` ≈ `* 3.4641`
- Columna resultante `fin_annual_volatility` mantiene nombre para compatibilidad downstream

**Sin impacto en:** modelos ML (clustering, EKC, XGBoost, Prophet) — no usan datos financieros directamente. `fact_kuznets` mantiene `fin_annual_volatility` como antes.

---

## [2026-03-11 12:00] CAMBIO — Unificación de carpeta de datos bajo `Lorca/data/`

---

## [2026-03-11 12:30] DECISIÓN — Flask API descartada; PostgreSQL como serving layer directo para Power BI

**Contexto:** Se planteó usar una Flask API como intermediario entre PostgreSQL y el dashboard. Se descartó por ser sobreingeniería para un proyecto universitario.

**Decisión:** Power BI se conecta **directamente a PostgreSQL** sin ninguna capa intermedia. `app.py` queda ignorado (no se ejecuta ni se configura). El foco es que `export_to_postgres.py` deje los datos listos en PostgreSQL y Power BI los consuma vía conector nativo.

**Flujo final:**
```
run_pipeline.py (Spark) → HDFS Gold → export_to_postgres.py → PostgreSQL gtp.* → Power BI
```

---

## [2026-03-11 12:30] CAMBIO — PostgreSQL preparado para Power BI: dim_city + v_powerbi_map

**Problema:** `fact_kuznets` y `city_ranking` no tenían coordenadas lat/lon. Power BI necesita lat/lon explícitos para visuales de mapa (sin ellos intenta geocodificar por nombre, lo cual falla con nombres como `Madrid_ES`).

**Archivos modificados:**

### `Lorca/BBDD/schemas/postgres_serving_ddl.sql`
- Eliminadas referencias a Flask en comentarios
- **Añadida tabla `gtp.dim_city`** (5 columnas: city_code, city_name, country_code, latitude, longitude). Fuente: `config.EURO_FUAS`. Índice en `country_code`.
- **Añadida vista `gtp.v_powerbi_map`**: join de `fact_kuznets` + `dim_city` para el último año disponible. Contiene lat/lon + investment_score + turning_point_phase + cluster_label. Lista para el visual "Mapa" de Power BI.
- Total tablas: 5 (antes 4). Total vistas: 5 (antes 4).
- Añadida nota al pie con instrucciones de conexión Power BI.

### `Lorca/BBDD/export_to_postgres.py`
- Eliminadas referencias a Flask en comentarios
- **Añadida función `build_dim_city_df()`**: lee `EURO_FUAS` de `config.py`, construye DataFrame pandas con city_code, city_name, country_code, lat, lon.
- **Añadida función `export_dim_city(engine, dry_run)`**: escribe `dim_city` a PostgreSQL.
- `dim_city` se exporta siempre (antes que las tablas Gold) en ambos modos (spark y local).
- Import de `EURO_FUAS` añadido al inicio del script.

**Cómo conectar Power BI:**
1. Inicio → Obtener datos → Base de datos → PostgreSQL
2. Servidor: `<PG_HOST>`, Base de datos: `gtp`
3. Tablas recomendadas para el dashboard: `v_powerbi_map` (mapa), `v_phase_distribution` (barras por país), `v_top_20_cities` (tabla ranking), `ekc_parameters` (parámetros EKC por cluster)

---

## [2026-03-11 13:00] CAMBIO — Setup Lorca simplificado: setup_lorca.sh + .env.example

**Objetivo:** Que después de GEE approval + copiar .env, baste con 2 comandos para tenerlo todo listo.

**Archivos creados:**

### `Lorca/setup_lorca.sh`
Script de setup único que ejecuta en orden:
1. `pip install -r requirements.txt` — todas las dependencias
2. `hdfs dfs -mkdir -p` — 6 directorios HDFS (bronze, silver, gold, models/*)
3. `hive -f` — aplica los 3 DDL (bronze, silver, gold)
4. `earthengine authenticate` — auth GEE

Uso en Lorca:
```bash
chmod +x setup_lorca.sh
./setup_lorca.sh
```

### `Lorca/.env.example`
Plantilla de variables de entorno para PostgreSQL. El puerto puede ser diferente al 5432 por defecto — preguntar al administrador.
```
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
```
Copiar a `.env` y rellenar antes de ejecutar `run_all.py`.

### `Lorca/BBDD/export_to_postgres.py`
Añadida carga automática de `.env` al inicio con `python-dotenv`. Busca `.env` en `Lorca/BBDD/` y en `Lorca/` (el que encuentre primero).

**Flujo completo de arranque en Lorca:**
```bash
# 1. Setup (una sola vez)
./setup_lorca.sh

# 2. Configurar credenciales PostgreSQL
cp .env.example .env
nano .env   # rellenar PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

# 3. Ejecutar pipeline completo
python ETL/script/run_all.py
```

**Problema:** Los CSVs del ETL se generaban en `Lorca/ETL/script/data/`, mezclando código y datos en la misma carpeta. `bronze_ingest.py` apuntaba también a esa ruta. En Lorca (cluster), esto obligaba a navegar dentro de la carpeta de scripts para encontrar los datos.

**Decisión:** Mover toda la carpeta de datos a `Lorca/data/` — un nivel claro de separación código/datos.

**Estructura resultante:**
```
Lorca/
  data/
    DatosProcesados/          ← sentinel2.csv, s5p.csv, hrl.csv
    hrl/                      ← GeoTIFFs de HRL_extract.py
    finance_monthly_2006_2025.csv
    merge.csv
  ETL/script/                 ← solo .py, sin datos
  BBDD/                       ← solo .py, sin datos
```

**Archivos modificados (3):**

### `Lorca/ETL/script/config.py`
- `DATA_DIR = BASE_DIR / "data"` → `DATA_DIR = BASE_DIR.parent.parent / "data"`
- `INPUT_FILE_FINANCE = BASE_DIR / "finance_monthly_2006_2025.csv"` → `INPUT_FILE_FINANCE = DATA_DIR / "finance_monthly_2006_2025.csv"`
- `INPUT_DIR_PROCESSED` y `OUTPUT_FILE_MASTER` sin cambio de lógica, se recalculan desde el nuevo `DATA_DIR`

### `Lorca/ETL/script/YFinance_extract.py`
- `output_file = config.BASE_DIR / args.out` → `output_file = config.DATA_DIR / args.out`

### `Lorca/BBDD/bronze_ingest.py`
- `ETL_BASE = Path(__file__).resolve().parent.parent / "ETL" / "script"` eliminado
- `ETL_DATA = ETL_BASE / "data"` → `ETL_DATA = Path(__file__).resolve().parent.parent / "data"` (sube a `Lorca/`, añade `data/`)
- `FINANCE_CSV` y `PROCESSED_DIR` se recalculan desde el nuevo `ETL_DATA`

**Scripts no modificados:** `Sentinel-2_extract.py`, `Sentinel-5p_extract.py`, `hrl_to_csv.py`, `merge.py` — todos usan `config.INPUT_DIR_PROCESSED` y `config.DATA_DIR` directamente, se benefician del cambio automáticamente.

---

## [2026-03-11 14:00] CAMBIO — 4 nuevas fuentes de datos automatizadas

**Decisión:** Añadir 4 fuentes de datos adicionales al pipeline GTP para enriquecer el análisis EKC. Todas completamente automatizadas (sin descarga manual).

### Fuentes añadidas:

| Script | Dataset | Output CSV | Granularidad |
|--------|---------|-----------|-------------|
| `era5_extract.py` | ERA5-Land (ECMWF) via GEE | `era5.csv` | Ciudad × Mes |
| `s5p_aerosol_extract.py` | S5P OFFL AER_AI via GEE | `s5p_aerosol.csv` | Ciudad × Mes |
| `urban_atlas_extract.py` | ESA WorldCover v100/v200 via GEE | `urban_atlas.csv` | Ciudad × Año (2020, 2021) |
| `edgar_co2_extract.py` | EDGAR v8 FT2022 (JRC/EU) HTTP | `edgar_co2.csv` | Ciudad × Año (1970-2022) |

### Variables nuevas:
- **ERA5:** `temp_annual_mean_c`, `precip_annual_sum_m` → `fact_environmental`
- **S5P Aerosol:** `uvai_annual_mean` (proxy PM2.5) → `fact_environmental`
- **WorldCover:** `wc_tree_pct`, `wc_built_pct`, `wc_crop_pct`, `wc_natural_pct` → `fact_environmental` + `fact_kuznets`
- **EDGAR CO2:** `co2_country_kt` (kt CO2/año por país) → `fact_economic` + `fact_kuznets`

### Archivos modificados:
- `ETL/script/run_all.py` — 4 nuevos scripts en SCRIPTS_AMBIENTALES
- `BBDD/schemas/bronze_ddl.sql` — 4 nuevas tablas Bronze (total: 10)
- `BBDD/bronze_ingest.py` — 4 nuevas funciones de ingesta (total: 9)
- `ETL/script/merge.py` — carga era5, s5p_aerosol, urban_atlas, edgar_co2
- `BBDD/silver_transform.py` — SOURCE_CATALOG 10 entradas; joins en `build_fact_environmental` y `build_fact_economic`
- `BBDD/gold_build.py` — eco_sel incluye `co2_country_kt`; `wc_natural_pct` derivado
- `BBDD/schemas/mariadb_serving_ddl.sql` — nuevas columnas en `fact_kuznets` + `v_powerbi_map` actualizada

---

## [2026-03-11 12:00] CAMBIO — Integración OECD e InvestEU en pipeline completo

**Contexto:** Los scripts OECD_extract.py e InvestEU_extract.py ya existían y descargaban datos a `data/raw/oecd/` y `data/raw/investeu/`, pero sus outputs no se usaban en ninguna capa del pipeline (Bronze/Silver/Gold). Decisión explícita del equipo: integrarlos.

**Cambio:** Dos nuevos post-procesadores + integración completa en el pipeline.

### Nuevos archivos creados:
| Script | Entrada | Salida | Granularidad |
|--------|---------|--------|--------------|
| `oecd_process.py` | `data/raw/oecd/{env_policy,env_tax,env_expenditure,air_ghg}.csv` | `oecd_indicators.csv` | Ciudad × Año (expandida desde país ISO3→ISO2) |
| `investeu_process.py` | `data/raw/investeu/final_recipients*.csv` | `investeu_summary.csv` | Ciudad × Año (expandida desde país, año extraído de pdf_url) |

### Variables nuevas:
- **OECD:** `eps_index` (Environmental Policy Stringency), `env_tax_usd` (impuestos ambientales USD), `env_expenditure` (gasto NEEP), `ghg_total_kt` (emisiones GHG totales país) → `fact_economic` + `fact_kuznets`
- **InvestEU:** `investeu_ops_count` (nº operaciones), `investeu_total_eur` (EUR total) → `fact_economic` + `fact_kuznets`

### Archivos modificados:
- `ETL/script/run_all.py` — oecd_process.py e investeu_process.py añadidos en SCRIPTS_ECONOMICOS (post-proceso tras sus extractores)
- `BBDD/schemas/bronze_ddl.sql` — 2 nuevas tablas: `bronze_oecd_raw`, `bronze_investeu_raw` (total: 12)
- `BBDD/bronze_ingest.py` — 2 nuevas funciones: `ingest_oecd`, `ingest_investeu` (total: 11 fuentes)
- `ETL/script/merge.py` — `load_oecd_data()`, `load_investeu_data()` añadidas; main() las fusiona
- `BBDD/silver_transform.py` — SOURCE_CATALOG 12 entradas (OECD sk=11, InvestEU sk=12); `build_fact_economic()` une `oecd_raw` e `investeu_raw` vía join city×year
- `BBDD/gold_build.py` — eco_cols incluye las 6 variables nuevas; NULL placeholders cuando eco=None
- `BBDD/schemas/mariadb_serving_ddl.sql` — 6 nuevas columnas en `fact_kuznets`; `v_powerbi_map` añade `eps_index` e `investeu_total_eur`

### Decisiones técnicas:
- **Año InvestEU:** se extrae del pdf_url con regex `20\d{2}` (los PDFs EIB incluyen año en nombre de fichero). Fallback: año de extraction_date. Default: 2022.
- **GHG OECD:** air_ghg.csv contiene múltiples gases → se suman todos por país×año → `ghg_total_kt`
- **EPS Index OECD:** env_policy puede tener sub-indicadores → se promedia por país×año
- **Join Silver:** OECD e InvestEU se almacenan a nivel ciudad en Bronze (ya expandidos). Join directo city×year en `build_fact_economic`.
