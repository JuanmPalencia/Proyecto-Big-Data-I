# CLAUDE.md — Instrucciones de contexto para Claude Code
# Proyecto: GTP (Green Turning Point)

> Este archivo se carga automáticamente al inicio de cada sesión.
> Su propósito es que el asistente retome el proyecto sin perder contexto.

---

## LECTURA OBLIGATORIA AL INICIO DE CADA SESIÓN

Al comenzar cualquier sesión, leer en este orden:

### 1. Estado actual del proyecto (SIEMPRE)
Leer las últimas 100 líneas de HISTORIAL.md para ver qué está hecho, qué está pendiente y cuál es la prioridad inmediata:
- **Archivo:** `HISTORIAL.md` (raíz del proyecto)
- **Qué buscar:** la sección "TO-DO LIST" y la entrada más reciente para saber dónde se quedó el trabajo

### 2. Documentación técnica de referencia (cuando vayas a tocar código)
- **Archivo:** `GTP_DOCUMENTACION_TECNICA.md` (raíz del proyecto)
- **Qué contiene:** fórmula EKC exacta, estructura de directorios, descripción de cada script, variables del dataset, reglas de imputación, stack tecnológico

### 3. Schemas de base de datos (cuando trabajes en BBDD o modelos)
- `Lorca/BBDD/schemas/bronze_ddl.sql` — contrato de datos capa Bronze (12 tablas)
- `Lorca/BBDD/schemas/silver_ddl.sql` — Star Schema Kimball (4 dims + 3 facts)
- `Lorca/BBDD/schemas/gold_ddl.sql` — tablas analíticas finales (4 tablas)
- `Lorca/BBDD/schemas/mariadb_serving_ddl.sql` — 5 tablas + 5 vistas (MariaDB)

### 4. Configuración central del ETL (cuando trabajes en scripts ETL o BBDD)
- **Archivo:** `Lorca/ETL/script/config.py`
- **Qué contiene:** `EURO_FUAS` (237 ciudades con lat/lon), `EUROSTAT_CODES`, todas las rutas del pipeline

---

## REGLAS INAMOVIBLES DEL PROYECTO

1. **HISTORIAL.md es append-only.** Nunca borrar contenido existente. Solo añadir nuevas entradas al final. Cada entrada lleva fecha Y hora: `[YYYY-MM-DD HH:MM]`.

2. **Todo cambio relevante se registra en HISTORIAL.md** el mismo día con contexto suficiente. Tipos: `CAMBIO`, `DECISIÓN`, `PLANTEAMIENTO`, `DESCARTADO`, `TO-DO`.

3. **El foco es Lorca** (sistema distribuido, cluster UEM). La carpeta `Docker/` es local/dev y tiene menor prioridad.

4. **Stack tecnológico fijo:** Apache Hive + Parquet sobre HDFS + Spark SQL para el pipeline principal. **MariaDB** es únicamente el serving layer de salida (cuadro de mando). No proponer alternativas al stack ya decidido.

5. **No hay UPDATE en HDFS.** Las actualizaciones se hacen sobrescribiendo particiones enteras (`df.write.mode("overwrite").parquet(...)`). Nunca intentar row-level updates.

6. **Fuente de verdad única:** `config.py` contiene `EURO_FUAS` y rutas. Nunca duplicar ese diccionario en otro archivo.

---

## ARQUITECTURA DEL PROYECTO (RESUMEN RÁPIDO)

```
Proyecto GTP — Green Turning Point
├── Lorca/                          ← FOCO PRINCIPAL (cluster distribuido UEM)
│   ├── ETL/script/                 ← Pipeline de extracción de datos
│   │   ├── config.py               ← FUENTE DE VERDAD: ciudades + rutas
│   │   ├── run_all.py              ← Orquestador ETL (reescrito, sin bucle por ciudad)
│   │   ├── Sentinel-2_extract.py   ← ✅ NDVI vía GEE reduceRegion → sentinel2.csv
│   │   ├── Sentinel-5p_extract.py  ← ✅ NO2 vía GEE reduceRegion → s5p.csv
│   │   ├── HRL_extract.py          ← Impermeabilización suelo → GeoTIFFs
│   │   ├── hrl_to_csv.py           ← ✅ GeoTIFFs HRL → hrl.csv (requiere rasterio)
│   │   ├── era5_extract.py         ← ✅ Temperatura/precipitación ERA5-Land (GEE)
│   │   ├── s5p_aerosol_extract.py  ← ✅ UVAI aerosol Sentinel-5P (GEE)
│   │   ├── urban_atlas_extract.py  ← ✅ ESA WorldCover cobertura suelo (GEE)
│   │   ├── edgar_co2_extract.py    ← ✅ Emisiones CO2 nacionales EDGAR v8
│   │   ├── OECD_extract.py         ← ✅ EPS / impuestos ambientales / GHG (OECD SDMX)
│   │   ├── oecd_process.py         ← ✅ CSV SDMX → oecd_indicators.csv (ISO3→ISO2+expand)
│   │   ├── InvestEU_extract.py     ← ✅ Financiación verde EIB/InvestEU
│   │   ├── investeu_process.py     ← ✅ final_recipients.csv → investeu_summary.csv
│   │   ├── YFinance_extract.py     ← ✅ Datos financieros bursátiles
│   │   ├── Eurostat_extract.py     ← ✅ GDP + población Eurostat
│   │   └── merge.py                ← ✅ Dataset maestro CSV (todas las fuentes)
│   ├── BBDD/                       ← Pipeline HDFS + Modelos ML + Serving Layer
│   │   ├── schemas/                ← ✅ DDL Hive y MariaDB creados
│   │   │   ├── bronze_ddl.sql      ← ✅ 12 tablas Bronze (Hive)
│   │   │   ├── silver_ddl.sql      ← ✅ 7 tablas Silver (Hive)
│   │   │   ├── gold_ddl.sql        ← ✅ 4 tablas Gold (Hive)
│   │   │   └── mariadb_serving_ddl.sql ← ✅ 5 tablas + 5 vistas (MariaDB bd_rvm_gtp)
│   │   ├── bronze_ingest.py        ← ✅ CSV → HDFS Bronze (12 fuentes)
│   │   ├── silver_transform.py     ← ✅ Bronze → Star Schema Silver (SOURCE_CATALOG 12)
│   │   ├── gold_build.py           ← ✅ Silver → Gold analítico
│   │   ├── export_to_postgres.py   ← ✅ Gold HDFS → MariaDB bd_rvm_gtp (nombre legacy)
│   │   ├── models/
│   │   │   ├── clustering.py       ← ✅ K-Means PySpark
│   │   │   ├── ekc_regression.py   ← ✅ Panel Regression EKC
│   │   │   ├── xgboost_classifier.py ← ✅ Fase DEGRADANDO/TURNING/RECUPERANDO
│   │   │   └── prophet_forecast.py ← ✅ Forecast NDVI 1Y/3Y/5Y
│   │   ├── run_pipeline.py         ← ✅ Orquestador maestro (8 fases: BBDD+ML+Export)
│   │   ├── spark.py                ← Existente (legacy)
│   │   └── exports.py              ← Existente (legacy)
│   ├── Web/
│   │   └── app.py                  ← ✅ Flask API (auth + 7 endpoints de datos)
│   ├── setup_lorca.sh              ← ✅ Setup entorno Lorca (venv + HDFS + DDL + GEE auth)
│   └── .env                        ← Credenciales (MariaDB, Copernicus, WEkEO, InvestEU)
├── Docker/                         ← Pipeline local/dev (menor prioridad)
├── HISTORIAL.md                    ← Log cronológico append-only con fecha+hora
├── GTP_DOCUMENTACION_TECNICA.md    ← Documentación técnica completa
└── CLAUDE.md                       ← Este archivo
```

**Arquitectura de datos (flujo completo):**
```
ETL (run_all.py) → CSVs en DatosProcesados/
    ↓ bronze_ingest.py
HDFS Bronze (12 tablas Parquet, particionadas year/country)
    ↓ silver_transform.py
HDFS Silver (4 dims + 3 facts, Star Schema Kimball, SOURCE_CATALOG sk 1-12)
    ↓ gold_build.py
HDFS Gold base (fact_kuznets, city_ranking, ekc_parameters, model_results)
    ↓ clustering → ekc → xgboost → prophet
HDFS Gold completo (con resultados ML)
    ↓ export_to_postgres.py (fase 8) [nombre legacy]
MariaDB bd_rvm_gtp.* (serving layer, 10.151.30.2:3306)
    ↓
Flask API /api/* ← Power BI / Tableau / Grafana
```

---

## CONCEPTO CENTRAL: EKC (Environmental Kuznets Curve)

La hipótesis que el proyecto quiere validar y operacionalizar:

**Ecuación del modelo de panel:**
```
ln(Eᵢₜ) = α + β₁·ln(Yᵢₜ) + β₂·[ln(Yᵢₜ)]² + μᵢ + λₜ + εᵢₜ
```

**Turning Point (punto de inflexión):**
```
Y* = exp( −β₁ / 2β₂ )
```

Donde:
- `Eᵢₜ` = indicador ambiental de la ciudad i en el año t (NDVI o NO2)
- `Yᵢₜ` = GDP per cápita PPS de la ciudad i en el año t
- `β₁ > 0`, `β₂ < 0` → curva en U invertida (hipótesis EKC confirmada)
- `Y*` = nivel de renta en el que la degradación ambiental se revierte

---

## PIPELINE DE MODELOS ML (orden fijo)

```
Datos Silver
    │
    ▼
1. K-Means Clustering (PySpark KMeans)
   → Agrupa ciudades por perfil económico-ambiental similar
   → Selección automática de K por índice Silhouette
    │
    ▼
2. EKC Panel Regression por cluster (statsmodels/linearmodels)
   → Estima β₁, β₂, Y* para cada cluster (no por ciudad individual)
   → Pool de datos: ~50 ciudades × 8 años = ~400 obs/cluster
    │
    ▼
3. XGBoost Classifier
   → Predice fase: DEGRADANDO / TURNING / RECUPERANDO
   → No asume forma de curva, complementa la regresión
    │
    ▼
4. Prophet Time Series (pandas UDF sobre Spark)
   → Forecast NDVI mensual por ciudad: 1Y / 3Y / 5Y
   → Estima prophet_turning_year con intervalos de confianza 95%
    │
    ▼
Gold Layer (fact_kuznets, city_ranking, ekc_parameters, model_results)
```

---

## FUENTES DE DATOS BRONZE (12 tablas)

| # | Tabla Hive | Script ETL | Fuente | Variables clave |
|---|---|---|---|---|
| 1 | bronze_sentinel2 | Sentinel-2_extract.py | GEE S2 | ndvi_mean, ndvi_std |
| 2 | bronze_s5p | Sentinel-5p_extract.py | GEE S5P | no2_mean |
| 3 | bronze_hrl | hrl_to_csv.py | Copernicus HRL | imperviousness_mean, tree_cover_pct |
| 4 | bronze_finance | YFinance_extract.py | Yahoo Finance | ticker, close_price, annual_volatility |
| 5 | bronze_eurostat | Eurostat_extract.py | Eurostat API | gdp_pps_per_capita, fua_population |
| 6 | bronze_era5 | era5_extract.py | GEE ERA5-Land | temp_annual_mean_c, precip_annual_sum_m |
| 7 | bronze_s5p_aerosol | s5p_aerosol_extract.py | GEE S5P UVAI | uvai_annual_mean |
| 8 | bronze_worldcover | urban_atlas_extract.py | GEE ESA WorldCover | wc_tree_pct, wc_built_pct, wc_crop_pct |
| 9 | bronze_edgar_co2 | edgar_co2_extract.py | EDGAR v8 | co2_country_kt |
| 10 | bronze_oecd_raw | oecd_process.py | OECD SDMX | eps_index, env_tax_usd, env_expenditure, ghg_total_kt |
| 11 | bronze_investeu_raw | investeu_process.py | EIB/InvestEU | investeu_ops_count, investeu_total_eur |

> Nota: raw OECD/InvestEU → `data/raw/oecd/` y `data/raw/investeu/`
> Post-procesadores: `oecd_process.py` e `investeu_process.py` → `DatosProcesados/`

---

## RUTAS HDFS (cluster Lorca)

```
hdfs:///user/gtp/bronze/   ← datos brutos particionados year/country
hdfs:///user/gtp/silver/   ← Star Schema Kimball
hdfs:///user/gtp/gold/     ← tablas analíticas finales
hdfs:///user/gtp/models/   ← artefactos ML (clustering, xgboost, prophet)
```

**Base del proyecto en Lorca:**
```
/home/223B3336juan/Big Data I/Proyecto-Big-Data-I/
```

---

## CREDENCIALES Y CONFIGURACIÓN

- **MariaDB serving layer:** `10.151.30.2:3306` / bd `bd_rvm_gtp` / user `bd_rvm_gtp` / pass `Sol2026A`
- **GEE proyecto:** `gtpuem23` (en todos los scripts GEE: Sentinel-2, Sentinel-5P, ERA5, S5P Aerosol, WorldCover)
- **Copernicus / WEkEO:** `juanmapalencia23@gmail.com` / `Cabure238974`
- **Virtualenv Lorca:** `~/gtp_venv` (activar: `source ~/gtp_venv/bin/activate`)
- **Todas las credenciales:** en `Lorca/.env`

---

## CONVENCIONES DE CÓDIGO

- Formato de city_code: `NombreCiudad_ISO2` (ej: `Madrid_ES`, `Paris_FR`)
- Particionado siempre: `year=YYYY/country=XX/`
- Columnas de metadata Bronze: `_ingestion_date`, `_source_system`, `_file_name`
- SCD-2 en dim_company: columnas `valid_from`, `valid_to`, `is_current`
- Spark session: siempre `enableHiveSupport()` para escribir tablas Hive
- Coalesce antes de escribir: `df.coalesce(N)` para evitar small files problem en HDFS
- Registrar particiones tras escritura directa: `MSCK REPAIR TABLE gtp_bronze.<tabla>`
- Finance Bronze: columnas `ticker, company_name, sector, industry, country, fua_country_code, year, close_price, annual_return, annual_volatility, volume_avg, current_pe, current_beta, extraction_date` (NO city_code, NO market_cap, NO dividend_yield)

---

## ESTADO RÁPIDO (actualizar en cada sesión tras leer HISTORIAL)

| Componente | Estado |
|---|---|
| ETL pipeline (12 fuentes) | ✅ Completado |
| oecd_process.py | ✅ Completado |
| investeu_process.py | ✅ Completado |
| Bronze DDL (12 tablas) | ✅ Completado |
| Silver DDL (7 tablas, SOURCE_CATALOG 12) | ✅ Completado |
| Gold DDL (4 tablas) | ✅ Completado |
| mariadb_serving_ddl.sql (5 tablas + 5 vistas) | ✅ Completado |
| bronze_ingest.py (12 fuentes) | ✅ Completado |
| silver_transform.py (OECD+InvestEU integrados) | ✅ Completado |
| gold_build.py (6 nuevas columnas OECD+InvestEU) | ✅ Completado |
| export_to_postgres.py → MariaDB | ✅ Completado |
| models/clustering.py | ✅ Completado |
| models/ekc_regression.py | ✅ Completado |
| models/xgboost_classifier.py | ✅ Completado |
| models/prophet_forecast.py | ✅ Completado |
| run_pipeline.py (8 fases) | ✅ Completado |
| app.py (auth + 7 endpoints datos) | ✅ Completado |
| setup_lorca.sh | ✅ Actualizado (venv + HDFS + MariaDB DDL + GEE) |
| Lorca/.env | ✅ Completo (MariaDB + Copernicus + WEkEO + InvestEU) |
| GTP_DOCUMENTACION_TECNICA.md | ⏳ Pendiente actualizar |
| Prueba end-to-end en Lorca | ⏳ Pendiente |
