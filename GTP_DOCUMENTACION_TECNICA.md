# Green Turning Point (GTP) — Documentación Técnica Completa

**Universidad Europea · Grado GIAMAD · Big Data I · 3º Curso**
**Autores:** Juan Manuel Palencia Osorio · Pablo Mata Rius · Pablo Sánchez Ruiz · María Paula Aguirre Palacio

---

## ÍNDICE

1. [Idea de negocio y motivación](#1-idea-de-negocio-y-motivación)
2. [La Curva de Kuznets Ambiental — Teoría y Fórmula](#2-la-curva-de-kuznets-ambiental--teoría-y-fórmula)
3. [Cómo GTP operacionaliza Kuznets](#3-cómo-gtp-operacionaliza-kuznets)
4. [Arquitectura del sistema — visión completa](#4-arquitectura-del-sistema--visión-completa)
5. [Por qué HDFS + Hive + Spark (y para qué sirve cada uno)](#5-por-qué-hdfs--hive--spark-y-para-qué-sirve-cada-uno)
6. [Estructura de directorios exacta](#6-estructura-de-directorios-exacta)
7. [Fuentes de datos — detalle completo](#7-fuentes-de-datos--detalle-completo)
8. [Pipeline ETL — scripts individuales](#8-pipeline-etl--scripts-individuales)
9. [Orquestador maestro — run_all.py](#9-orquestador-maestro--run_allpy)
10. [Capa Bronze — schema y contratos de datos](#10-capa-bronze--schema-y-contratos-de-datos)
11. [Capa Silver — Star Schema Kimball](#11-capa-silver--star-schema-kimball)
12. [Capa Gold — tablas analíticas finales](#12-capa-gold--tablas-analíticas-finales)
13. [Pipeline BBDD — scripts de ingesta y transformación](#13-pipeline-bbdd--scripts-de-ingesta-y-transformación)
14. [Pipeline ML — los 4 modelos en detalle](#14-pipeline-ml--los-4-modelos-en-detalle)
15. [Orquestador BBDD — run_pipeline.py](#15-orquestador-bbdd--run_pipelinepy)
16. [Serving Layer — MariaDB + export_to_postgres.py](#16-serving-layer--mariadb--export_to_postgrespy)
17. [API Web Flask — endpoints y autenticación](#17-api-web-flask--endpoints-y-autenticación)
18. [Variables del dataset maestro (merge.py output)](#18-variables-del-dataset-maestro-mergepy-output)
19. [Reglas de imputación y calidad de datos](#19-reglas-de-imputación-y-calidad-de-datos)
20. [Convenciones de código](#20-convenciones-de-código)
21. [Bugs corregidos — historial completo](#21-bugs-corregidos--historial-completo)
22. [Pendiente — tareas fuera del código](#22-pendiente--tareas-fuera-del-código)

---

## 1. Idea de negocio y motivación

**Green Turning Point (GTP)** es una plataforma de análisis Big Data que identifica las ciudades europeas que se encuentran en el **punto de inflexión ambiental**: ese momento concreto en el desarrollo económico en que el crecimiento deja de degradar el medioambiente y comienza una fase de regeneración activa.

El producto resultante es un **ranking de ciudades europeas por proximidad al turning point**, orientado a:

- **Fondos de inversión verde** que buscan mercados con alto potencial sostenible.
- **Instituciones públicas** en busca de ciudades modelo para política pública.
- **Planificadores urbanos** interesados en señales tempranas de regeneración.
- **Empresas** que quieren alinear expansión geográfica con criterios ESG.

El fundamento teórico es la **Curva de Kuznets Ambiental (EKC)**, cuya operacionalización práctica es el núcleo matemático de este proyecto.

---

## 2. La Curva de Kuznets Ambiental — Teoría y Fórmula

### 2.1 Qué es la EKC

La **Environmental Kuznets Curve** propone que la relación entre renta per cápita y degradación ambiental sigue una curva en forma de **U invertida**: a bajos niveles de renta, el crecimiento económico aumenta la contaminación; a partir de un umbral de renta crítico (el **turning point**), el crecimiento comienza a reducirla.

```
Degradación
ambiental (E)
     │
     │    ╭──────╮
     │   ╱        ╲
     │  ╱          ╲
     │ ╱            ╲___________
     │╱
     └──────────────────────────► PIB per cápita (Y)
                  ↑
           Turning Point Y*
```

### 2.2 Especificación econométrica

La forma funcional estándar de la EKC es una **regresión cuadrática en logaritmos**:

```
ln(Eᵢₜ) = α + β₁·ln(Yᵢₜ) + β₂·[ln(Yᵢₜ)]² + μᵢ + λₜ + εᵢₜ
```

| Símbolo | Significado |
|---------|-------------|
| `Eᵢₜ` | Indicador ambiental de la ciudad `i` en el año `t` (NDVI o NO2) |
| `Yᵢₜ` | PIB per cápita PPS de la ciudad `i` en el año `t` (EUR/hab) |
| `α` | Intercepto global |
| `β₁` | Coeficiente lineal del PIB — se espera `β₁ > 0` |
| `β₂` | Coeficiente cuadrático del PIB — se espera `β₂ < 0` |
| `μᵢ` | Efecto fijo por ciudad (diferencias estructurales) |
| `λₜ` | Efecto fijo temporal (shocks globales: crisis 2008, COVID) |
| `εᵢₜ` | Error idiosincrásico |

### 2.3 Cálculo del Turning Point Y*

```
d[ln(E)] / d[ln(Y)] = β₁ + 2·β₂·ln(Y) = 0
ln(Y*) = -β₁ / (2·β₂)
Y* = exp( -β₁ / (2·β₂) )
```

**Interpretación:**
- `β₁ > 0` y `β₂ < 0` → existe turning point → hipótesis EKC confirmada
- `PIB_ciudad ≈ Y*` → ciudad **en el turning point** → señal de inversión
- `PIB_ciudad > Y*` → ciudad ya en fase de **regeneración activa**

### 2.4 Indicadores ambientales en GTP

| Variable `E` | Fuente | Interpretación |
|---|---|---|
| `NDVI_Mean` | Sentinel-2 | NDVI alto = más vegetación = menor degradación |
| `NO2_Mean` | Sentinel-5P | NO₂ alto = más contaminación = mayor degradación |
| `Imperviousness` | HRL Copernicus | % suelo sellado = proxy urbanización |

El indicador principal es `ndvi_trend_slope` (pendiente NDVI temporal):
- `slope ≈ 0` → estabilización → candidato a turning point
- `slope > 0` → regeneración → ciudad ya pasó el turning point
- `slope < 0` → degradación continua

---

## 3. Cómo GTP operacionaliza Kuznets

```
PASO 1: Recopilar Yᵢₜ = GDP_Per_Capita (Eurostat + INE)
PASO 2: Recopilar Eᵢₜ = NDVI_Mean, NO2_Mean, Imperviousness (satélites)
PASO 3: merge.py → dataset panel [City, Year, GDP_Per_Capita, NDVI_Mean, ...]
PASO 4: bronze_ingest.py → HDFS Bronze (datos brutos Parquet)
PASO 5: silver_transform.py → Star Schema Kimball (dimensiones + hechos)
PASO 6: gold_build.py → tablas analíticas desnormalizadas
PASO 7: clustering.py → segmentar ciudades por perfil (K-Means)
PASO 8: ekc_regression.py → estimar β₁, β₂, Y* por cluster
PASO 9: xgboost_classifier.py → clasificar fase DEGRADANDO/TURNING/RECUPERANDO
PASO 10: prophet_forecast.py → forecast NDVI 1Y/3Y/5Y por ciudad
PASO 11: export_to_postgres.py → Gold → MariaDB bd_rvm_gtp (serving layer)
PASO 12: Flask API → ranking + detalle de ciudad para dashboards
```

---

## 4. Arquitectura del sistema — visión completa

### 4.1 Flujo de datos end-to-end

```
Fuentes externas
  GEE (Sentinel-2, S5P) + EEA (HRL) + Eurostat + OECD + INE + YFinance + InvestEU
         │
         ▼ ETL Python (run_all.py)
   CSVs locales en Lorca/ETL/script/data/DatosProcesados/
         │
         ▼ bronze_ingest.py (spark-submit YARN)
   HDFS Bronze  hdfs:///user/gtp/bronze/
   (Parquet particionado, append-only, con metadata de auditoría)
         │
         ▼ silver_transform.py (spark-submit YARN)
   HDFS Silver  hdfs:///user/gtp/silver/
   (Star Schema Kimball: 4 dims + 3 facts)
         │
         ▼ gold_build.py (spark-submit YARN)
   HDFS Gold base  hdfs:///user/gtp/gold/
   (fact_kuznets, city_ranking, ekc_parameters, model_results — placeholders ML)
         │
         ▼ clustering → ekc_regression → xgboost → prophet (spark-submit YARN)
   HDFS Gold completo
   (mismo Gold pero con todos los resultados ML rellenos)
         │
         ▼ export_to_postgres.py (spark-submit YARN) [nombre legacy]
   MariaDB  bd_rvm_gtp.*  (10.151.30.2:3306)
   (espejo del Gold, <10ms latencia para APIs)
         │
         ▼ Flask API  Lorca/Web/app.py
   /api/ranking, /api/city/<code>, /api/opportunities, /api/clusters, ...
         │
         ▼ Power BI / Tableau / Grafana / Web dashboard
```

### 4.2 Stack tecnológico

| Capa | Tecnología | Versión | Rol |
|------|-----------|---------|-----|
| Cómputo distribuido | Apache Spark | 3.x | ETL + ML + escritura HDFS |
| Cluster manager | YARN (Hadoop) | 3.x | Gestión de recursos Lorca |
| Almacenamiento distribuido | HDFS | 3.x | Datos Parquet (Bronze/Silver/Gold) |
| Catálogo de tablas | Apache Hive | 3.x | Metastore + HiveQL |
| Formato de ficheros | Parquet + Snappy | — | Columnar, comprimido, eficiente |
| ML Python | XGBoost + scikit-learn + Prophet + linearmodels | — | Modelos ML fuera de MLlib |
| ML Spark | PySpark MLlib (KMeans) | — | Clustering escalable |
| Serving layer | MariaDB 10.x (bd_rvm_gtp) | 14+ | Queries <10ms para API |
| API | Flask + PyMySQL/mysql-connector | — | REST API + auth |
| ETL fuentes | Python 3.11 + pandas + requests + yfinance + earthengine-api + rasterio | — | Extracción multifuente |

### 4.3 Ejecución

```bash
# Un único comando para todo el pipeline:
python Lorca/ETL/script/run_all.py

# Opciones:
python run_all.py --skip-bbdd          # Solo ETL
python run_all.py --only-bbdd          # Solo BBDD + ML
python run_all.py --only-bbdd --bbdd-args "--only-models"  # Solo reentrenar ML
python run_all.py --dry-run            # Mostrar sin ejecutar
```

---

## 5. Por qué HDFS + Hive + Spark (y para qué sirve cada uno)

Esta es la pregunta clave de arquitectura. Los tres componentes tienen roles distintos y complementarios.

### 5.1 HDFS — el sistema de ficheros distribuido

HDFS (Hadoop Distributed File System) almacena los datos físicamente. Los ficheros se dividen en bloques de 128 MB y se replican en 3 nodos del cluster para tolerancia a fallos.

```
HDFS
└── /user/gtp/bronze/sentinel2_raw/
    ├── year=2023/country=ES/
    │   └── part-00000-abc.snappy.parquet   ← fichero Parquet real
    ├── year=2023/country=FR/
    │   └── part-00000-def.snappy.parquet
    └── year=2022/country=DE/
        └── part-00000-ghi.snappy.parquet
```

**HDFS no entiende de "tablas" ni "columnas"**. Solo ve ficheros y directorios.

### 5.2 Hive Metastore — el catálogo de tablas

Apache Hive añade una capa de metadatos encima de HDFS. El **Metastore** es una base de datos (normalmente MySQL o Derby) que almacena la definición lógica de las tablas: nombre, columnas, tipos, particiones, y la ruta HDFS donde viven los datos.

Los ficheros DDL (`bronze_ddl.sql`, `silver_ddl.sql`, `gold_ddl.sql`) crean **tablas EXTERNAS de Hive**:

```sql
-- Esto NO crea datos. Solo registra en el Metastore:
-- "La tabla gtp_bronze.bronze_sentinel2_raw tiene estas columnas
--  y sus datos están en hdfs:///user/gtp/bronze/sentinel2_raw/"
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_sentinel2_raw (
    city STRING, ndvi_mean DOUBLE, ...
)
PARTITIONED BY (year INT, country STRING)
STORED AS PARQUET
LOCATION 'hdfs:///user/gtp/bronze/sentinel2_raw';
```

Sin el DDL ejecutado, tendrías que referenciar siempre la ruta HDFS completa. Con el DDL ejecutado puedes usar HiveQL.

### 5.3 Spark — el motor de procesado

Spark lee del Metastore de Hive para saber dónde están los datos, y luego accede directamente a los ficheros Parquet en HDFS. Por eso todos los scripts tienen `enableHiveSupport()`:

```python
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# Sin Hive registrado (ruta directa):
df = spark.read.parquet("hdfs:///user/gtp/bronze/sentinel2_raw/year=2023/country=ES")

# Con Hive registrado (equivalente):
df = spark.sql("SELECT * FROM gtp_bronze.bronze_sentinel2_raw WHERE year=2023 AND country='ES'")
```

### 5.4 MSCK REPAIR TABLE — sincronizar Hive con HDFS

Cuando Spark escribe Parquet directamente en HDFS (sin pasar por Hive), las nuevas particiones no aparecen en el Metastore. El comando `MSCK REPAIR TABLE` recorre el directorio HDFS y registra todas las particiones que encuentra:

```bash
# Tras escribir nueva data en HDFS, ejecutar:
MSCK REPAIR TABLE gtp_bronze.bronze_sentinel2_raw;
```

### 5.5 Resumen: quién hace qué

| Componente | Responsabilidad | Sin él |
|-----------|----------------|--------|
| **HDFS** | Almacenar ficheros Parquet distribuidos | Sin datos en disco |
| **Hive Metastore** | Catálogo de tablas (nombre → schema → ruta HDFS) | Sin SQL, solo rutas directas |
| **Spark** | Procesar datos (leer Parquet, calcular, escribir) | Sin computación distribuida |
| **DDL SQL** | Registrar la definición de cada tabla en Hive | Hive no sabe que existen las tablas |

---

## 6. Estructura de directorios exacta

```
Proyecto-Big-Data-I/
├── CLAUDE.md                            Instrucciones para el asistente IA
├── HISTORIAL.md                         Log de cambios (append-only)
├── GTP_DOCUMENTACION_TECNICA.md         Este archivo
├── COMANDOS_SETUP.md                    Comandos de instalación y setup en Lorca
│
├── Lorca/                               ◄ PIPELINE PRINCIPAL (cluster distribuido)
│   ├── requirements.txt
│   │
│   ├── ETL/
│   │   ├── script/
│   │   │   ├── config.py                FUENTE DE VERDAD: EURO_FUAS + EUROSTAT_CODES + rutas
│   │   │   ├── run_all.py               Orquestador maestro único (ETL + BBDD)
│   │   │   ├── merge.py                 Fusión de todas las fuentes → dataset maestro CSV
│   │   │   ├── Sentinel-2_extract.py    NDVI mensual via GEE reduceRegion → sentinel2.csv
│   │   │   ├── Sentinel-5p_extract.py   NO₂ mensual via GEE reduceRegion → s5p.csv
│   │   │   ├── HRL_extract.py           Impermeabilización EEA → descarga GeoTIFFs
│   │   │   ├── hrl_to_csv.py            GeoTIFFs HRL → hrl.csv (requiere rasterio)
│   │   │   ├── YFinance_extract.py      43 empresas verdes MENSUAL 2006-2025 → finance_monthly_2006_2025.csv
│   │   │   ├── eurostat_extract.py      GDP + población Eurostat SDMX → eurostat_PIB.csv
│   │   │   ├── OECD_extract.py          Indicadores ambientales OECD
│   │   │   ├── InvestEU_extract.py      Proyectos EIB (scraping HTML + PDF)
│   │   │   └── pib_ine_extract.py       PIB España INE API
│   │   ├── Limpieza/
│   │   │   └── limpieza.ipynb
│   │   └── catalog/
│   │       ├── hrl_datasets.yaml
│   │       ├── hrl_imd_urls.txt
│   │       └── hrl_tcd_urls.txt
│   │
│   ├── BBDD/
│   │   ├── schemas/
│   │   │   ├── bronze_ddl.sql              DDL Hive: 12 tablas Bronze
│   │   │   ├── silver_ddl.sql              DDL Hive: 4 dims + 3 facts Silver
│   │   │   ├── gold_ddl.sql                DDL Hive: 4 tablas Gold
│   │   │   └── mariadb_serving_ddl.sql     DDL MariaDB: 5 tablas + 5 vistas bd_rvm_gtp
│   │   ├── bronze_ingest.py             CSVs del ETL → HDFS Bronze (Parquet particionado, 12 fuentes)
│   │   ├── silver_transform.py          Bronze → Star Schema Kimball Silver (SOURCE_CATALOG sk 1-12)
│   │   ├── gold_build.py                Silver → tablas analíticas Gold
│   │   ├── export_to_postgres.py        Gold HDFS → MariaDB bd_rvm_gtp serving layer (nombre legacy)
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── clustering.py            K-Means PySpark (K auto por Silhouette)
│   │   │   ├── ekc_regression.py        Panel Regression EKC (linearmodels PanelOLS)
│   │   │   ├── xgboost_classifier.py    Clasificador fase EKC (3 clases)
│   │   │   └── prophet_forecast.py      Forecast NDVI 1Y/3Y/5Y (pandas UDF Spark)
│   │   ├── run_pipeline.py              Orquestador BBDD+ML (8 fases, spark-submit)
│   │   ├── spark.py                     (legacy — pipeline Spark anterior)
│   │   └── exports.py                   (legacy — conversión Parquet → CSV)
│   │
│   └── Web/
│       ├── app.py                       Flask REST API (auth + 7 endpoints datos)
│       └── data/
│           ├── users.txt                JSONL de usuarios registrados
│           └── newsletter.txt           JSONL de suscripciones newsletter
│
└── Docker/                              Pipeline local/dev (prioridad secundaria)
    ├── docker-compose.yml
    ├── ETL/
    └── Web/
```

### 6.1 Rutas HDFS del cluster Lorca

```
hdfs:///user/gtp/
├── bronze/
│   ├── sentinel2_raw/        year=YYYY/country=XX/part-*.parquet
│   ├── sentinel5p_raw/       year=YYYY/country=XX/part-*.parquet
│   ├── hrl_raw/              year=YYYY/country=XX/part-*.parquet
│   ├── finance_raw/          year=YYYY/country_code=XX/part-*.parquet
│   ├── eurostat_gdp_raw/     year=YYYY/country=XX/part-*.parquet
│   └── eurostat_population_raw/ year=YYYY/country=XX/part-*.parquet
├── silver/
│   ├── dim_city/             country_code=XX/part-*.parquet
│   ├── dim_date/             part-*.parquet  (sin partición — es pequeña)
│   ├── dim_source/           part-*.parquet
│   ├── dim_company/          country_code=XX/part-*.parquet
│   ├── fact_environmental/   year=YYYY/country=XX/part-*.parquet
│   ├── fact_economic/        year=YYYY/country=XX/part-*.parquet
│   └── fact_financial/       year=YYYY/country_code=XX/part-*.parquet
└── gold/
    ├── fact_kuznets/         year=YYYY/country=XX/part-*.parquet
    ├── city_ranking/         year=YYYY/part-*.parquet
    ├── ekc_parameters/       part-*.parquet
    └── model_results/        year=YYYY/country=XX/part-*.parquet
```

---

## 7. Fuentes de datos — detalle completo

| # | Fuente | Script(s) | Granularidad | Período | Cobertura | Actualización |
|---|--------|-----------|-------------|---------|-----------|--------------|
| 1 | Sentinel-2 GEE | `Sentinel-2_extract.py` | Ciudad × Mes | 2018–presente | 237 ciudades FUA | Mensual |
| 2 | Sentinel-5P GEE (NO₂) | `Sentinel-5p_extract.py` | Ciudad × Mes | 2018–presente | 237 ciudades FUA | Mensual |
| 3 | HRL Copernicus | `HRL_extract.py` + `hrl_to_csv.py` | Ciudad × Año | 2018, 2021... | Europa | Trienal |
| 4 | Yahoo Finance | `YFinance_extract.py` | Empresa × Mes | 2006–presente | 43 tickers verdes | Mensual |
| 5 | Eurostat GDP + Población | `Eurostat_extract.py` | Ciudad/País × Año | 2000–presente | EU27+EEA | Anual |
| 6 | ERA5-Land (clima) | `era5_extract.py` | Ciudad × Año | 1950–presente | Global | Anual |
| 7 | Sentinel-5P GEE (UVAI) | `s5p_aerosol_extract.py` | Ciudad × Año | 2018–presente | 237 ciudades FUA | Anual |
| 8 | ESA WorldCover | `urban_atlas_extract.py` | Ciudad × Año | 2020–2021 | Global | Bienal |
| 9 | EDGAR CO₂ v8 | `edgar_co2_extract.py` | País × Año | 1970–presente | Global | Anual |
| 10 | OECD Env Policy | `OECD_extract.py` + `oecd_process.py` | País → Ciudad × Año | 2000–presente | OCDE | Anual |
| 11 | InvestEU / EIB | `InvestEU_extract.py` + `investeu_process.py` | País → Ciudad × Año | 2021–presente | EU | Anual |

**Fuentes raw OECD/InvestEU:**
- `data/raw/oecd/` — CSVs SDMX (env_policy, env_tax, env_expenditure, air_ghg)
- `data/raw/investeu/` — final_recipients*.csv (tabla EIB de beneficiarios)

### 7.1 EURO_FUAS — las 237 ciudades

Definidas en `config.py` como diccionario `EURO_FUAS`:

```python
EURO_FUAS = {
    "Madrid_ES":    (40.4168, -3.7038),
    "Barcelona_ES": (41.3851,  2.1734),
    "Paris_FR":     (48.8566,  2.3522),
    # ... 237 ciudades europeas
}
```

- **Clave:** `CityName_ISO2` — es el `city_code` usado en todo el pipeline
- **Valor:** `(lat, lon)` — coordenadas del centroide FUA
- Las ciudades son **Functional Urban Areas (FUA)** de Eurostat — no municipios, sino áreas metropolitanas funcionales

### 7.2 Cartera YFinance — 43 empresas verdes

Selección de empresas líderes en transición ecológica europea (Green Deal Europe):

| País | Tickers |
|------|---------|
| España | IBE.MC, REP.MC, SAN.MC, BBVA.MC, ITX.MC, ANA.MC, FER.MC, ELE.MC, ENG.MC |
| Francia | AIR.PA, TTE.PA, BNP.PA, SU.PA, VIE.PA, DG.PA |
| Alemania | VOW3.DE, SIE.DE, BMW.DE, EOAN.DE, RWE.DE, ALV.DE |
| Italia | ENEL.MI, ISP.MI, ENI.MI |
| Países Bajos | INGA.AS, ASML.AS, PHIA.AS |
| Nórdicos | ORSTED.CO, VWS.CO, NOVO-B.CO, EQNR.OL, NHY.OL, VOLV-B.ST, ERIC-B.ST, NESTE.HE |
| Reino Unido | SHEL.L, BP.L, NG.L, SSE.L |

**Granularidad mensual** — estándar Fama-French:
- Volatilidad: `std(daily_return) × √21` (días bursátiles/mes)
- Retorno: `(P_final / P_inicial) - 1` por mes
- Umbral mínimo: 15 días cotizados en el mes

---

## 8. Pipeline ETL — scripts individuales

### 8.1 `config.py` — fuente de verdad

Contiene todo lo que es global al proyecto:
- `EURO_FUAS` — 237 ciudades con coordenadas
- `EUROSTAT_CODES` — mapeo ciudad → código Eurostat
- `BASE_DIR` — directorio base derivado de `Path(__file__).resolve().parent`
- Todas las rutas de ficheros CSV de salida

**Nunca duplicar `EURO_FUAS` en otro fichero.** Importar siempre desde `config`.

### 8.2 `Sentinel-2_extract.py` — NDVI mensual

- **API:** Google Earth Engine (GEE) — colección `COPERNICUS/S2_SR_HARMONIZED`
- **Método:** `reduceRegion` con `ee.Reducer.mean()` sobre el ROI de cada ciudad
- **Sin descarga de GeoTIFFs** — solo el valor estadístico agregado
- **Salida:** `sentinel2.csv`

Columnas: `City, Year, Month, NDVI_Mean, NDVI_Std, pixel_count`

### 8.3 `Sentinel-5p_extract.py` — NO₂ mensual

- **API:** GEE — colección `COPERNICUS/S5P/OFFL/L3_NO2`
- **Banda:** `tropospheric_NO2_column_number_density` (mol/m²)
- **Salida:** `s5p.csv`

Columnas: `City, Year, Month, NO2_Mean, NO2_Std, pixel_count`

### 8.4 `HRL_extract.py` + `hrl_to_csv.py` — Impermeabilización

- `HRL_extract.py`: descarga GeoTIFFs desde la API REST de Copernicus Land Service
- `hrl_to_csv.py`: procesa los GeoTIFFs con `rasterio`, calcula estadísticas zonales por ciudad FUA, genera `hrl.csv`
- **Capas:** Imperviousness Density (IMD) + Tree Cover Density (TCD) + Small Woody Features

Columnas: `City, Year, Imperviousness_Mean, Tree_Cover_Pct, Small_Woody_Mean`

### 8.5 `YFinance_extract.py` — Datos financieros mensuales

- **API:** `yfinance` (Yahoo Finance)
- **Período:** 2006–presente, `auto_adjust=True` (ajuste por dividendos y splits)
- **Granularidad:** **MENSUAL** (cambiado de anual en sesión 2026-03-11)

```python
grouped = hist.groupby([hist.index.year, hist.index.month])
for (year, month), group in grouped:
    if len(group) < 15:  # mín. 15 días cotizados
        continue
    volatility = group['Daily_Return'].std() * np.sqrt(21)   # volatilidad mensual
    monthly_return = (group['Close'].iloc[-1] / group['Close'].iloc[0]) - 1
```

**Justificación mensual:** estándar Fama-French; ~1.1M filas vs 13M diario; señal más rica que anual sin coste computacional excesivo.

Columnas salida: `Ticker, Company_Name, Sector, Industry, Country, FUA_Country_Code, Year, Month, Close_Price, Monthly_Return, Monthly_Volatility, Volume_Avg, Current_PE, Current_Beta, Extraction_Date`

Fichero: `finance_monthly_2006_2025.csv`

### 8.6 `era5_extract.py` — Temperatura y precipitación anual

- **API:** GEE — colección `ECMWF/ERA5_LAND/MONTHLY_AGGR`
- **Método:** `reduceRegion` por ciudad FUA, agregación anual
- **Salida:** `era5_climate.csv`

Columnas: `City, Year, Temp_Annual_Mean_C, Precip_Annual_Sum_m`

### 8.7 `s5p_aerosol_extract.py` — UVAI aerosol anual

- **API:** GEE — Sentinel-5P UVAI `COPERNICUS/S5P/OFFL/L3_AER_AI`
- **Salida:** `s5p_aerosol.csv`

Columnas: `City, Year, UVAI_Annual_Mean`

### 8.8 `urban_atlas_extract.py` — Cobertura suelo ESA WorldCover

- **API:** GEE — `ESA/WorldCover/v200`
- **Años disponibles:** 2020 y 2021
- **Salida:** `worldcover.csv`

Columnas: `City, Year, WC_Tree_Pct, WC_Built_Pct, WC_Crop_Pct, WC_Natural_Pct`

### 8.9 `edgar_co2_extract.py` — Emisiones CO₂ nacionales EDGAR v8

- **Fuente:** EDGAR v8 (descarga directa)
- **Mapeo:** ISO3 → ISO2 para unir con EURO_FUAS
- **Salida:** `edgar_co2.csv`

Columnas: `country_code, Year, CO2_Country_kt`

### 8.10 `oecd_process.py` — Indicadores política ambiental OECD

- **Input:** `data/raw/oecd/` — CSVs SDMX con columnas `REF_AREA, TIME_PERIOD, OBS_VALUE`
- **Procesamiento:** mapeo ISO3→ISO2, expansión país→ciudades con EURO_FUAS
- **Salida:** `DatosProcesados/oecd_indicators.csv`

Columnas: `City, Year, EPS_Index, Env_Tax_USD, Env_Expenditure, GHG_Total_kt`

- `EPS_Index`: Environmental Policy Stringency index (0–6, OECD)
- `Env_Tax_USD`: recaudación impuestos ambientales (mill. USD)
- `Env_Expenditure`: gasto en protección ambiental (% PIB, NEEP)
- `GHG_Total_kt`: suma de todos los gases GHG (kt CO₂eq)

### 8.11 `investeu_process.py` — Financiación verde InvestEU/EIB

- **Input:** `data/raw/investeu/final_recipients*.csv` (glob)
- **Año:** extraído de `pdf_url` via regex `20\d{2}` (rango 2021–2030); fallback `extraction_date`; default 2022
- **Mapeo:** Borrower Country (nombre inglés) → ISO2 via `COUNTRY_NAME_TO_ISO2`
- **Salida:** `DatosProcesados/investeu_summary.csv`

Columnas: `City, Year, InvestEU_Ops_Count, InvestEU_Total_EUR`

### 8.12 `merge.py` — Dataset maestro

Fusiona todos los CSVs en un único dataset panel `[city, year, ...]` listo para análisis EKC:
- Join por `city` y `year` (left join sobre base Sentinel-2)
- Incluye OECD (paso 4) e InvestEU (paso 5)
- Calcula `ln_gdp_pps` y `ln_gdp_pps_sq` para la regresión
- Calcula `ndvi_trend_slope` por ciudad (regresión lineal simple sobre series temporales)
- Produce el CSV maestro que es el input directo de `bronze_ingest.py`

---

## 9. Orquestador maestro — run_all.py

`run_all.py` es el **único punto de entrada** del proyecto completo. No hay que ejecutar nada más manualmente.

### 9.1 Estructura de fases

```
FASE 1: DATOS ECONÓMICOS Y FINANCIEROS
  → pib_ine_extract.py
  → eurostat_extract.py  --datasets all --eager-merge
  → OECD_extract.py      --pause 300 --penalty-429 300,600,1200
  → InvestEU_extract.py  --what all
  → YFinance_extract.py

FASE 2: DATOS AMBIENTALES (SATÉLITE)
  → Sentinel-2_extract.py   (itera todas las ciudades internamente)
  → Sentinel-5p_extract.py  (itera todas las ciudades internamente)
  → HRL_extract.py
  → hrl_to_csv.py

FASE 3: POST-PROCESO (MERGE)
  → merge.py

FASE 4: BBDD + ML + EXPORT
  → spark-submit run_pipeline.py  (8 fases internas)
```

### 9.2 Flags disponibles

```bash
python run_all.py                                        # Todo
python run_all.py --skip-bbdd                            # Solo ETL
python run_all.py --only-bbdd                            # Solo BBDD
python run_all.py --only-bbdd --bbdd-args "--only-models"  # Solo modelos ML
python run_all.py --dry-run                              # Mostrar sin ejecutar
```

### 9.3 Parámetros spark-submit (configurados en run_all.py)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 4 \
  Lorca/BBDD/run_pipeline.py
```

---

## 10. Capa Bronze — schema y contratos de datos

**Propósito:** datos brutos, append-only, con metadata de auditoría. Nunca se modifican. Son la "fuente de verdad" histórica.

**Base de datos Hive:** `gtp_bronze`
**Ruta HDFS:** `hdfs:///user/gtp/bronze/`
**Metadata en todas las tablas:** `_ingestion_date`, `_source_system`, `_file_name`

### 10.1 `bronze_sentinel2_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA: `Madrid_ES` |
| lat | DOUBLE | Latitud (null en Bronze, se añade en Silver) |
| lon | DOUBLE | Longitud (null en Bronze) |
| month | INT | Mes (1–12) |
| ndvi_mean | DOUBLE | Media NDVI en el ROI (GEE reduceRegion) |
| ndvi_std | DOUBLE | Desviación estándar espacial del NDVI |
| ndvi_valid_pixels | INT | Número de píxeles válidos |
| _ingestion_date | STRING | Fecha UTC de ingesta (YYYY-MM-DD) |
| _source_system | STRING | `GEE_S2` |
| _file_name | STRING | `sentinel2.csv` |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** (ISO-2 extraído del city_code) |

### 10.2 `bronze_sentinel5p_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA |
| lat | DOUBLE | Latitud (null) |
| lon | DOUBLE | Longitud (null) |
| month | INT | Mes (1–12) |
| no2_mean | DOUBLE | Densidad media NO₂ troposférica (mol/m²) |
| no2_std | DOUBLE | Desviación estándar espacial |
| no2_valid_pixels | INT | Píxeles válidos |
| _ingestion_date / _source_system / _file_name | STRING | Metadata |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

### 10.3 `bronze_hrl_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA |
| lat / lon | DOUBLE | null en Bronze |
| imperviousness_mean | DOUBLE | Impermeabilización media (0–100%) |
| tree_cover_pct | DOUBLE | % cobertura arbórea (TCD) |
| small_woody_mean | DOUBLE | % elementos leñosos pequeños |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

### 10.4 `bronze_finance_raw` ← ACTUALIZADO: granularidad mensual

| Columna | Tipo | Descripción |
|---------|------|-------------|
| ticker | STRING | Símbolo bursátil (`IBE.MC`) |
| company_name | STRING | Nombre corto |
| sector | STRING | Sector GICS Yahoo Finance |
| industry | STRING | Industria |
| country | STRING | País de cotización (nombre completo) |
| fua_country_code | STRING | ISO-2 para cruzar con EURO_FUAS |
| **month** | INT | **Mes (1–12) — nuevo campo** |
| close_price | DOUBLE | Precio de cierre promedio mensual |
| **monthly_return** | DOUBLE | **Retorno mensual: (P_final/P_inicial)−1** |
| **monthly_volatility** | DOUBLE | **Volatilidad mensual: std(daily_return)×√21** |
| volume_avg | DOUBLE | Volumen medio diario negociado |
| current_pe | DOUBLE | PER trailing (snapshot) |
| current_beta | DOUBLE | Beta de mercado (snapshot) |
| extraction_date | STRING | Fecha de extracción |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country_code** | STRING | **Partición** (= fua_country_code) |

### 10.5 `bronze_eurostat_gdp_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| geo_code | STRING | Código NUTS-0 del país (ES, FR...) |
| geo_label | STRING | Nombre del país |
| unit | STRING | Unidad (PPS_HAB, EUR_HAB, MIO_EUR) |
| gdp_value | DOUBLE | Valor GDP |
| obs_flag | STRING | Flag calidad (e=estimado, p=provisional) |
| _dataset_code | STRING | Código dataset Eurostat |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

### 10.6 `bronze_eurostat_population_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city_code | STRING | Código Eurostat ciudad (ES001C = Madrid) |
| city_name | STRING | Nombre ciudad |
| geo_code | STRING | Código NUTS-0 |
| indic_ur | STRING | Indicador Eurostat (POP = total) |
| population | BIGINT | Población FUA |
| obs_flag | STRING | Flag calidad |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

### 10.7 `bronze_era5_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA |
| temp_annual_mean_c | DOUBLE | Temperatura media anual (°C) |
| precip_annual_sum_m | DOUBLE | Precipitación anual acumulada (m) |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

### 10.8 `bronze_s5p_aerosol_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA |
| uvai_annual_mean | DOUBLE | UV Aerosol Index medio anual (Sentinel-5P UVAI) |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

### 10.9 `bronze_worldcover_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA |
| wc_tree_pct | DOUBLE | % cobertura árbol (ESA WorldCover clase 10) |
| wc_built_pct | DOUBLE | % área construida (clase 50) |
| wc_crop_pct | DOUBLE | % cultivos (clases 40/60) |
| wc_natural_pct | DOUBLE | % vegetación natural (resto) |
| Metadata | — | — |
| **year** | INT | **Partición** (2020 o 2021) |
| **country** | STRING | **Partición** |

### 10.10 `bronze_edgar_co2_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| country_code | STRING | ISO-2 del país |
| co2_country_kt | DOUBLE | Emisiones CO₂ totales del país (kt) |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** (= country_code) |

### 10.11 `bronze_oecd_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA (expandido de país→ciudades) |
| eps_index | DOUBLE | Environmental Policy Stringency (0–6) |
| env_tax_usd | DOUBLE | Recaudación impuestos ambientales (mill. USD) |
| env_expenditure | DOUBLE | Gasto protección ambiental NEEP (% PIB) |
| ghg_total_kt | DOUBLE | Emisiones GHG totales (kt CO₂eq, suma todos los gases) |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** (ISO-2) |

### 10.12 `bronze_investeu_raw`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city | STRING | Código FUA (expandido de país→ciudades) |
| investeu_ops_count | INT | Número de operaciones InvestEU en el país ese año |
| investeu_total_eur | DOUBLE | Importe total financiación InvestEU (EUR) |
| Metadata | — | — |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** (ISO-2) |

---

## 11. Capa Silver — Star Schema Kimball

**Propósito:** datos limpios, estructurados en modelo dimensional. Permite análisis SQL eficientes con filtros por ciudad, año, fuente.

**Base de datos Hive:** `gtp_silver`
**Ruta HDFS:** `hdfs:///user/gtp/silver/`

```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │ date_sk
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼──────┐  ┌────────▼──────┐  ┌───────▼────────┐
│dim_city      │  │fact_environ.  │  │fact_economic   │
│(city_sk)     │  │(city×year)    │  │(city×year)     │
└──────────────┘  └───────────────┘  └────────────────┘
        │
        │ (empresa asociada al país de la ciudad)
┌───────▼──────┐  ┌───────────────┐
│dim_company   │  │fact_financial │
│(company_sk)  │  │(empresa×año×mes)│
└──────────────┘  └───────────────┘
        │
┌───────▼──────┐
│dim_source    │
│(source_sk)   │
└──────────────┘
```

### 11.1 `dim_city` — SCD Tipo 1

| Columna | Tipo | Descripción |
|---------|------|-------------|
| city_sk | LONG | Surrogate key (secuencial desde 1) |
| city_code | STRING | `Madrid_ES` — clave de negocio |
| city_name | STRING | `Madrid` |
| country_code | STRING | `ES` |
| country_name | STRING | `Spain` |
| nuts_code | STRING | Código NUTS-2 (null si no disponible) |
| eurostat_city_code | STRING | `ES001C` |
| lat | DOUBLE | Latitud FUA |
| lon | DOUBLE | Longitud FUA |
| fua_area_km2 | DOUBLE | Área km² (null — pendiente fuente) |
| _last_updated | STRING | Fecha de última carga |
| **country_code** | STRING | **Partición** |

Construida directamente desde `EURO_FUAS` de `config.py`. **237 filas.**

### 11.2 `dim_date` — Estática 2000–2030

| Columna | Tipo | Descripción |
|---------|------|-------------|
| date_sk | INT | `YYYYMMDD` — clave surrogate |
| full_date | STRING | `2023-06-15` |
| year | INT | Año |
| quarter | INT | Trimestre (1–4) |
| month | INT | Mes (1–12) |
| month_name | STRING | `June` |
| week_of_year | INT | Semana ISO |
| day_of_month | INT | Día del mes |
| day_of_week | INT | Día semana ISO (1=Lunes, 7=Domingo) |
| day_name | STRING | `Thursday` |
| is_weekend | BOOLEAN | true si sáb/dom |
| is_leap_year | BOOLEAN | true si año bisiesto |
| fiscal_year | INT | = year |
| semester | INT | 1 (ene–jun) o 2 (jul–dic) |
| is_satellite_era | BOOLEAN | true si year ≥ 2018 (inicio Sentinel) |

**~11.000 filas** (31 años × 365 días). Sin partición — se lee completa siempre.

### 11.3 `dim_source` — SCD Tipo 1

| source_sk | source_code | source_name | update_frequency |
|-----------|-------------|-------------|-----------------|
| 1 | GEE_S2 | Sentinel-2 NDVI | Monthly |
| 2 | GEE_S5P | Sentinel-5P NO2 | Monthly |
| 3 | Copernicus_HRL | HRL Imperviousness | Triennial |
| 4 | YFinance | Yahoo Finance Stocks | **Monthly** |
| 5 | Eurostat_GDP | Eurostat GDP per capita | Annual |
| 6 | Eurostat_POP | Eurostat Population FUAs | Annual |

### 11.4 `dim_company` — SCD Tipo 2

| Columna | Tipo | Descripción |
|---------|------|-------------|
| company_sk | LONG | Surrogate key |
| ticker | STRING | `IBE.MC` — clave de negocio |
| company_name | STRING | Nombre corto |
| sector | STRING | Sector GICS |
| industry | STRING | Industria |
| stock_exchange | STRING | Bolsa (null — pendiente) |
| esg_classification | STRING | `GREEN` (todos en cartera son verdes) |
| country_code | STRING | ISO-2 |
| country_name | STRING | Nombre país |
| valid_from | STRING | Inicio vigencia (SCD-2) |
| valid_to | STRING | Fin vigencia (`9999-12-31` si activo) |
| is_current | BOOLEAN | true = registro vigente |
| **country_code** | STRING | **Partición** |

Lógica SCD-2: si cambia sector/industria, se cierra el registro antiguo y se crea uno nuevo.

### 11.5 `fact_environmental` — ciudad × año

Agregación de datos mensuales S2/S5P a granularidad anual. **Es la tabla central del modelo EKC.**

| Columna | Tipo | Descripción |
|---------|------|-------------|
| env_fact_sk | LONG | Surrogate key |
| city_sk | LONG | FK → dim_city |
| date_sk | INT | FK → dim_date (1 enero del año) |
| source_sk_s2/s5p/hrl | INT | FK → dim_source |
| ndvi_annual_mean | DOUBLE | Media NDVI anual |
| ndvi_annual_std | DOUBLE | Variabilidad NDVI anual |
| ndvi_spring_mean | DOUBLE | Media meses 3,4,5 |
| ndvi_summer_mean | DOUBLE | Media meses 6,7,8 |
| ndvi_autumn_mean | DOUBLE | Media meses 9,10,11 |
| ndvi_winter_mean | DOUBLE | Media meses 12,1,2 |
| ndvi_valid_months | INT | Meses con dato (max 12) |
| no2_annual_mean | DOUBLE | Media NO₂ anual (mol/m²) |
| no2_annual_std | DOUBLE | Variabilidad NO₂ anual |
| no2_valid_months | INT | Meses con dato |
| imperviousness_mean | DOUBLE | % suelo sellado (HRL) |
| tree_cover_pct | DOUBLE | % cobertura arbórea (HRL) |
| small_woody_mean | DOUBLE | % leñosos pequeños (HRL) |
| ndvi_yoy_change | DOUBLE | NDVI(año) − NDVI(año−1) |
| ndvi_trend_slope | DOUBLE | Pendiente OLS temporal NDVI por ciudad |
| green_index | DOUBLE | Índice compuesto GTP ∈ [0,1] |
| _silver_load_date | STRING | Fecha de carga |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

**Green Index (fórmula):**
```
green_index = 0.5 × NDVI_norm + 0.3 × (1 − NO2_norm) + 0.2 × (1 − Imperv_norm)
```
Normalización MinMax sobre el dataset completo: `norm = (x − min) / (max − min)`

**NDVI Trend Slope (cálculo en Spark):**
```
slope = Σ[(year − year_mean)(ndvi − ndvi_mean)] / Σ[(year − year_mean)²]
```
Implementado con Window functions de Spark (sin UDF costosa).

### 11.6 `fact_economic` — ciudad × año

| Columna | Tipo | Descripción |
|---------|------|-------------|
| econ_fact_sk | LONG | Surrogate key |
| city_sk | LONG | FK → dim_city |
| date_sk | INT | FK → dim_date |
| gdp_pps_per_capita | DOUBLE | PIB per cápita PPS (EUR/hab) |
| gdp_eur_per_capita | DOUBLE | PIB en EUR corrientes |
| gdp_pps_index | DOUBLE | Índice PPS (null — pendiente) |
| ln_gdp_pps | DOUBLE | `ln(gdp_pps_per_capita)` — variable EKC |
| ln_gdp_pps_sq | DOUBLE | `[ln(gdp_pps_per_capita)]²` — variable EKC |
| gdp_growth_rate | DOUBLE | Tasa crecimiento YoY (%) |
| fua_population | LONG | Población FUA |
| population_density | DOUBLE | Hab/km² (null — pendiente) |
| population_yoy_growth | DOUBLE | Crecimiento poblacional YoY |
| **year** | INT | **Partición** |
| **country** | STRING | **Partición** |

### 11.7 `fact_financial` — empresa × año × mes ← ACTUALIZADO

| Columna | Tipo | Descripción |
|---------|------|-------------|
| fin_fact_sk | LONG | Surrogate key |
| company_sk | LONG | FK → dim_company |
| date_sk | INT | `year × 10000 + month × 100 + 1` |
| source_sk | INT | 4 (YFinance) |
| close_price_avg | DOUBLE | Precio cierre promedio mensual |
| **monthly_return** | DOUBLE | **Retorno mensual** |
| **monthly_volatility** | DOUBLE | **Volatilidad mensual (std×√21)** |
| current_beta | DOUBLE | Beta de mercado |
| volume_avg | DOUBLE | Volumen diario promedio |
| current_pe | DOUBLE | PER trailing |
| **month** | INT | **Mes (1–12)** |
| _silver_load_date | STRING | — |
| **year** | INT | **Partición** |
| **country_code** | STRING | **Partición** |

---

## 12. Capa Gold — tablas analíticas finales

**Propósito:** tablas desnormalizadas listas para consumo analítico y ML. Se sobreescriben con cada ejecución de modelos.

**Base de datos Hive:** `gtp_gold`
**Ruta HDFS:** `hdfs:///user/gtp/gold/`

### 12.1 `fact_kuznets` — tabla central (ciudad × año)

La tabla más importante del proyecto. Consolida ambiental + económico + financiero + resultados de todos los modelos ML.

| Grupo | Columnas |
|-------|---------|
| **Identidad** | city_sk, city_code, city_name, country_code, country_name |
| **Ambiental** | ndvi_mean, ndvi_trend_slope, ndvi_yoy_change, ndvi_spring/summer/autumn/winter_mean, no2_mean, imperviousness_mean, tree_cover_pct, green_index |
| **Económico** | gdp_pps_per_capita, gdp_eur_per_capita, ln_gdp_pps, ln_gdp_pps_sq, gdp_growth_rate, fua_population |
| **Financiero** | fin_close_price_avg, fin_annual_volatility†, fin_tickers (JSON), fin_companies (JSON) |
| **Clustering** | cluster_id, cluster_label, cluster_silhouette |
| **EKC** | ekc_beta1, ekc_beta2, ekc_alpha, ekc_turning_point_y, ekc_r_squared, ekc_p_value_beta1/beta2, ekc_shape, gdp_gap_to_turning |
| **XGBoost** | turning_point_phase (DEGRADANDO/TURNING/RECUPERANDO), phase_confidence |
| **Prophet** | prophet_ndvi_forecast_1y/3y/5y, prophet_turning_year, prophet_forecast_lower/upper_95 |
| **Score** | investment_score (0–100), investment_recommendation (STRONG_BUY/BUY/HOLD/AVOID) |

† `fin_annual_volatility` = `avg(monthly_volatility) × √12` ≈ `avg_monthly_vol × 3.4641` (anualización estándar)

**Fórmula investment_score:**
```
investment_score = green_index × 50
                 + phase_confidence × 30
                 + (20 si TURNING | 15 si RECUPERANDO | 0 si DEGRADANDO)
```

**Recomendaciones:**
- `STRONG_BUY` → score ≥ 75
- `BUY` → score ≥ 55
- `HOLD` → score ≥ 35
- `AVOID` → score < 35

### 12.2 `city_ranking` — ranking por año

| Columna | Descripción |
|---------|-------------|
| rank_position | Posición global en el año |
| rank_within_country | Posición dentro del país |
| city_code / city_name / country_code / country_name | Identidad |
| investment_score | Puntuación 0–100 |
| investment_recommendation | STRONG_BUY / BUY / HOLD / AVOID |
| green_index | Índice ambiental |
| turning_point_phase | Fase EKC |
| prophet_turning_year | Año estimado del turning point |
| rank_change | Δ posición vs año anterior |
| score_change | Δ score vs año anterior |
| top_ticker / top_company | Empresa representativa del país |
| **year** | **Partición** |

### 12.3 `ekc_parameters` — parámetros EKC por cluster

Una fila por cluster y año de estimación.

| Columna | Descripción |
|---------|-------------|
| cluster_id / cluster_label | Identificador del cluster |
| estimation_year | Año de la estimación |
| n_cities / n_observations | Tamaño de la muestra |
| city_codes / countries_represented | JSON con las ciudades del cluster |
| gdp_pps_mean / gdp_pps_std | Estadísticas GDP del cluster |
| alpha / beta1 / beta2 | Parámetros EKC estimados |
| beta1/beta2_se | Errores estándar |
| beta1/beta2_pvalue | p-valores |
| turning_point_y_star | Y* = exp(-β₁/2β₂) |
| turning_point_ln_y | ln(Y*) |
| r_squared / r_squared_adj | R² y R² ajustado |
| f_statistic / f_pvalue | Test F global |
| aic / bic | Criterios de información |
| ekc_hypothesis_supported | true si β₁>0 y β₂<0 y Y*>0 |
| ekc_shape | `U_INVERTED` / `MONOTONIC_INC` / `MONOTONIC_DEC` / `N_SHAPE` |

### 12.4 `model_results` — outputs ML unificados (ciudad × año)

Consolidación de los 4 modelos para análisis comparativo. Columnas de K-Means, EKC, XGBoost y Prophet por ciudad-año.

---

## 13. Pipeline BBDD — scripts de ingesta y transformación

### 13.1 `bronze_ingest.py`

Toma los CSVs del ETL y los escribe en HDFS Bronze como Parquet particionado.

```bash
spark-submit bronze_ingest.py --source all --mode overwrite
# Fuentes individuales:
spark-submit bronze_ingest.py --source s2
spark-submit bronze_ingest.py --source finance
```

**Proceso por fuente:**
1. Leer CSV con schema explícito (`DROPMALFORMED` para filas corruptas)
2. `withColumnRenamed` — normalizar a minúsculas
3. `extract_country` — extraer ISO-2 del `city_code` para la partición
4. `add_metadata` — añadir `_ingestion_date`, `_source_system`, `_file_name`
5. `write_bronze` — escribir Parquet particionado en HDFS
6. `MSCK REPAIR TABLE` — registrar particiones en Hive

### 13.2 `silver_transform.py`

Transforma Bronze en Star Schema Kimball.

```bash
spark-submit silver_transform.py
spark-submit silver_transform.py --skip-dims   # Solo facts
spark-submit silver_transform.py --skip-facts  # Solo dims
```

**Transformaciones principales:**
- `build_dim_city`: importa `EURO_FUAS` de `config.py` (portátil, no hardcoded)
- `build_dim_company`: lógica SCD-2 completa (detecta cambios sector/industria)
- `build_fact_environmental`: agrega S2+S5P de mensual a anual; calcula `ndvi_trend_slope` y `green_index`
- `build_fact_financial`: usa `monthly_return` y `monthly_volatility`

### 13.3 `gold_build.py`

Construye las 4 tablas Gold desde Silver.

```bash
spark-submit gold_build.py
spark-submit gold_build.py --table fact_kuznets   # Solo una tabla
```

**Join de financiero en `build_fact_kuznets`:**
```python
# Agrega mensual → anual antes del join con fact_kuznets
fin_agg = fin.groupBy("country_code", "year").agg(
    F.avg("close_price_avg").alias("fin_close_price_avg"),
    (F.avg("monthly_volatility") * F.lit(3.4641)).alias("fin_annual_volatility"),
    # sqrt(12) = 3.4641 — anualización de volatilidad mensual
)
```

### 13.4 `export_to_postgres.py`

Copia las tablas Gold de HDFS a PostgreSQL para el serving layer.

```bash
# Modo Spark (en Lorca, usa JDBC):
spark-submit export_to_postgres.py --mode spark

# Modo local (sin Spark, usa pandas):
python export_to_postgres.py --mode local

# Solo una tabla:
spark-submit export_to_postgres.py --table city_ranking
```

**Variables de entorno necesarias:**
```bash
export PG_HOST="localhost"
export PG_PORT="5432"
export PG_DB="gtp_db"
export PG_USER="gtp_user"
export PG_PASSWORD="tu_password"
```

---

## 14. Pipeline ML — los 4 modelos en detalle

### 14.1 Modelo 1: K-Means Clustering (`clustering.py`)

**Propósito:** agrupar las 237 ciudades en clusters homogéneos antes de la regresión EKC. Si se hace una sola regresión global se mezclan dinámicas incompatibles (Noruega vs Rumanía).

**Features** (todas a nivel ciudad, promedio histórico):
```
ndvi_mean_city     — NDVI medio histórico
ndvi_slope_city    — Tendencia NDVI (positivo = reverdeciendo)
no2_mean_city      — NO₂ medio histórico
imperviousness_city — Impermeabilización media
ln_gdp_mean_city   — ln(GDP per cápita) medio
gdp_growth_mean_city — Crecimiento económico medio
```

**Proceso:**
1. `VectorAssembler` → vector de features
2. `StandardScaler` (media=0, std=1) → escalar
3. K-Means para K=2..8 → evaluar `ClusteringEvaluator` (Silhouette)
4. Seleccionar K óptimo (máximo Silhouette)
5. Asignar etiquetas interpretables según centroides:

| Etiqueta | Criterio |
|----------|---------|
| `High-Income-Greening` | GDP alto + NDVI slope positivo |
| `High-Income-Stagnant` | GDP alto + NDVI slope negativo + NO₂ alto |
| `High-Income-Stable` | GDP alto + NDVI slope negativo + NO₂ bajo |
| `Industrializing-Polluted` | GDP bajo + NO₂ alto |
| `Emerging-Improving` | GDP bajo + NDVI slope positivo |
| `Developing-Mixed` | resto |

**Salida en Gold:** `cluster_id`, `cluster_label`, `cluster_silhouette` en `fact_kuznets`

### 14.2 Modelo 2: EKC Panel Regression (`ekc_regression.py`)

**Propósito:** estimar β₁, β₂, Y* por cluster con regresión de datos de panel (efectos fijos ciudad + año).

**Librería:** `linearmodels.PanelOLS` (Python, ejecutado en el driver de Spark)

**Dataset:** pool de ciudades del cluster × años = ~50 ciudades × 8 años = ~400 observaciones

**Especificación:**
```python
from linearmodels import PanelOLS
import numpy as np

# La tabla Gold se carga como Pandas desde Spark (cabe en memoria del driver)
model = PanelOLS.from_formula(
    "ln_ndvi ~ ln_gdp_pps + ln_gdp_pps_sq + EntityEffects + TimeEffects",
    data=panel_data
)
result = model.fit(cov_type="clustered", cluster_entity=True)

beta1 = result.params["ln_gdp_pps"]
beta2 = result.params["ln_gdp_pps_sq"]
y_star = np.exp(-beta1 / (2 * beta2))

ekc_confirmed = (beta1 > 0) and (beta2 < 0) and (y_star > 0)
```

**AIC / BIC:**
```python
aic = -2 * result.loglik + 2 * n_params
bic = -2 * result.loglik + np.log(n_obs) * n_params
```

**Salida en Gold:** `ekc_parameters` (tabla) + actualiza `fact_kuznets` con EKC cols.

### 14.3 Modelo 3: XGBoost Classifier (`xgboost_classifier.py`)

**Propósito:** clasificar cada ciudad-año en una de 3 fases sin asumir la forma de la curva EKC. Es el "fact-check" del modelo paramétrico.

**Clases:** `DEGRADANDO` | `TURNING` | `RECUPERANDO`

**Features:**
```
ndvi_mean, ndvi_trend_slope, ndvi_yoy_change,
no2_mean, imperviousness_mean,
ln_gdp_pps, ln_gdp_pps_sq, gdp_growth_rate,
cluster_id, green_index, gdp_gap_to_turning, year
```

**Etiquetado automático:**
```python
def label_phase(row):
    # TURNING: GDP cercano al turning point (±15%)
    if abs(gdp_gap) <= 0.15 * Y_star:
        return "TURNING"
    # RECUPERANDO: NDVI creciendo Y GDP superó Y*
    if slope > 0.003 and gdp > Y_star:
        return "RECUPERANDO"
    # DEGRADANDO: por defecto
    return "DEGRADANDO"
```

**Parámetros XGBoost:**
```python
XGBClassifier(
    n_estimators=200, max_depth=5, learning_rate=0.05,
    subsample=0.8, colsample_bytree=0.8,
    min_child_weight=5, gamma=1,
    reg_alpha=0.1, reg_lambda=1.0,
    objective="multi:softprob", num_class=3,
    eval_metric="mlogloss", early_stopping_rounds=20,
    random_state=42
)
```

**Split temporal:** años ≤ (max_year − 2) para train, resto para test. Sin random split para evitar data leakage.

**Balanceo de clases:** `sample_weight` proporcional a la inversa de la frecuencia de cada clase (TURNING es la clase minoritaria).

**Métricas:** F1 Macro + F1 Weighted + classification report completo.

**Salida:** `turning_point_phase` (= xgb_predicted_phase), `phase_confidence`, `xgb_prob_degradando/turning/recuperando` en `fact_kuznets`.

### 14.4 Modelo 4: Prophet Forecast (`prophet_forecast.py`)

**Propósito:** predecir NDVI futuro por ciudad (1Y, 3Y, 5Y) y estimar cuándo una ciudad alcanzará el turning point.

**Implementación:** pandas UDF sobre Spark (cada worker procesa varias ciudades en paralelo).

**Serie temporal:** NDVI mensual desde `fact_environmental` Silver (columna `ndvi_annual_mean` agregada mensualmente).

```python
# Schema del UDF (IMPORTANTE: ds debe ser DateType, no StringType)
PROPHET_OUTPUT_SCHEMA = StructType([
    StructField("city_code", StringType(), True),
    StructField("ds",        DateType(),   True),   # ← DateType, no String
    StructField("yhat",      DoubleType(), True),
    StructField("yhat_lower", DoubleType(), True),
    StructField("yhat_upper", DoubleType(), True),
    StructField("trend",     DoubleType(), True),
])
```

**Proceso por ciudad:**
```python
m = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    seasonality_mode="multiplicative",
    interval_width=0.95,
)
m.fit(city_data)  # ds (DateType) + y (NDVI)
future = m.make_future_dataframe(periods=60, freq="M")  # 5 años
forecast = m.predict(future)
```

**prophet_turning_year:** año en que `yhat_slope` (derivada del trend) cruza cero → de creciente a decreciente (si ya está recuperando) o viceversa.

**Salida:** `prophet_ndvi_forecast_1y/3y/5y`, `prophet_turning_year`, `prophet_forecast_lower/upper_95` en `fact_kuznets`.

---

## 15. Orquestador BBDD — run_pipeline.py

Orquesta las 8 fases del pipeline Spark. Se invoca desde `run_all.py` via `spark-submit`.

```
FASE 1: bronze_ingest.py      (CSV → HDFS Bronze)
FASE 2: silver_transform.py   (Bronze → Star Schema Silver)
FASE 3: gold_build.py         (Silver → Gold base)
FASE 4: clustering.py         (K-Means → cluster_id en Gold)
FASE 5: ekc_regression.py     (Panel OLS → ekc_parameters en Gold)
FASE 6: xgboost_classifier.py (Fase EKC → turning_point_phase en Gold)
FASE 7: prophet_forecast.py   (Forecast NDVI → forecasts en Gold)
FASE 8: export_to_postgres.py (Gold HDFS → MariaDB bd_rvm_gtp serving layer)
```

**Flags de run_pipeline.py:**
```bash
spark-submit run_pipeline.py --only-models        # Solo fases 4-8
spark-submit run_pipeline.py --skip-bronze        # Saltar fase 1
spark-submit run_pipeline.py --skip-silver        # Saltar fase 2
spark-submit run_pipeline.py --skip-export        # Saltar fase 8 (sin export a MariaDB)
spark-submit run_pipeline.py --skip-gold          # Saltar gold_build
```

---

## 16. Serving Layer — MariaDB + export_to_postgres.py

**Propósito:** proporcionar consultas <10ms para la API Flask y herramientas BI. HDFS + Spark son excelentes para batch pero lentos para queries interactivas.

**Servidor:** `10.151.30.2:3306` — MariaDB del cluster Lorca (UEM)
**BD:** `bd_rvm_gtp` | **Usuario:** `bd_rvm_gtp` | **Contraseña:** `Sol2026A`
**DDL:** `Lorca/BBDD/schemas/mariadb_serving_ddl.sql`

> Nota: el script se llama `export_to_postgres.py` por razones históricas (nombre legacy). Escribe a MariaDB, no a PostgreSQL.

### 16.1 Tablas MariaDB `bd_rvm_gtp.*`

**5 tablas** (espejo del Gold HDFS + dimensión ciudad):
- `dim_city` — dimensión ciudad con coordenadas (para mapas Power BI)
- `fact_kuznets` — tabla central EKC (ciudad × año, ~85 columnas)
- `city_ranking` — ranking pre-calculado con rank_position y score_change
- `ekc_parameters` — parámetros de regresión EKC por cluster y año
- `model_results` — outputs detallados de los 4 modelos ML

**5 vistas para Power BI / API:**
- `v_powerbi_map` — mapa europeo último año (incluye lat/lon, eps_index, investeu_total_eur)
- `v_turning_opportunities` — ciudades en fase TURNING ordenadas por investment_score
- `v_ekc_summary` — resumen EKC por cluster (último año estimado)
- `v_top_20_cities` — top 20 ciudades del último año
- `v_phase_distribution` — distribución de fases por país y año

**Nuevas columnas OECD/InvestEU en `fact_kuznets`:**
```sql
eps_index           DOUBLE,     -- Environmental Policy Stringency (0–6)
env_tax_usd         DOUBLE,     -- Recaudación impuestos ambientales (mill. USD)
env_expenditure     DOUBLE,     -- Gasto protección ambiental NEEP (% PIB)
ghg_total_kt        DOUBLE,     -- Emisiones GHG totales del país (kt CO2eq)
investeu_ops_count  INTEGER,    -- Nº operaciones InvestEU en el país y año
investeu_total_eur  DOUBLE,     -- Importe total financiación InvestEU (EUR)
```

**Conexión Power BI:**
```
Inicio > Obtener datos > Base de datos > MySQL
Servidor: 10.151.30.2:3306   Base de datos: bd_rvm_gtp
```

### 16.2 Modos de exportación

```bash
# Modo Spark (JDBC desde Lorca — usa los recursos del cluster):
spark-submit export_to_postgres.py --mode spark

# Modo local (pandas — sin Spark, útil para desarrollo):
python export_to_postgres.py --mode local

# Tabla específica:
spark-submit export_to_postgres.py --table city_ranking --mode spark
```

**Variables de entorno requeridas (en Lorca/.env):**
```bash
PG_HOST=10.151.30.2
PG_PORT=3306
PG_DB=bd_rvm_gtp
PG_USER=bd_rvm_gtp
PG_PASSWORD=Sol2026A
```

---

## 17. API Web Flask — endpoints y autenticación

Fichero: `Lorca/Web/app.py`

### 17.1 Endpoints de autenticación

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | `/register` | Registro de usuario (nombre, email, contraseña) |
| POST | `/login` | Autenticación (email + contraseña) |
| POST | `/newsletter` | Suscripción al boletín (email) |

Datos almacenados en `Lorca/Web/data/users.txt` y `newsletter.txt` (JSONL).

### 17.2 Endpoints de datos

Todos leen desde MariaDB `bd_rvm_gtp.*` (configurado via env vars PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD).

| Método | Endpoint | Parámetros | Descripción |
|--------|----------|-----------|-------------|
| GET | `/api/ranking` | `year`, `country`, `limit` | Ranking de ciudades ordenado por investment_score |
| GET | `/api/city/<city_code>` | `year` | Detalle completo de una ciudad (todos los indicadores) |
| GET | `/api/opportunities` | `min_score`, `limit` | Ciudades con BUY/STRONG_BUY recomendación |
| GET | `/api/clusters` | `year` | Resumen estadístico de cada cluster |
| GET | `/api/years` | — | Años disponibles en el dataset |
| GET | `/api/countries` | — | Países disponibles |
| GET | `/api/summary` | — | Estadísticas globales (n_cities, n_years, avg_score...) |

**Ejemplo de respuesta `/api/ranking?year=2023&limit=5`:**
```json
{
  "year": 2023,
  "total": 237,
  "data": [
    {
      "rank_position": 1,
      "city_code": "Oslo_NO",
      "city_name": "Oslo",
      "country_code": "NO",
      "investment_score": 87.4,
      "investment_recommendation": "STRONG_BUY",
      "green_index": 0.823,
      "turning_point_phase": "RECUPERANDO",
      "prophet_turning_year": 2019
    }
  ]
}
```

---

## 18. Variables del dataset maestro (merge.py output)

El CSV maestro generado por `merge.py` (input de `bronze_ingest.py`) contiene:

| Variable | Fuente | Tipo | Descripción |
|----------|--------|------|-------------|
| City | config | STRING | Código FUA (`Madrid_ES`) |
| Year | todos | INT | Año de la observación |
| NDVI_Mean | Sentinel-2 | DOUBLE | NDVI medio anual |
| NDVI_Std | Sentinel-2 | DOUBLE | Variabilidad NDVI |
| NDVI_Spring/Summer/Autumn/Winter | Sentinel-2 | DOUBLE | NDVI estacional |
| NO2_Mean | Sentinel-5P | DOUBLE | NO₂ medio anual (mol/m²) |
| Imperviousness_Mean | HRL | DOUBLE | % suelo sellado |
| Tree_Cover_Pct | HRL | DOUBLE | % cobertura arbórea |
| GDP_Per_Capita | Eurostat/INE | DOUBLE | PIB per cápita (EUR/hab) |
| GDP_Growth_Rate | calculado | DOUBLE | Tasa crecimiento YoY (%) |
| ln_GDP | calculado | DOUBLE | ln(GDP_Per_Capita) |
| ln_GDP_sq | calculado | DOUBLE | [ln(GDP_Per_Capita)]² |
| NDVI_Slope | calculado | DOUBLE | Pendiente tendencia NDVI |
| Country | config | STRING | Código ISO-2 |
| lat / lon | config | DOUBLE | Coordenadas FUA |

---

## 19. Reglas de imputación y calidad de datos

| Fuente | Problema | Regla |
|--------|---------|-------|
| Sentinel-2 | Nubosidad → meses sin dato | `ndvi_valid_months` < 6 → ciudad excluida del análisis EKC |
| Sentinel-5P | Órbitas sin cobertura | `no2_valid_months` < 6 → imputar con mediana del cluster |
| HRL | Solo años 2018, 2021... | Forward-fill hasta el siguiente dato trienal |
| YFinance | Ticker deslistado | `len(hist) == 0` → skip; no genera registro |
| Eurostat GDP | `obs_flag = 'e'` (estimado) | Mantener, marcar con flag |
| Eurostat GDP | País sin dato en un año | Imputar con media del año anterior |
| KMeans | NaN en features | `approxQuantile(0.5)` → mediana de la columna |
| EKC Regression | Cluster con < 50 obs | Skip del cluster → log warning |
| XGBoost | NaN en features | `fillna(0)` |
| Prophet | Serie con < 24 meses | Skip de la ciudad → no forecast |

---

## 20. Convenciones de código

| Convención | Detalle |
|------------|---------|
| `city_code` | `CityName_ISO2` — ej: `Madrid_ES`, `Paris_FR` |
| Particionado | Siempre `year=YYYY/country=XX/` en todas las capas |
| Metadata Bronze | `_ingestion_date`, `_source_system`, `_file_name` — nunca modificar |
| SCD-2 | Columnas `valid_from`, `valid_to`, `is_current` en `dim_company` |
| Spark session | Siempre `enableHiveSupport()` para escribir tablas Hive |
| Small files | `df.coalesce(N)` antes de escribir a HDFS |
| MSCK | Siempre `MSCK REPAIR TABLE` tras escritura directa Parquet |
| Rutas | Usar `Path(__file__).resolve().parent` — nunca hardcodear `/home/...` |
| UPDATE en HDFS | No existe. Siempre sobrescribir particiones: `.mode("overwrite").partitionBy(...)` |
| Fuente de verdad | `EURO_FUAS` vive en `config.py` únicamente |
| Logs | `[OK]`, `[ERROR]`, `[WARN]`, `[GOLD]`, `[SILVER]` prefijos estandarizados |

---

## 21. Bugs corregidos — historial completo

| Fecha | Fichero | Bug | Fix |
|-------|---------|-----|-----|
| 2026-03-11 | `bronze_ingest.py` | Ruta hardcoded `/home/223B3336juan/...` | `Path(__file__).resolve().parent.parent / "ETL" / "script"` |
| 2026-03-11 | `silver_transform.py` | Misma ruta hardcoded en `build_dim_city()` | Misma fix portable |
| 2026-03-11 | `prophet_forecast.py` | `ds` declarado `StringType()` en schema UDF pero Spark produce `DateType` → crash en serialización | Cambiar a `DateType()` + añadir import |
| 2026-03-11 | `clustering.py` | Ternary DataFrameWriter `(...).parquet(...)` no verificaba columna `country` antes de `partitionBy` | Reemplazar con `if "year" in cols and "country" in cols` explícito |
| 2026-03-11 | `xgboost_classifier.py` | Mismo patrón ternario que clustering.py | Mismo fix |
| 2026-03-11 | `YFinance_extract.py` | Granularidad anual → señal financiera pobre | Migrar a mensual (`groupby([year, month])`, `std × √21`) |

---

## 22. Pendiente — tareas fuera del código

El código está completo. Lo que falta es ejecución e infraestructura en Lorca:

| Tarea | Comando clave | Estado |
|-------|---------------|--------|
| Autenticar GEE | `earthengine authenticate` | ⏳ Pendiente |
| Instalar dependencias Python | `pip install rasterio linearmodels xgboost scikit-learn prophet earthengine-api` | ⏳ Pendiente |
| Crear directorios HDFS | `hdfs dfs -mkdir -p /user/gtp/{bronze,silver,gold,models}` | ⏳ Pendiente |
| Ejecutar DDL Bronze | `beeline ... -f bronze_ddl.sql` | ⏳ Pendiente |
| Ejecutar DDL Silver | `beeline ... -f silver_ddl.sql` | ⏳ Pendiente |
| Ejecutar DDL Gold | `beeline ... -f gold_ddl.sql` | ⏳ Pendiente |
| Aplicar DDL MariaDB serving layer | `mysql -h 10.151.30.2 ... < mariadb_serving_ddl.sql` | ⏳ Pendiente |
| Verificar env vars en .env de Lorca | PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD ya rellenadas | ✅ Hecho |
| Test end-to-end en Lorca | `python run_all.py --dry-run` → `python run_all.py` | ⏳ Pendiente |
| Configurar cron jobs | `crontab -e` | ⏳ Pendiente |
| Crear e integrar frontend web | Mockup pendiente de revisión | ⏳ Pendiente |
| Actualizar DDL Bronze para columna `month` en finance | `bronze_ddl.sql` actualizado: `monthly_return`, `monthly_volatility`, `month INT` | ✅ Completado |

> Ver `COMANDOS_SETUP.md` para la secuencia exacta de comandos de cada tarea.
