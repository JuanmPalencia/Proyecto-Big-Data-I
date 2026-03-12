# Green Turning Point (GTP)

## Proyecto Big Data I

**Grado:** Ingeniería Matemática aplicada al Análisis de Datos
**Curso:** 3º
**Universidad:** Universidad Europea

**Autores:**

- Juan Manuel Palencia Osorio
- Pablo Mata Rius
- Pablo Sánchez Ruiz
- María Paula Aguirre Palacio

---

## 1. Idea de Negocio

**Green Turning Point (GTP)** es una plataforma de análisis basada en **Big Data, imágenes satelitales y modelos econométricos** que identifica **ciudades europeas en el punto de inflexión ambiental** de la Curva de Kuznets Ambiental (EKC).

El sistema detecta el momento en que el crecimiento económico deja de degradar el medioambiente y comienza una fase de regeneración activa, generando un **ranking europeo de ciudades por proximidad al Turning Point**.

GTP está orientado a:

- Fondos de inversión verde
- Instituciones públicas
- Planificadores urbanos
- Empresas con criterios ESG

---

## 2. Fundamento Teórico — Curva de Kuznets Ambiental

La **Environmental Kuznets Curve (EKC)** propone que la relación entre renta per cápita y degradación ambiental sigue una U invertida. El proyecto la operacionaliza mediante un modelo de panel econométrico:

```
ln(Eᵢₜ) = α + β₁·ln(Yᵢₜ) + β₂·[ln(Yᵢₜ)]² + μᵢ + λₜ + εᵢₜ
```

**Turning Point:**
```
Y* = exp( −β₁ / 2β₂ )
```

Donde `Eᵢₜ` es el indicador ambiental (NDVI o NO₂) y `Yᵢₜ` el PIB PPS per cápita de la ciudad `i` en el año `t`.

---

## 3. Arquitectura del Sistema

### Pipeline principal — Cluster Lorca (UEM)

```
ETL (run_all.py)  →  12 fuentes  →  CSVs locales
        ↓
HDFS Bronze  (12 tablas Parquet, particionadas year/country)
        ↓
HDFS Silver  (Star Schema Kimball, 4 dims + 3 facts)
        ↓
HDFS Gold    (fact_kuznets + clustering + EKC + XGBoost + Prophet)
        ↓
MariaDB bd_rvm_gtp  (serving layer, <10 ms latencia)
        ↓
Flask API  /api/*  ←  Power BI / Tableau / Grafana
```

**Stack tecnológico:**

| Componente | Tecnología |
|------------|-----------|
| Procesamiento | Apache Spark + PySpark |
| Almacenamiento | HDFS + Apache Hive + Parquet |
| Modelos ML | K-Means (PySpark MLlib), EKC Panel OLS, XGBoost, Prophet |
| Serving layer | MariaDB 10.x (`bd_rvm_gtp`) |
| API | Flask (Python) |
| ETL externo | Google Earth Engine, yfinance, OECD SDMX, EIB/InvestEU |

### Pipeline local/dev — Docker

Pipeline reproducible en local para desarrollo y pruebas. Ver carpeta `Docker/`.

---

## 4. Fuentes de Datos (12 fuentes)

| # | Fuente | Variable principal | Granularidad |
|---|--------|--------------------|-------------|
| 1 | Sentinel-2 (GEE) | NDVI mensual | Ciudad × Mes |
| 2 | Sentinel-5P NO₂ (GEE) | Dióxido de nitrógeno | Ciudad × Mes |
| 3 | HRL Copernicus | Impermeabilización suelo | Ciudad × Año |
| 4 | ERA5-Land (GEE) | Temperatura / Precipitación | Ciudad × Año |
| 5 | Sentinel-5P UVAI (GEE) | Índice aerosoles UV | Ciudad × Año |
| 6 | ESA WorldCover (GEE) | Cobertura del suelo | Ciudad × Año |
| 7 | EDGAR v8 | Emisiones CO₂ nacionales | País × Año |
| 8 | Yahoo Finance | Empresas verdes cotizadas | Empresa × Mes |
| 9 | Eurostat | PIB PPS + Población FUA | Ciudad × Año |
| 10 | OECD | Política ambiental (EPS) | País × Año |
| 11 | InvestEU / EIB | Financiación verde EU | País × Año |

Cobertura: **237 ciudades europeas** (Functional Urban Areas de Eurostat), **2018–2024**.

---

## 5. Variables Clave del Dataset

**Ambientales:**
- `NDVI_Mean`, `NDVI_Slope` — vegetación media y tendencia temporal
- `NO2_Mean` — contaminación troposférica
- `Imperviousness_Mean` — sellado del suelo (%)
- `Temp_Annual_Mean_C`, `Precip_Annual_Sum_m` — clima
- `UVAI_Annual_Mean` — aerosoles
- `WC_Tree_Pct`, `WC_Built_Pct` — cobertura suelo

**Económicas:**
- `gdp_pps_per_capita`, `ln_gdp_pps`, `ln_gdp_pps_sq` — variables EKC
- `CO2_Country_kt` — emisiones nacionales
- `EPS_Index`, `GHG_Total_kt` — política ambiental
- `investeu_total_eur` — inversión verde

**Modelos ML:**
- `cluster_id`, `turning_point_phase` — clustering + clasificación XGBoost
- `ekc_turning_point_y`, `ekc_shape` — regresión EKC
- `prophet_ndvi_forecast_1y/3y/5y` — predicción Prophet
- `investment_score`, `investment_recommendation` — score de inversión

---

## 6. Reglas de Imputación y Limpieza

| Fuente | Problema | Tratamiento |
|--------|----------|-------------|
| NDVI | Píxeles nubosos | Máscara QA60 en GEE antes del cálculo |
| NDVI | Meses sin datos | Fila no generada (GEE devuelve None) |
| NO₂ | Fracción nubosa | `cloud_fraction < 0.3` aplicado en GEE |
| HRL | `nodata = 255` | Filtrado en `hrl_to_csv.py` con rasterio |
| HRL | Años sin ciclo | Imputación por vecino más cercano (2018 o 2021) |
| ERA5 | Temperatura en K | Conversión `K → °C` en GEE |
| Finance | < 15 días/mes | Mes descartado |
| OECD/EDGAR | Granularidad país | Expansión país → ciudades (ISO3 → ISO2) |
| InvestEU | Año no en URL | Fallback `extraction_date`; default 2022 |

---

## 7. Pipeline ML (orden fijo)

```
K-Means Clustering   →   EKC Panel Regression   →   XGBoost Classifier   →   Prophet Forecast
     (PySpark)             (statsmodels OLS)         (DEGRADANDO/TURNING/        (NDVI 1Y/3Y/5Y)
                                                       RECUPERANDO)
```

---

## 8. Ejecución

### Cluster Lorca

```bash
# 1. Setup del entorno (una sola vez)
chmod +x Lorca/setup_lorca.sh
cd Lorca && ./setup_lorca.sh

# 2. Pipeline ETL completo
source ~/gtp_venv/bin/activate
python Lorca/ETL/script/run_all.py

# 3. Pipeline BBDD + ML + Export a MariaDB
spark-submit Lorca/BBDD/run_pipeline.py
```

### Docker (local/dev)

```bash
docker-compose --profile etl --profile db up --build
```

---

## 9. Análisis Exploratorio

Ver **`EDA_GTP.ipynb`** para el análisis exploratorio completo de las 12 fuentes de datos,
con justificación de cada decisión de limpieza y transformación del pipeline ETL.

---

## 10. Resultado Final

- Ranking europeo de 237 ciudades por `investment_score`
- Clasificación de ciudades en fases: **DEGRADANDO / TURNING / RECUPERANDO**
- Previsión NDVI a 1, 3 y 5 años por ciudad
- API REST con 7 endpoints para dashboards en tiempo real
- Cuadro de mando Power BI / Tableau conectado a MariaDB

---

## 11. Enfoque Académico

Este proyecto integra:

- Estadística avanzada y econometría ambiental (EKC, panel OLS)
- Ingeniería de datos y arquitectura Big Data distribuida (Medallion Architecture)
- Machine Learning aplicado (clustering, clasificación, series temporales)
- Aplicación real a sostenibilidad y finanzas verdes

Desarrollado íntegramente en el contexto académico de la **Universidad Europea**,
demostrando la aplicación práctica de la ingeniería matemática a datos reales y complejos.
