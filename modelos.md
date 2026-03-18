# Modelos de Machine Learning — Green Turning Point (GTP)

## Contexto del proyecto

El proyecto GTP (Green Turning Point) tiene como objetivo identificar en qué momento del desarrollo económico las ciudades europeas alcanzan el **punto de inflexión de la Curva de Kuznets Ambiental (EKC)**: el umbral de renta per cápita a partir del cual el crecimiento económico deja de degradar el medio ambiente y empieza a mejorarlo.

Para ello se han diseñado cuatro modelos que se ejecutan en secuencia sobre un pipeline distribuido (Apache Spark + HDFS), trabajando con datos de 237 ciudades europeas entre 2017 y 2024, obtenidos de fuentes satelitales (Sentinel-2, Sentinel-5P), Eurostat y Copernicus.

---

## Flujo del pipeline de modelos

```
Datos Silver (Star Schema Kimball — HDFS)
        │
        ▼
 1. K-Means Clustering         → Segmentación de ciudades
        │
        ▼
 2. EKC Panel Regression       → Estimación del turning point teórico (Y*)
        │
        ▼
 3. XGBoost Classifier         → Clasificación de la fase actual de cada ciudad
        │
        ▼
 4. Prophet Forecast           → Proyección temporal del NDVI (1Y / 3Y / 5Y)
        │
        ▼
Capa Gold (tablas analíticas — HDFS → MariaDB → Dashboard)
```

El orden es estrictamente secuencial: cada modelo consume outputs del anterior.

---

## Modelo 1 — K-Means Clustering

### Motivación

Las 237 ciudades del dataset abarcan desde Oslo hasta Bucarest, con perfiles económicos y ambientales radicalmente distintos. Ajustar una única regresión EKC sobre todas ellas mezclaría dinámicas incompatibles, produciendo coeficientes sin interpretación estadística válida. La segmentación previa garantiza que cada grupo de ciudades comparte un patrón estructural similar antes de aplicar la regresión.

### Algoritmo

- **Método:** K-Means (PySpark MLlib), escalable al volumen completo del dataset.
- **Features de entrada** (promedio histórico por ciudad):
  - `ndvi_mean_city` — verdor urbano medio histórico
  - `ndvi_slope_city` — tendencia temporal del NDVI
  - `no2_mean_city` — contaminación por dióxido de nitrógeno
  - `imperviousness_city` — porcentaje de suelo impermeabilizado
  - `ln_gdp_mean_city` — logaritmo del PIB per cápita medio
  - `gdp_growth_mean_city` — tasa de crecimiento económico media
- **Selección de K:** automática por índice Silhouette (rango K = 2 a 8). Se elige el K que maximiza la cohesión intra-cluster y la separación inter-cluster.
- **Normalización:** StandardScaler (media 0, varianza 1) aplicado antes del ajuste.

### Etiquetas interpretables (post-hoc)

Los centroides se analizan tras el ajuste para asignar etiquetas semánticas:

| Etiqueta | Perfil del centroide |
|---|---|
| High-Income-Greening | PIB alto + NDVI creciente |
| High-Income-Stagnant | PIB alto + NDVI estable + NO₂ alto |
| High-Income-Stable | PIB alto + NDVI estable + NO₂ bajo |
| Industrializing-Polluted | PIB bajo + NO₂ alto |
| Emerging-Improving | PIB bajo + NDVI creciente |
| Developing-Mixed | resto |

### Output

Cada ciudad recibe `cluster_id`, `cluster_label` y `cluster_silhouette` en la capa Gold.

---

## Modelo 2 — EKC Panel Regression

### Motivación

La hipótesis EKC (Environmental Kuznets Curve) postula que la relación entre renta per cápita y degradación ambiental sigue una U invertida: en fases iniciales del desarrollo, el crecimiento económico empeora el medioambiente; superado un umbral de renta (Y*), la tendencia se invierte. Este modelo estima si dicha hipótesis se sostiene empíricamente para cada grupo de ciudades y calcula el valor concreto de Y*.

### Ecuación del modelo

$$\ln(E_{it}) = \alpha + \beta_1 \cdot \ln(Y_{it}) + \beta_2 \cdot [\ln(Y_{it})]^2 + \mu_i + \lambda_t + \varepsilon_{it}$$

Donde:
- $E_{it}$ = indicador ambiental de la ciudad $i$ en el año $t$ (NDVI o NO₂)
- $Y_{it}$ = PIB per cápita PPS de la ciudad $i$ en el año $t$
- $\mu_i$ = efectos fijos de ciudad (heterogeneidad no observada constante)
- $\lambda_t$ = efectos fijos temporales (shocks comunes a todas las ciudades)
- $\varepsilon_{it}$ = término de error

### Condición EKC

La hipótesis se confirma si $\beta_1 > 0$ y $\beta_2 < 0$, produciendo una curva en U invertida.

### Turning Point

$$Y^* = \exp\left(\frac{-\beta_1}{2\beta_2}\right)$$

$Y^*$ es el nivel de PIB per cápita al que la degradación ambiental alcanza su máximo y comienza a revertirse.

### Implementación

- **Librería:** `linearmodels` (Python) — regresión de panel con efectos fijos de dos vías.
- **Estimación:** Within estimator (demeaning por ciudad y año).
- **Datos de entrada:** ~50 ciudades × 8 años ≈ 400 observaciones por cluster.
- **Se ejecuta por cluster**, no sobre el total de ciudades.

### Output

Coeficientes $\beta_1$, $\beta_2$, $R^2$, p-valores y el turning point $Y^*$ por cluster, almacenados en `ekc_parameters` y añadidos a `fact_kuznets`.

---

## Modelo 3 — XGBoost Classifier

### Motivación

La regresión EKC proporciona un $Y^*$ teórico, pero no indica en qué fase se encuentra cada ciudad en este momento. Además, si la curva EKC no se cumple para una ciudad concreta (por políticas atípicas, shocks externos, etc.), la regresión lo ignoraría. XGBoost actúa como verificación empírica: clasifica directamente la fase observada sin asumir ninguna forma funcional de la curva.

### Clases del clasificador

| Fase | Criterio |
|---|---|
| **DEGRADANDO** | NDVI bajando y PIB lejos del $Y^*$ del cluster |
| **TURNING** | NDVI estabilizándose y PIB dentro del ±15% de $Y^*$ |
| **RECUPERANDO** | NDVI con pendiente positiva y PIB superó $Y^*$ |

Las etiquetas se generan automáticamente combinando `ndvi_yoy_change`, `ndvi_trend_slope` y `gdp_gap_to_turning` (distancia relativa al $Y^*$ del cluster).

### Features del modelo

```
ndvi_mean, ndvi_trend_slope, ndvi_yoy_change,
no2_mean, imperviousness_mean,
ln_gdp_pps, ln_gdp_pps_sq, gdp_growth_rate,
cluster_id, green_index, gdp_gap_to_turning, year
```

### Configuración del modelo

- `n_estimators = 200`, `max_depth = 5`, `learning_rate = 0.05`
- Regularización: `gamma = 1`, `reg_alpha = 0.1`, `reg_lambda = 1.0`
- Balanceo de clases: pesos inversamente proporcionales a la frecuencia (TURNING es minoritaria)
- **Split temporal:** años ≤ (max_year − 2) para entrenamiento, años restantes para test. Se evita el random split para no introducir data leakage temporal.
- Early stopping: 20 rondas sin mejora en `mlogloss`.

### Output

`turning_point_phase`, `phase_confidence`, probabilidades por clase (`xgb_prob_degradando`, etc.) e `investment_score` (métrica compuesta 0–100 que combina la fase, el green_index y la confianza del modelo) en `fact_kuznets`.

---

## Modelo 4 — Prophet Forecast

### Motivación

Los tres modelos anteriores analizan el estado actual e histórico de las ciudades. Prophet añade la dimensión prospectiva: proyecta la trayectoria futura del NDVI y estima en qué año cada ciudad alcanzará su turning point si mantiene la tendencia observada. Esto convierte el análisis descriptivo en una herramienta de apoyo a decisiones de inversión sostenible.

### Algoritmo

- **Modelo base:** Facebook Prophet — series temporales aditivas con tendencia, estacionalidad y efectos de festivos/anomalías.
- **Serie de entrada:** NDVI mensual por ciudad (Bronze Sentinel-2), al menos 24 meses de historia.
- **Horizontes de forecast:** 12, 36 y 60 meses (1, 3 y 5 años).
- **Intervalos de confianza:** 95%.
- **Paralelización:** pandas UDF sobre Spark, ajustando los 237 modelos de forma distribuida.

### Estimación del turning year

Una vez obtenida la proyección de NDVI, se compara con el $Y^*$ del cluster (convertido a NDVI esperado vía la relación EKC inversa). El primer año en que el NDVI proyectado cruza ese umbral se almacena como `prophet_turning_year`.

### Output

Columnas de forecast en `fact_kuznets` y en `model_results`:
- `ndvi_forecast_1y`, `ndvi_forecast_3y`, `ndvi_forecast_5y`
- `ndvi_forecast_lower_95`, `ndvi_forecast_upper_95`
- `prophet_turning_year`

---

## Relación entre modelos y por qué el orden es obligatorio

| Modelo | Necesita obligatoriamente |
|---|---|
| K-Means | Solo Silver (datos históricos por ciudad) |
| EKC Regression | `cluster_id` asignado por K-Means |
| XGBoost | `ekc_turning_point_y` (Y* por cluster) calculado por EKC |
| Prophet | Serie mensual Bronze Sentinel-2 |

XGBoost usa `gdp_gap_to_turning` como feature — la distancia del PIB actual al $Y^*$. Si EKC no se ha ejecutado antes, esa columna no existe y el clasificador no puede entrenarse.

---

## Stack tecnológico

| Componente | Tecnología |
|---|---|
| Orquestación | `run_pipeline.py` (PySpark + subprocess) |
| Clustering | PySpark MLlib (`KMeans`, `StandardScaler`) |
| Regresión EKC | `linearmodels` (Python, pandas, via `.toPandas()`) |
| Clasificador | `xgboost` + `scikit-learn` (Python, via `.toPandas()`) |
| Forecast | `prophet` (pandas UDF sobre Spark) |
| Almacenamiento | HDFS Parquet particionado por `year/country` |
| Serving layer | MariaDB `bd_rvm_gtp` → Flask API → Dashboard |

---

## Dato de escala

- **237 ciudades** europeas (Functional Urban Areas, Eurostat)
- **8 años** de datos (2017–2024)
- **~12 fuentes** de datos integradas (satélite, API, web scraping)
- **~1.900 observaciones** ciudad-año en el dataset panel
