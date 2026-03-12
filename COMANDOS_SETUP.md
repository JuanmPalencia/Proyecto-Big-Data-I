# GTP — Comandos de Setup en Lorca

**Orden de ejecución:** seguir las secciones de arriba a abajo.
**Directorio de trabajo asumido:** `/home/223B3336juan/Big Data I/Proyecto-Big-Data-I/`

---

## 0. Pre-requisitos del sistema (verificar antes de empezar)

```bash
# Verificar que Spark está disponible
spark-submit --version

# Verificar que HDFS está activo
hdfs dfs -ls /

# Verificar que Hive está activo
beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"

# Verificar que Python 3.x está disponible
python --version
python3 --version
```

---

## 1. Clonar / actualizar el repositorio

```bash
# Si es la primera vez
git clone https://github.com/<tu-repo>/Proyecto-Big-Data-I.git
cd "Big Data I/Proyecto-Big-Data-I"

# Si ya existe, actualizar
cd "Big Data I/Proyecto-Big-Data-I"
git pull origin main
```

---

## 2. Instalar dependencias Python

```bash
# Instalar todas las dependencias del ETL y modelos
pip install --user \
    yfinance \
    pandas \
    numpy \
    requests \
    earthengine-api \
    rasterio \
    geopandas \
    linearmodels \
    statsmodels \
    xgboost \
    scikit-learn \
    prophet \
    flask \
    flask-cors \
    psycopg2-binary \
    SQLAlchemy \
    pyarrow \
    joblib \
    pyyaml

# Verificar instalaciones críticas
python -c "import xgboost; print('xgboost', xgboost.__version__)"
python -c "import prophet; print('prophet OK')"
python -c "from linearmodels import PanelOLS; print('linearmodels OK')"
python -c "import rasterio; print('rasterio', rasterio.__version__)"
python -c "import ee; print('earthengine-api OK')"
```

---

## 3. Autenticar Google Earth Engine (GEE)

> Necesario para Sentinel-2 (NDVI) y Sentinel-5P (NO2). Sin esto el ETL falla.

```bash
# Iniciar autenticación (abre enlace en el navegador)
earthengine authenticate

# Verificar que la autenticación fue exitosa
python -c "import ee; ee.Initialize(); print('GEE autenticado correctamente')"

# Si el entorno de Lorca no tiene navegador, usar modo no-interactivo:
earthengine authenticate --quiet
# Pegar el token de verificación cuando se solicite
```

---

## 4. Crear directorios en HDFS

```bash
# Directorios principales del Data Lakehouse
hdfs dfs -mkdir -p /user/gtp/bronze
hdfs dfs -mkdir -p /user/gtp/silver
hdfs dfs -mkdir -p /user/gtp/gold
hdfs dfs -mkdir -p /user/gtp/models/clustering
hdfs dfs -mkdir -p /user/gtp/models/xgboost
hdfs dfs -mkdir -p /user/gtp/models/prophet
hdfs dfs -mkdir -p /user/gtp/models/ekc

# Verificar que se crearon correctamente
hdfs dfs -ls /user/gtp/
```

---

## 5. Ejecutar DDL de Hive (crear tablas y bases de datos)

### 5.1 Capa Bronze (6 tablas)

```bash
beeline -u "jdbc:hive2://localhost:10000" \
  -f "Lorca/BBDD/schemas/bronze_ddl.sql"

# Verificar tablas creadas
beeline -u "jdbc:hive2://localhost:10000" \
  -e "USE gtp_bronze; SHOW TABLES;"
```

### 5.2 Capa Silver (4 dims + 3 facts)

```bash
beeline -u "jdbc:hive2://localhost:10000" \
  -f "Lorca/BBDD/schemas/silver_ddl.sql"

# Verificar
beeline -u "jdbc:hive2://localhost:10000" \
  -e "USE gtp_silver; SHOW TABLES;"
```

### 5.3 Capa Gold (4 tablas analíticas)

```bash
beeline -u "jdbc:hive2://localhost:10000" \
  -f "Lorca/BBDD/schemas/gold_ddl.sql"

# Verificar
beeline -u "jdbc:hive2://localhost:10000" \
  -e "USE gtp_gold; SHOW TABLES;"
```

> **Alternativa:** Si beeline falla, ejecutar desde PySpark:
> ```python
> spark.sql(open("Lorca/BBDD/schemas/bronze_ddl.sql").read())
> ```

---

## 6. Crear schema en PostgreSQL (serving layer)

```bash
# Configurar variables de entorno con las credenciales
export PG_HOST="localhost"
export PG_PORT="5432"
export PG_DB="gtp_db"
export PG_USER="gtp_user"
export PG_PASSWORD="tu_password_aqui"

# Crear la base de datos si no existe (como superuser)
psql -h $PG_HOST -U postgres -c "CREATE DATABASE $PG_DB;"
psql -h $PG_HOST -U postgres -c "CREATE USER $PG_USER WITH PASSWORD '$PG_PASSWORD';"
psql -h $PG_HOST -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE $PG_DB TO $PG_USER;"

# Ejecutar el DDL del schema gtp (tablas + vistas)
psql -h $PG_HOST -U $PG_USER -d $PG_DB \
  -f "Lorca/BBDD/schemas/postgres_serving_ddl.sql"

# Verificar
psql -h $PG_HOST -U $PG_USER -d $PG_DB \
  -c "\dt gtp.*"
```

> **Nota:** Si PostgreSQL no está disponible en Lorca, omitir esta sección y ejecutar `run_pipeline.py` con `--skip-export`.

---

## 7. Verificar estructura de directorios locales

```bash
# El ETL necesita estos directorios para guardar los CSVs procesados
ls "Lorca/ETL/script/data/DatosProcesados/"

# Si no existen, crearlos
mkdir -p "Lorca/ETL/script/data/DatosProcesados"
mkdir -p "Lorca/ETL/script/data/raw/oecd"
mkdir -p "Lorca/ETL/script/data/raw/hrl"
```

---

## 8. Ejecutar el pipeline completo

### 8.1 Pipeline completo (ETL + BBDD + ML + Export)

```bash
cd "Lorca/ETL/script"

# Ejecución completa desde cero
python run_all.py

# Ver qué haría sin ejecutar nada (dry-run)
python run_all.py --dry-run
```

### 8.2 Solo ETL (sin Spark/HDFS)

```bash
# Útil para probar solo la descarga de datos
python run_all.py --skip-bbdd
```

### 8.3 Solo BBDD + ML (datos ya descargados)

```bash
# Útil cuando los CSVs ya están en disco y solo falta procesar en Spark
python run_all.py --only-bbdd
```

### 8.4 Solo reentrenar modelos ML (sin re-ingestar)

```bash
# Los datos ya están en Bronze/Silver/Gold — solo re-ejecutar los 4 modelos
python run_all.py --only-bbdd --bbdd-args "--only-models"
```

### 8.5 Control granular con spark-submit (por fase)

```bash
# Desde Lorca/BBDD/
cd "Lorca/BBDD"

# Fase 1: Bronze
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  bronze_ingest.py --source all --mode overwrite

# Fase 2: Silver
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  silver_transform.py

# Fase 3: Gold base
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  gold_build.py

# Fase 4: Clustering
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  models/clustering.py --k-min 2 --k-max 8

# Fase 5: EKC Regression
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  models/ekc_regression.py

# Fase 6: XGBoost Classifier
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  models/xgboost_classifier.py

# Fase 7: Prophet Forecast
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  models/prophet_forecast.py

# Fase 8: Export a PostgreSQL
spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g --executor-memory 4g \
  export_to_postgres.py --mode spark
```

---

## 9. Verificar resultados en HDFS

```bash
# Ver particiones Bronze
hdfs dfs -ls /user/gtp/bronze/sentinel2_raw/
hdfs dfs -ls /user/gtp/bronze/finance_raw/

# Ver tablas Silver
hdfs dfs -ls /user/gtp/silver/

# Ver Gold con resultados ML
hdfs dfs -ls /user/gtp/gold/

# Contar registros de una partición específica (requiere Spark)
spark-shell --master yarn << 'EOF'
val fk = spark.read.parquet("hdfs:///user/gtp/gold/fact_kuznets")
println(s"fact_kuznets: ${fk.count()} filas")
fk.groupBy("turning_point_phase").count().show()
EOF
```

---

## 10. Verificar resultados en Hive

```bash
beeline -u "jdbc:hive2://localhost:10000" << 'EOF'
-- Contar registros por tabla
SELECT COUNT(*) FROM gtp_bronze.bronze_sentinel2_raw;
SELECT COUNT(*) FROM gtp_silver.fact_environmental;
SELECT COUNT(*) FROM gtp_gold.fact_kuznets;

-- Ver distribución de fases por año
SELECT year, turning_point_phase, COUNT(*) as n_cities
FROM gtp_gold.fact_kuznets
GROUP BY year, turning_point_phase
ORDER BY year, turning_point_phase;

-- Top 10 ciudades por investment_score (último año disponible)
SELECT city_code, city_name, country_code,
       investment_score, investment_recommendation,
       turning_point_phase
FROM gtp_gold.city_ranking
WHERE year = (SELECT MAX(year) FROM gtp_gold.city_ranking)
ORDER BY rank_position
LIMIT 10;
EOF
```

---

## 11. Levantar la API Flask

```bash
# Configurar variables de entorno PostgreSQL (necesario para los endpoints de datos)
export PG_HOST="localhost"
export PG_PORT="5432"
export PG_DB="gtp_db"
export PG_USER="gtp_user"
export PG_PASSWORD="tu_password_aqui"

# Arrancar la API
cd "Lorca/Web"
python app.py

# Verificar que responde
curl http://localhost:5000/api/summary
curl http://localhost:5000/api/years
curl "http://localhost:5000/api/ranking?year=2023&limit=10"
curl "http://localhost:5000/api/city/Madrid_ES"
curl "http://localhost:5000/api/opportunities?min_score=60"
```

---

## 12. Configurar actualizaciones automáticas (cron)

```bash
# Abrir el crontab del usuario
crontab -e
```

Añadir las siguientes líneas:

```cron
# GTP — actualización mensual de satélite (NDVI + NO2) + re-entrenamiento parcial
# Primer día de cada mes a las 06:00
0 6 1 * * cd "/home/223B3336juan/Big Data I/Proyecto-Big-Data-I/Lorca/ETL/script" && \
  python run_all.py --only-bbdd --bbdd-args "--only-models" \
  >> /home/223B3336juan/logs/gtp_monthly.log 2>&1

# GTP — actualización anual completa (todos los datos + pipeline completo)
# Día 2 de enero a las 09:00
0 9 2 1 * cd "/home/223B3336juan/Big Data I/Proyecto-Big-Data-I/Lorca/ETL/script" && \
  python run_all.py \
  >> /home/223B3336juan/logs/gtp_annual.log 2>&1
```

```bash
# Crear directorio de logs si no existe
mkdir -p /home/223B3336juan/logs
```

---

## 13. Diagnóstico de errores comunes

### Error: GEE no autenticado
```bash
# Síntoma: "Please authorize Earth Engine before use"
earthengine authenticate
python -c "import ee; ee.Initialize()"
```

### Error: HDFS directory not found
```bash
# Síntoma: "No such file or directory: hdfs:///user/gtp/..."
hdfs dfs -mkdir -p /user/gtp/bronze /user/gtp/silver /user/gtp/gold
```

### Error: Hive table not found
```bash
# Síntoma: "Table or view not found: gtp_bronze.bronze_sentinel2_raw"
beeline -u "jdbc:hive2://localhost:10000" -f "Lorca/BBDD/schemas/bronze_ddl.sql"
# Luego registrar particiones manualmente:
beeline -u "jdbc:hive2://localhost:10000" \
  -e "MSCK REPAIR TABLE gtp_bronze.bronze_sentinel2_raw;"
```

### Error: rasterio no disponible
```bash
# Síntoma: "ImportError: No module named 'rasterio'"
pip install --user rasterio
# Si falla por GDAL:
pip install --user GDAL==$(gdal-config --version)
pip install --user rasterio
```

### Error: linearmodels no disponible
```bash
# Síntoma: "ImportError: No module named 'linearmodels'"
pip install --user linearmodels statsmodels
```

### Error: Prophet installation
```bash
# En algunos entornos prophet necesita pystan primero
pip install --user pystan==2.19.1.1
pip install --user prophet
```

### Error: PostgreSQL connection refused
```bash
# Verificar que PostgreSQL está corriendo
pg_isready -h $PG_HOST -p $PG_PORT
# Si no está corriendo:
sudo service postgresql start
# O ejecutar sin export a PostgreSQL:
python run_all.py --only-bbdd --bbdd-args "--skip-export"
```

### Error: spark-submit: command not found
```bash
# Añadir Spark al PATH
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
# Añadir a ~/.bashrc para que persista
echo 'export SPARK_HOME=/usr/local/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

### Error: Small files en HDFS (muchas particiones pequeñas)
```bash
# Si hay demasiadas particiones pequeñas, compactar:
spark-shell --master yarn << 'EOF'
spark.read.parquet("hdfs:///user/gtp/silver/fact_environmental")
  .coalesce(10)
  .write.mode("overwrite")
  .partitionBy("year", "country")
  .parquet("hdfs:///user/gtp/silver/fact_environmental")
EOF
```

---

## 14. Frecuencias de actualización por fuente

| Fuente | Granularidad | Frecuencia actualización | Script |
|--------|-------------|--------------------------|--------|
| Sentinel-2 (NDVI) | Mensual | Mensual | `Sentinel-2_extract.py` |
| Sentinel-5P (NO2) | Mensual | Mensual | `Sentinel-5p_extract.py` |
| HRL (Impermeabilización) | Trienal (2018, 2021...) | Cada 3 años | `HRL_extract.py` + `hrl_to_csv.py` |
| YFinance (Financiero) | **Mensual** | Mensual | `YFinance_extract.py` |
| Eurostat (GDP) | Anual | Anual | `eurostat_extract.py` |
| OECD (Ambiental) | Anual | Anual | `OECD_extract.py` |
| INE (PIB España) | Anual | Anual | `pib_ine_extract.py` |
| InvestEU (EIB) | Ad hoc | Anual | `InvestEU_extract.py` |

---

## 15. Resumen de lo que falta por hacer (fuera del código)

| Tarea | Responsable | Estado |
|-------|-------------|--------|
| Autenticar GEE en Lorca (`earthengine authenticate`) | Equipo | ⏳ Pendiente |
| Instalar dependencias Python en Lorca (`pip install ...`) | Equipo | ⏳ Pendiente |
| Crear directorios HDFS (`hdfs dfs -mkdir -p ...`) | Equipo | ⏳ Pendiente |
| Ejecutar DDL Hive (bronze + silver + gold) | Equipo | ⏳ Pendiente |
| Crear schema `gtp` en PostgreSQL real | Equipo | ⏳ Pendiente |
| Configurar variables de entorno PostgreSQL | Equipo | ⏳ Pendiente |
| Ejecutar pipeline completo en Lorca (prueba real) | Equipo | ⏳ Pendiente |
| Verificar resultados en Hive y PostgreSQL | Equipo | ⏳ Pendiente |
| Configurar cron jobs | Equipo | ⏳ Pendiente |
| Actualizar `GTP_DOCUMENTACION_TECNICA.md` | Claude | ✅ Completado |
| Crear mockup/frontend de la web | Equipo | ⏳ Pendiente |
