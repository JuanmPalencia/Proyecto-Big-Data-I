FROM python:3.11-slim-bookworm

# Java 17 headless — requerido por PySpark (Spark corre en JVM)
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jdk-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME para PySpark (OpenJDK 17 en Debian Bookworm amd64)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Instalar dependencias Python primero (capa cacheada)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código del proyecto
COPY ETL  /app/ETL
COPY BBDD /app/BBDD
COPY Web  /app/Web

# Config Hadoop/Hive que PySpark lee via HADOOP_CONF_DIR / HIVE_CONF_DIR
COPY docker/conf /app/conf

# Directorios de datos y logs
RUN mkdir -p \
        /app/data/DatosProcesados \
        /app/data/raw/oecd \
        /app/data/raw/investeu \
        /app/data/finance_data \
        /app/logs \
        /app/exports

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# PySpark usa estos para encontrar core-site.xml y hive-site.xml
ENV HADOOP_CONF_DIR=/app/conf
ENV HIVE_CONF_DIR=/app/conf

# Por defecto arranca el ETL; override con 'command:' en docker-compose
CMD ["python", "ETL/script/run_all.py"]
