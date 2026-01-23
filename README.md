# Proyecto Big Data I

## Información académica

**Universidad:** Universidad Europea
**Grado:** Ingeniería Matemática Aplicada al Análisis de Datos
**Curso:** 3.º
**Asignatura:** Big Data I

### Autores

- Juan Manuel Palencia Osorio
- Pablo Mata Rius
- Pablo Sánchez Ruiz
- María Paula Aguirre Palacio

---

## Descripción general

Este proyecto implementa un **pipeline completo de Big Data** orientado al análisis socioeconómico, ambiental y financiero en el contexto europeo.La arquitectura sigue un enfoque profesional y modular, separando claramente:

- Procesamiento Big Data (Spark / LORCA)
- Persistencia y explotación relacional (Docker / SQL)

Ambos entornos funcionan como **entidades independientes**, permitiendo flexibilidad, escalabilidad y reproducibilidad.

---

## Arquitectura general

```
LORCA (Spark, Parquet)
        |
        | exports.py (opcional)
        v
       CSV
        |
        v
Docker (MySQL) -> Power BI
```

---

## 1. Entorno LORCA — Big Data (Spark)

### Objetivo

Procesar grandes volúmenes de datos heterogéneos, integrarlos y generar datasets listos para análisis y modelado.

### Tecnologías

- Apache Spark (PySpark)
- Python
- Parquet
- Entorno distribuido LORCA

### Estructura principal

```
Proyecto-Big-Data-I/
├── ETL/
│   ├── run_all.py
│   ├── exports.py
│   ├── config.py
│   └── scripts/
├── data/
│   ├── raw/
│   ├── processed/
│   └── model_ready/
```

### Fuentes de datos integradas

- Eurostat
- OECD
- InvestEU
- INE (PIB trimestral)
- Datos financieros (YFinance)
- Kuznets Curve
- Datos ambientales (NDVI, NO2, HRL)

### Formato de salida

- Parquet
- Optimizado para Big Data
- No consumible directamente por Power BI

---

## Exportación a CSV (LORCA)

Debido a que Power BI y herramientas SQL no consumen Parquet de forma nativa, se incluye el script:

```
ETL/exports.py
```

Este script permite:

- Convertir datasets Parquet a CSV
- Facilitar su uso en SQL, Power BI o Docker

> La exportación a CSV **solo se realiza en LORCA**.

---

## 2. Entorno Docker — SQL y Visualización

### Objetivo

Ofrecer una implementación relacional y portable del proyecto, orientada a consultas SQL y visualización.

### Tecnologías

- Docker
- Docker Compose
- MySQL 8
- Adminer
- Power BI

### Estructura

```
Proyecto-Big-Data-I/
├── docker-compose.yml
├── Dockerfile
├── .env
├── data/
│   └── mysql/
└── logs/
```

### Ejecución

```
docker-compose --profile db up -d
```

### Accesos

- Adminer: http://localhost:8080
- MySQL: localhost:3307

---

## Relación entre LORCA y Docker

### Principio clave

LORCA y Docker son **entidades independientes**.

### Flujo recomendado

```
LORCA (Spark)
→ Parquet
→ exports.py
→ CSV
→ Docker (MySQL)
→ Power BI
```

### Escenarios válidos

- Ejecutar solo LORCA
- Ejecutar solo Docker
- Ejecutar ambos conjuntamente

---

## Justificación técnica y académica

| Entorno | Función                | Formato   |
| ------- | ----------------------- | --------- |
| LORCA   | Big Data, ETL, modelado | Parquet   |
| Docker  | SQL, BI, explotación   | CSV / SQL |

Esta separación:

- Refleja arquitecturas reales de Big Data
- Mejora escalabilidad y mantenibilidad
- Facilita análisis avanzado y visualización
- Permite evaluación modular del proyecto

---

## Resumen para defensa

El proyecto se estructura en dos entornos independientes.
LORCA se encarga del procesamiento Big Data mediante Apache Spark, generando datasets optimizados en Parquet.
Docker proporciona una implementación basada en MySQL orientada a explotación relacional y visualización.
Ambos entornos pueden ejecutarse de forma autónoma, garantizando flexibilidad, escalabilidad y reproducibilidad.

---

## Estado del proyecto

- ETL completo
- Integración de múltiples fuentes
- Arquitectura Big Data + SQL
- Exportación para BI
- Docker reproducible
