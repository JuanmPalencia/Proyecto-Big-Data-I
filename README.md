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

**Green Turning Point (GTP)** es una plataforma de análisis basada en **Big Data, imágenes satelitales y modelos econométricos** cuyo objetivo es identificar **ciudades europeas que se encuentran en el punto de inflexión ambiental**.

El proyecto transforma la **Curva de Kuznets Ambiental** en una **herramienta práctica de inversión**, detectando aquellas ciudades donde:

- El crecimiento económico deja de aumentar la contaminación.
- Comienza una fase de regeneración ambiental.
- Existe alto potencial de rentabilidad con bajo impacto ambiental.

GTP está orientado a:

- Fondos de inversión verde.
- Instituciones públicas.
- Planificadores urbanos.
- Empresas interesadas en sostenibilidad y transición energética.

---

## 2. Arquitectura General del Proyecto

El repositorio se divide en **dos grandes bloques independientes pero complementarios**:

### A) Pipeline Analítico (LORCA)

- Basado en **Spark + Parquet**.
- Optimizado para procesamiento masivo.
- No orientado directamente a herramientas BI.
- Incluye un script `exports.py` que transforma los datos finales a **CSV** para Power BI, Tableau u otros sistemas.

### B) Pipeline Productivo (Docker + SQL)

- Pensado para despliegue local reproducible.
- Uso de **Docker Compose**.
- Base de datos SQL local.
- Preparado para servir datos estructurados a aplicaciones externas.

Ambos pipelines comparten la lógica de negocio y los datos base, pero **pueden ejecutarse de forma independiente**.

---

## 3. Fuentes de Datos

- **Eurostat:** PIB, pobreza, productividad, empleo, energías renovables, emisiones.
- **OECD:** Indicadores ambientales y económicos.
- **Copernicus Sentinel-2:** NDVI (vegetación).
- **Copernicus Sentinel-5P:** NO₂ (contaminación).
- **HRL:** Imperviousness (suelo sellado).
- **Yahoo Finance:** Datos financieros de empresas energéticas europeas.
- **INE:** Series macroeconómicas españolas.
- **InvestEU:** Inversión pública europea.

---

## 4. Variables Clave

- NDVI (media, mediana, percentiles, Gini).
- Contaminación NO₂ (media, máximo, mínimo, desviación).
- Suelo sellado (Imperviousness).
- PIB per cápita.
- Indicadores financieros sectoriales.
- Pendiente NDVI (detección de Turning Point).

---

## 5. Reglas de Imputación

- **NO₂ faltante:** media por ciudad y año.
- **Imperviousness faltante:** valor base de 2018 por ciudad.
- **Datos financieros faltantes:** uso de países proxy (ej. Bélgica = media Francia + Países Bajos).

---

## 6. Ejecución

### Pipeline LORCA

```bash
python run_all.py
python exports.py
```

### Pipeline Docker

```bash
docker-compose --profile etl --profile db up --build
```

---

## 7. Resultado Final

- Dataset maestro a nivel ciudad-año.
- Ranking europeo de ciudades en Turning Point.
- Base sólida para decisiones de inversión sostenible.

---

## 8. Enfoque Académico

Este proyecto integra:

- Estadística avanzada.
- Econometría ambiental.
- Ingeniería de datos.
- Big Data distribuido.
- Aplicación real a sostenibilidad.

Desarrollado íntegramente en el contexto académico de la **Universidad Europea**, demostrando una aplicación práctica de la ingeniería matemática al análisis de datos reales y complejos.
