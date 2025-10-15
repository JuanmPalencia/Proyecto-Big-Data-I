# Ejercicio Docker – Pipeline ETL con Python y MariaDB

## Descripción
Este proyecto implementa un pipeline ETL (Extract, Transform, Load) que obtiene información meteorológica de la API pública Open-Meteo, la transforma y la almacena en una base de datos relacional MariaDB, utilizando contenedores Docker para garantizar la portabilidad y la reproducibilidad del entorno.
--- 

## Estructura del proyecto
tarea_docker/
│
├── db/
│   ├── Dockerfile
│   └── init.sql
│
├── script/
│   ├── Dockerfile
│   ├── script.py
│   ├── requirements.txt
│   └── .env
│
├── docker-compose.yml
└── README.md


## Contenido principal

- db/Dockerfile: construye la imagen de la base de datos MariaDB.
- db/init.sql: crea la base de datos weather y la tabla info_meteorologica.
- script/Dockerfile: define la imagen de Python para ejecutar el script ETL.
- script/script.py: extrae datos de la API, los transforma y los inserta en la base de datos.
- script/.env: variables de entorno para la conexión con la base de datos.
- docker-compose.yml: orquesta los servicios de base de datos y Python.


## Ejecución del proyecto
1. **Moverse a la ubicacion del proyecto:**
    cd "C:\UbicacionDeRutaDeArchivo"

2. **Construcción de contenedores:**
    docker compose up -d db 

3. **Ejecución del flujo ETL:**
    docker compose run --rm script


4. **Verificación de datos:**
    docker exec -it proyecto_db mariadb -uappuser -proot weather -e "SELECT date, tmax, tmin, precipitation_sum, weather_code FROM info_meteorologica ORDER BY date DESC;"
    
---
## Datos almacenados
La tabla info_meteorologica contiene los siguientes campos:
| Campo             | Descripción                  |
| ----------------- | ---------------------------- |
| date              | Fecha del registro           |
| tmax              | Temperatura máxima (°C)      |
| tmin              | Temperatura mínima (°C)      |
| precipitation_sum | Precipitación acumulada (mm) |
| weather_code      | Código del estado del tiempo |


## Notas

- El sistema evita duplicados mediante ON DUPLICATE KEY UPDATE.

- Los volúmenes garantizan la persistencia de los datos.

- La API de Open-Meteo no requiere autenticación y proporciona datos diarios para Villaviciosa de Odón, Madrid.