import os, time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import pymysql
import requests_cache
from retry_requests import retry
import openmeteo_requests

# Configuracion de la DB y localizacion
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "weather")
DB_USER = os.getenv("DB_USER", "appuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "app_password")

LAT = 40.3581
LON = -3.9043
TZ = "Europe/Madrid"

# Rango de fechas
hoy_local = datetime.now(ZoneInfo(TZ)).date()
fechaInicio = hoy_local - timedelta(days=7)   # hace 7 días
fechaFin   = hoy_local - timedelta(days=1)   # ayer

# Esperar a que la DB este disponible
t0 = time.time()
while True:
    try:
        conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, autocommit=True)
        conn.close()
        break
    except Exception:
        if time.time() - t0 > 90:
            raise
        time.sleep(3)

# Creacion de la DB y tabla en caso de que no existan
with pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, autocommit=True) as c, c.cursor() as cur:
    cur.execute("""CREATE DATABASE IF NOT EXISTS weather CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;""")

with pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=True) as c, c.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS info_meteorologica (
          date DATE PRIMARY KEY,
          tmax DOUBLE,
          tmin DOUBLE,
          precipitation_sum DOUBLE,
          weather_code INT
        );
    """)

#Llamada de la api
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
client = openmeteo_requests.Client(session=retry_session)

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": LAT,
    "longitude": LON,
    "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "rain_sum"],
    "start_date": fechaInicio.isoformat(),
    "end_date": fechaFin.isoformat(),
    "timezone": TZ  
}
response = client.weather_api(url, params=params)[0]

daily = response.Daily()
wc   = daily.Variables(0).ValuesAsNumpy()
tmax = daily.Variables(1).ValuesAsNumpy()
tmin = daily.Variables(2).ValuesAsNumpy()
rain = daily.Variables(3).ValuesAsNumpy()

dates = pd.date_range(start=fechaInicio, end=fechaFin, freq="D")
df = pd.DataFrame({
    "date": dates.date,
    "tmax": pd.to_numeric(tmax, errors="coerce"),
    "tmin": pd.to_numeric(tmin, errors="coerce"),
    "precipitation_sum": pd.to_numeric(rain, errors="coerce"),
    "weather_code": pd.Series(wc).astype("float").astype("Int64"),
})


print("[ETL] Fechas recopiladas:", ", ".join([d.isoformat() for d in df["date"]]))

#El save del DataFrame en la DB
sql = """
INSERT INTO info_meteorologica (date, tmax, tmin, precipitation_sum, weather_code)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  tmax=VALUES(tmax),
  tmin=VALUES(tmin),
  precipitation_sum=VALUES(precipitation_sum),
  weather_code=VALUES(weather_code);
"""
rows = list(zip(df["date"], df["tmax"], df["tmin"], df["precipitation_sum"], df["weather_code"].astype(object)))

with pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=True) as c, c.cursor() as cur:
    cur.executemany(sql, rows)

print(f"[ETL] Guardado en DB: {len(rows)} filas.")
