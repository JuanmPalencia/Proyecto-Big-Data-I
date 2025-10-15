CREATE DATABASE IF NOT EXISTS weather CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE weather;

CREATE TABLE IF NOT EXISTS info_meteorologica (
  date DATE PRIMARY KEY,
  tmax DOUBLE,
  tmin DOUBLE,
  precipitation_sum DOUBLE,
  weather_code INT
);
