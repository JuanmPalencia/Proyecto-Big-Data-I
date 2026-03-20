"""
GTP — MariaDB Connection Pool
Gestiona el pool de conexiones a la base de datos usando mysql.connector.
Lee la configuración desde config.ini (nunca hardcodeada en código).
"""
import os
import configparser
import mysql.connector
from mysql.connector import pooling


class MariaDBPool:

    def __init__(self, config_file="config.ini"):
        self.config = self._load_config(config_file)
        self.pool   = self._create_pool()

    def _load_config(self, config_file):
        """
        Lee config.ini y permite sobreescritura vía variables de entorno.
        Las env vars PG_HOST / PG_PORT / PG_USER / PG_PASSWORD / PG_DB
        se inyectan automáticamente en contenedores Docker (docker-compose.yml).
        Así el mismo config.ini sirve tanto en local (127.0.0.1:3307)
        como dentro de Docker (mariadb:3306).
        """
        config = configparser.ConfigParser()
        config.read(config_file)
        db = config["database"]
        return {
            "host":      os.getenv("PG_HOST",     db.get("host")),
            "port":      int(os.getenv("PG_PORT", db.get("port", "3306"))),
            "user":      os.getenv("PG_USER",     db.get("user")),
            "password":  os.getenv("PG_PASSWORD", db.get("password")),
            "database":  os.getenv("PG_DB",       db.get("database")),
            "pool_name": db.get("pool_name", "gtp_pool"),
            "pool_size": db.getint("pool_size", fallback=5),
        }

    def _create_pool(self):
        return pooling.MySQLConnectionPool(
            pool_name=self.config["pool_name"],
            pool_size=self.config["pool_size"],
            host=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            password=self.config["password"],
            database=self.config["database"],
        )

    def get_connection(self):
        return self.pool.get_connection()

    def execute_query(self, query, params=None):
        conn   = self.get_connection()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()
            conn.close()

    def execute_many(self, query, data):
        """Inserción masiva con executemany — para carga de facts."""
        conn   = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.executemany(query, data)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def execute_write(self, query, params=None):
        """INSERT / UPDATE / DELETE de una sola fila."""
        conn   = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or ())
            conn.commit()
            return cursor.rowcount
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
