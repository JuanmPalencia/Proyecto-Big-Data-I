# app.py — GTP (Green Turning Point) Flask API
#
# Endpoints:
#   AUTH        POST /api/register, POST /api/login, POST /api/newsletter
#   DATOS       GET  /api/ranking, /api/city/<city_code>, /api/opportunities,
#               GET  /api/clusters, /api/years, /api/countries, /api/summary
#
# Base de datos:
#   PostgreSQL (schema gtp.*) alimentada desde HDFS Gold vía export_to_postgres.py
#   Configurar con variables de entorno: PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
#
# Si PostgreSQL no está configurado, los endpoints de auth/newsletter siguen funcionando.
# Los endpoints de datos devuelven 503 con mensaje explicativo.

from __future__ import annotations
import hashlib
import hmac
import json
import os
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
APP_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

USERS_FILE = os.path.join(DATA_DIR, "users.txt")
NEWS_FILE  = os.path.join(DATA_DIR, "newsletter.txt")
SECRET     = os.environ.get("GTP_SECRET", "gtp-demo-secret")

app = Flask(__name__, static_folder=None)
CORS(app, resources={r"/api/*": {"origins": "*"}})


# ==============================================================================
# CONEXIÓN POSTGRESQL
# ==============================================================================
def _get_pg_conn():
    """
    Devuelve una conexión psycopg2 a PostgreSQL.
    Lanza ImportError si psycopg2 no está instalado.
    Lanza OperationalError si no se puede conectar.
    Configurar con env vars: PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    """
    import psycopg2
    import psycopg2.extras
    return psycopg2.connect(
        host     = os.environ.get("PG_HOST",     "localhost"),
        port     = int(os.environ.get("PG_PORT", 5432)),
        dbname   = os.environ.get("PG_DB",       "gtp"),
        user     = os.environ.get("PG_USER",     "postgres"),
        password = os.environ.get("PG_PASSWORD", ""),
    )


def _query(sql: str, params=None) -> list[dict]:
    """
    Ejecuta una query SELECT y devuelve lista de dicts.
    Gestiona conexión + cursor con context managers.
    """
    import psycopg2.extras
    with _get_pg_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def _pg_available() -> bool:
    """Comprueba si PostgreSQL está accesible."""
    try:
        _query("SELECT 1")
        return True
    except Exception:
        return False


def require_pg(f):
    """Decorador: devuelve 503 si PostgreSQL no está disponible."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            err = str(e)
            if "psycopg2" in err or "connection" in err.lower() or "refused" in err.lower():
                return jsonify({
                    "error": "Base de datos no disponible",
                    "detail": "PostgreSQL no está configurado o no está en línea.",
                    "hint": "Configura PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD"
                }), 503
            raise
    return wrapper


# ==============================================================================
# AUTH HELPERS (sin cambios)
# ==============================================================================
def _hash_pw(pw: str) -> str:
    return hmac.new(SECRET.encode(), pw.encode(), hashlib.sha256).hexdigest()


def _append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _find_user(email: str):
    if not os.path.exists(USERS_FILE):
        return None
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get("email", "").lower() == email.lower():
                return row
    return None


# ==============================================================================
# ENDPOINTS DE AUTH (sin cambios)
# ==============================================================================
@app.post("/api/register")
def api_register():
    data  = request.get_json(force=True)
    req   = ["name", "last", "email", "password", "birth"]
    if not all(data.get(k) for k in req):
        return jsonify({"message": "Campos incompletos"}), 400
    email = data["email"].strip().lower()
    if _find_user(email):
        return jsonify({"message": "Ya existe un usuario con ese email"}), 409
    user = {
        "name":      data["name"].strip(),
        "last":      data["last"].strip(),
        "email":     email,
        "pass_hash": _hash_pw(data["password"]),
        "birth":     data["birth"],
        "dni":       data.get("dni", "").strip(),
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    _append_jsonl(USERS_FILE, user)
    return jsonify({"ok": True, "email": email})


@app.post("/api/login")
def api_login():
    data  = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()
    pw    = (data.get("password") or "")
    u     = _find_user(email)
    if not u:
        return jsonify({"message": "Usuario no encontrado"}), 404
    if _hash_pw(pw) != u.get("pass_hash"):
        return jsonify({"message": "Contraseña incorrecta"}), 401
    token = hmac.new(
        SECRET.encode(),
        (email + u["created_at"]).encode(),
        hashlib.sha256
    ).hexdigest()
    return jsonify({"ok": True, "email": email, "token": token})


@app.post("/api/newsletter")
def api_news():
    data  = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()
    topic = (data.get("topic") or "General").strip()
    if not email:
        return jsonify({"message": "Email requerido"}), 400
    rec = {"email": email, "topic": topic, "ts": datetime.utcnow().isoformat() + "Z"}
    _append_jsonl(NEWS_FILE, rec)
    return jsonify({"ok": True})


# ==============================================================================
# ENDPOINTS DE DATOS — consultan PostgreSQL (schema gtp.*)
# ==============================================================================

@app.get("/api/ranking")
@require_pg
def api_ranking():
    """
    Ranking europeo de ciudades por investment_score.

    Parámetros:
        year    (int)  — año del ranking (default: último disponible)
        limit   (int)  — máximo de ciudades (default: 50, max: 237)
        country (str)  — filtrar por código ISO-2 (ej: ES, FR)
    """
    year    = request.args.get("year",    type=int)
    limit   = min(request.args.get("limit", 50, type=int), 237)
    country = request.args.get("country", type=str)

    if year is None:
        rows = _query("SELECT MAX(year) AS y FROM gtp.city_ranking")
        year = rows[0]["y"]

    if country:
        rows = _query(
            """
            SELECT rank_position, rank_within_country, city_code, city_name,
                   country_code, country_name, investment_score,
                   investment_recommendation, green_index, turning_point_phase,
                   prophet_turning_year, rank_change, score_change,
                   top_ticker, top_company
            FROM gtp.city_ranking
            WHERE year = %s AND country_code = %s
            ORDER BY rank_position ASC
            LIMIT %s
            """,
            (year, country.upper(), limit)
        )
    else:
        rows = _query(
            """
            SELECT rank_position, rank_within_country, city_code, city_name,
                   country_code, country_name, investment_score,
                   investment_recommendation, green_index, turning_point_phase,
                   prophet_turning_year, rank_change, score_change,
                   top_ticker, top_company
            FROM gtp.city_ranking
            WHERE year = %s
            ORDER BY rank_position ASC
            LIMIT %s
            """,
            (year, limit)
        )

    return jsonify({"year": year, "count": len(rows), "ranking": rows})


@app.get("/api/city/<city_code>")
@require_pg
def api_city(city_code: str):
    """
    Datos completos de una ciudad a lo largo del tiempo (o un año concreto).

    Parámetros:
        year (int) — si se especifica, devuelve solo ese año; si no, toda la serie
    """
    year = request.args.get("year", type=int)

    if year:
        rows = _query(
            "SELECT * FROM gtp.fact_kuznets WHERE city_code = %s AND year = %s",
            (city_code, year)
        )
    else:
        rows = _query(
            "SELECT * FROM gtp.fact_kuznets WHERE city_code = %s ORDER BY year",
            (city_code,)
        )

    if not rows:
        return jsonify({"error": f"Ciudad '{city_code}' no encontrada"}), 404

    # Obtener también los resultados detallados de los modelos ML
    ml_rows = _query(
        "SELECT * FROM gtp.model_results WHERE city_code = %s ORDER BY year",
        (city_code,)
    )

    return jsonify({
        "city_code": city_code,
        "city_name": rows[0].get("city_name"),
        "country_code": rows[0].get("country_code"),
        "ekc_series": rows,
        "ml_detail": ml_rows,
    })


@app.get("/api/opportunities")
@require_pg
def api_opportunities():
    """
    Ciudades en fase TURNING del último año: oportunidades de inversión inmediatas.

    Parámetros:
        year    (int) — año a consultar (default: último disponible)
        country (str) — filtrar por código ISO-2
    """
    year    = request.args.get("year",    type=int)
    country = request.args.get("country", type=str)

    if year is None:
        rows = _query("SELECT MAX(year) AS y FROM gtp.fact_kuznets")
        year = rows[0]["y"]

    if country:
        rows = _query(
            """
            SELECT city_name, country_code, country_name, year,
                   investment_score, green_index, gdp_pps_per_capita,
                   ekc_turning_point_y, gdp_gap_to_turning,
                   prophet_turning_year, phase_confidence,
                   fin_tickers, investment_recommendation
            FROM gtp.fact_kuznets
            WHERE turning_point_phase = 'TURNING'
              AND year = %s AND country_code = %s
            ORDER BY investment_score DESC NULLS LAST
            """,
            (year, country.upper())
        )
    else:
        rows = _query(
            """
            SELECT city_name, country_code, country_name, year,
                   investment_score, green_index, gdp_pps_per_capita,
                   ekc_turning_point_y, gdp_gap_to_turning,
                   prophet_turning_year, phase_confidence,
                   fin_tickers, investment_recommendation
            FROM gtp.fact_kuznets
            WHERE turning_point_phase = 'TURNING'
              AND year = %s
            ORDER BY investment_score DESC NULLS LAST
            """,
            (year,)
        )

    return jsonify({"year": year, "count": len(rows), "opportunities": rows})


@app.get("/api/clusters")
@require_pg
def api_clusters():
    """
    Parámetros EKC estimados por cluster K-Means (última estimación disponible).
    Incluye β₁, β₂, Y*, R², forma de curva y ciudades de cada cluster.
    """
    rows = _query("SELECT * FROM gtp.v_ekc_summary ORDER BY cluster_id")
    return jsonify({"count": len(rows), "clusters": rows})


@app.get("/api/years")
@require_pg
def api_years():
    """Lista de años disponibles en el dataset."""
    rows = _query("SELECT DISTINCT year FROM gtp.fact_kuznets ORDER BY year")
    return jsonify({"years": [r["year"] for r in rows]})


@app.get("/api/countries")
@require_pg
def api_countries():
    """Lista de países con datos, con conteo de ciudades."""
    rows = _query(
        """
        SELECT country_code, country_name, COUNT(DISTINCT city_code) AS n_cities
        FROM gtp.fact_kuznets
        GROUP BY country_code, country_name
        ORDER BY country_name
        """
    )
    return jsonify({"count": len(rows), "countries": rows})


@app.get("/api/summary")
@require_pg
def api_summary():
    """
    Estadísticas agregadas del último año: resumen ejecutivo para el dashboard.
    """
    rows = _query("SELECT MAX(year) AS y FROM gtp.fact_kuznets")
    year = rows[0]["y"]

    stats = _query(
        """
        SELECT
            COUNT(DISTINCT city_code)                               AS total_cities,
            COUNT(DISTINCT country_code)                            AS total_countries,
            ROUND(AVG(green_index)::numeric, 4)                     AS avg_green_index,
            ROUND(AVG(investment_score)::numeric, 2)                AS avg_investment_score,
            COUNT(*) FILTER (WHERE turning_point_phase = 'TURNING')      AS cities_turning,
            COUNT(*) FILTER (WHERE turning_point_phase = 'RECUPERANDO')  AS cities_recuperando,
            COUNT(*) FILTER (WHERE turning_point_phase = 'DEGRADANDO')   AS cities_degradando,
            COUNT(*) FILTER (WHERE investment_recommendation = 'STRONG_BUY') AS strong_buy,
            COUNT(*) FILTER (WHERE investment_recommendation = 'BUY')        AS buy,
            COUNT(*) FILTER (WHERE investment_recommendation = 'HOLD')       AS hold,
            COUNT(*) FILTER (WHERE investment_recommendation = 'AVOID')      AS avoid
        FROM gtp.fact_kuznets
        WHERE year = %s
        """,
        (year,)
    )

    return jsonify({"year": year, "summary": stats[0] if stats else {}})


# ==============================================================================
# STATIC / INDEX
# ==============================================================================
@app.get("/")
def root():
    return send_from_directory(APP_DIR, "index.html")


@app.get("/<path:path>")
def any_static(path):
    return send_from_directory(APP_DIR, path)


# ==============================================================================
# ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    print("=" * 55)
    print("  GTP Flask API arrancando en http://0.0.0.0:5050")
    pg_ok = _pg_available()
    print(f"  PostgreSQL: {'✓ conectado' if pg_ok else '✗ no disponible (solo auth)'}")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5050, debug=True)
