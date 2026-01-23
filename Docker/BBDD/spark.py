# %%
import os, sys, re, subprocess, importlib.util
from pathlib import Path

# ============================================================
# 0) ✅ AJUSTES PRINCIPALES (RUTAS Y POSTGRES)
# ============================================================
PROJECT_ROOT_HINT = Path(r"/home/223B3336juan/Big Data I/Proyecto-Big-Data-I/BBDD")
CONFIG_PY_PATH    = Path(r"/home/223B3336juan/Big Data I/Proyecto-Big-Data-I/ETL/script/config.py")

KUZNETS_DIR       = Path(r"/home/223B3336juan/data/DatosProcesados")
KUZNETS_FILE      = KUZNETS_DIR / "Kuznets.csv"

SPARK_SHUFFLE_PARTITIONS = "200"

# ---- POSTGRES (docker local) ----
PG_HOST = "localhost"
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB   = os.environ.get("POSTGRES_DB", "gtp")
PG_USER = os.environ.get("POSTGRES_USER", "gtp_user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "gtp_pass")

PG_SCHEMA = "public"  # usa "public" para no complicarte
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

# JDBC package (Spark descargará el jar)
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"

# ============================================================
# 1) BOOTSTRAP SPARK (Linux/Windows)
# ============================================================
if os.name == "nt":
    py = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", py)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", py)

import socketserver
if not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer
if not hasattr(socketserver, "UnixDatagramServer"):
    socketserver.UnixDatagramServer = socketserver.UDPServer

print(subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode())
print("Python:", sys.version)

# ============================================================
# 2) CARGA config.py (OBLIGATORIO)
# ============================================================
if not CONFIG_PY_PATH.exists():
    raise FileNotFoundError(f"❌ No encuentro config.py en: {CONFIG_PY_PATH}")

spec = importlib.util.spec_from_file_location("config", str(CONFIG_PY_PATH))
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)

print("✅ config.py cargado desde:", CONFIG_PY_PATH)

EURO_FUAS = getattr(config, "EURO_FUAS", None)
CITY_MAPPING = getattr(config, "CITY_MAPPING", {})
FINANCE_PROXY_MAP = getattr(config, "FINANCE_PROXY_MAP", {"BE": ["FR", "NL"]})
NO_NORMALIZE = getattr(config, "NO_NORMALIZE", {"population_by_age"})

if EURO_FUAS is None or len(EURO_FUAS) == 0:
    raise ValueError("❌ En config.py necesito EURO_FUAS (dict) y está vacío / no existe.")

print(f"✅ EURO_FUAS: {len(EURO_FUAS)} ciudades | CITY_MAPPING: {len(CITY_MAPPING)} | FINANCE_PROXY_MAP: {len(FINANCE_PROXY_MAP)}")

# ============================================================
# 3) SPARK SESSION (con JDBC package)
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

if os.name == "nt":
    TMP_LOCAL = Path(r"C:\tmp\spark-local")
    TMP_WH    = Path(r"C:\tmp\spark-warehouse")
else:
    TMP_LOCAL = Path("/tmp/spark-local")
    TMP_WH    = Path("/tmp/spark-warehouse")

TMP_LOCAL.mkdir(parents=True, exist_ok=True)
TMP_WH.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("GTP_to_Postgres")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
    .config("spark.sql.warehouse.dir", str(TMP_WH.resolve()))
    .config("spark.local.dir", str(TMP_LOCAL.resolve()))
    .config("spark.hadoop.fs.permissions.enabled", "false")
    .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)  # ✅
    .getOrCreate()
)
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)

# ============================================================
# 4) RESOLVER PROJECT_ROOT (desde PROJECT_ROOT_HINT)
# ============================================================
root_start = Path(PROJECT_ROOT_HINT).resolve()
for p in [root_start] + list(root_start.parents):
    if p.name == "Proyecto-Big-Data-I":
        PROJECT_ROOT = p
        break
else:
    raise RuntimeError(f"❌ No se encontró 'Proyecto-Big-Data-I' buscando desde: {root_start}")

print("📌 PROJECT_ROOT:", PROJECT_ROOT)

DATA_ROOT   = PROJECT_ROOT / "data"
CRUDO       = DATA_ROOT / "raw"

# ============================================================
# 5) HELPERS GENERALES + SQL
# ============================================================
def log_error(context: str, exc: Exception):
    print(f"\n❌ Error en {context}: {type(exc).__name__}: {exc}")

def std_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_"))
    cols = df.columns
    new_cols, seen = [], {}
    for c in cols:
        if c not in seen:
            seen[c] = 0
            new_cols.append(c)
        else:
            seen[c] += 1
            new_cols.append(f"{c}__dup{seen[c]}")
    return df.toDF(*new_cols)

def safe_to_int(col):
    return F.regexp_extract(F.col(col).cast("string"), r"(\d+)", 1).cast("int")

def minmax_normalize(df, col, out_col):
    stats = df.select(F.min(col).alias("mn"), F.max(col).alias("mx")).collect()[0]
    mn, mx = stats["mn"], stats["mx"]
    if mn is None or mx is None or mn == mx:
        return df.withColumn(out_col, F.lit(None).cast("double"))
    return df.withColumn(out_col, (F.col(col) - F.lit(mn)) / (F.lit(mx) - F.lit(mn)))

def spark_path(path: Path) -> str:
    return str(path.resolve())

def list_csv_files(folder: Path):
    if not folder.exists():
        raise FileNotFoundError(f"No existe la carpeta: {folder}")
    return sorted(folder.glob("*.csv"))

def safe_read_csv(path: Path, context: str):
    try:
        if not path.exists():
            raise FileNotFoundError(f"No existe el fichero: {path}")
        return spark.read.option("header", True).csv(spark_path(path))
    except Exception as e:
        log_error(f"lectura CSV ({context})", e)
        return None

# ---------- SQL helpers ----------
def write_sql(df, table: str, mode: str = "overwrite", schema: str = PG_SCHEMA):
    full_table = f"{schema}.{table}"
    try:
        (df.write
           .mode(mode)
           .jdbc(url=PG_URL, table=full_table, properties=PG_PROPS))
        print(f"   ✔ SQL escrito en {full_table}")
    except Exception as e:
        log_error(f"write_sql({full_table})", e)

def read_sql(table: str, schema: str = PG_SCHEMA):
    full_table = f"{schema}.{table}"
    try:
        return spark.read.jdbc(url=PG_URL, table=full_table, properties=PG_PROPS)
    except Exception as e:
        log_error(f"read_sql({full_table})", e)
        return None

print("✅ JDBC URL:", PG_URL)

# ============================================================
# 6) GEO: FUA -> SQL
# ============================================================
fuas_rows = [(k, float(v[0]), float(v[1]), k.split("_")[-1]) for k, v in EURO_FUAS.items()]
fuas_schema = T.StructType([
    T.StructField("fua_id", T.StringType(), False),
    T.StructField("lat", T.DoubleType(), False),
    T.StructField("lon", T.DoubleType(), False),
    T.StructField("geo_std", T.StringType(), True),
])
fuas_df = spark.createDataFrame(fuas_rows, fuas_schema)
write_sql(fuas_df, table="geo_fuas", mode="overwrite")
print("✅ GEO FUA generado (SQL).")

# ============================================================
# 7) EUROSTAT — FASE 1/2 + PANEL (SQL)
# ============================================================
EUROSTAT_IN  = CRUDO / "eurostat"
euro_logs = []

csv_files = list_csv_files(EUROSTAT_IN)
print("CSV encontrados en Eurostat:", [f.name for f in csv_files])

for csv_path in csv_files:
    name = csv_path.stem.lower()
    if "part" in name:
        continue

    print(f"\n→ Procesando EUROSTAT: {csv_path.name}")

    df = (spark.read.option("header", True).option("inferSchema", False).csv(spark_path(csv_path)))
    df = std_cols(df).dropDuplicates()

    if "time" in df.columns:
        df = df.withColumn("time", safe_to_int("time"))

    if "geo" in df.columns and "geo_std" not in df.columns:
        df = df.withColumn("geo_std", F.upper(F.trim(F.col("geo").cast("string"))))

    if "value" in df.columns:
        df = df.withColumn("value", F.col("value").cast("double"))
        df = df.withColumn("value_is_na", F.col("value").isNull())

        out_of_range = F.lit(False)
        if name in {"poverty_rate", "renewable_energy"}:
            out_of_range = (F.col("value") < 0) | (F.col("value") > 100)
        if name == "household_size":
            out_of_range = (F.col("value") <= 0)
        if name == "population_density":
            out_of_range = (F.col("value") < 0)

        df = df.withColumn("value_out_of_range", out_of_range)

        if name not in NO_NORMALIZE:
            df = minmax_normalize(df, "value", "value_normalized")

    write_sql(df, table=f"eurostat_{name}", mode="overwrite")
    euro_logs.append((name, int("value_normalized" in df.columns)))

if euro_logs:
    euro_log_df = spark.createDataFrame(euro_logs, ["dataset", "has_value_normalized"])
    write_sql(euro_log_df, table="eurostat_logs_phase12", mode="overwrite")
    print("✅ euro_logs guardados (SQL).")
else:
    print(f"⚠ No se generó ningún log de Eurostat. Revisa CSVs en: {EUROSTAT_IN}")

# ============================================================
# 7B) EUROSTAT — GDP per capita por ciudad (SQL)
# ============================================================
METRO_TSV_GZ = EUROSTAT_IN / "met_10r_3gdp.tsv.gz"
NUTS3_TSV_GZ = EUROSTAT_IN / "nama_10r_3gdp.tsv.gz"

if (not METRO_TSV_GZ.exists()) or (not NUTS3_TSV_GZ.exists()):
    print("⚠ GDP city extra saltado: faltan TSV.gz en data/raw/eurostat/")
else:
    def read_tsvgz(path: Path):
        return spark.read.option("header", True).option("sep", "\t").csv(spark_path(path))

    raw_metro = std_cols(read_tsvgz(METRO_TSV_GZ))
    raw_nuts3 = std_cols(read_tsvgz(NUTS3_TSV_GZ))

    def sdmx_to_long(df, region_hint: str):
        if df is None or len(df.columns) == 0:
            return None

        first_col = df.columns[0]
        dims_part = first_col.split("\\")[0]
        dim_names = [x.strip() for x in dims_part.split(",") if x.strip()]

        dims = F.split(F.col(first_col), ",")
        for i, d in enumerate(dim_names):
            df = df.withColumn(d, dims.getItem(i))

        region_col = region_hint if region_hint in df.columns else ("metroreg" if "metroreg" in df.columns else ("geo" if "geo" in df.columns else None))
        if region_col is None:
            return None

        year_cols = [c for c in df.columns if re.fullmatch(r"(19|20)\d{2}", str(c))]
        if not year_cols:
            return None

        expr = "stack({0}, {1}) as (year_str, value_raw)".format(
            len(year_cols),
            ",".join([f"'{c}', `{c}`" for c in year_cols])
        )

        out = df.select(*[F.col(c) for c in dim_names], F.expr(expr))

        out = (
            out
            .withColumn("year", safe_to_int("year_str"))
            .withColumn("value_clean", F.regexp_replace(F.col("value_raw").cast("string"), r"[^0-9\.\-]", ""))
            .withColumn("value", F.col("value_clean").cast("double"))
            .drop("year_str", "value_raw", "value_clean")
            .withColumnRenamed(region_col, "region")
            .filter(F.col("region").isNotNull() & F.col("year").isNotNull() & F.col("value").isNotNull())
        )
        return out

    metro_long = sdmx_to_long(raw_metro, "metroreg")
    nuts3_long = sdmx_to_long(raw_nuts3, "geo")

    if metro_long is None or nuts3_long is None:
        print("⚠ GDP city extra saltado: no se pudo normalizar SDMX TSV.")
    else:
        if "unit" in metro_long.columns:
            metro_long = metro_long.filter(F.upper(F.col("unit")).contains("EUR_HAB"))
        if "unit" in nuts3_long.columns:
            nuts3_long = nuts3_long.filter(F.upper(F.col("unit")).contains("EUR_HAB"))

        if len(CITY_MAPPING) == 0:
            print("⚠ GDP city extra: CITY_MAPPING vacío en config.py. No se generó output.")
        else:
            mapping_rows = [(k, v[0], v[1]) for k, v in CITY_MAPPING.items()]
            map_df = spark.createDataFrame(mapping_rows, ["city_id", "metro_code", "nuts3_code"])

            metro_join = (
                map_df.join(metro_long, metro_long.region.startswith(map_df.metro_code), "left")
                      .select("city_id", "year", F.col("value").alias("gdp_pc"), F.lit("METRO").alias("source"))
            )

            nuts3_join = (
                map_df.join(nuts3_long, nuts3_long.region == map_df.nuts3_code, "left")
                      .select("city_id", "year", F.col("value").alias("gdp_pc"), F.lit("NUTS3").alias("source"))
            )

            nuts3_country = nuts3_long.withColumn("cc", F.substring(F.col("region"), 1, 2))
            nat_avg = (
                nuts3_country.groupBy("cc", "year")
                             .agg(F.mean("value").alias("gdp_pc"))
                             .withColumn("source", F.lit("NATIONAL_AVG"))
            )
            nat_join = (
                map_df.withColumn("cc", F.substring(F.col("metro_code"), 1, 2))
                      .join(nat_avg, on=["cc"], how="left")
                      .select("city_id", "year", "gdp_pc", "source")
            )

            combo = (
                metro_join.alias("m")
                .join(nuts3_join.alias("n"), on=["city_id", "year"], how="full_outer")
                .join(nat_join.alias("a"), on=["city_id", "year"], how="full_outer")
                .select(
                    F.col("city_id"),
                    F.col("year"),
                    F.coalesce(F.col("m.gdp_pc"), F.col("n.gdp_pc"), F.col("a.gdp_pc")).alias("gdp_per_capita"),
                    F.coalesce(F.col("m.source"), F.col("n.source"), F.col("a.source")).alias("source")
                )
                .filter(F.col("gdp_per_capita").isNotNull())
                .withColumn("geo_std", F.upper(F.regexp_extract("city_id", r"_([A-Z]{2,3})$", 1)))
            )

            fuas_sql = read_sql("geo_fuas")
            if fuas_sql is not None:
                combo = combo.join(fuas_sql.select(F.col("fua_id").alias("city_id2"), "lat", "lon"),
                                   combo.city_id == F.col("city_id2"), "left").drop("city_id2")

            write_sql(combo.dropDuplicates(["city_id", "year"]), table="eurostat_gdp_city_per_capita", mode="overwrite")
            print("✅ GDP city per capita guardado (SQL).")

# -------- Panel country-year (EUROSTAT_PANEL) --------
EURO_PANEL_VARS = {
    "gdp_per_capita":       {"unit": "CP_EUR_HAB"},
    "productivity":         {"unit": "I15"},
    "poverty_rate":         {"unit": "PC"},
    "employment_by_sector": {"unit": "THS_PER"},
    "renewable_energy":     {"unit": "PC"},
    "ghg_emissions":        {"unit": "T"},
}

def euro_panelize(varname: str, unit: str | None = None):
    # Lee desde SQL eurostat_<varname>
    df = read_sql(f"eurostat_{varname}")
    if df is None:
        return None

    if unit and "unit" in df.columns:
        df = df.withColumn("unit", F.upper(F.trim(F.col("unit")))).filter(F.col("unit") == unit)

    if "geo_std" not in df.columns and "geo" in df.columns:
        df = df.withColumn("geo_std", F.upper(F.trim(F.col("geo").cast("string"))))

    return (
        df.filter(F.col("value").isNotNull() & F.col("time").isNotNull() & F.col("geo_std").isNotNull())
          .groupBy("geo_std", "time")
          .agg(F.mean("value").alias(varname))
    )

panel = None
for var, cfg in EURO_PANEL_VARS.items():
    tmp = euro_panelize(var, unit=cfg.get("unit"))
    if tmp is None:
        continue
    panel = tmp if panel is None else panel.join(tmp, on=["geo_std", "time"], how="outer")

if panel is not None:
    write_sql(panel, table="eurostat_panel", mode="overwrite")
    print("✅ eurostat_panel guardado (SQL).")
else:
    print("⚠ No se generó panel eurostat.")

# ============================================================
# 8) OECD — FASE 1/2 (SQL)
# ============================================================
OECD_IN  = CRUDO / "oecd"

def detect_year_cols(cols):
    return [c for c in cols if re.fullmatch(r"(19|20)\d{2}", str(c).strip())]

def pick_first_existing(cols, candidates):
    cols_set = set(cols)
    for c in candidates:
        if c in cols_set:
            return c
    return None

GEO_CANDS   = ["geo_std", "geo", "location", "country", "loc", "ref_area", "cou"]
TIME_CANDS  = ["time", "year", "time_period"]
VALUE_CANDS = ["value", "obs_value", "indicator_value", "val", "data"]

oecd_meta = []

if not OECD_IN.exists():
    print(f"⚠ OECD_IN no existe: {OECD_IN} (saltando OECD)")
else:
    for csv_path in list_csv_files(OECD_IN):
        var = csv_path.stem.lower()
        print(f"\n→ Procesando OECD: {csv_path.name}")

        df0 = (spark.read.option("header", True).option("inferSchema", False).csv(spark_path(csv_path)))
        df0 = std_cols(df0)

        year_cols0 = detect_year_cols(df0.columns)
        was_wide = len(year_cols0) >= 3

        if was_wide:
            df_w = df0
            for t in TIME_CANDS:
                if t in df_w.columns:
                    df_w = df_w.drop(t)

            year_cols = detect_year_cols(df_w.columns)
            if len(year_cols) == 0:
                print(f"   ⚠ Saltado {csv_path.name}: no quedan columnas de años.")
                continue

            id_cols = [c for c in df_w.columns if c not in year_cols]
            expr = "stack({0}, {1}) as (year_str, value_raw)".format(
                len(year_cols),
                ",".join([f"'{c}', `{c}`" for c in year_cols])
            )
            df1 = df_w.select(*[F.col(c) for c in id_cols], F.expr(expr))

            geo_col = pick_first_existing(df1.columns, GEO_CANDS)
            if geo_col is None:
                print(f"   ⚠ Saltado {csv_path.name}: falta geo en formato ancho")
                continue

            df2 = df1
            if geo_col != "geo_std":
                df2 = df2.withColumnRenamed(geo_col, "geo_std")

            df2 = (
                df2.withColumnRenamed("value_raw", "value")
                   .withColumn("time_int", safe_to_int("year_str"))
                   .drop("year_str")
            )
            for extra in TIME_CANDS:
                if extra in df2.columns:
                    df2 = df2.drop(extra)
            df2 = df2.withColumnRenamed("time_int", "time")

        else:
            cols_now = df0.columns
            geo_col   = pick_first_existing(cols_now, GEO_CANDS)
            time_col0 = pick_first_existing(cols_now, TIME_CANDS)
            val_col0  = pick_first_existing(cols_now, VALUE_CANDS)

            if geo_col is None or time_col0 is None or val_col0 is None:
                print(f"   ⚠ Saltado {csv_path.name}: falta geo/time/value en formato largo")
                continue

            df2 = df0
            if geo_col != "geo_std":
                df2 = df2.withColumnRenamed(geo_col, "geo_std")
            if val_col0 != "value":
                df2 = df2.withColumnRenamed(val_col0, "value")

            df2 = df2.withColumn("time_int", safe_to_int(time_col0))
            for extra in TIME_CANDS:
                if extra in df2.columns:
                    df2 = df2.drop(extra)
            df2 = df2.withColumnRenamed("time_int", "time")

        df2 = (
            df2.withColumn("geo_std", F.upper(F.trim(F.col("geo_std").cast("string"))))
               .withColumn("value", F.col("value").cast("double"))
               .filter(F.col("geo_std").isNotNull() & F.col("time").isNotNull() & F.col("value").isNotNull())
               .withColumn("variable", F.lit(var))
        )

        df3 = df2.groupBy("geo_std", "time", "variable").agg(F.mean("value").alias("value"))
        write_sql(df3, table=f"oecd_{var}", mode="overwrite")
        oecd_meta.append((var, was_wide))
        print(f"   ✔ Guardado SQL oecd_{var} | wide={was_wide}")

    if oecd_meta:
        oecd_meta_df = spark.createDataFrame(oecd_meta, ["dataset", "was_wide"])
        write_sql(oecd_meta_df, table="oecd_logs_phase12", mode="overwrite")

# ============================================================
# 9) InvestEU — (SQL)
# ============================================================
INV_IN  = CRUDO / "investeu"

ops    = safe_read_csv(INV_IN / "operations_list.csv", "InvestEU operations")
finals = safe_read_csv(INV_IN / "final_recipients.csv", "InvestEU finals")

if ops is None or finals is None:
    print("⚠ InvestEU saltado: faltan CSV en data/raw/investeu.")
else:
    ops, finals = std_cols(ops), std_cols(finals)

    amount_candidates = [c for c in finals.columns if "amount" in c.lower() and ("eur" in c.lower() or "euro" in c.lower())]
    if not amount_candidates:
        raise ValueError(f"No encuentro columna de importe EUR en finals. Columnas: {finals.columns}")

    AMOUNT_COL = amount_candidates[0]
    print(f"✅ InvestEU usando columna: {AMOUNT_COL}")

    COUNTRY_MAP = {
        "SPAIN":"ES","FRANCE":"FR","GERMANY":"DE","ITALY":"IT","PORTUGAL":"PT","BELGIUM":"BE","NETHERLANDS":"NL",
        "SWEDEN":"SE","DENMARK":"DK","FINLAND":"FI","IRELAND":"IE","POLAND":"PL","AUSTRIA":"AT","CZECH REPUBLIC":"CZ",
        "GREECE":"EL","ROMANIA":"RO","BULGARIA":"BG","HUNGARY":"HU","SLOVAKIA":"SK","SLOVENIA":"SI","LUXEMBOURG":"LU",
        "CYPRUS":"CY","ESTONIA":"EE","LATVIA":"LV","LITHUANIA":"LT"
    }
    map_expr = F.create_map([F.lit(x) for kv in COUNTRY_MAP.items() for x in kv])

    finals = (
        finals.withColumn("borrower_country", F.upper(F.trim(F.col("borrower_country"))))
              .withColumn("geo_std", map_expr.getItem(F.col("borrower_country")))
    )

    amount_raw = F.col(AMOUNT_COL).cast("string")
    amount_str = F.regexp_replace(amount_raw, r"[^0-9\.\-]", "")
    finals = finals.withColumn("amount_eur", amount_str.cast("double"))

    agg_base = (
        finals.filter(F.col("geo_std").isNotNull())
              .groupBy("geo_std")
              .agg(
                  F.count("*").alias("investeu_projects"),
                  F.countDistinct("borrower_name").alias("investeu_unique_borrowers"),
                  F.sum(F.coalesce(F.col("amount_eur"), F.lit(0.0))).alias("investeu_total_amount")
              )
    )

    inv = (
        agg_base
        .withColumn("inv_amount_per_project",
                    F.when(F.col("investeu_projects") > 0,
                           F.col("investeu_total_amount") / F.col("investeu_projects"))
                     .otherwise(F.lit(0.0)))
        .withColumn("inv_amount_per_borrower",
                    F.when(F.col("investeu_unique_borrowers") > 0,
                           F.col("investeu_total_amount") / F.col("investeu_unique_borrowers"))
                     .otherwise(F.lit(0.0)))
        .withColumn("log_investeu_total_amount", F.log1p(F.col("investeu_total_amount")))
    )

    write_sql(inv, table="investeu_country", mode="overwrite")

# ============================================================
# 10) INE PIB — (SQL)
# ============================================================
INE_IN = PROJECT_ROOT / "data" / "ine" / "pib_ine_latest.csv"
print("INE_IN:", INE_IN)

if not INE_IN.exists():
    raise FileNotFoundError(f"No se encuentra el fichero INE en: {INE_IN}")

ine_raw = spark.read.option("header", True).csv(spark_path(INE_IN))
ine_raw = std_cols(ine_raw)

ine = (
    ine_raw
    .withColumnRenamed("indicador", "indicator")
    .withColumnRenamed("periodo", "period")
    .withColumnRenamed("valor", "value")
    .withColumnRenamed("variacion_interanual_%", "yoy_change")
    .withColumn("geo_std", F.lit("ES"))
    .withColumn("period", F.upper(F.trim(F.col("period").cast("string"))))
)

ine = (
    ine.withColumn("year", F.regexp_extract("period", r"(\d{4})", 1).cast("int"))
       .withColumn("quarter", F.regexp_extract("period", r"T(\d)", 1).cast("int"))
       .withColumn("value", F.col("value").cast("double"))
       .withColumn("yoy_change", F.col("yoy_change").cast("double"))
       .filter(F.col("year").isNotNull() & F.col("quarter").isNotNull() & F.col("value").isNotNull())
)

ine = (
    ine.withColumn("indicator_group",
                   F.when(F.lower(F.col("indicator")).contains("ajustad"), F.lit("seasonally_adjusted"))
                    .otherwise(F.lit("raw")))
       .withColumn("time_index", F.col("year") * 10 + F.col("quarter"))
)

q = ine.approxQuantile("value", [0.25, 0.75], 0.001)
q1, q3 = q[0], q[1]
iqr = q3 - q1
low = q1 - 1.5 * iqr
high = q3 + 1.5 * iqr

ine_f3 = (
    ine.withColumn("log_value", F.when(F.col("value") > 0, F.log(F.col("value"))).otherwise(F.lit(None)))
       .withColumn("is_q1", (F.col("quarter") == 1).cast("int"))
       .withColumn("is_q4", (F.col("quarter") == 4).cast("int"))
       .withColumn("outlier_flag", ((F.col("value") < low) | (F.col("value") > high)).cast("int"))
)

write_sql(ine_f3, table="ine_timeseries", mode="overwrite")

# ============================================================
# 11) KUZNETS (CSV externo) -> SQL
# ============================================================
print("\n📂 Leyendo KUZNETS desde →", KUZNETS_FILE)
if not KUZNETS_FILE.exists():
    raise FileNotFoundError(f"❌ No se encuentra Kuznets.csv en: {KUZNETS_FILE}")

kuz = spark.read.option("header", True).csv(spark_path(KUZNETS_FILE))
kuz = std_cols(kuz).dropDuplicates()
write_sql(kuz, table="kuznets", mode="overwrite")

# ============================================================
# 12) YFINANCE — (SQL)
# ============================================================
FIN_RAW_NEW = PROJECT_ROOT / "data" / "raw" / "finance" / "financial_market_data.csv"
print(f"\n📂 Leyendo YFINANCE desde → {FIN_RAW_NEW}")

if not FIN_RAW_NEW.exists():
    raise FileNotFoundError(f"No se encuentra financial_market_data.csv en: {FIN_RAW_NEW}")

fin = (spark.read.option("header", True).option("inferSchema", False).csv(spark_path(FIN_RAW_NEW)))
fin = std_cols(fin).dropDuplicates()

fin = (
    fin.withColumn("ticker", F.upper(F.trim(F.col("ticker").cast("string"))))
       .withColumn("company", F.trim(F.col("company").cast("string")))
       .withColumn("sector", F.trim(F.col("sector").cast("string")))
       .withColumn("industry", F.trim(F.col("industry").cast("string")))
       .withColumn("country", F.upper(F.trim(F.col("country").cast("string"))))
       .withColumn("year", safe_to_int("year"))
       .withColumn("close_price_avg", F.col("close_price_avg").cast("double"))
       .withColumn("annual_return", F.col("annual_return").cast("double"))
       .withColumn("volatility", F.col("volatility").cast("double"))
       .withColumn("volume_avg", F.col("volume_avg").cast("double"))
       .withColumn("current_pe", F.col("current_pe").cast("double"))
       .withColumn("current_beta", F.col("current_beta").cast("double"))
)

fin = fin.filter(F.col("ticker").isNotNull() & F.col("year").isNotNull())

fin = (
    fin.withColumn("log_close_price_avg", F.when(F.col("close_price_avg") > 0, F.log1p("close_price_avg")))
       .withColumn("log_volume_avg", F.when(F.col("volume_avg") > 0, F.log1p("volume_avg")))
       .withColumn("abs_annual_return", F.abs(F.col("annual_return")))
       .withColumn("return_is_na", F.col("annual_return").isNull().cast("int"))
       .withColumn("volatility_is_na", F.col("volatility").isNull().cast("int"))
)

fin_panel = fin.dropDuplicates(["ticker", "year"])
write_sql(fin_panel, table="finance_panel", mode="overwrite")

fin_country_year = (
    fin_panel.filter(F.col("annual_return").isNotNull() | F.col("volatility").isNotNull())
             .groupBy("country", "year")
             .agg(
                 F.countDistinct("ticker").alias("n_tickers"),
                 F.avg("annual_return").alias("avg_annual_return"),
                 F.avg("volatility").alias("avg_volatility"),
                 F.avg("close_price_avg").alias("avg_close_price"),
                 F.avg("volume_avg").alias("avg_volume"),
                 F.avg("current_beta").alias("avg_beta"),
             )
)
write_sql(fin_country_year, table="finance_country_year", mode="overwrite")

# ============================================================
# 13) MASTER DATASET — Limpieza + imputaciones (SQL)
# ============================================================
MASTER_IN  = PROJECT_ROOT / "data" / "Master_Dataset_Final.csv"
print(f"\n📂 Leyendo MASTER desde → {MASTER_IN}")
if not MASTER_IN.exists():
    raise FileNotFoundError(f"No se encuentra el Master CSV en: {MASTER_IN}")

md = spark.read.option("header", True).option("inferSchema", False).csv(spark_path(MASTER_IN))
md = std_cols(md).dropDuplicates()

if "city" in md.columns:
    md = md.withColumnRenamed("city", "fua_id")

md = md.withColumn("year", safe_to_int("year"))
md = md.withColumn("month", safe_to_int("month") if "month" in md.columns else F.lit(None).cast("int"))

if "country_code" in md.columns:
    md = md.withColumn("geo_std", F.upper(F.trim(F.col("country_code").cast("string"))))
elif "fua_id" in md.columns:
    md = md.withColumn("geo_std", F.upper(F.regexp_extract("fua_id", r"_([A-Z]{2,3})$", 1)))

if "country_name" in md.columns:
    md = md.withColumn("country_name", F.trim(F.col("country_name").cast("string")))

num_cols = [
    "ndvi_mean","ndvi_median","ndvi_std","ndvi_gini","ndvi_p10","ndvi_p90",
    "pct_grey_area","pct_green_area",
    "no2_mean","no2_max","no2_min","no2_std",
    "ndvi_slope",
    "imperviousness_mean","imperviousness_pct_cover","imperviousness_pct_dense",
    "gdp_per_capita",
    "fin1_sector_avg_price","fin1_sector_avg_volatility",
    "fin2_sector_avg_pe","fin2_sector_avg_beta","fin2_sector_avg_return","fin2_sector_avg_volatility",
]
for c in num_cols:
    if c in md.columns:
        md = md.withColumn(c, F.col(c).cast("double"))

def parse_list_col(df, colname: str, outname: str | None = None):
    outname = outname or colname
    cleaned = F.trim(F.col(colname).cast("string"))
    as_json = F.regexp_replace(cleaned, r"^'$|^\"$|\"$|'$", "")
    as_json = F.regexp_replace(as_json, r"'", '"')
    fallback_json = F.when(
        (F.length(cleaned) > 0) & (~cleaned.startswith("[")) & (cleaned.contains(",")),
        F.concat(F.lit('["'), F.regexp_replace(cleaned, r"\s*,\s*", '","'), F.lit('"]'))
    ).otherwise(as_json)
    arr = F.from_json(fallback_json, T.ArrayType(T.StringType()))
    return df.withColumn(
        outname,
        F.when((cleaned.isNull()) | (F.length(cleaned) == 0), F.array().cast(T.ArrayType(T.StringType())))
         .otherwise(F.coalesce(arr, F.array().cast(T.ArrayType(T.StringType()))))
    )

for lc in ["fin1_tickers","fin1_companies","fin2_tickers"]:
    if lc in md.columns:
        md = parse_list_col(md, lc, lc)

if "fin1_tickers" in md.columns:
    md = md.withColumn("fin1_n_tickers", F.size(F.col("fin1_tickers")))
if "fin2_tickers" in md.columns:
    md = md.withColumn("fin2_n_tickers", F.size(F.col("fin2_tickers")))

if "ndvi_mean" in md.columns:
    md = md.withColumn("ndvi_out_of_range", ((F.col("ndvi_mean") < -1) | (F.col("ndvi_mean") > 1)).cast("int"))

for c in ["pct_grey_area","pct_green_area","imperviousness_pct_cover","imperviousness_pct_dense"]:
    if c in md.columns:
        md = md.withColumn(f"{c}_out_of_range", ((F.col(c) < 0) | (F.col(c) > 100)).cast("int"))

if "ndvi_slope" in md.columns:
    md = md.withColumn("turning_point_flag", (F.abs(F.col("ndvi_slope")) < F.lit(0.01)).cast("int"))

# 13A NO2 por media ciudad-año
NO2_COLS = [c for c in ["no2_mean","no2_max","no2_min","no2_std"] if c in md.columns]
if len(NO2_COLS) > 0 and ("fua_id" in md.columns) and ("year" in md.columns):
    no2_stats = md.groupBy("fua_id","year").agg(*[F.avg(c).alias(f"{c}__cy_mean") for c in NO2_COLS])
    md = md.join(no2_stats, on=["fua_id","year"], how="left")
    for c in NO2_COLS:
        md = md.withColumn(c, F.coalesce(F.col(c), F.col(f"{c}__cy_mean"))).drop(f"{c}__cy_mean")
    print("✅ NO2 imputado por media ciudad-año.")

# 13B Imperviousness por valor 2018 ciudad
IMP_COLS = [c for c in ["imperviousness_mean","imperviousness_pct_cover","imperviousness_pct_dense"] if c in md.columns]
if len(IMP_COLS) > 0 and ("fua_id" in md.columns) and ("year" in md.columns):
    base2018 = (
        md.filter(F.col("year") == 2018)
          .select("fua_id", *[F.col(c).alias(f"{c}__2018") for c in IMP_COLS])
          .dropDuplicates(["fua_id"])
    )
    md = md.join(base2018, on=["fua_id"], how="left")
    for c in IMP_COLS:
        md = md.withColumn(c, F.coalesce(F.col(c), F.col(f"{c}__2018"))).drop(f"{c}__2018")
    print("✅ Imperviousness imputado usando valor 2018 por ciudad.")

# 13C FIN proxies por país-año
FIN_COLS = [c for c in [
    "fin1_sector_avg_price","fin1_sector_avg_volatility",
    "fin2_sector_avg_pe","fin2_sector_avg_beta","fin2_sector_avg_return","fin2_sector_avg_volatility",
] if c in md.columns]

if len(FIN_COLS) > 0 and ("geo_std" in md.columns) and ("year" in md.columns):
    fin_base = (
        md.groupBy("geo_std","year")
          .agg(*[F.avg(c).alias(f"{c}__base") for c in FIN_COLS])
    )

    proxy_rows = []
    for target, proxies in FINANCE_PROXY_MAP.items():
        for pxy in proxies:
            proxy_rows.append((target, pxy))
    proxy_map_df = spark.createDataFrame(proxy_rows, ["geo_std", "proxy_geo_std"]).dropDuplicates()

    fin_proxy = (
        proxy_map_df.join(
            fin_base.select(F.col("geo_std").alias("proxy_geo_std"), "year", *[F.col(f"{c}__base") for c in FIN_COLS]),
            on=["proxy_geo_std"], how="left"
        )
        .groupBy("geo_std", "year")
        .agg(*[F.avg(F.col(f"{c}__base")).alias(f"{c}__proxy") for c in FIN_COLS])
    )

    md = md.join(fin_proxy, on=["geo_std","year"], how="left")
    for c in FIN_COLS:
        md = md.withColumn(c, F.coalesce(F.col(c), F.col(f"{c}__proxy"))).drop(f"{c}__proxy")
    print("✅ FIN imputado por proxies de país (FINANCE_PROXY_MAP).")

# Guardar MASTER a SQL
write_sql(md, table="master_dataset", mode="overwrite")

# Validación rápida
chk = read_sql("master_dataset")
if chk is not None:
    print("➡️ master_dataset | filas:", chk.count(), "| cols:", len(chk.columns))
    chk.show(5, truncate=False)

print("\n✅ Pipeline finalizado. Todo guardado en Postgres (Docker).")
