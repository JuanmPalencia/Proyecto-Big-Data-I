"""
hdfs_writer.py — GTP Utility
Escribe un pandas DataFrame como Parquet en HDFS usando PySpark local.
Se importa desde los modelos ML para persistir resultados en el Data Lake
además de la escritura en MariaDB (serving layer).

No lanza excepción si HDFS no está disponible: solo loguea un warning,
para que el pipeline no falle si se ejecuta sin Hadoop.
"""
import os


def write_df_to_hdfs(df_pandas, hdfs_path, partition_cols=None):
    """
    Escribe df_pandas como Parquet en hdfs_path (overwrite).
    Usa SPARK_MASTER del entorno (local[2] por defecto).
    Devuelve True si OK, False si falló.
    """
    if df_pandas is None or len(df_pandas) == 0:
        print(f"  [HDFS SKIP] DataFrame vacío, no se escribe en {hdfs_path}")
        return False

    try:
        from pyspark.sql import SparkSession

        master = os.environ.get("SPARK_MASTER", "local[2]")
        spark = (
            SparkSession.builder
            .appName("GTP-HDFSWriter")
            .master(master)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        # Spark no puede inferir el tipo de columnas que son enteramente NULL.
        # Castear esas columnas a float64 para que sean DoubleType en Spark.
        df_pandas = df_pandas.copy()
        for col in df_pandas.columns:
            if df_pandas[col].isna().all():
                df_pandas[col] = df_pandas[col].astype("float64")

        df_spark = spark.createDataFrame(df_pandas)
        writer = df_spark.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(hdfs_path)

        print(f"  [HDFS] {len(df_pandas):,} filas escritas → {hdfs_path}")
        spark.stop()
        return True

    except Exception as e:
        print(f"  [HDFS WARN] No se pudo escribir en {hdfs_path}: {e}")
        return False
