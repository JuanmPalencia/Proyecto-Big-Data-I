# ============================================================
# 11) EXPORTS A CSV PARA BBDD / POWER BI
# ============================================================
EXPORT_DIR = PROJECT_ROOT / "exports"
EXPORT_DIR.mkdir(exist_ok=True)

datasets_to_export = [
    ("eurostat_panel", MODEL_READY / "eurostat_panel"),
    ("investeu_country", MODEL_READY / "investeu_country"),
    ("ine_timeseries", MODEL_READY / "ine_timeseries"),
    ("finance", MODEL_READY / "finance"),
]

for name, path in datasets_to_export:
    if not path.exists():
        print(f"⚠ No se exporta {name}: {path} no existe")
        continue
    df_exp = spark.read.parquet(str(path))
    out_dir = EXPORT_DIR / name
    (
        df_exp
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(out_dir))
    )
    print(f"📤 Exportado {name} a {out_dir}")

print("✅ Pipeline completo ejecutado.")

