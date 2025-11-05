from ine_api import obtener_datos_pib
from utils import limpiar_datos_pib, calcular_variacion_interanual

def main():
    print("📡 Descargando datos del PIB desde el INE...")
    df = obtener_datos_pib()

    print("🧹 Limpiando y procesando datos...")
    df_pib = limpiar_datos_pib(df)
    df_pib = calcular_variacion_interanual(df_pib)

    print("\n📊 Últimos datos del PIB:")
    print(df_pib.tail(8))

    # Guardar en CSV
    df_pib.to_csv("pib_ine.csv", index=False, encoding="utf-8")
    print("\n✅ Datos guardados en 'pib_ine.csv'")

if __name__ == "__main__":
    main()
