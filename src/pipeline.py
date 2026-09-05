import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.enrichement import enrich_orders
from src.reader import load_raw_data
from src.transformer import build_enriched as transform_data
from src.writer import write_to_clean


def run_pipeline():
    print("=== DÉMARRAGE DU PIPELINE ETL TRADECORP ===")
    spark = SparkSession.builder.appName("TradeCorp_ETL_Pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("1 Téléchargement et lecture des données brutes...")
        raw_dfs = load_raw_data(spark)
        print("Clés disponibles dans raw_dfs :", list(raw_dfs.keys()))

        print("2 Transformation et nettoyage des données...")
        transformed_df = transform_data(raw_dfs)

        print("3 Enrichissement devises et taux de change...")
        enriched_df = enrich_orders(
            transformed_df,
            raw_dfs["country_currency"],
            raw_dfs["exchange_rates"]
        )

        print("4 Sauvegarde en Parquet dans le conteneur 'clean'...")
        write_to_clean(enriched_df)

        print("\n=== LE PIPELINE S'EST EXÉCUTÉ AVEC SUCCÈS ! ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()