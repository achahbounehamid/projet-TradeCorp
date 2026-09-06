import logging
import sys
from pathlib import Path

# Ajout de la racine du projet dans le PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

# Imports des modules internes
from src.enrichement import add_currency_column
from src.reader import load_raw_data
from src.transformer import build_enriched as transform_data
from src.writer import write_to_clean

# Configuration du logging structuré et horodaté
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("PipelineETL")


def run_pipeline():
    logger.info("=== DÉMARRAGE DU PIPELINE ETL TRADECORP ===")
    spark = None

    try:
        # Initialisation de Spark
        spark = (
            SparkSession.builder.appName("TradeCorp_ETL_Pipeline").getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Step 1: Lecture
        logger.info("1. Téléchargement et lecture des données brutes...")
        raw_dfs = load_raw_data(spark)
        logger.info(f"Clés disponibles dans raw_dfs : {list(raw_dfs.keys())}")

        # Step 2: Transformation
        logger.info("2. Transformation et nettoyage des données...")
        transformed_df = transform_data(raw_dfs)

        # Step 3: Enrichissement
        logger.info("3. Enrichissement devises et taux de change...")
        enriched_df = add_currency_column(
            transformed_df,
            raw_dfs["country_currency"],
            raw_dfs["exchange_rates"],
        )
        # Affichage du schéma final dans les logs pour validation
        logger.info("Schéma du DataFrame enrichi :")
        enriched_df.printSchema()
        # Step 4: Écriture
        logger.info("4. Sauvegarde en Parquet dans le conteneur 'clean'...")
        write_to_clean(enriched_df)

        logger.info("=== LE PIPELINE S'EST EXÉCUTÉ AVEC SUCCÈS ! ===")

    except Exception as e:
        logger.error(f"Échec de l'exécution du pipeline : {e}", exc_info=True)
        raise e

    finally:
        if spark is not None:
            logger.info("Fermeture propre de la session Spark...")
            spark.stop()


if __name__ == "__main__":
    run_pipeline()