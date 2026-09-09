import os
from pathlib import Path
import shutil
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.utils import get_azure_blob_client


def write_to_clean(df, container_name="clean"):
    """Écrit le DataFrame PySpark au format Parquet puis le téléverse sur Azure dans le conteneur 'clean'."""
    # Dossier temporaire interne au conteneur Docker sans conflit de permissions hôte
    local_output_dir = "/tmp/data/clean_output"

    # Nettoyage préalable du dossier temporaire local s'il existe déjà
    if os.path.exists(local_output_dir):
        shutil.rmtree(local_output_dir)

    # 1. Écriture temporaire au format Parquet en local via PySpark
    print("Écriture des données au format Parquet en local...")
    df.write.mode("overwrite").parquet(local_output_dir)

    # 2. Récupération du client Azure Blob 
    blob_service_client = get_azure_blob_client()
    container_client = blob_service_client.get_container_client(container_name)

    # Création du conteneur 'clean' s'il n'existe pas encore sur Azure
    try:
        container_client.create_container()
    except Exception:
        pass 

    # 3. Téléversement des fichiers Parquet générés vers le conteneur 'clean'
    print(f"Téléversement vers le conteneur Azure '{container_name}'...")
    for root, _, files in os.walk(local_output_dir):
        for file in files:
            # Ignore les fichiers système cachés de checksum (.crc)
            if file.startswith("."):
                continue

            local_file_path = os.path.join(root, file)

            # Reconstitution de la structure de dossier sur Azure
            relative_path = os.path.relpath(local_file_path, local_output_dir)
            blob_path = f"tradecorp_enriched.parquet/{relative_path}"

            blob_client = container_client.get_blob_client(blob_path)

            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print(f" -> Téléversé : {blob_path}")

    print("--- SAUVEGARDE SUR AZURE TERMINÉE AVEC SUCCÈS ---")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TestWriter").getOrCreate()
    test_df = spark.createDataFrame([(1, "TradeCorp")], ["id", "company"])
    write_to_clean(test_df)
    spark.stop()