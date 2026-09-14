import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.utils import download_csv


def load_raw_data(spark):
    # 1. Téléchargement des données depuis le conteneur 'raw' d'Azure ADLS Gen2
    download_csv(container_name="raw")

    dataframes = {}
    tmp_dir = "/home/jovyan/data/raw"

    # 2. Parcours récursif du répertoire temporaire et lecture PySpark
    for root, _, files in os.walk(tmp_dir):
        for file_name in files:
            # Filtrage des fichiers temporaires système, des checkpoints Spark et des cachés
            if file_name.startswith(".") or "checkpoint" in file_name:
                continue

            file_path = os.path.join(root, file_name)
            table_name, ext = os.path.splitext(file_name)
            ext_lower = ext.lower()
            # Ingestion des fichiers structurés CSV
            if ext_lower == ".csv":
                dataframes[table_name] = spark.read.csv(
                    file_path, header=True, inferSchema=True
                )
            # Ingestion des fichiers semi-structurés JSON    
            elif ext_lower == ".json":
                # Utilise le nom du fichier JSON comme clé dynamiquement
                dataframes[table_name] = spark.read.option(
                    "multiline", "true"
                ).json(file_path)

    # 3. Fallback local si exchange_rates n'était pas sur Azure
    if "exchange_rates" not in dataframes:
        local_json = "/home/jovyan/data/exchange_rates.json"
        if os.path.exists(local_json):
            dataframes["exchange_rates"] = spark.read.option(
                "multiline", "true"
            ).json(local_json)

    return dataframes


if __name__ == "__main__":
    # Définition sécurisée de la SparkSession dans l'environnement Docker
    spark = SparkSession.builder \
        .appName("TradeCorp_Reader") \
        .getOrCreate()

    dfs = load_raw_data(spark)

    print("\n--- DataFrames chargés ---")
    for key in dfs.keys():
        print(f" - {key}")

    # Vérifications des tables de référence de devises et taux de change
    if "country_currency" in dfs:
        print("\n[OK] Table 'country_currency' chargée :")
        dfs["country_currency"].show(5)

    if "exchange_rates" in dfs:
        print("\n[OK] Table 'exchange_rates' chargée :")
        dfs["exchange_rates"].show(5)

    spark.stop()