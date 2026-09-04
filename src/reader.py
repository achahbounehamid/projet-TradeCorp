import os
from pathlib import Path
import sys

# Ajout du chemin racine pour éviter l'erreur ModuleNotFoundError
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.utils import FILES, download_csv


def load_raw_data(spark):
    # 1. Télécharge les fichiers depuis Azure Blob Storage
    download_csv(container_name="raw")

    dataframes = {}

    # 2. Lit les fichiers avec PySpark
    for file_name in FILES:
        table_name = file_name.replace(".csv", "")
        file_path = f"/home/jovyan/work/data/tmp/{file_name}"
        dataframes[table_name] = spark.read.csv(
            file_path, header=True, inferSchema=True
        )

    return dataframes


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dfs = load_raw_data(spark)
    print("Tous les DataFrames ont été chargés avec succès dans PySpark !")
    spark.stop()