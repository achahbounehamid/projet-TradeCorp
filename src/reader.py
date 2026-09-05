import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.utils import download_csv


def load_raw_data(spark):
    # 1. Télécharge les fichiers depuis Azure
    download_csv(container_name="raw")

    dataframes = {}
    tmp_dir = "/home/jovyan/work/data/tmp"

    # 2. Parcours du dossier temporaire
    for root, _, files in os.walk(tmp_dir):
        for file_name in files:
            # Ignore les fichiers cachés ou les checkpoints
            if file_name.startswith(".") or "checkpoint" in file_name:
                continue

            file_path = os.path.join(root, file_name)
            table_name, ext = os.path.splitext(file_name)
            ext_lower = ext.lower()

            if ext_lower == ".csv":
                dataframes[table_name] = spark.read.csv(
                    file_path, header=True, inferSchema=True
                )
            elif ext_lower == ".json":
                # On force explicitement la clé exchange_rates
                dataframes["exchange_rates"] = spark.read.option(
                    "multiline", "true"
                ).json(file_path)

    # 3. si exchange_rates n'était pas sur Azure, recherche en local
    if "exchange_rates" not in dataframes:
        local_json = "/home/jovyan/work/data/exchange_rates.json"
        if os.path.exists(local_json):
            dataframes["exchange_rates"] = spark.read.option(
                "multiline", "true"
            ).json(local_json)

    return dataframes


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TestReader").getOrCreate()
    dfs = load_raw_data(spark)
    print("Clés chargées dans dataframes :", list(dfs.keys()))
    spark.stop()