import os
from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from dotenv import load_dotenv

# Chargement dynamique de l'environnement d'exécution
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(BASE_DIR, ".env"))

#Configuration des arguments par défaut du DAG Airflow
default_args = {
    "owner": "tradecorp",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
# Configuration des Bind Mounts Docker (Volumes partagés hôte / conteneur)
mounts_list = [
    Mount(
        source="//c/Users/Utilisateur/Projets/projet-TradeCorp/src",
        target="/home/jovyan/src",
        type="bind",
    ),
    Mount(
        source="//c/Users/Utilisateur/Projets/projet-TradeCorp/data",
        target="/home/jovyan/data",
        type="bind",
    ),
    Mount(
        source="//c/Users/Utilisateur/Projets/projet-TradeCorp/.env",
        target="/home/jovyan/.env",
        type="bind",
    ),
]
# Déclaration du DAG Airflow et enchaînement des DockerOperators
with DAG(
    dag_id="tradecorp_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["tradecorp", "etl", "spark"],
) as dag:
    # Task 1 : Ingestion des taux de change
    fetch_exchange_rates_task = DockerOperator(
        task_id="fetch_exchange_rates",
        image="projet-tradecorp-spark",
        command="spark-submit /home/jovyan/src/fetch_exchange_rates.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="projet-tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=mounts_list,
        environment={
            "AZURE_STORAGE_ACCOUNT_URL": os.getenv("AZURE_STORAGE_ACCOUNT_URL"),
            "AZURE_STORAGE_ACCOUNT_KEY": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
        },
    )
    # Task 2 : Chargement et vérification des tables brutes
    reader_task = DockerOperator(
        task_id="reader",
        image="projet-tradecorp-spark",
        command="spark-submit /home/jovyan/src/reader.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="projet-tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=mounts_list,
        environment={
            "AZURE_STORAGE_ACCOUNT_URL": os.getenv("AZURE_STORAGE_ACCOUNT_URL"),
            "AZURE_STORAGE_ACCOUNT_KEY": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
        },
    )
    # Task 3 : Nettoyage, transformations et jointures enrichies
    transformer_task = DockerOperator(
        task_id="transformer",
        image="projet-tradecorp-spark",
        command="spark-submit /home/jovyan/src/transformer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="projet-tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=mounts_list,
        environment={
            "AZURE_STORAGE_ACCOUNT_URL": os.getenv("AZURE_STORAGE_ACCOUNT_URL"),
            "AZURE_STORAGE_ACCOUNT_KEY": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
        },
    )
    # Task 4 : Écriture au format Parquet et téléversement vers Azure
    writer_task = DockerOperator(
        task_id="writer",
        image="projet-tradecorp-spark",
        command="spark-submit /home/jovyan/src/writer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="projet-tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=mounts_list,
        environment={
            "AZURE_STORAGE_ACCOUNT_URL": os.getenv("AZURE_STORAGE_ACCOUNT_URL"),
            "AZURE_STORAGE_ACCOUNT_KEY": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
        },
    )

    # Indentation bien intégrée dans le bloc "with DAG"
    fetch_exchange_rates_task >> reader_task >> transformer_task >> writer_task