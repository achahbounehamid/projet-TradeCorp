import json
import logging
import os
import sys
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
# Chargement des variables d'environnement
load_dotenv("/home/jovyan/.env")
# Configuration des logs pour qu'ils soient capturés par Airflow
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Endpoint API pour les taux de change (Devise pivot : USD)
API_URL = "https://api.exchangerate-api.com/v4/latest/USD"


def fetch_and_upload_exchange_rates():
    # 1. Récupération des données depuis l'API
    logging.info(f"Récupération des taux de change depuis {API_URL}...")
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    raw_json_data = response.json()

    # 2. Authentification ADLS Gen2 / Blob Storage via les variables d'environnement
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    
    # Vérification de la présence des secrets d'accès
    if not account_url or not account_key:
        raise ValueError("Les variables AZURE_STORAGE_ACCOUNT_URL et AZURE_STORAGE_ACCOUNT_KEY doivent être définies.")

    #Conversion du endpoint Data Lake (dfs) en endpoint Blob compatible avec BlobSe  
    blob_url = account_url.replace(".dfs.core.windows.net", ".blob.core.windows.net")

    blob_service_client = BlobServiceClient(
        account_url=blob_url,
        credential=account_key
    )

    # 3. Upload vers raw/reference/exchange_rates.json
    blob_client = blob_service_client.get_blob_client(
        container="raw",
        blob="reference/exchange_rates.json"
    )

    json_bytes = json.dumps(raw_json_data, indent=2).encode("utf-8")
    blob_client.upload_blob(json_bytes, overwrite=True)

    logging.info("[SUCCESS] Taux mis à jour dans raw/reference/exchange_rates.json")


if __name__ == "__main__":
    fetch_and_upload_exchange_rates()