import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pyspark.sql.functions import col, concat, initcap, lit, round, to_date, trim, upper, when

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_PATH)


def get_azure_blob_client():
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    azure_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

    if not account_url or not azure_key:
        raise ValueError(f"Identifiants Azure manquants dans le fichier : {ENV_PATH}")

    return BlobServiceClient(account_url, credential=azure_key)


def download_csv(container_name="raw"):
    blob_service_client = get_azure_blob_client()
    container_client = blob_service_client.get_container_client(container_name)

    # Modification : Utilisation du dossier /tmp/data/raw pour garantir les droits d'écriture
    local_path = "/tmp/data/raw"
    os.makedirs(local_path, exist_ok=True)

    print("Début du téléchargement depuis Azure...")
    blobs = list(container_client.list_blobs())

    if not blobs:
        print("  Aucun fichier trouvé dans le conteneur Azure.")
        return

    for blob in blobs:
        file_name = blob.name
        blob_client = container_client.get_blob_client(file_name)
        download_file_path = os.path.join(local_path, file_name)

        # Crée le sous-dossier local si nécessaire
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        print(f" -> Fichier {file_name} téléchargé avec succès.")


# Fonctions de nettoyage et transformation PySpark

def clean_customers(df):
    return (
        df.withColumn("company_name", trim(col("company_name")))
        .withColumn("contact_name", initcap(trim(col("contact_name"))))
        .withColumn("country", upper(trim(col("country"))))
        .dropDuplicates(["customer_id"])
    )


def clean_orders(df):
    return (
        df.withColumn(
            "is_shipped",
            col("shipped_date").isNotNull() & (trim(col("shipped_date")) != ""),
        )
        .withColumn(
            "order_date",
            when(trim(col("order_date")) == "", None).otherwise(to_date(col("order_date"))),
        )
        .withColumn(
            "required_date",
            when(trim(col("required_date")) == "", None).otherwise(to_date(col("required_date"))),
        )
        .withColumn(
            "shipped_date",
            when(trim(col("shipped_date")) == "", None).otherwise(to_date(col("shipped_date"))),
        )
        .withColumn("freight", col("freight").cast("double"))
        .withColumnRenamed("ship_via", "shipper_id")
    )


def clean_order_details(df):
    return (
        df.withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("discount", col("discount").cast("double"))
        .withColumnRenamed("unit_price", "prix_unitaire")
        .withColumnRenamed("quantity", "quantite")
    )


def add_sous_total(df):
    return df.withColumn(
        "sous_total",
        round(col("prix_unitaire") * col("quantite") * (1 - col("discount")), 2),
    )


def clean_employees(df):
    return df.select(
        "employee_id",
        "first_name",
        "last_name",
        "title",
        "hire_date",
        "city",
        "country",
    ).withColumn(
        "full_name", concat(col("first_name"), lit(" "), col("last_name"))
    )


def clean_products(df):
    return df.withColumn("unit_price", col("unit_price").cast("double")).withColumn(
        "en_stock", col("units_in_stock") > 0 
    )