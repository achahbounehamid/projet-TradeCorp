import os          
from dotenv import load_dotenv 
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, initcap, upper, to_date
from pyspark.sql.types import DoubleType, IntegerType 
#============Connexion=================#

# 1. Charger le fichier .env
load_dotenv()

# 2. Récupérer et VERIFIER les variables d'environnement
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
azure_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

# 3. Connexion
account_url = "https://hamidstockage.blob.core.windows.net"
azure_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
blob_service_client = BlobServiceClient(account_url, credential=azure_key)
print("liste des containers")
for containers in blob_service_client.list_containers():
     print(containers.name)

#==================Fonction des transformations===============#

def clean_customers(df):
    clean_customer = df \
        .withColumn("company_name", trim(col("company_name"))) \
        .withColumn("contact_name", initcap(col("contact_name"))) \
        .withColumn("country", upper(col("country"))) \
        .dropDuplicates(["customer_id"])

    return clean_customer

def clean_orders(df):
    df_clean = df \
        .withColumn("is_shipped", col("shipped_date").isNotNull() & (col("shipped_date") != "")) \
        .dropna(subset=["shipped_date"]) \
        .withColumn("order_date", to_date(col("order_date"))) \
        .withColumn("required_date", to_date(col("required_date"))) \
        .withColumn("shipped_date", to_date(col("shipped_date"))) \
        .withColumn("freight", col("freight").cast(DoubleType())) \
        .withColumnRenamed("ship_via", "shipper_id")

    return df_clean

def clean_order_details(df):
    df_order_detail = df \
       .withColumn("unit_price", col("unit_price").cast(DoubleType)) \
       .withColumn("quantity", col("quantity").cast(IntegerType())) \
       .withColumn("discount", col("discount").cast(DoubleType)) \
       .withColumnRenamed("unit_price", "prix_unitaire")\
       .withColumnsRenamed("quantuty","quantite")

    return df_order_detail

def add_sous_total(df):
    df_add_sous_total = df \
    .w
    .
    return df_add_sous_total 

     
     
     