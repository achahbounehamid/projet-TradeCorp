import os          
from dotenv import load_dotenv 
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, initcap, upper, to_date, lit, concat
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
    df_clean_orders = df \
        .withColumn("is_shipped", col("shipped_date").isNotNull() & (col("shipped_date") != "")) \
        .dropna(subset=["shipped_date"]) \
        .withColumn("order_date", to_date(col("order_date"))) \
        .withColumn("required_date", to_date(col("required_date"))) \
        .withColumn("shipped_date", to_date(col("shipped_date"))) \
        .withColumn("freight", col("freight").cast("double")) \
        .withColumnRenamed("ship_via", "shipper_id")

    return df_clean_orders

def clean_order_details(df):
    df_order_detail = df \
        .withColumn("unit_price", col("unit_price").cast("double")) \
        .withColumn("quantity", col("quantity").cast("integer")) \
        .withColumn("discount", col("discount").cast("double")) \
        .withColumnRenamed("unit_price", "prix_unitaire") \
        .withColumnRenamed("quantity", "quantite")

    return df_order_detail

def add_sous_total(df):
    df_add_sous_total = df \
        .withColumn("sous_total", round(col("prix_unitaire") * col("quantite") * (1 - col("discount")), 2))
    
    return df_add_sous_total 

def clean_employees(df):
    df_clean_employee = df \
        .select("employee_id", "first_name", "last_name", "title", "hire_date", "city", "country") \
        .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    
    return df_clean_employee

def clean_products(df):
    df_clean_product = df \
        .withColumn("unit_price", col("unit_price").cast("double")) \
        .withColumn("en_stock", col("units_in_stock") > 0)
    
    return df_clean_product

def build_enriched(dataframes):
    # 1. Nettoyage individuel des tables
    df_customers = clean_customers(dataframes["customers"])
    df_orders = clean_orders(dataframes["orders"])
    
    df_order_details = clean_order_details(dataframes["order_details"])
    df_order_details = add_sous_total(df_order_details)
    
    df_employees = clean_employees(dataframes["employees"]) 
    df_products = clean_products(dataframes["products"])   
    
    df_categories = dataframes["categories"]
    df_shippers = dataframes["shippers"]

    # 2. Ajout du nom de la catégorie à chaque produit
    df_products_enriched = df_products.join(df_categories, "category_id", "left")

    # 3. Renommage des colonnes conflictuelles
    df_customers_renamed = df_customers \
        .withColumnRenamed("company_name", "customer_name") \
        .withColumnRenamed("country", "customer_country") \
        .withColumnRenamed("city", "customer_city")
        
    df_shippers_renamed = df_shippers \
        .withColumnRenamed("company_name", "shipper_name")

    df_employees_renamed = df_employees \
        .withColumnRenamed("country", "employee_country") \
        .withColumnRenamed("city", "employee_city")

    # 4. Jointure globale de toutes les tables
    df_enriched = df_order_details \
        .join(df_orders, "order_id", "inner") \
        .join(df_customers_renamed, "customer_id", "left") \
        .join(df_products_enriched, "product_id", "left") \
        .join(df_employees_renamed, "employee_id", "left") \
        .join(df_shippers_renamed, "shipper_id", "left")

    return df_enriched


    



     