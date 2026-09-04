from pathlib import Path
import sys

# Ingestion du PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
from src.utils import (
    add_sous_total,
    clean_customers,
    clean_employees,
    clean_order_details,
    clean_orders,
    clean_products,
)


def build_enriched(dataframes):
    # 1. Nettoyage individuel via utils.py
    df_customers = clean_customers(dataframes["customers"])
    df_orders = clean_orders(dataframes["orders"])

    df_order_details = clean_order_details(dataframes["order_details"])
    df_order_details = add_sous_total(df_order_details)

    df_employees = clean_employees(dataframes["employees"])
    df_products = clean_products(dataframes["products"])

    df_categories = dataframes["categories"]
    df_shippers = dataframes["shippers"]

    # 2. Enrichissement des produits avec la catégorie
    df_products_enriched = df_products.join(
        df_categories, "category_id", "left"
    )

    # 3. Renommage des colonnes conflictuelles
    df_customers_renamed = (
        df_customers.withColumnRenamed("company_name", "customer_name")
        .withColumnRenamed("country", "customer_country")
        .withColumnRenamed("city", "customer_city")
    )

    df_shippers_renamed = df_shippers.withColumnRenamed(
        "company_name", "shipper_name"
    )

    df_employees_renamed = df_employees.withColumnRenamed(
        "country", "employee_country"
    ).withColumnRenamed("city", "employee_city")

    # 4. Jointure globale des 7 tables
    df_enriched = (
        df_order_details.join(df_orders, "order_id", "inner")
        .join(df_customers_renamed, "customer_id", "left")
        .join(df_products_enriched, "product_id", "left")
        .join(df_employees_renamed, "employee_id", "left")
        .join(df_shippers_renamed, "shipper_id", "left")
    )

    return df_enriched