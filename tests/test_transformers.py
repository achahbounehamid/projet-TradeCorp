import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Ingestion du PYTHONPATH
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import des fonctions à tester 
from src.enrichement import add_currency_column
from src.utils import add_sous_total, clean_customers, clean_orders


@pytest.fixture(scope="session")
def spark():
    """Récupère ou crée la SparkSession initialisée par spark-submit."""
    spark_session = SparkSession.builder.appName("TestTransformers").getOrCreate()
    yield spark_session


def test_clean_orders_shipped_date(spark):
    """Vérifie le traitement de shipped_date par clean_orders()."""
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("required_date", StringType(), True),
            StructField("shipped_date", StringType(), True),
            StructField("freight", StringType(), True),
            StructField("ship_via", StringType(), True),
        ]
    )

    data = [
        ("1", "2023-01-01", "2023-01-10", "2023-01-05", "10.0", "1"),
        ("2", "2023-01-02", "2023-01-11", None, "15.0", "2"),
    ]

    input_df = spark.createDataFrame(data, schema)
    cleaned_df = clean_orders(input_df)

    filtered_df = cleaned_df.filter("shipped_date IS NOT NULL")
    results = filtered_df.collect()

    assert len(results) == 1
    assert results[0]["order_id"] == "1"
    assert results[0]["is_shipped"] is True


def test_add_sous_total(spark):
    """Vérifie le calcul du sous-total : prix_unitaire * quantite * (1 - discount)."""
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("prix_unitaire", DoubleType(), True),
            StructField("quantite", IntegerType(), True),
            StructField("discount", DoubleType(), True),
        ]
    )

    data = [
        ("1001", 10.0, 2, 0.0),
        ("1002", 20.0, 5, 0.10),
        ("1003", 15.0, 1, 0.20),
    ]

    input_df = spark.createDataFrame(data, schema)
    result_df = add_sous_total(input_df)

    rows = {row["order_id"]: row["sous_total"] for row in result_df.collect()}

    assert rows["1001"] == 20.0
    assert abs(rows["1002"] - 90.0) < 1e-4
    assert abs(rows["1003"] - 12.0) < 1e-4


def test_clean_customers(spark):
    """Vérifie le nettoyage des espaces, la mise en majuscule de la 1re lettre de contact_name et le pays en majuscules."""
    schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("contact_name", StringType(), True),
            StructField("country", StringType(), True),
        ]
    )

    data = [
        ("C001", "  Company A ", "  jean dupont  ", " france "),
        ("C002", "Company B", "marie CURIE", "germany"),
    ]

    input_df = spark.createDataFrame(data, schema)
    result_df = clean_customers(input_df)

    rows = {row["customer_id"]: row for row in result_df.collect()}

    # Vérification pour C001
    assert rows["C001"]["contact_name"] == "Jean Dupont"
    assert rows["C001"]["country"] == "FRANCE"

    # Vérification pour C002
    assert rows["C002"]["contact_name"] == "Marie Curie"
    assert rows["C002"]["country"] == "GERMANY"


def test_add_currency_column(spark):
    """Vérifie l'enrichissement par la devise et le calcul de sous_total_local."""
    # 1. Jeux de données simulés (sans réseau)
    orders_data = [
        ("1001", "FRANCE", 100.0),   # Devise EUR -> 100 * 0.85 = 85.0
        ("1002", "USA", 50.0),       # Devise USD -> 50 * 1.0 = 50.0
        ("1003", "INCONNU", 30.0),   # Pays non répertorié -> fallback USD (30 * 1.0 = 30.0)
    ]
    orders_df = spark.createDataFrame(
        orders_data, ["order_id", "ship_country", "sous_total"]
    )

    country_curr_data = [("FRANCE", "EUR"), ("USA", "USD")]
    country_currency_df = spark.createDataFrame(
        country_curr_data, ["country", "currency"]
    )

    # Simulation du dictionnaire/table de taux sous forme de DataFrame local
    rates_data = [("EUR", 0.85), ("USD", 1.0)]
    exchange_rates_df = spark.createDataFrame(
        rates_data, ["currency", "rate"]
    )

    # 2. Exécution de la fonction
    result_df = add_currency_column(
        orders_df, country_currency_df, exchange_rates_df
    )
    rows = {row["order_id"]: row for row in result_df.collect()}

    # 3. Assertions (Vérification des colonnes et des résultats)
    assert "currency" in result_df.columns
    assert "sous_total_local" in result_df.columns

    # Vérification FRANCE (EUR)
    assert rows["1001"]["currency"] == "EUR"
    assert rows["1001"]["sous_total_local"] == 85.0

    # Vérification USA (USD)
    assert rows["1002"]["currency"] == "USD"
    assert rows["1002"]["sous_total_local"] == 50.0

    # Vérification Fallback
    assert rows["1003"]["currency"] == "USD"
    assert rows["1003"]["sous_total_local"] == 30.0