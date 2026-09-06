from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_currency_column(orders_df, country_currency_df, exchange_rates_df):
    #Enrichit les commandes avec la devise et calcule sous_total_local

    # 1. Sélection  de la colonne de pays
    country_col = (
        "ship_country"
        if "ship_country" in orders_df.columns
        else "customer_country"
    )

    # 2. Jointure pour ajouter la devise du pays
    enriched_df = orders_df.join(
        country_currency_df,
        orders_df[country_col] == country_currency_df["country"],
        "left",
    ).drop("country")

    # 3. Devise par défaut (USD) si absente
    enriched_df = enriched_df.withColumn(
        "currency",
        F.coalesce(F.col("currency"), F.lit("USD")),
    )

    # 4. Normalisation de exchange_rates_df
    if "rates" in exchange_rates_df.columns:
        # Extraire les noms de champs de la STRUCT rates
        rates_schema = exchange_rates_df.schema["rates"].dataType
        
        if hasattr(rates_schema, "fields"):
            # Si rates est une STRUCT, on la transforme en MAP avant d'exploser
            map_expr = []
            for field in rates_schema.fields:
                map_expr.extend([F.lit(field.name), F.col(f"rates.{field.name}").cast("double")])
            
            rates_clean = exchange_rates_df.select(
                F.explode(F.create_map(*map_expr)).alias("currency", "rate")
            )
        else:
            # Si c'est déjà une MAP
            rates_clean = exchange_rates_df.select(
                F.explode(F.col("rates")).alias("currency", "rate")
            )
    else:
        # Si DataFrame plat de test
        rate_col = (
            "rate"
            if "rate" in exchange_rates_df.columns
            else "exchange_rate"
        )
        rates_clean = exchange_rates_df.select(
            "currency", F.col(rate_col).alias("rate")
        )

    # 5. Jointure avec les taux de change nettoyés
    enriched_df = enriched_df.join(
        rates_clean,
        on="currency",
        how="left",
    )

    # 6. Calcul de sous_total_local
    enriched_df = (
        enriched_df.withColumn("rate", F.coalesce(F.col("rate"), F.lit(1.0)))
        .withColumn(
            "sous_total_local",
            F.round(F.col("sous_total") * F.col("rate"), 2),
        )
        .drop("rate")
    )

    return enriched_df