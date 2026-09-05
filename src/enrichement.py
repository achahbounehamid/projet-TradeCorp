from pyspark.sql import functions as F


def enrich_orders(orders_df, country_currency_df, exchange_rates_df):
    """Enrichit les commandes avec la gestion des devises et conversions."""

    # 1. Sélection dynamique de la colonne de pays disponible
    country_col = "ship_country" if "ship_country" in orders_df.columns else "customer_country"

    # 2. Jointure pour ajouter la devise
    enriched_df = orders_df.join(
        country_currency_df,
        orders_df[country_col] == country_currency_df["country"],
        "left",
    ).drop(country_currency_df["country"])

    # 3. Devise par défaut (USD)
    enriched_df = enriched_df.withColumn(
        "currency",
        F.coalesce(F.col("currency"), F.lit("USD")),
    )

    # 4. Jointure pour ajouter le taux de change
    enriched_df = enriched_df.join(
        exchange_rates_df,
        enriched_df["currency"] == exchange_rates_df["currency"],
        "left",
    ).drop(exchange_rates_df["currency"])

    # 5. Calcul du montant en USD (gestion du cas USD où taux = 1)
    enriched_df = enriched_df.withColumn(
        "total_amount_usd",
        F.when(F.col("currency") == "USD", F.col("sous_total"))
        .otherwise(F.round(F.col("sous_total") / F.col("exchange_rate"), 2))
    )

    # 6. Extraction du taux EUR et calcul du montant en EUR
    eur_rate_row = exchange_rates_df.filter(F.col("currency") == "EUR").select("exchange_rate").first()
    eur_rate = eur_rate_row["exchange_rate"] if eur_rate_row else 0.85

    enriched_df = enriched_df.withColumn(
        "total_amount_eur",
        F.round(F.col("total_amount_usd") * F.lit(eur_rate), 2)
    )

    return enriched_df