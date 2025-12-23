from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def clean_data(spark: SparkSession,starting_version=None):
    try:
        df_changes = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", last_version) \
            .table("catalogym.bronze.ventes_paris")
    except Exception:
        df_changes = spark.table("catalogym.bronze.ventes_paris")


    if df_changes.count() == 0:
        print("Pas de nouvelles données à traiter dans Silver")
        return

    df_silver = (
        df_changes
        .withColumn("Devise", F.lit("EUR"))
        .withColumn("Nom_Boutique", F.lit("boutique_paris"))
        .withColumn("Ville", F.lit("Paris"))
        .withColumn("Pays", F.lit("France"))
        .withColumn("Montant_EUR", F.col("Montant_Total"))
        .withColumn("Year", F.year("Date_Vente"))
        .withColumn("Month", F.month("Date_Vente"))
    )

    df_silver.write.format("delta").mode("append").saveAsTable("catalogym.silver.ventes_paris")

    print("Nettoyage Silver terminé : catalogym.silver.ventes_paris")
