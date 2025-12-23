from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def aggregate_data(spark: SparkSession):
    df_silver = spark.table("catalogym.silver.ventes_paris")

    df_CA_mois = (
        df_silver
        .groupBy("Year", "Month")
        .agg(F.sum("Montant_EUR").alias("CA_mois"))
    )

    df_CA_boutique_mois = (
        df_silver
        .groupBy("Year", "Month", "Nom_Boutique")
        .agg(F.sum("Montant_EUR").alias("CA_mois"))
    )

    df_classement_produit_valeur = (
        df_silver
        .groupBy("Nom_Produit")
        .agg(F.sum("Montant_EUR").alias("Prix_total"))
        .orderBy(F.desc("Prix_total"))
    )

    df_classement_produit_quantite = (
        df_silver
        .groupBy("Nom_Produit")
        .agg(F.sum("Quantité").alias("Nombre_vente"))
        .orderBy(F.desc("Nombre_vente"))
    )

    df_classement_produit_valeur.write.format("delta").mode("overwrite") \
        .saveAsTable("catalogym.gold.table_classement_valeur")

    df_classement_produit_quantite.write.format("delta").mode("overwrite") \
        .saveAsTable("catalogym.gold.table_classement_quantite")

    df_CA_mois.write.format("delta").mode("overwrite") \
        .saveAsTable("catalogym.gold.table_CA_mois")

    df_CA_boutique_mois.write.format("delta").mode("overwrite") \
        .saveAsTable("catalogym.gold.table_CA_boutique_mois")

    print("Agrégation Gold terminée : tables catalogym.gold.*")
