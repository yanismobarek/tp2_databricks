from sales_pipeline.gold.aggregation import aggregate_data 
from pyspark.sql import functions as F


def test_aggregate_data_tables_exist(spark_session):
    aggregate_data(spark_session)
    
    tables = [
        "catalogym.gold.table_classement_valeur",
        "catalogym.gold.table_classement_quantite",
        "catalogym.gold.table_CA_mois",
        "catalogym.gold.table_CA_boutique_mois"
    ]
    
    for t in tables:
        assert spark_session.catalog.tableExists(t), f"La table {t} n'a pas été créée."


def test_classement_valeur_is_ordered(spark_session):
    df_gold = spark_session.table("catalogym.gold.table_classement_valeur")
    
    valeurs = [row["Prix_total"] for row in df_gold.select("Prix_total").collect()]
    
    assert valeurs == sorted(valeurs, reverse=True)

def test_gold_schema_columns(spark_session):
    df_ca_boutique = spark_session.table("catalogym.gold.table_CA_boutique_mois")
    
    expected_cols = ["Year", "Month", "Nom_Boutique", "CA_mois"]
    for col in expected_cols:
        assert col in df_ca_boutique.columns