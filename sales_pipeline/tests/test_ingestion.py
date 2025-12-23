
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from unittest.mock import patch, MagicMock
from datetime import date, datetime
from pyspark.sql import types as T
from sales_pipeline.bronze.ingestion import ingest_ventes_paris


def test_ingest_ventes_paris_execution(spark_session):
    test_path = "dbfs:/Volumes/catalogym/raw/raw_data/boutique_paris/"
    ingest_ventes_paris(spark_session, input_dir=test_path)
    df_result = spark_session.table("catalogym.bronze.ventes_paris")
    
    assert df_result.count() >= 0
    assert "Ingestion_Timestamp" in df_result.columns

def test_columns_check(spark_session):
    df = spark_session.table("catalogym.bronze.ventes_paris")
    
    expected_cols= ["ID_Vente",
        "Date_Vente",
        "Nom_Produit",
        "Catégorie",
        "Prix_Unitaire",
        "Quantité",
        "Montant_Total",
        "Ingestion_Timestamp"
    ]

    for col in expected_cols:
        assert col in df.columns


def test_ingest_ventes_paris_timestamp_is_recent(spark_session):
    df = spark_session.table("catalogym.bronze.ventes_paris")
    
    # On récupère le max timestamp
    res = df.select(F.max("Ingestion_Timestamp")).collect()[0][0]
    
    if res is None:
        pytest.fail("La table est vide, aucune donnée n'a été ingérée.")
        
    last_ingestion_date = res.date()

    assert last_ingestion_date <= date.today()

def test_ventes_paris_data_types(spark_session):

    df = spark_session.table("catalogym.bronze.ventes_paris")
    actual_schema = {field.name: field.dataType for field in df.schema}
    
    expected_types = {
        "ID_Vente": T.IntegerType(),
        "Prix_Unitaire": T.DoubleType(),
        "Quantité": T.IntegerType(),
        "Montant_Total": T.DoubleType(),
        "Ingestion_Timestamp": T.TimestampType()
    }
    
    for col_name, expected_type in expected_types.items():
        assert col_name in actual_schema, f"La colonne {col_name} est absente de la table"
        
        actual_type = actual_schema[col_name]
        

        assert isinstance(actual_type, type(expected_type)), \
            f"Erreur sur {col_name}: attendu {expected_type}, obtenu {actual_type}"

def test_ingest_ventes_paris_no_empty_prices(spark_session):
    df = spark_session.table("catalogym.bronze.ventes_paris")
    
    invalid_prices = df.filter((F.col("Prix_Unitaire") <= 0) | (F.col("Prix_Unitaire").isNull())).count()
    
    assert invalid_prices == 0
