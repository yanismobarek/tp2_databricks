from sales_pipeline.silver.cleaning import clean_data

from pyspark.sql import functions as F
from pyspark.sql import types as T

def test_clean_data_adds_silver_columns(spark_session):
    clean_data(spark_session)
    df_silver = spark_session.table("catalogym.silver.ventes_paris")
    
    expected_silver_columns = ["Devise", "Nom_Boutique", "Ville", "Pays", "Montant_EUR", "Year", "Month"]
    for col in expected_silver_columns:
        assert col in df_silver.columns
    
    first_row = df_silver.limit(1).collect()[0]
    assert first_row["Ville"] == "Paris"
    assert first_row["Devise"] == "EUR"

def test_clean_data_date_extraction(spark_session):
    df_silver = spark_session.table("catalogym.silver.ventes_paris")
    
    sample = df_silver.select("Date_Vente", "Year", "Month").filter(F.col("Date_Vente").isNotNull()).first()
    
    if sample:
        assert sample["Year"] == sample["Date_Vente"].year
        assert sample["Month"] == sample["Date_Vente"].month

def test_silver_types_integrity(spark_session):
    df_silver = spark_session.table("catalogym.silver.ventes_paris")
    schema = {field.name: field.dataType for field in df_silver.schema}
    
    assert isinstance(schema["Year"], T.IntegerType)
    assert isinstance(schema["Month"], T.IntegerType)
    assert isinstance(schema["Montant_EUR"], (T.DoubleType, T.FloatType, T.DecimalType))