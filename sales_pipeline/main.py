from sales_pipeline.bronze.ingestion import ingest_ventes_paris
from sales_pipeline.silver.cleaning import clean_data
from sales_pipeline.gold.aggregation import aggregate_data
from sales_pipeline.utils.spark_session import get_spark_session


def func():
    spark = get_spark_session("SalesPipeline")

    try:
        ingest_ventes_paris(spark)
        print("==== Bronze OK ====")

        clean_data(spark)
        print("==== Silver OK ====")

        aggregate_data(spark)
        print("==== Gold OK ====")

        print("Pipeline terminé avec succès !")

    except Exception as e:
        print("Erreur dans le pipeline :", e)

if __name__ == "__main__":
    func()
