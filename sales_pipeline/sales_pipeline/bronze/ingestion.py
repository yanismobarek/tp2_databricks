from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def ingest_ventes_paris(spark: SparkSession, input_dir="dbfs:/Volumes/catalogym/raw/raw_data/boutique_paris/"):
    
    try:
        dbutils
    except NameError:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    archive_dir = input_dir + "archive/"

    files = [f.path for f in dbutils.fs.ls(input_dir) if f.path.endswith(".csv")]
    print("Found files:", files)

    for file_path in files:
        df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
        
        df = df.withColumn("Ingestion_Timestamp", F.current_timestamp())
        df = df.withColumn("File_Name", F.lit(file_path.split("/")[-1]))
        
        df.write.format("delta").option("mergeSchema", "true").mode("append") \
            .saveAsTable("catalogym.bronze.ventes_paris")
        
        archive_path = archive_dir + file_path.split("/")[-1]
        dbutils.fs.mv(file_path, archive_path, True)

    print("Ingestion Bronze terminée : catalogym.bronze.ventes_paris")
