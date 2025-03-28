from pyspark.sql import SparkSession
from src.transforms import clean_data, analyze_data

def run_etl():
    spark = SparkSession.builder.appName("YouTube Analytics").getOrCreate()

    df = spark.read.option("header", True).csv("data/USvideos.csv")

    df_clean = clean_data(df)
    df_result = analyze_data(df_clean)

    df_result.write.mode("overwrite").parquet("output/processed_data")

    spark.stop()
