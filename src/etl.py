from pyspark.sql import SparkSession
from src.transforms import clean_data, analyze_data
from src.utils import get_logger





logger = get_logger("ETL")



def run_etl():
    logger.info("Starting Spark Session...")
    spark = SparkSession.builder.appName("YouTube Analytics").getOrCreate()

    logger.info("Reading CSV file...")
    #df = spark.read.option("header", True).csv("data/USvideos.csv")

   
    # Correct local path to the file
    df = spark.read.csv("file:///mnt/sda4/Project/youtube-analytics/data/USvideos.csv", header=True, inferSchema=True)




    logger.info("Cleaning data...")
    df_clean = clean_data(df)
    df_clean.show()

    logger.info("Analyzing data...")
    df_result = analyze_data(df_clean)

    
    logger.info("Writing output to Parquet...")
    #df_result.write.mode("overwrite").parquet("output/processed_data")
    df_result.write.mode("overwrite").parquet("file:///mnt/sda4/Project/youtube-analytics/output/processed_data")

    logger.info("ETL process completed.")
    spark.stop()
