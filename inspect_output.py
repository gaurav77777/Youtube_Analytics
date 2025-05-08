from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Inspect").getOrCreate()
df = spark.read.parquet("file:///mnt/sda4/Project/youtube-analytics/output/processed_data")


#df = spark.read.parquet("output/processed_data")
df.show(10)

spark.stop()
