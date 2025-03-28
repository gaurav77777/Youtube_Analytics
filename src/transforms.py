from pyspark.sql.functions import col, desc

def clean_data(df):
    return df.select(
        "video_id", "title", "channel_title", "category_id",
        col("views").cast("int"),
        col("likes").cast("int"),
        col("dislikes").cast("int")
    ).na.drop()

def analyze_data(df):
    return df.groupBy("channel_title") \
             .sum("views") \
             .withColumnRenamed("sum(views)", "total_views") \
             .orderBy(desc("total_views"))
