from pyspark.sql.functions import col, desc
from src.utils import get_logger


logger = get_logger("Transforms")

def clean_data(df):
    logger.info("Selecting and casting relevant columns...")
    return df.select(
        "video_id", "title", "channel_title", "category_id",
        col("views").cast("int"),
        col("likes").cast("int"),
        col("dislikes").cast("int")
    ).na.drop()

def analyze_data(df):
    logger.info("Grouping by channel_title and summing views...")
    return df.groupBy("channel_title") \
             .sum("views") \
             .withColumnRenamed("sum(views)", "total_views") \
             .orderBy(desc("total_views"))
