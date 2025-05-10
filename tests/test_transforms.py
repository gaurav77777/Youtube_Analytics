# tests/test_transforms.py

import pytest
from pyspark.sql import SparkSession
from src.transforms import clean_data, analyze_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Test YouTube ETL") \
        .getOrCreate()

def test_clean_data(spark):
    # Sample input with nulls and wrong types
    data = [
        ("id1", "Video A", "Channel X", "22", "1000", "100", "10"),
        ("id2", "Video B", "Channel Y", "24", None, "50", "5"),
        ("id3", "Video C", "Channel Z", "25", "2000", "150", None)
    ]
    columns = ["video_id", "title", "channel_title", "category_id", "views", "likes", "dislikes"]
    df = spark.createDataFrame(data, columns)

    # Clean data
    cleaned_df = clean_data(df)

    # Expect only rows with complete numeric data to remain
    assert cleaned_df.count() == 1

    # Check schema
    assert dict(cleaned_df.dtypes)["views"] == "int"

def test_analyze_data(spark):
    # Sample input DataFrame
    data = [
        ("Channel A", 1000),
        ("Channel A", 2000),
        ("Channel B", 3000)
    ]
    columns = ["channel_title", "views"]
    df = spark.createDataFrame(data, columns)

    result_df = analyze_data(df)
    result = result_df.collect()

    # Validate total views per channel
    totals = {row["channel_title"]: row["total_views"] for row in result}
    assert totals["Channel A"] == 3000
    assert totals["Channel B"] == 3000
