from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


def read_file(spark: SparkSession, data_source: str) -> DataFrame:
    # Loading CSV file
    return spark.read.option("header", "true").csv(data_source)


def rename_columns(df: DataFrame, table_name: str) -> DataFrame:
    temp_view = []
    df = df.select(
        col("Name").alias("Restaurant_Name"),
        col("Violation Type").alias("Violation Level"),
    )
    # Creating a temporary view
    df.createOrReplaceTempView(table_name)

    return df
