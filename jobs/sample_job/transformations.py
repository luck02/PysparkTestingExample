from pyspark.sql import SparkSession, DataFrame


def join_by_ids(dataframe1: DataFrame , dataframe2: DataFrame):
    return dataframe1.join(dataframe2, on="id", how="inner")


def aggregate_by_date(dataframe1: DataFrame, dataframe2: DataFrame):
    return None
