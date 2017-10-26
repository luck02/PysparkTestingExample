from pyspark.sql import SparkSession, DataFrame


def join_by_ids(dataframe1: DataFrame , dataframe2: DataFrame):
    return dataframe1.join(dataframe2, on="id", how="inner")


def aggregate_by_date(spark_session: SparkSession, dataframe1: DataFrame):
    dataframe1.createOrReplaceTempView("dataframe1")
    grouped = spark_session.sql("""
        select 
          to_date(date) as date,
          sum(amount) amount
        from 
          dataframe1
        group by 
          to_date(date)
    """)
    return grouped
