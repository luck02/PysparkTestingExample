from pyspark.sql import SparkSession
import os


def get_derps(spark_session: SparkSession):
    df = spark_session.read.jdbc(
        url=os.environ["environVar"],
        table="sample.testtable"
    )
    return df

