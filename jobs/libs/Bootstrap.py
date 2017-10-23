from pyspark.sql import SparkSession
import os


def get_spark_session(app_name = "spark_app"):
    master = os.environ["SPARK_MASTER"]
    spark = SparkSession.builder.\
        appName(app_name).\
        master(master).\
        getOrCreate()
    return spark
