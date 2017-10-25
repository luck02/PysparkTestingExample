from jobs.sample_job import transformations
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from sparktestingbase.sqltestcase import SQLTestCase

df1_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("value", StringType(), True)
])

df1_fixture = [(1, "some_value"),
               (2, "some_other_value")]

df2_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df2_fixture = [
    (1, "name"),
    (2, "another name"),
    (3, "missing name")
]

df_expected_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", StringType(), True)]
)

df_expected_fixture = \
    [(1, "name", "some_value"),
     (2, "another name", "some_other_value")]


class test_transformations(SQLTestCase):
    def test_join_by_ids(self):
        spark_session = SparkSession(self.sc)
        df1 = spark_session.createDataFrame(df1_fixture, df1_schema)
        df2 = spark_session.createDataFrame(df2_fixture, df2_schema)

        df_expected = spark_session.createDataFrame(df_expected_fixture, df_expected_schema)
        df_expected.show()

        result = transformations.join_by_ids(df2, df1)
        result.show()
        SQLTestCase.assertDataFrameEqual(self, result, df_expected, "dfs don't match")


    def test_aggregate_by_date(self):
        spark_session = SparkSession(self.sc)

