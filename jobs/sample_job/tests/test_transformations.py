from jobs.sample_job import transformations
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from sparktestingbase.sqltestcase import SQLTestCase
import datetime


class TestTransformations(SQLTestCase):
    def test_join_by_ids(self):

        # fixtures
        df1_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True)
        ])

        df1_fixture = [(1, "some_value"),
                       (2, "some_other_value")]

        df2_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True)
        ])

        df2_fixture = [
            (1, "name"),
            (2, "another name"),
            (3, "missing name")
        ]

        df_expected_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True)]
        )

        df_expected_fixture = \
            [(1, "name", "some_value"),
             (2, "another name", "some_other_value")]

        # test
        spark_session = SparkSession(self.sc)
        df1 = spark_session.createDataFrame(df1_fixture, df1_schema)
        df2 = spark_session.createDataFrame(df2_fixture, df2_schema)

        df_expected = spark_session.createDataFrame(df_expected_fixture, df_expected_schema)
        result = transformations.join_by_ids(df2, df1)

        SQLTestCase.assertDataFrameEqual(self, result, df_expected, "dfs don't match")

    def test_aggregate_by_date(self):
        df1_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("amount", DoubleType(), False),
            StructField("date", DateType(), False)
        ])

        df1_fixture = [
            (1, 2.50, datetime.datetime(2017, 1, 1, 0, 0, 0, 0)),
            (2, 3.50, datetime.datetime(2017, 1, 1, 0, 1, 0, 0)),
            (3, 1.25, datetime.datetime(2017, 1, 2, 0, 2, 0, 0))
        ]

        df_expected_schema = StructType([
            StructField("date", DateType(), False),
            StructField("amount", DoubleType(), True)
        ])

        df_expected_fixture = [
            (datetime.date(2017, 1, 1), 6.0),
            (datetime.date(2017, 1, 2), 1.25)
        ]

        spark_session = SparkSession(self.sc)

        df1 = spark_session.createDataFrame(df1_fixture, df1_schema)

        df_expected = spark_session.createDataFrame(df_expected_fixture, df_expected_schema)

        result = transformations.aggregate_by_date(spark_session, df1)
        df1.show()
        result.show()

        SQLTestCase.assertDataFrameEqual(self, result, df_expected, "data frames do not match, aggregation failed")



