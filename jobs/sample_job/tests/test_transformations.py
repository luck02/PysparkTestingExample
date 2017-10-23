from jobs.sample_job import transformations
from pyspark.sql import SparkSession
from sparktestingbase.testcase import SparkTestingBaseTestCase

df1_fixture = [{"value": "some_value", "id": 1},
               {"value": "some_other_value", "id": 2}]
df2_fixture = [
    {"name": "derp", "id": 1},
    {"name": "non_derp", "id": 2},
    {"name": "missing_Derp", "id": 3}
]


df_expected_fixture = \
    [{"value": "some_value", "name": "derp", "id": 1},
     {"value": "some_other_value", "name": "non_derp", "id": 2}]

class HelloWorldTest(SparkTestingBaseTestCase):
    def test_join_by_ids(self):
        spark_session = SparkSession(self.sc)
        df1 = spark_session.read.json(self.sc.parallelize(df1_fixture))
        df2 = spark_session.read.json(self.sc.parallelize(df2_fixture))
        df_expected = spark_session.read(self.sc.parallelize(df_expected_fixture))
        x = transformations.join_by_ids(self.sc, df1, df2)
        x.show()
        df_expected.show()
        SparkTestingBaseTestCase.assertEqual(self, x, df_expected, "dfs don't match")
