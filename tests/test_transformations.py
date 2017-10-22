import transformations
from sparktestingbase.testcase import SparkTestingBaseTestCase
from pyspark.sql import SparkSession


df1_fixture = [{"name": "derp", "id": 1},
               {"name": "non_derp", "id": 2}]
df2_fixture = [
    {"name": "derp", "id": 1},
    {"name": "non_derp", "id": 2},
    {"name": "missing_Derp", "id":3}
]


class HelloWorldTest(SparkTestingBaseTestCase):
    def test_join_by_ids(self):
        spark_session = SparkSession(self.sc)
        df1 = spark_session.read.json(self.sc.parallelize(df1_fixture))
        df2 = spark_session.read.json(self.sc.parallelize(df2_fixture))

        x = transformations.join_by_ids(self.sc,
                                        df1,
                                        df2)
        x.show()
        assert(True)