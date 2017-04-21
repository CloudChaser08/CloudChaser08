import unittest
import datetime

from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col

from spark.spark_setup import init
from spark.runner import Runner
import spark.helpers.explode as explode


class TestExplode(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        spark, sqlContext = init("Explode Test", True)
        runner = Runner(sqlContext)

        max_days = 10  # won't explode more than this date range
        filter_condition = col('type') == 'explode'

        sqlContext.sql('DROP TABLE IF EXISTS explosion_test')

        data = [
            # test row '10row' should be exploded into ten rows
            ['1', '10row', datetime.date(2016, 1, 1), datetime.date(2016, 1, 10), 'explode'],

            # test row 'toobig' should not be exploded because the daterange is too big
            ['2', 'toobig', datetime.date(2016, 1, 1), datetime.date(2016, 1, 12), 'explode'],

            # id 3 should not be exploded due to the filter condition
            ['3', 'noexplode', datetime.date(2016, 1, 1), datetime.date(2016, 1, 3), 'dont-explode']
        ]

        schema = StructType([StructField('id', StringType(), True),
                             StructField('test_id', StringType(), True),
                             StructField('date_start', DateType(), True),
                             StructField('date_end', DateType(), True),
                             StructField('type', StringType(), True)])

        spark.sparkContext.parallelize(data) \
                          .toDF(schema) \
                          .write \
                          .saveAsTable('explosion_test')

        explode.explode_dates(
            runner, 'explosion_test', 'date_start', 'date_end',
            'id', max_days, filter_condition
        )

        self.results = sqlContext.sql('select * from explosion_test').collect()

    # explode dates tests
    def test_10row(self):
        "Test ID '10row' exploded into 10 rows"
        results_10row = filter(lambda r: r.test_id == '10row', self.results)

        self.assertEqual(
            len(results_10row), 10
        )
        for r in results_10row:
            self.assertTrue(
                r.date_start == r.date_end
            )

    def test_toobig(self):
        "Test ID 'toobig' did not explode"
        results_toobig = filter(lambda r: r.test_id == 'toobig', self.results)

        self.assertEqual(
            len(results_toobig), 1
        )

    def test_noexplode(self):
        "Test ID 'noexplode' did not explode"
        results_noexplode = filter(
            lambda r: r.test_id == 'noexplode', self.results
        )

        self.assertEqual(
            len(results_noexplode), 1
        )
