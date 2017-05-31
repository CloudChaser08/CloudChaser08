import pytest

import datetime

from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col

import spark.helpers.explode as explode

date_explode_results = []


@pytest.mark.usefixtures("spark")
def test_init(spark):
    max_days = 10  # won't explode more than this date range
    filter_condition = col('type') == 'explode'

    spark['sqlContext'].sql('DROP TABLE IF EXISTS explosion_test')

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

    spark['spark'].sparkContext.parallelize(data) \
                               .toDF(schema) \
                               .write \
                               .saveAsTable('explosion_test')

    explode.explode_dates(
        spark['runner'], 'explosion_test',
        'date_start', 'date_end', 'id', max_days, filter_condition
    )

    global date_explode_results
    date_explode_results = spark['sqlContext'] \
        .sql('select * from explosion_test').collect()


# explode dates tests
def test_10row():
    "Test ID '10row' exploded into 10 rows"
    date_explode_results_10row = filter(lambda r: r.test_id == '10row', date_explode_results)

    assert len(date_explode_results_10row) == 10

    for r in date_explode_results_10row:
        assert r.date_start == r.date_end


def test_toobig():
    "Test ID 'toobig' did not explode"
    date_explode_results_toobig = filter(lambda r: r.test_id == 'toobig', date_explode_results)

    assert len(date_explode_results_toobig) == 1


def test_noexplode():
    "Test ID 'noexplode' did not explode"
    date_explode_results_noexplode = filter(
        lambda r: r.test_id == 'noexplode', date_explode_results
    )

    assert len(date_explode_results_noexplode) == 1


def test_exploded_table_drop(spark):
    "Exploded table can be dropped and recreated"

    # both of these statements are required to drop a table after it
    # has been 'exploded' via the explode_dates function. This is
    # because explosion_test was created both as a concrete table and
    # as a temp table
    spark['sqlContext'].dropTempTable("explosion_test")
    spark['sqlContext'].sql('DROP TABLE IF EXISTS explosion_test')

    spark['sqlContext'].sql('CREATE TABLE explosion_test (id int)')
    assert spark['sqlContext'].sql('select * from explosion_test') \
                              .collect() == []


def test_generate_exploder_table(spark):
    "Test that exploder table was properly generated"
    length = 200

    explode.generate_exploder_table(spark['spark'], length, 'test_exploder')

    results = map(
        lambda r: r.n,
        spark['sqlContext'].sql('select * from test_exploder').collect()
    )

    assert sorted(results) == range(0, 200)
