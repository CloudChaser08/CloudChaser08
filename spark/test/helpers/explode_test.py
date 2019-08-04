import pytest

from pyspark.sql.functions import col

import spark.helpers.explode as explode

date_explode_results = []


@pytest.mark.usefixtures("spark")
def test_init(spark):
    max_days = 10  # won't explode more than this date range
    filter_condition = col('type') == 'explode'

    spark['sqlContext'].sql('DROP TABLE IF EXISTS explosion_test')

    spark['sqlContext'].sql('CREATE TABLE explosion_test (id string, test_id string, date_start date, date_end date, type string)')

    spark['sqlContext'].sql("INSERT INTO explosion_test VALUES ('1', '10row', '2016-01-01', '2016-01-10', 'explode'), "
                            + "('2', 'toobig', '2016-01-01', '2016-01-12', 'explode'), "
                            + "('3', 'noexplode', '2016-01-01', '2016-01-03', 'dont-explode')")

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
    date_explode_results_10row = [r for r in date_explode_results if r.test_id == '10row']

    assert len(date_explode_results_10row) == 10

    for r in date_explode_results_10row:
        assert r.date_start == r.date_end


def test_toobig():
    "Test ID 'toobig' did not explode"
    date_explode_results_toobig = [r for r in date_explode_results if r.test_id == 'toobig']

    assert len(date_explode_results_toobig) == 1


def test_noexplode():
    "Test ID 'noexplode' did not explode"
    date_explode_results_noexplode = [r for r in date_explode_results if r.test_id == 'noexplode']

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

    results = [r.n for r in spark['sqlContext'].sql('select * from test_exploder').collect()]

    assert sorted(results) == list(range(0, 200))
