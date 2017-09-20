import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col, isnan, trim

import spark.helpers.stats.select_distinct_column as select_distinct_column

df = None
results = None
expected_df = None

distinct_column_name = None

def cleanup(spark):
    pass


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, results, expected_df, distinct_column_name
    distinct_column_name = 'claim_id'
    data_row = Row('claim_id', 'col_1', 'col_2', 'col_3')
    df = spark['spark'].sparkContext.parallelize([
        data_row('0', 'a', None, None),
        data_row('0', None, 'b', None),
        data_row('2828', '   ', 'hey', 'hi'),
        data_row('2929', None, '', ' '),
        data_row('2828', None, None, 'oh no'),
        data_row('0', float('Nan'), None, 'c')
    ]).toDF()
    results = select_distinct_column.select_distinct_column(distinct_column_name)(df)
    expected_df = spark['spark'].sparkContext.parallelize([
        data_row('0', 1, 1, 1),
        data_row('2929', None, None, None),
        data_row('2828', None, 1, 2)
    ]).toDF()


def test_expected_values():
    assert expected_df.subtract(results).count() == 0


def test_distinct_row_count_equal():
    distinct_df = df.select(distinct_column_name).distinct()
    assert distinct_df.count() == results.count()


def test_no_nulls_distinct_column():
    assert df.filter(col(distinct_column_name).isNull()).count() == 0


