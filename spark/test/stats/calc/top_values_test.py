import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col

import spark.stats.calc.top_values as top_values

df = None
max_top_values = 2
results_no_distinct = None
results_distinct = None
expected_df = None
no_top_value_columns_df = None


@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results_no_distinct, results_distinct, expected_df, \
           no_top_value_columns_df
    data_row = Row('a', 'b', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('a', 'b', 'c', 'd', 'e'),
        data_row('a', 'b', 'e', 'f', 'g'),
        data_row('e', 'a', 's', 'u', 'x'),
        data_row(None, 'c', 's', 'a', 'b')
    ]).toDF()
    results_no_distinct = top_values.calculate_top_values(df, max_top_values)
    results_distinct = top_values.calculate_top_values(df, max_top_values, 'b')

    data_row = Row('a')
    no_top_value_columns_df = spark['spark'].sparkContext.parallelize([
        data_row('a'),
        data_row('b'),
        data_row('c'),
        data_row('d')
    ]).toDF()


def test_top_values_created_for_each_column():
    # expected = num_cols * max_top_values
    assert results_no_distinct.count() == 5 * max_top_values
    assert results_distinct.count() == 4 * max_top_values 


def test_duplicate_values_not_counted_twice_when_distinct_column_is_not_none():
    assert results_no_distinct.where(col('name') == 'a').where(col('col') == 'a').collect()[0]['count'] == 2
    assert results_distinct.where(col('name') == 'a').where(col('col') == 'a').collect()[0]['count'] == 1


def test_exception_thrown_when_no_columns_have_top_values_to_calculate():
    with pytest.raises(Exception) as e_info:
        tv_df = top_values.calculate_top_values(no_top_value_columns_df, 10, 'a')

    exception = e_info.value
    assert exception.message.startswith('Dataframe with no columns passed in for top values calculation')

