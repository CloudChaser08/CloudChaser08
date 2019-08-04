import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col

import spark.stats.calc.top_values as top_values

df = None
max_top_values = 2
results_no_distinct = None
results_distinct = None
results_threshold = None
results_distinct_threshold = None
expected_df = None
no_top_value_columns_df = None


@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results_no_distinct, results_distinct, expected_df, \
           no_top_value_columns_df, results_threshold, results_distinct_threshold
    data_row = Row('a', 'b', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('a', 'b', 'c', 'd', 'e'),
        data_row('a', 'b', 'e', 'f', 'g'),
        data_row('e', 'a', 's', 'u', 'x'),
        data_row(None, 'c', 's', 'a', 'b')
    ]).toDF()
    results_no_distinct = top_values.calculate_top_values(df, max_top_values)
    results_distinct = top_values.calculate_top_values(df, max_top_values, 'b')
    results_threshold = top_values.calculate_top_values(df, max_top_values, threshold=0.5)
    results_distinct_threshold = top_values.calculate_top_values(df, max_top_values, distinct_column='b', threshold=0.5)

    data_row = Row('a')
    no_top_value_columns_df = spark['spark'].sparkContext.parallelize([
        data_row('a'),
        data_row('b'),
        data_row('c'),
        data_row('d')
    ]).toDF()


def test_top_values_created_for_each_column():
    # expected = num_cols * max_top_values
    assert len(results_no_distinct) == 5 * max_top_values
    assert len(results_distinct) == 4 * max_top_values


def test_threshold():
    assert sorted(results_threshold, key=lambda r: r['column']) == [
        {'column': 'a', 'count': 2, 'percentage': 0.5, 'value': 'a'},
        {'column': 'b', 'count': 2, 'percentage': 0.5, 'value': 'b'},
        {'column': 'c', 'count': 2, 'percentage': 0.5, 'value': 's'}
    ]

    assert results_distinct_threshold == [
        {'column': 'c', 'count': 2, 'percentage': 0.6667, 'value': 's'}
    ]


def test_duplicate_values_not_counted_twice_when_distinct_column_is_not_none():
    assert [x for x in results_no_distinct if x['column'] == 'a' and x['value'] == 'a'][0]['count'] == 2
    assert [x for x in results_distinct if x['column'] == 'a' and x['value'] == 'a'][0]['count'] == 1


def test_no_top_values_to_calculate():
    assert top_values.calculate_top_values(no_top_value_columns_df, 10, 'a') == []
