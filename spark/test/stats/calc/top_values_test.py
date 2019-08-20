import pytest

from pyspark.sql import Row

import spark.stats.calc.top_values as top_values

MAX_TOP_VALUES = 2

@pytest.fixture(scope='module', name='dataframe')
def _get_dataframe(spark):
    data_row = Row('a', 'b', 'c', 'd', 'e')
    yield spark['spark'].sparkContext.parallelize([
        data_row('a', 'b', 'c', 'd', 'e'),
        data_row('a', 'b', 'e', 'f', 'g'),
        data_row('e', 'a', 's', 'u', 'x'),
        data_row(None, 'c', 's', 'a', 'b')
    ]).toDF()


@pytest.fixture(scope='module', name='results_no_distinct')
def _get_results_no_distinct(dataframe):
    yield top_values.calculate_top_values(dataframe, MAX_TOP_VALUES)


@pytest.fixture(scope='module', name='results_distinct')
def _get_results_distinct(dataframe):
    yield top_values.calculate_top_values(dataframe, MAX_TOP_VALUES, 'b')


@pytest.fixture(scope='module', name='results_threshold')
def _get_results_threshold(dataframe):
    yield top_values.calculate_top_values(dataframe, MAX_TOP_VALUES, threshold=0.5)


@pytest.fixture(scope='module', name='results_distinct_threshold')
def _get_results_distinct_threshold(dataframe):
    yield top_values.calculate_top_values(dataframe, MAX_TOP_VALUES, distinct_column='b', threshold=0.5)


def test_top_values_created_for_each_column(results_no_distinct, results_distinct):
    # expected = num_cols * MAX_TOP_VALUES
    assert len(results_no_distinct) == 5 * MAX_TOP_VALUES
    assert len(results_distinct) == 4 * MAX_TOP_VALUES


def test_threshold(results_threshold, results_distinct_threshold):
    assert sorted(results_threshold) == [
        {'column': 'a', 'count': 2, 'percentage': 0.5, 'value': 'a'},
        {'column': 'b', 'count': 2, 'percentage': 0.5, 'value': 'b'},
        {'column': 'c', 'count': 2, 'percentage': 0.5, 'value': 's'}
    ]

    assert results_distinct_threshold == [
        {'column': 'c', 'count': 2, 'percentage': 0.6667, 'value': 's'}
    ]


def test_duplicate_values_not_counted_twice_when_distinct_column_is_not_none(results_distinct, results_no_distinct):
    assert filter(lambda x: x['column'] == 'a' and x['value'] == 'a', results_no_distinct)[0]['count'] == 2
    assert filter(lambda x: x['column'] == 'a' and x['value'] == 'a', results_distinct)[0]['count'] == 1


def test_no_top_values_to_calculate(spark):
    data_row = Row('a')
    no_top_value_columns_df = spark['spark'].sparkContext.parallelize([
        data_row('a'),
        data_row('b'),
        data_row('c'),
        data_row('d')
    ]).toDF()
    assert top_values.calculate_top_values(no_top_value_columns_df, 10, 'a') == []
