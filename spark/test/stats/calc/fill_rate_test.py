import pytest

from pyspark.sql import Row

import spark.stats.calc.fill_rate as fill_rate

df = None
results = None
expected_results = None

def cleanup(spark):
    pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results, expected_results
    data_row = Row('a', 'b', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('1', ' ', '', 'null', ' '),
        data_row('2', '', '2', '', 'hey'),
        data_row(None, None, 'a', None, 'hi'),
        data_row('', '', 'b', '', 'hello')
    ]).toDF()
    results = fill_rate.calculate_fill_rate(df)
    expected_results = [0.5, 0.0, 0.75, 0.25, 0.75]

def test_num_columns_equal():
    num_df_cols = len(df.columns)

    # We should have 1 result per column in the original dataframe
    num_results = len(results)
    assert num_df_cols == num_results
    

def test_column_names_equal():
    df_cols = df.columns
    results_cols = [r['field'] for r in results]
    assert df_cols == results_cols


def test_expected_values():
    # This tests has some holes in it, so we need to make sure
    # the arrays idential even if this passes
    assert set(expected_results) - set([r['fill'] for r in results]) == set()
    assert expected_results == [r['fill'] for r in results]


