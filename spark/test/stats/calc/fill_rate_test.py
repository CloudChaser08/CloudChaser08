import pytest

from pyspark.sql import Row

import spark.stats.calc.fill_rate as fill_rate

df = None
results = None
expected_df = None

def cleanup(spark):
    pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results, expected_df
    data_row = Row('a', 'b', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('1', ' ', '', 'null', ' '),
        data_row('2', '', '2', 'NaN', 'hey'),
        data_row(None, None, 'a', None, 'hi'),
        data_row('', float('Nan'), 'b', float('Nan'), 'hello')
    ]).toDF()
    results = fill_rate.calculate_fill_rate(df)
    expected_df = spark['spark'].sparkContext.parallelize([
        data_row(0.5, 0.0, 0.75, 0.25, 0.75)
    ]).toDF()


def test_num_columns_equal():
    num_df_cols = len(df.columns)
    num_results_cols = len(results.columns)
    assert num_df_cols == num_results_cols
    

def test_column_names_equal():
    df_cols = df.columns
    results_cols = results.columns
    assert df_cols == results_cols


def test_expected_values():
    assert expected_df.subtract(results).count() == 0


