import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col

import spark.helpers.stats.limit_date_range as limit_date_range

df = None
results = None
expected_df = None

start_date = None
end_date = None
date_column_name = None

def cleanup(spark):
    pass


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, results, expected_df, start_date, end_date, date_column_name
    data_row = Row('date', 'value')
    df = spark['spark'].sparkContext.parallelize([
        data_row('2011-01-01', 'hey'),
        data_row('1992-11-07', 'woah'),
        data_row('2015-05-24', 'yeah'),
        data_row('1999-07-11', 'cool'),
        data_row('2013-03-15', 'beware')
    ]).toDF()
    start_date = '1999-07-11'
    end_date = '2013-03-15'
    date_column_name = 'date'
    results = limit_date_range.limit_date_range(start_date, end_date, date_column_name)(df)
    expected_df = spark['spark'].sparkContext.parallelize([
        data_row('2011-01-01', 'hey'),
        data_row('1999-07-11', 'cool'),
        data_row('2013-03-15', 'beware')
    ]).toDF()


def test_dates_in_range():
    out_of_range = (col(date_column_name) < start_date) | (col(date_column_name) > end_date)
    out_of_range_count = results.filter(out_of_range).count()
    assert out_of_range_count == 0


def test_expected_values():
    assert expected_df.subtract(results).count() == 0


def test_column_names_equal():
    df_cols = df.columns
    results_cols = results.columns
    assert df_cols == results_cols


