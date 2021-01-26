import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, StructField

import spark.helpers.stats.utils as stats_utils
import spark.helpers.file_utils as file_utils

script_path = __file__

df = None
results = None
expected_df = None

start_date = None
end_date = None
date_column_name = None

data_row = Row('date', 'value')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, results, expected_df, start_date, end_date, date_column_name
    df = spark['spark'].sparkContext.parallelize([
        data_row('2011-01-01', 'hey'),
        data_row('1992-11-07', 'woah'),
        data_row('2015-05-24', 'yeah'),
        data_row('1999-07-11', 'cool'),
        data_row('2013-03-15', 'beware'),
        data_row(None, 'null date')
    ]).toDF()
    start_date = '1999-07-11'
    end_date = '2013-03-15'
    date_column_name = 'date'
    results = stats_utils.select_data_in_date_range(start_date, end_date, date_column_name)(df)
    expected_df = spark['spark'].sparkContext.parallelize([
        data_row('2011-01-01', 'hey'),
        data_row('1999-07-11', 'cool')
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


def test_limit_date_range_sample(spark):
    df = spark['spark'].read.csv(
        file_utils.get_abs_path(script_path, './resources/full_sample/'), sep='|', schema=StructType([
            StructField('date', StringType(), True),
            StructField('val', StringType(), True),
            StructField('rec', StringType(), True)
        ])
    )

    multiplier, sample = stats_utils.select_data_sample_in_date_range(
        '1990-01-01', '2018-01-01', 'date', max_sample_size=300
    )(df)
    assert multiplier == 1.0 / (300.0 / 5000.0)
    assert 250 < sample.count() < 350


def test_limit_date_range_sample_records(spark):
    rec_df = spark['spark'].read.csv(
        file_utils.get_abs_path(script_path, './resources/full_sample/'), sep='|', schema=StructType([
            StructField('date', StringType(), True),
            StructField('val', StringType(), True),
            StructField('rec', StringType(), True)
        ])
    )

    multiplier, sample = stats_utils.select_data_sample_in_date_range(
        '1990-01-01', '2018-01-01', 'date', max_sample_size=300, record_field='rec'
    )(rec_df)
    assert multiplier == 1.0 / (1500.0 / 5000.0)

    # sample target size is around 1500, but the actual size will not
    # be exactly this number. Just assert that it's in the ballpark.
    assert sample.count() > 1000 and sample.count() < 2000


def test_null_dates(spark):
    null_results = stats_utils.select_data_in_date_range('1999-07-11', '2013-03-15', 'date', True)(df)
    null_expected_df = spark['spark'].sparkContext.parallelize([
        data_row('2011-01-01', 'hey'),
        data_row('1999-07-11', 'cool'),
        data_row(None, 'null date')
    ]).toDF()

    assert null_expected_df.subtract(null_results).count() == 0
