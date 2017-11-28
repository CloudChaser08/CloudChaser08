import pytest

from pyspark.sql import Row

import spark.stats.calc.top_values as top_values

df = None
results_no_distinct = None
results_distinct = None
expected_df = None


@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results_no_distinct, results_distinct, expected_df
    data_row = Row('a', 'b', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('a', 'b', 'c', 'd', 'e'),
        data_row('a', 'b', 'e', 'f', 'g'),
        data_row('e', 'a', 's', 'u', 'x'),
        data_row(None, 'c', 's', 'a', 'b')
    ]).toDF()
    results_no_distinct = top_values.calculate_top_values(df, 2)
    expected_df = spark['spark'].sparkContext.parallelize([
        data_row(0.5, 0.0, 0.75, 0.25, 0.75)
    ]).toDF()


def test_something():
    results_no_distinct.show()


