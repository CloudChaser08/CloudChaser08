import pytest

from pyspark.sql import Row

import spark.stats.calc.year_over_year as year_over_year

df = None
results = None

df = None
results = None

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results
    conf = { 'date_field': 'date',
             'year_over_year': {
                 'patient_id_field': 'b'
              }
            }
    data_row = Row('date', 'b', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('1975-12-11', 'b', 'c', 'd', 'e'),
        data_row('2017-11-08', 'b', 'e', 'f', 'g'),
        data_row('2017-11-07', 'a', 's', 'u', 'x'),
        data_row('2017-11-01', 'c', 's', 'a', 'b'),
        data_row('2017-12-21', 'j', 'o', 'e', 'y'),
        data_row('2016-11-07', 'z', 'y', 'x', 'w'),
        data_row('2015-01-01', 'z', 'a', 'a', 'a'),
        data_row('2015-01-01', 'j', 'a', 'a', 'a'),
        data_row('2015-01-01', 'c', 'a', 'a', 'a'),
        data_row('2015-01-01', 'a', 'a', 'a', 'a'),
        data_row('2016-03-01', 'a', 'b', 'c', 'd')
    ]).toDF()
    results = year_over_year.calculate_year_over_year(df, '2015-01-01', '2017-12-31', conf)


def test_something():
    print results


