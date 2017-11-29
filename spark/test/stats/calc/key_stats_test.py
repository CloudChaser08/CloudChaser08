import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col

import spark.stats.calc.key_stats as key_stats

df = None
results = None

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results
    conf = { 'date_field': 'date',
             'key_stats': {
                 'patient_attribute': 'distinct(a)',
                 'record_attribute': 'distinct(b)',
                 'row_attribute': '*'
              }
            }
    data_row = Row('date', 'b', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('1975-12-11', 'b', 'c', 'd', 'e'),
        data_row('2014-01-01', 'b', 'e', 'f', 'g'),
        data_row('2017-11-07', 'a', 's', 'u', 'x'),
        data_row('2017-11-01', 'c', 's', 'a', 'b')
    ]).toDF()
    results = keys_stats.calculate_key_stats(spark['sqlContext'],
                df, '2014-01-01', '2015-01-01', '2017-12-01',
                conf)


def test_something:
    print results


