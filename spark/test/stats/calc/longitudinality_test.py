import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col

import spark.stats.calc.longitudinality as longitudinality

df = None
results = None

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results
    conf = { 'date_field': 'date',
             'longitudinality': {
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
        data_row('2016-11-07', 'z', 'y', 'x', 'w')
    ]).toDF()
    results = longitudinality.calculate_longitudinality(spark['sqlContext'],
                df, conf)


def test_something():
    print results
    assert True


