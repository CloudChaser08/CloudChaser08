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
        data_row('2015-12-21', 'j', 'a', 'a', 'a'),
        data_row('2016-11-07', 'z', 'y', 'x', 'w')
    ]).toDF()
    results = longitudinality.calculate_longitudinality(df, conf)


def test_num_of_months_rows_are_correct():
    print results
    months_rows = filter(lambda x: x['value'].endswith('months'), results)
    assert len(months_rows) == 2


def test_num_of_years_rows_are_correct():
    years_rows = filter(lambda x: x['value'].endswith('years'), results)
    assert len(years_rows) == 2


def test_num_of_patients_per_group_correct():
    twenty_four_months = filter(lambda x: x['value'].endswith('24 months'), results)[0]
    two_years = filter(lambda x: x['value'].endswith('2 years'), results)[0]
    forty_one_years = filter(lambda x: x['value'].endswith('41 years'), results)[0]

    assert twenty_four_months['patients'] == 1
    assert two_years['patients'] == 1
    assert forty_one_years['patients'] == 1


