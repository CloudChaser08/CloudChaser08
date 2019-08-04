import pytest

from pyspark.sql import Row

import spark.stats.calc.longitudinality as longitudinality

df = None
results = None

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results
    conf = {
        'date_field': ['date'],
        'longitudinality': True,
        'earliest_date': '1975-12-01'
    }
    data_row = Row('coalesced_date', 'hvid', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('1974-12-11', 'b', 'c', 'd', 'e'),
        data_row('1975-12-11', 'b', 'c', 'd', 'e'),
        data_row('2017-11-08', 'b', 'e', 'f', 'g'),
        data_row('2017-11-07', 'a', 's', 'u', 'x'),
        data_row('2017-11-01', 'c', 's', 'a', 'b'),
        data_row('2017-12-21', 'j', 'o', 'e', 'y'),
        data_row('2015-12-21', 'j', 'a', 'a', 'a'),
        data_row('2016-11-07', 'z', 'y', 'x', 'w'),
        data_row('2016-12-16', 'z', 'a', 'b', 'c')
    ]).toDF()
    results = longitudinality.calculate_longitudinality(df, conf)


def test_num_of_months_rows_are_correct():
    months_rows = [x for x in results if x['duration'].endswith('months')]
    assert len(months_rows) == 2


def test_num_of_years_rows_are_correct():
    years_rows = [x for x in results if x['duration'].endswith('years')]
    assert len(years_rows) == 2


def test_num_of_patients_per_group_correct():
    one_months = [x for x in results if x['duration'].endswith('1 months')][0]
    two_years = [x for x in results if x['duration'].endswith('2 years')][0]
    forty_one_years = [x for x in results if x['duration'].endswith('41 years')][0]
    forty_two_years = [x for x in results if x['duration'].endswith('42 years')]

    assert one_months['value'] == 1
    assert two_years['value'] == 1
    assert forty_one_years['value'] == 1
    assert len(forty_two_years) == 0 # should get minimized
