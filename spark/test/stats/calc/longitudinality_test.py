import pytest

from pyspark.sql import Row

import spark.stats.calc.longitudinality as longitudinality


@pytest.fixture(scope='module', name='dataframe')
def _get_dataframe(spark):
    data_row = Row('coalesced_date', 'hvid', 'c', 'd', 'e')
    yield spark['spark'].sparkContext.parallelize([
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

@pytest.fixture(scope='module', name='results')
def _get_results(dataframe, provider_conf):
    conf = provider_conf.copy_with(
        date_fields=['date'],
        longitudinality=True,
        earliest_date='1975-12-01'
    )
    yield longitudinality.calculate_longitudinality(dataframe, conf)


def test_num_of_months_rows_are_correct(results):
    months_rows = filter(lambda x: x['duration'].endswith('months'), results)
    assert len(months_rows) == 2


def test_num_of_years_rows_are_correct(results):
    years_rows = filter(lambda x: x['duration'].endswith('years'), results)
    assert len(years_rows) == 2


def test_num_of_patients_per_group_correct(results):
    one_months = filter(lambda x: x['duration'].endswith('1 months'), results)[0]
    two_years = filter(lambda x: x['duration'].endswith('2 years'), results)[0]
    forty_one_years = filter(lambda x: x['duration'].endswith('41 years'), results)[0]
    forty_two_years = filter(lambda x: x['duration'].endswith('42 years'), results)

    assert one_months['value'] == 1
    assert two_years['value'] == 1
    assert forty_one_years['value'] == 1
    assert len(forty_two_years) == 0 # should get minimized
