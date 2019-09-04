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
    assert len(_get_field_values(results, 'months')) == 2


def test_num_of_years_rows_are_correct(results):
    assert len(_get_field_values(results, 'years')) == 2


def test_num_of_patients_per_group_correct(results):
    assert _get_field_val(results, '1 months') == 1
    assert _get_field_val(results, '2 years') == 1
    assert _get_field_val(results, '41 years') == 1
    assert not _get_field_values(results, '42 years') # should get minimized


def _get_field_val(results, duration):
    vals = _get_field_values(results, duration)
    if len(vals) == 1:
        return vals[0]

    raise ValueError(
        '{} results with a duration ending in {}'.format(len(vals), duration)
    )


def _get_field_values(results, duration):
    return [
        res.value for res in results if res.duration.endswith(duration)
    ]
