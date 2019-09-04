"""
    Key stats calculation tests
"""

import pytest

from pyspark.sql import Row

import spark.stats.calc.key_stats as key_stats
from spark.stats.models.results import GenericStatsResult


@pytest.fixture(scope='module', name='dataframe')
def _get_dataframe(spark):
    data_row = Row('coalesced_date', 'hvid', 'c', 'd', 'e')
    yield spark['spark'].sparkContext.parallelize([
        data_row('1975-12-11', 'b', 'c', 'd', 'e'),
        data_row('2017-11-08', 'b', 'e', 'f', 'g'),
        data_row('2017-11-07', 'a', 's', 'u', 'x'),
        data_row('2017-11-01', 'c', 's', 'a', 'b'),
        data_row('2017-12-21', 'j', 'o', 'e', 'y'),
        data_row('2016-11-07', 'z', 'y', 'x', 'w'),
        # "null" date
        data_row('   ', 'p', 'q', 'r', 's')
    ]).toDF()


def test_counts_are_correct_for_date_range(provider_conf, dataframe):
    """ Gets key stats within date range """
    conf = provider_conf.copy_with(
        date_fields=['date'],
        record_field='c',
        index_all_dates=False
    )
    results = key_stats.calculate_key_stats(
        df=dataframe,
        earliest_date='2015-11-01',
        start_date='2017-01-01',
        end_date='2017-12-01',
        provider_conf=conf
    )

    assert _only_totals(results) == [
        GenericStatsResult(field='total_patient', value=4),
        GenericStatsResult(field='total_24_month_patient', value=3),
        GenericStatsResult(field='total_row', value=4),
        GenericStatsResult(field='total_24_month_row', value=3),
        GenericStatsResult(field='total_record', value=3),
        GenericStatsResult(field='total_24_month_record', value=2),
    ]


def test_record_wildcard(provider_conf, dataframe):
    """ Gets key stats within record_field='*' """
    conf = provider_conf.copy_with(
        date_fields=['date'],
        record_field='*',
        index_all_dates=False
    )
    results = key_stats.calculate_key_stats(
        df=dataframe,
        earliest_date='2015-11-01',
        start_date='2017-01-01',
        end_date='2017-12-01',
        provider_conf=conf
    )

    assert _only_totals(results) == [
        GenericStatsResult(field='total_patient', value=4),
        GenericStatsResult(field='total_24_month_patient', value=3),
        GenericStatsResult(field='total_row', value=4),
        GenericStatsResult(field='total_24_month_row', value=3),
        GenericStatsResult(field='total_record', value=4),
        GenericStatsResult(field='total_24_month_record', value=3),
    ]


def test_all_dates(provider_conf, dataframe):
    """ Gets key stats when index_all_dates is True """
    conf = provider_conf.copy_with(
        date_fields=['date'],
        record_field='c',
        index_all_dates=True
    )
    results = key_stats.calculate_key_stats(
        df=dataframe,
        earliest_date='2015-11-01',
        start_date='2017-01-01',
        end_date='2017-12-01',
        provider_conf=conf
    )

    # Note that all of the totals should match their 24 month counterparts.
    # Indexing all dates is a bit of a misnomer, it simply means indexing dates
    # from earliest -> end_date instead of from start -> end date
    assert _only_totals(results) == [
        GenericStatsResult(field='total_patient', value=4),
        GenericStatsResult(field='total_24_month_patient', value=4),
        GenericStatsResult(field='total_row', value=4),
        GenericStatsResult(field='total_24_month_row', value=4),
        GenericStatsResult(field='total_record', value=3),
        GenericStatsResult(field='total_24_month_record', value=3),
    ]


def test_null_dates(provider_conf, dataframe):
    """ Gets key stats when index_null_dates is True """
    conf = provider_conf.copy_with(
        date_fields=['date'],
        record_field='c',
        index_all_dates=False,
        index_null_dates=True,
    )
    results = key_stats.calculate_key_stats(
        df=dataframe,
        earliest_date='2015-11-01',
        start_date='2017-01-01',
        end_date='2017-12-01',
        provider_conf=conf
    )

    assert _only_totals(results) == [
        GenericStatsResult(field='total_patient', value=5),
        GenericStatsResult(field='total_24_month_patient', value=4),
        GenericStatsResult(field='total_row', value=5),
        GenericStatsResult(field='total_24_month_row', value=4),
        GenericStatsResult(field='total_record', value=4),
        GenericStatsResult(field='total_24_month_record', value=3),
    ]


def test_abbrev_date_format(provider_conf, dataframe):
    """ Tests key stats accepts dates in YYYY-MM format """
    conf = provider_conf.copy_with(
        date_fields=['date'],
        record_field='c',
    )
    assert key_stats.calculate_key_stats(
        df=dataframe,
        earliest_date='2015-11',
        start_date='2017-01',
        end_date='2017-12',
        provider_conf=conf
    )


def _get_value(results, field):
    """ Gets the value from results with field name """
    return [r for r in results if r.field == field][0].value


def _only_totals(results):
    return [r for r in results if r.field.startswith('total')]
