"""
    Tests for processor module
"""
import pytest
from mock import Mock, patch

from pyspark.sql import Row
from pyspark.sql.functions import coalesce

import spark.stats.processor as processor
from spark.stats.models import Column, TableMetadata
from spark.stats.models.results import (
    FillRateResult, GenericStatsResultSet, GenericStatsResult,
    TopValuesResult, LongitudinalityResult
)

TABLE = TableMetadata(
    name='tbl',
    description='desc',
    columns=[
        Column(
            name='hvid', field_id='1', sequence='1', top_values=True,
            datatype='string', description='HV ID', category='Baseline',
        ),
        Column(
            name='col_2', field_id='2', sequence='2', top_values=True,
            datatype='string', description='Column 2', category='Baseline',
        )
    ]
)


@pytest.fixture(name='prov_conf', scope='module')
def _get_proc_prov_config(provider_conf):
    yield provider_conf.copy_with(
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field='claim_id',
        longitudinality=True,
        epi_calcs=True,
        key_stats=True,
        year_over_year=True,
        top_values=True,
        fill_rate=True,
        table=TABLE,
        earliest_date='1992-11-07'
    )

@pytest.fixture(name='prov_conf_no_rf', scope='module')
def _get_proc_prov_conf_no_rf(prov_conf):
    yield prov_conf.copy_with(
        record_field=None
    )

@pytest.fixture(name='prov_conf_no_stats', scope='module')
def _get_proc_prov_conf_no_stats(prov_conf):
    yield prov_conf.copy_with(
        longitudinality=False,
        epi_calcs=False,
        key_stats=False,
        year_over_year=False,
        top_values=False,
        fill_rate=False,
    )


@pytest.fixture(name='df_provider', scope='module')
def _get_df_provider(spark):
    columns = {'claim_id': 0, 'col_2': 3, 'col_3': 4, 'hvid': 2, 'service_date': 1}
    data_row = Row(*sorted(columns.keys()))

    dataframe = spark['spark'].sparkContext.parallelize([
        data_row('0', 'a', 'b', None, '1995-10-11'),
        data_row('0', 'b', 'c', 'a', '2016-01-12'),
        data_row('1', 'b', '  ', 'a', '2015-11-08'),
        data_row('1', 'b', 'c', '   ', '1974-03-02'),
        data_row('1', '       ', 'c', 'a', '1993-07-13'),
        data_row('1', '    ', None, 'a', '2017-03-15'),
        data_row('2', 'b', 'c', 'a', '1800-01-01'),
        data_row('2', 'b', 'c', 'a', '1850-01-01'),
        data_row('2', 'b', 'c', 'a', '1900-01-01')
    ]).toDF().withColumn(
        'coalesced_date', coalesce('service_date')
    )

    yield Mock(
        all_data=dataframe,
        sampled_data=dataframe,
        sampled_data_multiplier=1
    )


def test_fill_rate_record_field(prov_conf, df_provider):
    """ Tests fill rate with a record field"""
    res = processor.run_fill_rates(prov_conf, df_provider)
    assert res is not None
    assert sorted(res) == [
        FillRateResult(field='claim_id', fill=1.0),
        FillRateResult(field='col_2', fill=1.0),
        FillRateResult(field='hvid', fill=1.0),
    ]


def test_fill_rate_no_record_field(prov_conf_no_rf, df_provider):
    """ Tests fill rate without a record field """

    res = processor.run_fill_rates(prov_conf_no_rf, df_provider)
    assert res is not None
    assert sorted(res) == [
        FillRateResult(field='col_2', fill=7.0 / 9.0),
        FillRateResult(field='hvid', fill=7.0 / 9.0),
    ]


def test_no_fill_rate(prov_conf_no_stats, df_provider):
    """ Tests fill rate without a record field """

    res = processor.run_fill_rates(prov_conf_no_stats, df_provider)
    assert res is None


def test_run_top_values(prov_conf_no_rf, df_provider):
    """ Tests runs top values """
    res = processor.run_top_values(prov_conf_no_rf, df_provider)
    assert res == [
        TopValuesResult(
            field='col_2',
            value='b',
            count=6,
            percentage=0.6667
        ),
        TopValuesResult(
            field='col_2',
            value='a',
            count=1,
            percentage=0.1111
        ),
        TopValuesResult(
            field='hvid',
            value='a',
            count=7,
            percentage=0.7778
        )
    ]


def test_run_top_values_distinct(prov_conf, df_provider):
    """ Tests runs top values with claim_id as distinct column """
    res = processor.run_top_values(prov_conf, df_provider)
    assert res == [
        TopValuesResult(
            field='col_2',
            value='b',
            count=3,
            percentage=1.0
        ),
        TopValuesResult(
            field='col_2',
            value='a',
            count=1,
            percentage=0.3333
        ),
        TopValuesResult(
            field='hvid',
            value='a',
            count=3,
            percentage=1.0
        )
    ]


def test_run_top_values_off(prov_conf_no_stats, df_provider):
    """ Tests top values does not run """
    assert processor.run_top_values(prov_conf_no_stats, df_provider) is None


def test_run_longitudinality_off(prov_conf_no_stats, df_provider):
    """ Tests longitudinality does not run """
    assert processor.run_longitudinality(
        prov_conf_no_stats, '2018-01-01', df_provider
    ) is None


def test_run_longitudinality_bad_date(prov_conf, df_provider):
    """ Tests runs longidutinality """
    assert processor.run_longitudinality(
        prov_conf,
        '1993-11-07', #end date within 2 years of earliest date
        df_provider
    ) is None


def test_run_longitudinality(prov_conf, df_provider):
    """ Tests runs longitudinality """
    assert processor.run_longitudinality(
        prov_conf, '2018-01-01', df_provider
    ) == [
        LongitudinalityResult(average=1, duration='0 months', std_dev=0, value=2),
        LongitudinalityResult(average=5, duration='24 years', std_dev=0, value=1)
    ]


def test_key_stats(prov_conf, df_provider):
    """ Tests runs key stats """
    assert processor.run_key_stats(
        prov_conf, '1900-01-01', '2018-01-01', df_provider
    ) == [
        GenericStatsResult(field='total_patient', value=2),
        GenericStatsResult(field='total_24_month_patient', value=3),
        GenericStatsResult(field='daily_avg_patient', value=0),
        GenericStatsResult(field='monthly_avg_patient', value=0),
        GenericStatsResult(field='yearly_avg_patient', value=0),
        GenericStatsResult(field='total_row', value=5),
        GenericStatsResult(field='total_24_month_row', value=7),
        GenericStatsResult(field='daily_avg_row', value=0),
        GenericStatsResult(field='monthly_avg_row', value=0),
        GenericStatsResult(field='yearly_avg_row', value=0),
        GenericStatsResult(field='total_record', value=2),
        GenericStatsResult(field='total_24_month_record', value=3),
        GenericStatsResult(field='daily_avg_record', value=0),
        GenericStatsResult(field='monthly_avg_record', value=0),
        GenericStatsResult(field='yearly_avg_record', value=0)
    ]


def test_key_stats_off(prov_conf_no_stats, df_provider):
    """ Tests key stats does not run """
    assert processor.run_key_stats(
        prov_conf_no_stats, '2016-01-01', '2018-01-01', df_provider
    ) is None


def test_year_over_year(prov_conf, df_provider):
    """ Tests runs year-over-year """
    assert processor.run_year_over_year(
        prov_conf, '2018-01-01', df_provider
    ) == []


def test_year_over_year_off(prov_conf_no_stats, df_provider):
    """ Tests key stats does not run """
    assert processor.run_year_over_year(
        prov_conf_no_stats, '2018-01-01', df_provider
    ) is None


def test_year_over_year_bad_date(prov_conf, df_provider):
    """ Tests runs year_over_year """
    assert processor.run_year_over_year(
        prov_conf,
        '1993-11-07', #end date within 2 years of earliest date
        df_provider
    ) is None


@patch('spark.stats.calc.epi.calculate_epi', autospec=True)
def test_get_epi_calcs(epi_mock, prov_conf):
    """ Tests runs get_epi_calcs """
    results = [
        GenericStatsResultSet(
            results=[GenericStatsResult(field=str(i), value=i)]
        ) for i in range(4)
    ]
    epi_mock.side_effect = results
    assert processor.get_epi_calcs(prov_conf) == {
        'age': results[0],
        'gender': results[1],
        'state': results[2],
        'region': results[3],
    }


def test_get_epi_calcs_off(prov_conf_no_stats):
    """ Tests get_epi_calcs is not run """
    assert processor.get_epi_calcs(prov_conf_no_stats) is None
