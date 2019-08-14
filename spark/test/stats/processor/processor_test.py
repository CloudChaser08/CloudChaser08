import pytest
from mock import patch

from pyspark.sql import Row

import spark.stats.processor as processor
from spark.stats.models import FillRateConfig, Column

import spark.helpers.stats.utils as stats_utils

FILL_RATE_CONF = FillRateConfig(
    columns={
        'hvid': Column(name='hvid', field_id='1', sequence='1'),
        'col_2': Column(name='col_2', field_id='2', sequence='2')
    }
)

QUARTER = 'Q32017'
START_DATE = '2015-06-27'
END_DATE = '2017-03-15'


@pytest.fixture(name='dataframe', scope='module')
def _get_dataframe(spark):
    columns = {'claim_id': 0, 'col_2': 3, 'col_3': 4, 'hvid': 2, 'service_date': 1}
    data_row = Row(*sorted(columns.keys()))
    yield spark['spark'].sparkContext.parallelize([
        data_row('0', 'a', 'b', None, '1995-10-11'),
        data_row('0', 'b', 'c', 'a', '2016-01-12'),
        data_row('1', 'b', '  ', 'a', '2015-11-08'),
        data_row('1', 'b', 'c', '   ', '1974-03-02'),
        data_row('1', '       ', 'c', 'a', '1993-07-13'),
        data_row('1', '    ', None, 'a', '2017-03-15'),
        data_row('2', 'b', 'c', 'a', '1800-01-01'),
        data_row('2', 'b', 'c', 'a', '1850-01-01'),
        data_row('2', 'b', 'c', 'a', '1900-01-01')
    ]).toDF()

@pytest.fixture(autouse=True, scope='module')
def _patch_get_provider_data(dataframe):
    with patch.object(stats_utils, 'get_provider_data', return_value=dataframe) as get_provider_data:
        yield get_provider_data


@pytest.fixture(name='results_distinct_column', scope='module')
def _get_results_distinct_column(spark, provider_conf):
    prov_conf = provider_conf.copy_with(
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field='claim_id',
        fill_rate=True,
        fill_rate_conf=FILL_RATE_CONF,
        key_stats=False,
        top_values=False,
        longitudinality=False,
        year_over_year=False,
        epi_calcs=False,
        earliest_date='1992-11-07'
    )

    yield processor.run_marketplace_stats(
        spark['spark'],
        spark['sqlContext'],
        QUARTER,
        START_DATE,
        END_DATE,
        prov_conf
    )


@pytest.fixture(name='results_no_distinct_column', scope='module')
def _get_results_no_distinct_column(spark, provider_conf):
    prov_conf = provider_conf.copy_with(
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field=None,
        fill_rate=True,
        fill_rate_conf=FILL_RATE_CONF,
        key_stats=False,
        top_values=False,
        longitudinality=False,
        year_over_year=False,
        epi_calcs=False,
        earliest_date='1992-11-07'
    )

    yield processor.run_marketplace_stats(
        spark['spark'],
        spark['sqlContext'],
        QUARTER,
        START_DATE,
        END_DATE,
        prov_conf
    )

@pytest.fixture(name='results_no_fill_rate', scope='module')
def _get_results_no_fill_rate(spark, provider_conf):
    prov_conf = provider_conf.copy_with(
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field=None,
        fill_rate=False,
        key_stats=False,
        top_values=False,
        longitudinality=False,
        year_over_year=False,
        epi_calcs=False,
        earliest_date='1992-11-07'
    )

    yield processor.run_marketplace_stats(
        spark['spark'],
        spark['sqlContext'],
        QUARTER,
        START_DATE,
        END_DATE,
        prov_conf
    )


def test_fill_rate_calculated(results_distinct_column, results_no_distinct_column):
    assert results_distinct_column['fill_rate'] is not None
    assert results_no_distinct_column['fill_rate'] is not None


def test_fill_rate_values(results_distinct_column):
    assert sorted(results_distinct_column['fill_rate']) == [
        {'field': 'claim_id', 'fill': 1.0},
        {'field': 'col_2', 'fill': 1.0},
        {'field': 'hvid', 'fill': 1.0}
    ]


def test_fill_rate_dataframe_count(results_distinct_column, results_no_distinct_column):
    assert len(results_distinct_column['fill_rate']) == 3
    assert len(results_no_distinct_column['fill_rate']) == 2


def test_no_df_if_fill_rates_is_none_in_provider_conf(results_no_fill_rate):
    assert results_no_fill_rate['fill_rate'] is None
