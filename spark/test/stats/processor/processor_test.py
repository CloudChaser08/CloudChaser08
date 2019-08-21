"""
    Tests for processor module
"""
import pytest
from mock import Mock

from pyspark.sql import Row

import spark.stats.processor as processor
from spark.stats.models import Column

COLUMNS = {
    'hvid': Column(
        name='hvid', field_id='1', sequence='1', top_values=True,
        datatype='string', description='HV ID'
    ),
    'col_2': Column(
        name='col_2', field_id='2', sequence='2', top_values=True,
        datatype='string', description='Column 2'
    )
}


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
    ]).toDF()

    yield Mock(
        all_data=dataframe,
        sampled_data=dataframe,
        sampled_data_multiplier=1
    )


def test_fill_rate_record_field(provider_conf, df_provider):
    """ Tests fill rate with a record field"""
    prov_conf = provider_conf.copy_with(
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field='claim_id',
        fill_rate=True,
        columns=COLUMNS,
        earliest_date='1992-11-07'
    )

    res = processor.run_fill_rates(prov_conf, df_provider)
    assert res is not None
    assert sorted(res) == [
        {'field': 'claim_id', 'fill': 1.0},
        {'field': 'col_2', 'fill': 1.0},
        {'field': 'hvid', 'fill': 1.0}
    ]


def test_fill_rate_no_record_field(provider_conf, df_provider):
    """ Tests fill rate without a record field """

    prov_conf = provider_conf.copy_with(
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field=None,
        fill_rate=True,
        columns=COLUMNS,
        earliest_date='1992-11-07'
    )

    res = processor.run_fill_rates(prov_conf, df_provider)
    assert res is not None
    assert sorted(res) == [
        {'field': 'col_2', 'fill': 7.0 / 9.0},
        {'field': 'hvid', 'fill': 7.0 / 9.0}
    ]


def test_no_fill_rate(provider_conf, df_provider):
    """ Tests fill rate without a record field """

    prov_conf = provider_conf.copy_with(
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field=None,
        fill_rate=False,
        columns=COLUMNS,
        earliest_date='1992-11-07'
    )

    res = processor.run_fill_rates(prov_conf, df_provider)
    assert res is None
