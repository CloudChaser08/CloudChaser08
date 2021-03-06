import pytest
from mock import patch

from pyspark.sql import Row

import spark.helpers.stats.utils as stats_utils
import spark.stats.stats_runner as stats_runner
from spark.stats.models import Provider, ProviderModel, Column, TableMetadata
from spark.stats.models.results import (
    ProviderStatsResult, StatsResult, FillRateResult, YearOverYearResult,
    LongitudinalityResult
)

TABLE = TableMetadata(
    name='tbl',
    description='desc',
    columns=[
        Column(
            name='claim_id', field_id='1', sequence='1', top_values=False,
            datatype='string', description='Claim ID', category='Baseline',
        ),
        Column(
            name='service_date', field_id='2', sequence='2', top_values=False,
            datatype='string', description='Service Date', category='Baseline',
        ),
        Column(
            name='col_3', field_id='2', sequence='2', top_values=False,
            datatype='string', description='Column 3', category='Baseline',
        ),
    ]
)

@pytest.fixture(autouse=True)
def setup_teardown(spark):
    data_row = Row('claim_id', 'service_date', 'hvid', 'col_2', 'col_3')
    prov_data = spark['spark'].sparkContext.parallelize([
        data_row('0', '1995-10-11', None, 'a', 'b'),
        data_row('0', '2016-01-12', 'a', 'b', 'c'),
        data_row('0', '2017-01-12', 'a', 'b', 'c'),
        data_row('1', '2015-11-08', 'a', 'b', '  '),
        data_row('1', '1974-03-02', '   ', 'b', 'c'),
        data_row('1', '1993-07-13', 'a', '       ', 'c'),
        data_row('1', '2017-03-15', 'a', '    ', None),
        data_row('2', '1800-01-01', 'a', 'b', 'c'),
        data_row('2', '1850-01-01', 'a', 'b', 'c'),
        data_row('2', '1900-01-01', 'a', 'b', 'c')
    ]).toDF()
    emr_union = spark['spark'].sparkContext.parallelize([
        data_row('0', '1995-10-11', None, 'a', 'b'),
        data_row('0', '2016-01-12', 'a', 'b', 'c'),
        data_row('0', '2017-01-12', 'a', 'b', 'c'),
        data_row('1', '1974-03-02', '   ', 'b', 'c'),
        data_row('1', '1993-07-13', 'a', '       ', 'c'),
        data_row('1', '2017-03-15', 'a', '    ', None),
        data_row('2', '1800-01-01', 'a', 'b', 'c'),
        data_row('2', '1850-01-01', 'a', 'b', 'c'),
        data_row('2', '1900-01-01', 'a', 'b', 'c')
    ]).toDF()
    with patch.object(stats_utils, 'get_provider_data', return_value=prov_data), \
        patch.object(stats_utils, 'get_emr_union', return_value=emr_union):

        yield


def test_standard_stats(spark):
    provider_config = Provider(
        name='test',
        datafeed_id='27',
        datatype='medicalclaims',
        date_fields=['service_date'],
        record_field='claim_id',
        fill_rate=True,
        table=TABLE,
        key_stats=False,
        top_values=False,
        longitudinality=False,
        year_over_year=False,
        epi_calcs=False,
        earliest_date='1990-01-01'
    )

    start_date = '2015-04-01'
    end_date = '2017-04-01'
    results = stats_runner.run(spark['spark'], spark['sqlContext'],
                               start_date, end_date, provider_config)

    assert results == ProviderStatsResult(
        results=StatsResult(
            fill_rate=[
                FillRateResult(field='service_date', fill=1.0),
                FillRateResult(field='claim_id', fill=1.0),
                FillRateResult(field='col_3', fill=0.5),
            ],
        ),
        model_results={},
        config=provider_config
    )


def test_emr_fill_rates(spark):
    provider_config = Provider(
        name='test_emr',
        datafeed_id='48',
        datatype='emr',
        date_fields=['emr_date'],
        models=[
            ProviderModel(
                datatype='emr_diag',
                date_fields=['service_date'],
                record_field='claim_id',
                fill_rate=True,
                table=TABLE
            ),
            ProviderModel(
                datatype='emr_clin_obsn',
                date_fields=['service_date'],
                record_field='claim_id',
                fill_rate=True,
                table=TABLE,
            )
        ],
        earliest_date='1990-01-01',
        epi_calcs=False
    )

    start_date = '2015-04-01'
    end_date = '2017-04-01'
    results = stats_runner.run(spark['spark'], spark['sqlContext'],
                               start_date, end_date, provider_config)

    assert results == ProviderStatsResult(
        results=StatsResult(),
        model_results={
            'emr_diag': StatsResult(
                fill_rate=[
                    FillRateResult(field='service_date', fill=1.0),
                    FillRateResult(field='claim_id', fill=1.0),
                    FillRateResult(field='col_3', fill=0.5),
                ],
            ),
            'emr_clin_obsn': StatsResult(
                fill_rate=[
                    FillRateResult(field='service_date', fill=1.0),
                    FillRateResult(field='claim_id', fill=1.0),
                    FillRateResult(field='col_3', fill=0.5),
                ],
            ),
        },
        config=provider_config
    )


def test_emr_year_over_year_long(spark):
    enc_provider_config = Provider(
        name='test_emr',
        datafeed_id='48',
        datatype='emr',
        date_fields=['emr_date'],
        models=[
            ProviderModel(
                datatype='emr_diag',
                date_fields=['service_date'],
                record_field='claim_id'
            ),
            ProviderModel(
                datatype='emr_clin_obsn',
                date_fields=['service_date'],
                record_field='claim_id'
            ),
            ProviderModel(
                datatype='emr_enc',
                date_fields=['service_date'],
                record_field='claim_id'
            )
        ],
        earliest_date='1990-01-01',
        year_over_year=True,
        longitudinality=True,
        epi_calcs=False
    )

    union_provider_config = Provider(
        name='test_emr',
        datafeed_id='48',
        datatype='emr',
        date_fields=['service_date'],
        models=[
            ProviderModel(
                datatype='emr_diag',
                date_fields=['service_date'],
                record_field='claim_id'
            ),
            ProviderModel(
                datatype='emr_clin_obsn',
                date_fields=['service_date'],
                record_field='claim_id'
            )
        ],
        earliest_date='1990-01-01',
        year_over_year=True,
        longitudinality=True,
        epi_calcs=False
    )

    start_date = '2015-04-01'
    end_date = '2017-04-01'
    enc_results = stats_runner.run(spark['spark'], spark['sqlContext'],
                                   start_date, end_date, enc_provider_config)
    union_results = stats_runner.run(spark['spark'], spark['sqlContext'],
                                     start_date, end_date, union_provider_config)

    assert sorted(enc_results.results.year_over_year, key=lambda r: r.year) == [
        YearOverYearResult(count=1, year=2015),
        YearOverYearResult(count=1, year=2016),
        YearOverYearResult(count=1, year=2017)
    ]
    assert sorted(union_results.results.year_over_year, key=lambda r: r.year) == [
        YearOverYearResult(count=1, year=2016),
        YearOverYearResult(count=1, year=2017)
    ]

    # since these results are different, we can infer that different
    # datasets were used. Since the average is lower for union_resuls
    # (and this table has less data), this means that the encounter
    # table is used for enc_results, and the union table is used for
    # union_results.
    assert sorted(enc_results.results.longitudinality, key=lambda r: r.average) == [
        LongitudinalityResult(
            average=1, duration='0 months', std_dev=0, value=2
        ),
        LongitudinalityResult(
            average=6, duration='27 years', std_dev=0, value=1
        )
    ]
    assert sorted(union_results.results.longitudinality) == [
        LongitudinalityResult(
            average=1, duration='0 months', std_dev=0, value=2
        ),
        LongitudinalityResult(
            average=5, duration='27 years', std_dev=0, value=1
        )
    ]
