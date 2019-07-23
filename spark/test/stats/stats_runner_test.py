import pytest
from mock import Mock

import spark.stats.stats_runner as stats_runner

from pyspark.sql import Row

import spark.helpers.stats.utils as stats_utils
import spark.stats.stats_writer as stats_writer

# convenience datatype used in tests
data_row = Row('claim_id', 'service_date', 'hvid', 'col_2', 'col_3')
quarter = 'Q12017'

expected_results_dict = {
    'fill_rate': [{'field': u'claim_id', 'fill': 1.0},
                  {'field': u'service_date', 'fill': 1.0},
                  {'field': u'col_3', 'fill': 0.5}],
    'key_stats': None,
    'longitudinality': None,
    'top_values': None,
    'year_over_year': None
}

@pytest.mark.usefixtures('spark')
@pytest.fixture(autouse=True)
def setup_teardown(spark):
    old_get_data_func = stats_utils.get_provider_data
    old_get_emr_union = stats_utils.get_emr_union
    old_write_to_s3 = stats_writer.write_to_s3

    stats_writer.write_to_s3 = Mock()
    stats_utils.get_provider_data = Mock(
        return_value = spark['spark'].sparkContext.parallelize([
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
    )
    stats_utils.get_emr_union = Mock(
        return_value = spark['spark'].sparkContext.parallelize([
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
    )

    yield

    stats_utils.get_provider_data = old_get_data_func
    stats_utils.get_emr_union = old_get_emr_union
    stats_writer.write_to_s3 = old_write_to_s3


def test_standard_stats(spark):
    provider_config = {
        'name'              : 'test',
        'datafeed_id'       : '27',
        'datatype'          : 'medicalclaims',
        'date_field'        : ['service_date'],
        'record_field'      : 'claim_id',
        'fill_rates'        : True,
        'fill_rate_conf'    : {'columns': {'claim_id': 1, 'service_date': 2, 'col_3': 3}},
        'key_stats'         : None,
        'top_values'        : None,
        'longitudinality'   : None,
        'year_over_year'    : None,
        'epi_calcs'         : None,
        'earliest_date'     : '1990-01-01'
    }

    start_date = '2015-04-01'
    end_date = '2017-04-01'
    results = stats_runner.run(spark['spark'], spark['sqlContext'],
                               quarter, start_date, end_date, provider_config)

    assert results == expected_results_dict


def test_emr_fill_rates(spark):
    provider_config = {
        'name'         : 'test_emr',
        'datafeed_id'  : '48',
        'datatype'     : 'emr',
        'date_field'   : ['emr_date'],
        'models'       : [
            {
                'datatype'        : 'emr_diag',
                'date_field'      : ['service_date'],
                'record_field'    : 'claim_id',
                'fill_rates'      : True,
                'fill_rate_conf'  : {'columns': {'claim_id': 1, 'service_date': 2, 'col_3': 3}},
            }, {
                'datatype'        : 'emr_clin_obsn',
                'date_field'      : ['service_date'],
                'record_field'    : 'claim_id',
                'fill_rates'      : True,
                'fill_rate_conf'  : {'columns': {'claim_id': 1, 'service_date': 2, 'col_3': 3}},
            }
        ],
        'earliest_date'     : '1990-01-01',
        'epi_calcs'         : False
    }

    start_date = '2015-04-01'
    end_date = '2017-04-01'
    results = stats_runner.run(spark['spark'], spark['sqlContext'],
                               quarter, start_date, end_date, provider_config)

    assert sorted(results) == sorted({
        'emr_diag': expected_results_dict,
        'emr_clin_obsn': expected_results_dict
    })


def test_emr_year_over_year_long(spark):
    enc_provider_config = {
        'name'         : 'test_emr',
        'datafeed_id'  : '48',
        'datatype'     : 'emr',
        'date_field'   : ['emr_date'],
        'models'       : [
            {
                'datatype'        : 'emr_diag',
                'date_field'      : ['service_date'],
                'record_field'    : 'claim_id'
            }, {
                'datatype'        : 'emr_clin_obsn',
                'date_field'      : ['service_date'],
                'record_field'    : 'claim_id'
            },
            {
                'datatype'        : 'emr_enc',
                'date_field'      : ['service_date'],
                'record_field'    : 'claim_id'
            }
        ],
        'earliest_date'     : '1990-01-01',
        'year_over_year'    : True,
        'longitudinality'   : True,
        'epi_calcs'         : False
    }

    union_provider_config = {
        'name'         : 'test_emr',
        'datafeed_id'  : '48',
        'datatype'     : 'emr',
        'date_field'   : ['service_date'],
        'models'       : [
            {
                'datatype'        : 'emr_diag',
                'date_field'      : ['service_date'],
                'record_field'    : 'claim_id'
            }, {
                'datatype'        : 'emr_clin_obsn',
                'date_field'      : ['service_date'],
                'record_field'    : 'claim_id'
            }
        ],
        'earliest_date'     : '1990-01-01',
        'year_over_year'    : True,
        'longitudinality'   : True,
        'epi_calcs'         : False
    }

    start_date = '2015-04-01'
    end_date = '2017-04-01'
    enc_results = stats_runner.run(spark['spark'], spark['sqlContext'],
                                   quarter, start_date, end_date, enc_provider_config)
    union_results = stats_runner.run(spark['spark'], spark['sqlContext'],
                                     quarter, start_date, end_date, union_provider_config)

    assert sorted(enc_results['year_over_year']) == [{'count': 1, 'year': 2015},
                                                     {'count': 1, 'year': 2016},
                                                     {'count': 1, 'year': 2017}]
    assert sorted(union_results['year_over_year']) == [{'count': 1, 'year': 2016},
                                                       {'count': 1, 'year': 2017}]

    # since these results are different, we can infer that different
    # datasets were used. Since the average is lower for union_resuls
    # (and this table has less data), this means that the encounter
    # table is used for enc_results, and the union table is used for
    # union_results.
    assert sorted(enc_results['longitudinality']) == [
        {'average': 1, 'duration': '0 months', 'std_dev': 0, 'value': 2},
        {'average': 6, 'duration': '27 years', 'std_dev': 0, 'value': 1}
    ]
    assert sorted(union_results['longitudinality']) == [
        {'average': 1, 'duration': '0 months', 'std_dev': 0, 'value': 2},
        {'average': 5, 'duration': '27 years', 'std_dev': 0, 'value': 1}
    ]
