import pytest
from mock import Mock

from pyspark.sql import Row

import spark.stats.processor as processor

import spark.stats.config.reader.config_reader as config_reader
import spark.helpers.stats.utils as stats_utils

results_distinct_column = None
results_no_distinct_column = None
results_no_fill_rate = None

provider_name = None

columns = None
data_row = None

fill_rate_conf = None

@pytest.fixture(autouse=True)
def setup_teardown():
    old_get_data_func = stats_utils.get_provider_data

    yield

    stats_utils.get_provider_data = old_get_data_func


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, provider_name, results_distinct_column, \
            results_no_distinct_column, results_no_fill_rate, \
            columns, data_row, fill_rate_conf, old_get_data_func, \
            old_get_provider_config_func

    spark_obj = spark['spark']
    sqlContext = spark['sqlContext']

    quarter = 'Q32017'
    start_date = '2015-06-27'
    end_date = '2017-03-15'

    columns = {'claim_id': 0, 'col_2': 3, 'col_3': 4, 'hvid': 2, 'service_date': 1}
    data_row = Row(*sorted(columns.keys()))

    inject_data_mock = Mock(
        return_value = spark['spark'].sparkContext.parallelize([
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
    )

    stats_utils.get_provider_data = inject_data_mock

    fill_rate_conf = {"columns": {'hvid': 1, 'col_2': 2}}

    prov_conf = {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : ['service_date'],
            'record_field'      : 'claim_id',
            'fill_rate'         : True,
            'fill_rate_conf'    : fill_rate_conf,
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calcs'         : None,
            'earliest_date'     : '1992-11-07'
        }

    prov_conf_no_unique_column = {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : ['service_date'],
            'record_field'      : None,
            'fill_rate'         : True,
            'fill_rate_conf'    : fill_rate_conf,
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calcs'         : None,
            'earliest_date'     : '1992-11-07'
        }

    prov_conf_no_fill_rate_calc = {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : ['service_date'],
            'record_field'      : None,
            'fill_rate'         : None,
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calcs'         : None,
            'earliest_date'     : '1992-11-07'
        }

    results_distinct_column = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    quarter, start_date, \
                    end_date, prov_conf)

    results_no_distinct_column = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    quarter, start_date, \
                    end_date, prov_conf_no_unique_column)

    results_no_fill_rate = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    quarter, start_date, \
                    end_date, prov_conf_no_fill_rate_calc)

def test_fill_rate_calculated():
    assert results_distinct_column['fill_rate'] is not None
    assert results_no_distinct_column['fill_rate'] is not None


def test_fill_rate_values():
    assert sorted(results_distinct_column['fill_rate'], key=lambda c: c['field']) == [
        {'field': 'claim_id', 'fill': 1.0},
        {'field': 'col_2', 'fill': 1.0},
        {'field': 'hvid', 'fill': 1.0}
    ]


def test_fill_rate_dataframe_count():
    assert len(results_distinct_column['fill_rate']) == 3
    assert len(results_no_distinct_column['fill_rate']) == 2


def test_no_df_if_fill_rates_is_none_in_provider_conf():
    assert results_no_fill_rate['fill_rate'] == None
