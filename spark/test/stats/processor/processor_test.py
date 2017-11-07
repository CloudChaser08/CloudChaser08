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

old_get_data_func = None
old_generate_get_provider_config_function_func = None

def cleanup(spark):
    pass


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, provider_name, results_distinct_column, \
            results_no_distinct_column, results_no_fill_rate, \
            columns, data_row, fill_rate_conf, old_get_data_func, \
            old_generate_get_provider_config_function_func

    provider_name = 'test'

    spark_obj = spark['spark']
    sqlContext = spark['sqlContext']
    
    quarter = 'Q32017'
    start_date = '2015-06-27'
    end_date = '2017-03-15'
    earliest_date = '1992-11-07'

    columns = ['claim_id', 'service_date', 'col_1', 'col_2', 'col_3']
    data_row = Row(*columns)

    old_get_data_func = stats_utils.get_provider_data
    old_generate_get_provider_config_function_func = config_reader.generate_get_provider_config_function

    inject_data_mock = Mock(
        return_value = spark['spark'].sparkContext.parallelize([
            data_row('0', '1995-10-11', None, 'a', 'b'),
            data_row('0', '2016-01-12', 'a', 'b', 'c'),
            data_row('1', '2015-11-08', 'a', 'b', '  '),
            data_row('1', '1974-03-02', '   ', 'b', 'c'),
            data_row('1', '1993-07-13', 'a', '       ', 'c'),
            data_row('1', '2017-03-15', 'a', '    ', None),
            data_row('2', '1800-01-01', 'a', 'b', 'c'),
            data_row('2', '1850-01-01', 'a', 'b', 'c'),
            data_row('2', '1900-01-01', 'a', 'b', 'c')
        ]).toDF()
    )

    stats_utils.get_provider_data = inject_data_mock

    fill_rate_conf = { 'blacklist_columns': ['claim_id', \
                               'service_date', 'col_3'] }

    get_prov_conf = Mock(
        return_value = lambda *x: {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : 'service_date',
            'record_field'      : 'claim_id',
            'fill_rates'        : fill_rate_conf,
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calcs'         : None
        }
    )

    get_prov_conf_no_unique_column = Mock(
        return_value = lambda *x: 
        {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : 'service_date',
            'record_field'      : None,
            'fill_rates'        : fill_rate_conf,
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calcs'         : None
        }
    )

    get_prov_conf_no_fill_rate_calc = Mock(
        return_value = lambda *x: {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : 'service_date',
            'record_field'      : None,
            'fill_rates'        : None,
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calcs'         : None
        }
    )

    config_reader.generate_get_provider_config_function = get_prov_conf
    results_distinct_column = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    provider_name, quarter, start_date, end_date, \
                    earliest_date)

    config_reader.generate_get_provider_config_function = get_prov_conf_no_unique_column
    results_no_distinct_column = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    provider_name, quarter, start_date, end_date, \
                    earliest_date)

    config_reader.generate_get_provider_config_function = get_prov_conf_no_fill_rate_calc
    results_no_fill_rate = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    provider_name, quarter, start_date, end_date, \
                    earliest_date)

def test_fill_rate_calculated():
    assert results_distinct_column['fill_rates'] is not None
    assert results_no_distinct_column['fill_rates'] is not None


def test_fill_rate_dataframe_count():
    assert len(results_distinct_column['fill_rates']) == 1
    assert len(results_no_distinct_column['fill_rates']) == 1


def test_fill_rate_column_are_blacklisted():
    assert len(set(results_distinct_column['fill_rates'][0].asDict().keys()) \
                .intersection(set(fill_rate_conf['blacklist_columns']))) == 0
    assert len(set(results_no_distinct_column['fill_rates'][0].asDict().keys()) \
                .intersection(set(fill_rate_conf['blacklist_columns']))) == 0


def test_no_df_if_fill_rates_is_none_in_provider_conf():
    assert results_no_fill_rate['fill_rates'] == None


def test_cleanup():
    stats_utils.get_provider_data = old_get_data_func
    config_reader.generate_get_provider_config_function = old_generate_get_provider_config_function_func


