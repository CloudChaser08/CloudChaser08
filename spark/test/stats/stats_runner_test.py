import pytest
from mock import Mock

import spark.stats.stats_runner as stats_runner

from pyspark.sql import Row
from subprocess import check_call, check_output

import spark.stats.config.reader.config_reader as config_reader
import spark.helpers.stats.utils as stats_utils

results = None
output_dir = None
data_row = None

provider = None
quarter = None
start_date = None
end_date = None
earliest_date = None

# Mocked functions
old_get_data_func = None
old_generate_get_provider_config_function_func = None

@pytest.mark.usefixtures('spark')
def test_init(spark):
    global results, output_dir, data_row, provider, quarter, \
            start_date, end_date, earliest_date, output_dir, \
            old_get_data_func, old_generate_get_provider_config_function_func

    output_dir = '/'.join(__file__.split('/')[:-1]) + '/output/'

    data_row = Row('claim_id', 'service_date', 'col_1', 'col_2', 'col_3')
    
    # Mock get_provider_data
    old_get_data_func = stats_utils.get_provider_data
    stats_utils.get_provider_data = Mock(
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

    # Mock generate_get_provider_config_function
    old_generate_get_provider_config_function_func = config_reader.generate_get_provider_config_function
    config_reader.generate_get_provider_config_function = Mock(
        return_value = lambda *x: {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : 'service_date',
            'record_field'      : 'claim_id',
            'fill_rates'        : { 'blacklist_columns': ['col_1', 'col_2'] },
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calcs'         : None
        }
    )
    
    provider = 'test_provider'
    quarter = 'Q12017'
    start_date = '2015-04-01'
    end_date = '2017-04-01'
    earliest_date = '1990-01-01'
    results = stats_runner.run(spark['spark'], spark['sqlContext'], \
            provider, quarter, start_date, end_date, \
            earliest_date, output_dir)


def test_output_directory_created():
    test_dir = '/'.join(__file__.split('/')[:-1]) + '/'
    assert 'output' in check_output(['ls', test_dir]).split('\n')


def test_csv_made_for_each_non_null_stat_calc():
    output_files = check_output(['ls', output_dir]).split('\n')
    created_stats = ['fill_rates']
    for stat in created_stats:
        assert 1 in [1 if stat in f else 0 for f in output_files]


def test_cleanup():
    # Replace mocked functions
    stats_utils.get_provider_data = old_get_data_func
    config_reader.generate_get_provider_config_function = old_generate_get_provider_config_function_func

    check_call(['rm', '-r', output_dir])


