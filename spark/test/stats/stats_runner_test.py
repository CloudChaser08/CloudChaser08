import pytest
from mock import Mock

import spark.stats.stats_runner as stats_runner

from pyspark.sql import Row

import spark.helpers.stats.utils as stats_utils
import spark.stats.stats_writer as stats_writer

results = None
output_dir = None
data_row = None

provider = None
quarter = None
start_date = None
end_date = None
earliest_date = None

@pytest.fixture(autouse=True)
def setup_teardown():
    old_get_data_func = stats_utils.get_provider_data
    old_write_to_db = stats_writer.write_to_db

    yield

    stats_utils.get_provider_data = old_get_data_func
    stats_writer.write_to_db = old_write_to_db


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global results, output_dir, data_row, provider, quarter, \
            start_date, end_date, earliest_date, output_dir, \
            old_get_data_func, old_get_provider_config_func

    data_row = Row('claim_id', 'service_date', 'hvid', 'col_2', 'col_3')

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

    stats_writer.write_to_db = Mock()

    provider_config = {
            'name'              : 'test',
            'datafeed_id'       : '27',
            'datatype'          : 'medicalclaims',
            'date_field'        : 'service_date',
            'record_field'      : 'claim_id',
            'fill_rates'        : True,
            'fill_rate_conf'    : {'columns': {'claim_id': 1, 'service_date': 2, 'col_3': 3}},
            'key_stats'         : None,
            'top_values'        : None,
            'longitudinality'   : None,
            'year_over_year'    : None,
            'epi_calc'          : None,
            'earliest_date'     : '1990-01-01'
        }

    quarter = 'Q12017'
    start_date = '2015-04-01'
    end_date = '2017-04-01'
    results = stats_runner.run(spark['spark'], spark['sqlContext'],
                               quarter, start_date, end_date, provider_config)


def test_stats():
    assert results == {
        'fill_rates': [{'field': u'claim_id', 'fill': 1.0},
                       {'field': u'service_date', 'fill': 1.0},
                       {'field': u'col_3', 'fill': 0.3333333333333333}],
        'key_stats': None,
        'longitudinality': None,
        'top_values': None,
        'year_over_year': None
    }
