import pytest

import spark.stats.stats_runner as stats_runner

from pyspark.sql import Row

results = None
output_dir = None
data_row = None

@pytest.mark.usefixtures('spark')
def test_init(spark):
    global results, output_dir, data_row

    output_dir = '/'.join(__file__.split('/')[:-1]) + '/output/'

    data_row = Row('claim_id', 'service_date', 'col_1', 'col_2', 'col_3')
    
    def _inject_get_data(*params):
        df = spark['spark'].sparkContext.parallelize([
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
        return df


    fill_rate_conf = { 'columns': ['claim_id', 'service_date', 'col_3'] }

    def _inject_get_provider_conf(*params):
        return {
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
    
    results = stats_runner.run(spark['spark'], spark['sqlContext'], \
            'test_provider', 'Q12017', '2015-04-01', '2017-04-01', \
            '1990-01-01', _inject_get_data, _inject_get_provider_conf, \
            output_dir)


def test_something():
    print results
    results['fill_rates'].show()


def test_output_directory_created():
    pass


def test_directories_made_for_each_stat_calc():
    pass


def test_csv_for_each_stat_calc():
    pass


