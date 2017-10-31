import pytest

from pyspark.sql import Row

import spark.stats.processor as processor

results_distinct_column = None
results_no_distinct_column = None
results_no_fill_rate = None

provider_name = None

columns = None
data_row = None

fill_rate_conf = None

def cleanup(spark):
    pass


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, provider_name, results_distinct_column, \
            results_no_distinct_column, results_no_fill_rate, \
            columns, data_row, fill_rate_conf

    provider_name = 'test'

    spark_obj = spark['spark']
    sqlContext = spark['sqlContext']
    
    quarter = 'Q32017'
    start_date = '2015-06-27'
    end_date = '2017-03-15'
    earliest_date = '1992-11-07'

    columns = ['claim_id', 'service_date', 'col_1', 'col_2', 'col_3']
    data_row = Row(*columns)

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


    fill_rate_conf = { 'blacklist_columns': ['claim_id', \
                                            'service_date', 'col_3'] }

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


    def _inject_get_provider_conf_no_unique_column(*params):
        return {
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


    def _inject_get_provider_conf_no_fill_rate_calc(*params):
        return {
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


    results_distinct_column = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    provider_name, quarter, start_date, end_date, \
                    earliest_date, _inject_get_data, \
                    _inject_get_provider_conf)

    results_no_distinct_column = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    provider_name, quarter, start_date, end_date, \
                    earliest_date, _inject_get_data, \
                    _inject_get_provider_conf_no_unique_column)

    results_no_fill_rate = processor.run_marketplace_stats( \
                    spark_obj, sqlContext, \
                    provider_name, quarter, start_date, end_date, \
                    earliest_date, _inject_get_data, \
                    _inject_get_provider_conf_no_fill_rate_calc)

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


