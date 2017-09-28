import pytest

from pyspark.sql import Row

import spark.stats.processor.processor as processor

results_distinct_column = None
results_no_distinct_column = None

provider_name = None

data_row = None

def cleanup(spark):
    pass


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, provider_name, results_distinct_column, \
            results_no_distinct_column, data_row

    provider_name = 'test'

    spark_obj = spark['spark']
    sqlContext = spark['sqlContext']
    
    quarter = 'Q32017'
    start_date = '2015-06-27'
    end_date = '2017-03-15'
    earliest_date = '1992-11-07'

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


    def _inject_get_provider_conf(*params):
        return {
            'datafeed_id'   : '27',
            'datatype'      : 'medicalclaims',
            'date_field'    : 'service_date',
            'record_field'  : 'claim_id'
        }


    def _inject_get_provider_conf_no_unique_column(*params):
        return {
            'datafeed_id'   : '27',
            'datatype'      : 'medicalclaims',
            'date_field'    : 'service_date',
            'record_field'  : None
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

def test_fill_rate_calculated():
    assert results_distinct_column['fill_rates'] != None
    assert results_no_distinct_column['fill_rates'] != None


def test_fill_rate_dataframe_count():
    assert len(results_distinct_column['fill_rates']) == 1
    assert len(results_no_distinct_column['fill_rates']) == 1


