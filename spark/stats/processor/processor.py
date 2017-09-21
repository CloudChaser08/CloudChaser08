import spark.stats.calc.fill_rate as fill_rate
import spark.helpers.stats.limit_date_range as limit_date_range
import spark.helpers.stats.select_distinct_column as select_distinct_column
import spark.helpers.postprocessor as postprocessor

def _run_fill_rates(df):
    '''
    A wrapper around fill_rates calculate fill rate method
    Input:
        -df: a dataframe
    Output:
        - _: a dataframe with the result of fill_rate.calculate_fill_rate
    '''
    return fill_rate.calculate_fill_rate(df)


def _get_all_data(sqlContext, datatype, provider_name):
    '''
    Retrieves all the data for a provider from our metastore
    Input:
        - sqlContext: a pyspark.sql.SQLContext object for querying the metastore
        - datatype: a string describing the datatype for the provider (i.e. medicalclaims, events, etc...)
        - provider_name: a string that is the name of the provider in the part_provider field
    Output:
        - all_data_df: a dataframe of all the data for the given provider
    '''
    all_data_df = sqlContext.sql(
            'SELECT * FROM {datatype} WHERE part_provider = {provider_name}'.format(
                datatype = datatype,
                provider_name = provider_name
            )
    )
    return all_data_df


def _get_provider_conf(provider_name):
    '''
    Input:
        provider_name: a string that is the name of the provider
    Output:
        datafeed_id: the datafeed id for the provider on HealthVerity Marketplace
        datatype: a string describing the datatype for the provider (i.e. medicalclaims, events, etc...)
        date_field: the column in the data to do date comparisons on
        record_field: the column in the data to determine what to consider as a distinct "row" count.
                      if None, then just use number of rows in data
    '''
    #TODO: Implement after module complete
    datafeed_id = None
    datatype = None
    date_field = None
    record_field = None

    return {
        'datafeed_id': datafeed_id,
        'datatype': datatype,
        'date_field': date_field,
        'record_field': record_field
    }


def run_marketplace_stats(spark, sqlContext, provider_name, quarter, \
                          start_date, end_date, earliest_date, \
                          get_data_func, get_provider_conf_func):
    '''
    Runs all the relevant marketplace stats for a provider in a given
    date range / quarter
    Input:
        - spark: spark session object
        - sqlContext: SQLContext of the spark session
        - provider_name: name of the provider we are running stats on
        - quarter: quarter to run stats on 
        - start_date: starting date of the date range
        - end_date: ending date of the date range
        - earliest_date: earliest date for a particular stat calc (forget right now, not fill rates)
        - get_data_func: function that fetches the data that we'll calculate stats on
        - get_provider_conf_func: function that gets the providers config info
    Output:
        - all_dfs: a dict of dataframes for each marketplace stat calculated
    '''

    # Get provider config
    provider_conf = get_provider_conf_func(provider_name)

    # pull out some variables from provider_conf
    datatype = provider_conf['datatype']
    date_column_field = provider_conf['date_field']
    distinct_column_name = provider_conf['record_field']

    # Get data
    all_data_df = get_data_func(sqlContext, datatype, provider_name)

    # provider, start_date, end_date df cache
    # used for fill rate, top values, and key stats
    if distinct_column_name:
        reduced_df_1 = postprocessor.compose(
            limit_date_range.limit_date_range(start_date, end_date, date_column_field),
            select_distinct_column.select_distinct_column(distinct_column_name)
        )(all_data_df).cache()
    else:
        reduced_df_1 = postprocessor.compose(
            limit_date_range.limit_date_range(start_date, end_date, date_column_field)
        )(all_data_df).cache()

    print 'reduced_df_1'
    reduced_df_1.show()

    # datatype, provider, earliest_date, end_date df cache
    # used for longitudinality and year over year
    reduced_df_2 = None

    # datafeed_id, provider, datatype, quarter df cache
    # used for epidemiological
    reduced_df_3 = None

    # Generate fill rates
    fill_rates_df = _run_fill_rates(reduced_df_1)

    # Generate key stats
    key_stats_df = None

    # Generate top values
    top_values_df = None

    # Generate Longitudinality
    longitudinality_df = None

    # Generate Epidemiological calculations
    epi_calcs_df = None

    # Return all the dfs
    all_dfs = {
        'fill_rates': fill_rates_df,
        'key_stats': key_stats_df,
        'top_values': top_values_df,
        'longitudinality': longitudinality_df,
        'epi_calcs': epi_calcs_df
    }
    return all_dfs


def run(spark, sqlContext, provider_name, datafeed_id, quarter, start_date, end_date, earliest_date):
    return run_marketplace_stats(spark, sqlContext, provider_name, datafeed_id, quarter, start_date, end_date, earliest_date, _do_get_all_data)


