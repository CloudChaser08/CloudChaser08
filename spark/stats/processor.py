import spark.stats.calc.fill_rate as fill_rate
import spark.helpers.stats.limit_date_range as limit_date_range
import spark.helpers.stats.select_distinct_column as select_distinct_column
import spark.helpers.postprocessor as postprocessor

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
            'SELECT * FROM {datatype} WHERE {partition_column} = {provider_name}'.format(
                datatype = datatype,
                partition_column = 'part_provider' if datatype != 'emr' else None,       # Can change once known
                provider_name = provider_name
            )
    )
    return all_data_df


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
        - all_stats: a dict of lists of Rows for each marketplace stat calculated
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
        gen_stats_df = postprocessor.compose(
            limit_date_range.limit_date_range(start_date, end_date, date_column_field),
            select_distinct_column.select_distinct_column(distinct_column_name)
        )(all_data_df).cache()
    else:
        gen_stats_df = limit_date_range.limit_date_range(start_date, \
                        end_date, date_column_field)(all_data_df).cache()


    # Generate fill rates
    fill_rates = fill_rate.calculate_fill_rate(gen_stats_df).collect()

    # Generate key stats
    key_stats = None

    # Generate top values
    top_values = None

    gen_stats_df.unpersist()

    # datatype, provider, earliest_date, end_date df cache
    # used for longitudinality and year over year
    date_stats_df = None

    # Generate Longitudinality
    longitudinality = None

    # Generate year over year
    year_over_year = None

    # datafeed_id, provider, datatype, quarter df cache
    # used for epidemiological
    epi_stats_df = None

    # Generate Epidemiological calculations
    epi_calcs = None

    # Return all the dfs
    all_stats = {
        'fill_rates': fill_rates,
        'key_stats': key_stats,
        'top_values': top_values,
        'longitudinality': longitudinality,
        'year_over_year': year_over_year,
        'epi_calcs': epi_calcs
    }
    return all_stats


