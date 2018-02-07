import spark.stats.calc.fill_rate as fill_rate
import spark.stats.calc.top_values as top_values
import spark.stats.calc.key_stats as key_stats
import spark.stats.calc.longitudinality as longitudinality
import spark.stats.calc.year_over_year as year_over_year
import spark.stats.config.reader.config_reader as config_reader
import spark.helpers.stats.utils as utils
import spark.helpers.postprocessor as postprocessor
import spark.helpers.file_utils as file_utils
import inspect

from pyspark.sql.functions import col

def _run_fill_rates(df, provider_conf):
    '''
    A wrapper around fill_rates calculate fill rate method
    Input:
        -df: a dataframe
        -provider_conf: a dictionary w/ the providers configuration data
    Output:
        - _: a dataframe with the result of fill_rate.calculate_fill_rate
             or None if provider_conf specifies not to calculate
    '''
    if provider_conf.get('fill_rate_conf'):
        # Get only the columns needed to calculate fill rates on
        cols = [c for c in df.columns if c in provider_conf['fill_rate_conf']['columns']]
        fill_rate_cols_df = df.select(*cols)
        return fill_rate.calculate_fill_rate(fill_rate_cols_df)

    return None

def _run_top_values(df, provider_conf):
    '''
    A wrapper around top_values method calculating the N most common values
    in each column
    Input:
        -df: a dataframe
        -provider_conf: a dictionary w/ the providers configuration data
    Output:
        - _: a dataframe with the result of top_values.calculate_top_values
             or None if provider_conf specifies not to calculate
    '''
    if provider_conf.get('top_values_conf'):
        # Get only the columns needed to calculate fill rates on
        cols = [c for c in df.columns if c in provider_conf['top_values_conf']['columns']]
        max_num_values = provider_conf['top_values_conf']['max_values']
        top_values_cols_df = df.select(*cols)
        return top_values.calculate_top_values(top_values_cols_df, max_num_values)

    return None

def _run_key_stats(df, earliest_date, start_date, end_date, provider_conf):
    if provider_conf.get('key_stats'):
        return key_stats.calculate_key_stats(df, earliest_date, start_date,
                end_date, provider_conf)

    return None

def _run_longitudinality(df, provider_conf):
    if provider_conf.get('longitudinality'):
        return longitudinality.calculate_longitudinality(df, provider_conf)

    return None

def _run_year_over_year(df, earliest_date, end_date, provider_conf):
    if provider_conf.get('year_over_year'):
        return year_over_year.calculate_year_over_year(df, earliest_date, end_date, provider_conf)

    return None


def run_marketplace_stats(spark, sqlContext, feed_id, quarter, \
                          start_date, end_date):
    '''
    Runs all the relevant marketplace stats for a provider in a given
    date range / quarter
    Input:
        - spark: spark session object
        - sqlContext: SQLContext of the spark session
        - feed_id: id of the provider feed we are running stats on
        - quarter: quarter to run stats on
        - start_date: starting date of the date range
        - end_date: ending date of the date range
    Output:
        - all_stats: a dict of lists of Rows for each marketplace stat calculated
    '''

    # Get provider config
    this_file = inspect.getmodule(inspect.stack()[1][0]).__file__
    config_file = file_utils.get_abs_path(this_file, 'config/providers.json')
    provider_conf = config_reader.get_provider_config(
                                    config_file, feed_id)

    # pull out some variables from provider_conf
    datatype = provider_conf['datatype']
    date_column_field = provider_conf['date_field']
    distinct_column_name = provider_conf['record_field']
    earliest_date = provider_conf['earliest_date']
    index_all_dates = provider_conf.get('index_all_dates', False)

    # Get data
    all_data_df = utils.get_provider_data(sqlContext, datatype, provider_conf['name'])

    # Desired number of partitions when calculating
    partitions = int(spark.conf.get('spark.sql.shuffle.partitions'))

    # provider, start_date, end_date df cache
    # used for fill rate, top values, and key stats
    if index_all_dates:
        gen_stats_df = utils.select_data_in_date_range('1900-01-01', \
                        end_date, date_column_field)(all_data_df).coalesce(partitions)
    else:
        gen_stats_df = utils.select_data_in_date_range(start_date, \
                        end_date, date_column_field)(all_data_df).coalesce(partitions)

    # Generate fill rates
    fill_rates = _run_fill_rates(gen_stats_df, provider_conf)

    # Generate top values
    top_values = _run_top_values(gen_stats_df, provider_conf)

    # Generate key stats
    if index_all_dates:
        key_stats = _run_key_stats(all_data_df, earliest_date, \
                        '1900-01-01', end_date, provider_conf)
    else:
        key_stats = _run_key_stats(all_data_df, earliest_date, \
                        start_date, end_date, provider_conf)

    # datatype, provider, earliest_date, end_date df cache
    # used for longitudinality and year over year
    date_stats_df = all_data_df.select("hvid", provider_conf['date_field']).coalesce(partitions)

    # Generate Longitudinality
    longitudinality = _run_longitudinality(date_stats_df, provider_conf)

    # Generate year over year
    year_over_year = _run_year_over_year(date_stats_df, earliest_date, end_date, provider_conf)

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
