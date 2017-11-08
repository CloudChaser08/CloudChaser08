import spark.stats.calc.fill_rate as fill_rate
import spark.stats.config.reader.config_reader as config_reader
import spark.helpers.stats.utils as utils
import spark.helpers.postprocessor as postprocessor

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
    if provider_conf['fill_rates']:
        # Get only the columns needed to calculate fill rates on
        cols = [c for c in df.columns if c not in \
                provider_conf['fill_rates']['blacklist_columns']]
        fill_rate_cols_df = df.select(*[col(c) for c in cols])
        fill_rates = fill_rate.calculate_fill_rate(fill_rate_cols_df).collect()
        return fill_rates

    return None


def run_marketplace_stats(spark, sqlContext, provider_name, quarter, \
                          start_date, end_date, earliest_date):
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
    Output:
        - all_stats: a dict of lists of Rows for each marketplace stat calculated
    '''

    # Get provider config
    config_dir = '/'.join(__file__.split('/')[:-1]) + '/config/'
    provider_conf = config_reader.generate_get_provider_config_function(
                                    config_dir, provider_name)

    # pull out some variables from provider_conf
    datatype = provider_conf['datatype']
    date_column_field = provider_conf['date_field']
    distinct_column_name = provider_conf['record_field']

    # Get data
    all_data_df = utils.get_provider_data(sqlContext, datatype, provider_name)

    # provider, start_date, end_date df cache
    # used for fill rate, top values, and key stats
    if distinct_column_name:
        gen_stats_df = postprocessor.compose(
            utils.select_data_in_date_range(start_date, end_date, date_column_field),
            utils.select_distinct_values_from_column(distinct_column_name)
        )(all_data_df).cache()
    else:
        gen_stats_df = utils.select_data_in_date_range(start_date, \
                        end_date, date_column_field)(all_data_df).cache()


    # Generate fill rates
    fill_rates = _run_fill_rates(gen_stats_df, provider_conf)

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


