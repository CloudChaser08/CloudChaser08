import spark.stats.calc.fill_rate as fill_rate
import spark.stats.calc.top_values as top_values
import spark.stats.calc.key_stats as key_stats
import spark.stats.calc.longitudinality as longitudinality
import spark.stats.calc.year_over_year as year_over_year
import spark.stats.calc.epi as epi
import spark.helpers.stats.utils as utils


def _run_fill_rates(df, conf):
    '''
    A wrapper around fill_rates calculate fill rate method
    Input:
        -df: a dataframe
        -conf: a dictionary w/ the providers configuration data
    Output:
        - _: a dataframe with the result of fill_rate.calculate_fill_rate
             or None if conf specifies not to calculate
    '''
    if conf.get('fill_rate_conf'):
        # Get only the columns needed to calculate fill rates on
        cols = [c for c in df.columns if c in conf['fill_rate_conf']['columns'].keys()]
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
        cols = [c for c in df.columns if c in provider_conf['top_values_conf']['columns'].keys()]
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


def run_marketplace_stats(spark, sqlContext, quarter, \
                          start_date, end_date, provider_conf):
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

    # pull out some variables from provider_conf
    datatype = provider_conf['datatype']
    date_column_field = provider_conf['date_field']
    earliest_date = provider_conf['earliest_date']
    index_all_dates = provider_conf.get('index_all_dates', False)

    # Get data
    all_data_df = utils.get_provider_data(
        sqlContext, datatype,
        provider_conf['datafeed_id'] if datatype.startswith('emr') else provider_conf['name']
    )

    # Desired number of partitions when calculating
    partitions = int(spark.conf.get('spark.sql.shuffle.partitions'))

    # provider, start_date, end_date df cache
    # used for fill rate, top values, and key stats
    if index_all_dates:
        gen_stats_df = utils.select_data_in_date_range(
            '1900-01-01', end_date, date_column_field, include_nulls=provider_conf.get('index_null_dates')
        )(all_data_df).coalesce(partitions)
    else:
        gen_stats_df = utils.select_data_in_date_range(
            start_date, end_date, date_column_field, include_nulls=provider_conf.get('index_null_dates')
        )(all_data_df).coalesce(partitions)

    # Generate fill rates
    fill_rates = _run_fill_rates(gen_stats_df, provider_conf)

    # Generate top values
    top_values = _run_top_values(gen_stats_df, provider_conf)

    # Generate key stats
    key_stats = _run_key_stats(
        all_data_df, earliest_date, start_date, end_date, provider_conf
    )

    # datatype, provider, earliest_date, end_date df cache
    # used for longitudinality and year over year
    date_stats_df = all_data_df.select("hvid", provider_conf['date_field']).coalesce(partitions)

    # Generate Longitudinality
    longitudinality = _run_longitudinality(date_stats_df, provider_conf)

    # Generate year over year
    year_over_year = _run_year_over_year(date_stats_df, earliest_date, end_date, provider_conf)

    # Return all the dfs
    all_stats = {
        'fill_rates': fill_rates,
        'key_stats': key_stats,
        'top_values': top_values,
        'longitudinality': longitudinality,
        'year_over_year': year_over_year
    }
    return all_stats


def get_epi_calcs(provider_conf):
    all_epi = {}

    if not provider_conf['epi_calcs']:
        return all_epi

    fields = provider_conf.get('epi_calc_fields', ['age', 'gender', 'state', 'region'])

    for f in fields:
        all_epi[f] = epi.calculate_epi(provider_conf, f)

    return all_epi
