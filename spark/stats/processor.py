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
        if conf.get('record_field'):
            if conf.get('record_field') not in cols:
                cols.append(conf.get('record_field'))
            df = utils.select_distinct_values_from_column(conf.get('record_field'))(df)
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
        if provider_conf.get('record_field') and provider_conf.get('record_field') not in cols:
            cols.append(provider_conf['record_field'])
        max_num_values = provider_conf['top_values_conf']['max_values']
        top_values_cols_df = df.select(*cols)
        return top_values.calculate_top_values(top_values_cols_df, max_num_values, distinct_column=provider_conf.get('record_field'))

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


def run_marketplace_stats(
        spark, sqlContext, quarter, start_date, end_date, provider_conf, stats_to_calculate=[
            'key_stats', 'longitudinality', 'year_over_year', 'fill_rates', 'top_values', 'epi'
        ]
):
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

    # if earliest_date is greater than start_date, use earliest_date as start_date
    if earliest_date > start_date:
        start_date = earliest_date

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
        multiplier, sampled_gen_stats_df = utils.select_data_sample_in_date_range(
            '1900-01-01', end_date, date_column_field, include_nulls=provider_conf.get('index_null_dates'),
            record_field=provider_conf.get('record_field')
        )(all_data_df)
    else:
        multiplier, sampled_gen_stats_df = utils.select_data_sample_in_date_range(
            start_date, end_date, date_column_field, include_nulls=provider_conf.get('index_null_dates'),
            record_field=provider_conf.get('record_field')
        )(all_data_df)

    sampled_gen_stats_df = sampled_gen_stats_df.coalesce(partitions).cache()

    # Generate fill rates
    if 'fill_rates' in stats_to_calculate:
        fill_rates = _run_fill_rates(sampled_gen_stats_df, provider_conf)

    # Generate top values
    if 'top_values' in stats_to_calculate:
        top_values = _run_top_values(sampled_gen_stats_df, provider_conf)

        if top_values:
            for top_value_stat in top_values:
                top_value_stat['count'] = int(top_value_stat['count'] * multiplier)

    if 'key_stats' in stats_to_calculate:
        # Generate key stats
        key_stats = _run_key_stats(
            all_data_df, earliest_date, start_date, end_date, provider_conf
        )

    # datatype, provider, earliest_date, end_date df cache
    # used for longitudinality and year over year
    date_stats_df = all_data_df.select("hvid", provider_conf['date_field']).coalesce(partitions)

    if 'longitudinality' in stats_to_calculate:
        # Generate Longitudinality
        longitudinality = _run_longitudinality(date_stats_df, provider_conf)

    if 'year_over_year' in stats_to_calculate:
        # Generate year over year
        year_over_year = _run_year_over_year(date_stats_df, earliest_date, end_date, provider_conf)

    # Return all the dfs
    all_stats = {}
    for stat in stats_to_calculate:
        if stat == 'fill_rates':
            all_stats[stat] = fill_rates
        elif stat == 'key_stats':
            all_stats[stat] = key_stats
        elif stat == 'top_values':
            all_stats[stat] = top_values
        elif stat == 'longitudinality':
            all_stats[stat] = longitudinality
        elif stat == 'year_over_year':
            all_stats[stat] = year_over_year
    return all_stats


def get_epi_calcs(provider_conf):
    all_epi = {}

    if not provider_conf['epi_calcs']:
        return all_epi

    fields = provider_conf.get('epi_calc_fields', ['age', 'gender', 'state', 'region'])

    for f in fields:
        all_epi[f] = epi.calculate_epi(provider_conf, f)

    return all_epi
