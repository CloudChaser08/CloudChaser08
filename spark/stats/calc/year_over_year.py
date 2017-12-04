from pyspark.sql.functions import collect_set, year, array_contains, col
import datetime

def _parse_year(s):
        return datetime.strptime(s, "%Y-%m-%d").year


def calculate_year_over_year(df, start_date, earliest_date, provider_conf):
    '''
    Calculate patient year over year on the data set
    Input:
        -df: a pyspark.sql.DataFrame
        -start_date: ---
        -end_date: ---
        -provider_conf: a dict of the providers config
    Output:
        yoy_stats: a Dictionary of the stats
    '''

    patient_identifier = provider_conf['year_over_year']['patient_id_field']
    date_field = provider_conf['date_field']

    patient_dates_df = df.select(col(patient_identifier), col(date_field)) \
                         .where((col(date_field) >= start_date) & (col(date_field) <= earliest_date))
    hvid_years = df.groupby(patient_identifier).agg(collect_set(year(date_field)).alias('years'))

    start_year = _parse_year(args.earliest_date)
    end_year = _parse_year(args.end_date)
    year_range = range(start_year, end_year + 1)

    # Add boolean columns for each year
    for yr in year_range:
        hvid_years = hvid_years.withColumn("in_{}".format(yr), array_contains(hvid_years.years, "{}".format(yr)))

    reversed_year_range = list(reversed(year_range))

    # Starting from most recent year, calcualate year over year
    yoy_stats = {}
    for i, yar in enumerate(reversed_year_range, 1):
	yoy_calc = hvid_years.where(reduce(and_, map(lambda x: col("in_{}".format(x)), reversed_year_range[:i]))).count()

	fp.write("{},{}\n".format('_'.join(map(str, reversed_year_range[:i])), yoy_calc))
        yoy_stats['_'.join(map(str, reversed_year_range[:i]))] = yoy_calc
    return yoy_stats


