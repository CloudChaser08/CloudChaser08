from datetime import datetime
from operator import and_
from pyspark.sql.functions import collect_set, year, array_contains, col

def _parse_year(s):
        return datetime.strptime(s, "%Y-%m-%d").year


def calculate_year_over_year(df, earliest_date, end_date, provider_conf):
    '''
    Calculate patient year over year on the data set
    Input:
        -df: a pyspark.sql.DataFrame
        -earliest_date: the very beginning of the date range
        -end_date: the end of the date range
        -provider_conf: a dict of the providers config
    Output:
        yoy_stats: a Dictionary of the stats
    '''

    patient_identifier = provider_conf['year_over_year']['patient_id_field']
    date_field = provider_conf['date_field']

    patient_visit_dates = df.select(col(patient_identifier), col(date_field)) \
                         .where((col(date_field) >= earliest_date) & (col(date_field) <= end_date))
    hvid_years = patient_visit_dates.groupby(patient_identifier).agg(collect_set(year(date_field)).alias('years'))

    start_year = _parse_year(earliest_date)
    end_year = _parse_year(end_date)
    year_range = range(start_year, end_year + 1)

    # df.select('b', year('date').alias('date')).withColumn('rank', rank().over(window)).where(end_year - col('rank') + 1 == col('date')).groupby('date').count().show()
    # Add boolean columns for each year and set to true if a patient had a visit in that year
    for yr in year_range:
        hvid_years = hvid_years.withColumn("in_{}".format(yr), array_contains(hvid_years.years, "{}".format(yr)))

    reversed_year_range = list(reversed(year_range))

    # Starting from most recent year, calcualate year over year
    yoy_stats = {}
    for i, yar in enumerate(reversed_year_range, 1):
        # Filter based on patients who have had visits for every year from year i to most recent year and take the count
        yoy_calc = hvid_years.where(reduce(and_, map(lambda x: col("in_{}".format(x)), reversed_year_range[:i]))).count()
        yoy_key = '_'.join(map(str, reversed_year_range[:i]))
        yoy_stats[yoy_key] = yoy_calc
    return yoy_stats


