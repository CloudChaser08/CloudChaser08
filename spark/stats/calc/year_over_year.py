from datetime import datetime
from operator import and_
from pyspark.sql.functions import rank, year, col
from pyspark.sql.window import Window

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

    end_year = _parse_year(end_date)

    window = Window.partitionBy(col(patient_identifier)).orderBy(col('year').desc())
    yoy_stats = df.select(col(patient_identifier), year(col(date_field)).alias('year'))         \
                  .where((col(date_field) >= earliest_date) & (col(date_field) <= end_date))    \
                  .withColumn('rank', rank().over(window))                                      \
                  .where(end_year - col('rank') + 1 == col('year'))                             \
                  .groupby('year').count()                                                      \
                  .collect()

    # Starting from most recent year, calcualate year over year
    stats = {}
    for stat in yoy_stats:
        stats[str(stat.year)] = stat['count']
    return stats


