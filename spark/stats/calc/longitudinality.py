from pyspark.sql.functions import col, min, max, countDistinct, mean, stddev, count, months_between
from pyspark.sql.types import IntegerType

def _years(s):
    return s / 12


def calculate_longitudinality(sqlc, df, provider_conf):
    '''
    Calculate the longitudinality for a given set of data
    Input:
        -sqlc: a pyspark.sql.SQLContext
        -df: a pyspark.sql.DataFrame
        -provider_conf: A dict of the providers config
    Output:
        - long_stats: the longitudinal stats for the data
    '''
    # Get the field names we need
    patient_identifier = provider_conf['longitudinality']['patient_id_field']
    date_field = provider_conf['date_field']

    patient_dates_df = df.select(col(patient_identifier), col(date_field)).distinct()
    min_max_date_df = patient_dates_df.groupby(col(patient_identifier)) \
                                      .agg(min(col(date_field)).alias('min_date'),
                                           max((date_field)).alias('max_date'),
                                           countDistinct(date_field).alias('visits')
                                        )
    # Calculate the stats
    dates = min_max_date_df.withColumn('months',                   \
                                    months_between(             \
                                        min_max_date_df.max_date,  \
                                        min_max_date_df.min_date   \
                                        ).cast(IntegerType())   \
                                    )
    dates = dates.withColumn("years", _years(dates.months).cast(IntegerType()))

    months = dates.where('months <= 24')                                \
                  .groupby('months')                                    \
                  .agg(count('*').alias('patients'),                    \
                       mean('visits').cast('int').alias('avg'),         \
                       stddev('visits').cast('int').alias('stddev'))    \
                  .orderBy('months', ascending=False)                   \
                  .collect()
    years_long = dates.where('years >= 2')                                  \
                      .groupby('years')                                     \
                      .agg(count('*').alias('patients'),                    \
                           mean('visits').cast('int').alias('avg'),         \
                           stddev('visits').cast('int').alias('stddev'))    \
                      .orderBy('years', ascending=False)                    \
                      .collect()
    # Write out to dict
    long_stats = []
    fieldnames = ['value', 'patients', 'avg', 'std']
    for row in months:
	row_dict = dict(zip(fieldnames, row))
	row_dict['value'] = str(row_dict['value']) + ' months'
	long_stats.append(row_dict)

    for row in years_long:
	row_dict = dict(zip(fieldnames, row))
	row_dict['value'] = str(row_dict['value']) + ' years'
	long_stats.append(row_dict)

    return long_stats


