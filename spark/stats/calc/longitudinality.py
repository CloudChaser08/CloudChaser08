from pyspark.sql.functions import col, min, max, countDistinct, mean, stddev, count, months_between, \
    when, lit
from pyspark.sql.types import IntegerType

from ..models.results import LongitudinalityResult

PATIENT_IDENTIFIER = 'hvid'

# date field to ensure that we never drop below the provider's
# earliest date
MINIMIZED_DATE_FIELD = 'minimized_date'

def _years(s):
    return s / 12


def calculate_longitudinality(df, provider_conf):
    '''
    Calculate the longitudinality for a given set of data
    Input:
        -df: a pyspark.sql.DataFrame
        -provider_conf: A dict of the providers config
    Output:
        - long_stats: the longitudinal stats for the data
    '''
    # Get the field names we need
    patient_identifier = PATIENT_IDENTIFIER
    date_field = 'coalesced_date'

    df = df.withColumn(MINIMIZED_DATE_FIELD, when(
        col(date_field) > lit(provider_conf.earliest_date), col(date_field)
    ).otherwise(lit(provider_conf.earliest_date)))

    # Select the columns we care about
    patient_dates = df.select(col(patient_identifier), col(MINIMIZED_DATE_FIELD)).distinct()
    # Calculate the min_date, max_date, and num_visits for each patient
    patient_visits = patient_dates.groupby(col(patient_identifier)) \
                                      .agg(
                                          min(col(MINIMIZED_DATE_FIELD)).alias('min_date'),
                                          max((MINIMIZED_DATE_FIELD)).alias('max_date'),
                                          countDistinct(MINIMIZED_DATE_FIELD).alias('visits')
                                      )
    # Calculate the stats
    dates = patient_visits.withColumn('months',                             \
                                    months_between(                         \
                                        patient_visits.max_date,            \
                                        patient_visits.min_date             \
                                        ).cast(IntegerType())               \
                                    )
    dates = dates.withColumn("years", _years(dates.months).cast(IntegerType()))

    months = dates.where('months < 24')                                     \
                  .groupby('months')                                        \
                  .agg(count('*').alias('patients'),                        \
                       mean('visits').cast('int').alias('avg'),             \
                       stddev('visits').cast('int').alias('stddev'))        \
                  .orderBy('months', ascending=False)                       \
                  .collect()
    years_long = dates.where('months >= 24')                                \
                      .groupby('years')                                     \
                      .agg(count('*').alias('patients'),                    \
                           mean('visits').cast('int').alias('avg'),         \
                           stddev('visits').cast('int').alias('stddev'))    \
                      .orderBy('years', ascending=False)                    \
                      .collect()
    # Write out to dict
    long_stats = []
    fieldnames = ['duration', 'value', 'average', 'std_dev']
    for row in months:
        row_dict = dict(zip(fieldnames, row))
        row_dict['duration'] = str(row_dict['duration']) + ' months'
        long_stats.append(row_dict)

    for row in years_long:
        row_dict = dict(zip(fieldnames, row))
        row_dict['duration'] = str(row_dict['duration']) + ' years'
        long_stats.append(row_dict)

    return [
        LongitudinalityResult(**stat) for stat in long_stats
    ]
