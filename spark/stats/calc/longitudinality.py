from pyspark.sql.functions import mean, stddev, count, months_between
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

    df.createTempView('provider_data')

    # Create a view with only relevant information
    date_sql = 'SELECT DISTINCT {0}, {1} FROM provider_data'
    date_query = date_sql.format(patient_identifier, date_field)
    date_df = sqlc.sql(date_query)

    date_df.createTempView('patient_dates')

    min_max_date_sql = '''
        SELECT 
            {0},
            MIN({1}) AS min_date,
            MAX({1}) AS max_date,
            COUNT(DISTINCT {1})
            AS visits 
            FROM patient_dates
            GROUP BY {0}
        '''
    min_max_date_query = min_max_date_sql.format(patient_identifier, date_field)
    min_max_date_df = sqlc.sql(min_max_date_query)

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

    sqlc.dropTempTable('provider_data')
    sqlc.dropTempTable('patient_dates')

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


