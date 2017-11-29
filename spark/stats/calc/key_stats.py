import datetime
from pyspark.sql.functions import ***stuff***

def _get_row_count(sqlc, attribute, start_date, end_date):
    '''
    Get the row count for the attribute between the start and end date
    Input:
        - attribute: what we're counting (i.e. *, distinct(hvid), etc...)
        - start_date: beginning of date range
        - end_date: end of date range
    '''
    template = 'SELECT COUNT(*) FROM (SELECT COUNT({}) FROM provider_data WHERE {} >= {} AND {} <= {}'
    query = template.format(attribute, date_col, start_date, date_col, end_date)
    count = sqlc.sql(query).collect()[0][0]

    return count


def calculate_key_stats(sqlc, df, earliest_date, start_date, end_date, \
                        provider_key_stats_conf):
    '''
    Calculate the key stats for a given provider
    Input:
        - sqlc: a pyspark.sql.SQLContext
        - df: a pyspark.sql.DataFrame
        - earliest_date: start of 24-month date range
        - start_date: beginning of date range
        - end_date: end of date range
    Output:
        - key_stats: a Dictionary of the key stats for 
                     patient, record, and row
    '''

    df.createTempView('provider_data')

    total_patient = _get_row_count(df, earliest_date, end_date, 
                                   provider_key_stats_conf['patient_attribute'])
    total_24_month_patient = _get_row_count(df, start_date, end_date,
                                   provider_key_stats_conf['patient_attribute'])
    total_record = _get_row_count(df, earliest_date, end_date,
                                   provider_key_stats_conf['record_attribute'])
    total_24_month_record = _get_row_count(df, start_date, end_date,
                                   provider_key_stats_conf['record_attribute'])
    total_row = _get_row_count(df, earliest_date, start_date, end_date,
                                   provider_key_stats_conf['row_attribute'])
    total_24_moth_row = _get_row_count(df, start_date, end_date,
                                   provider_key_stats_conf['row_attribute'])

    try:
        earliest_date_dt = datetime.datetime.strptime(earliest_date, "%Y-%m-%d")
        end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except:
        earliest_date_dt = datetime.datetime.strptime(earliest_date, "%Y-%m")
        end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m")
    days = (end_date_dt - earliest_date_dt).days

    metrics = {
        'total_patient': total_patient,
        'total_24_month_patient': total_24_month_patient,
        'daily_avg_patient': total_patient / days,
        'monthly_avg_patient': ((total_patient / days) * 365) / 12,
        'yearly_avg_patient': (total_patient / days) * 365,

        'total_row': total_row,
        'total_24_month_row': total_24_month_row,
        'daily_avg_row': total_row / days,
        'monthly_avg_row': ((total_row / days) * 365) / 12,
        'yearly_avg_row': (total_row / days) * 365,

        'total_record': total_record,
        'total_24_month_record': total_24_month_record,
        'daily_avg_record': total_record / days,
        'monthly_avg_record': ((total_record / days) * 365) / 12,
        'yearly_avg_record': (total_record / days) * 365,
    }

    return metrics


