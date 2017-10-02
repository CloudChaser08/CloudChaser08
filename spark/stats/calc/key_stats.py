from pyspark.sql.functions import ***stuff***

def _get_row_count(attribute, start_date, end_date):
    '''
    Get the row count for the attribute between the start and end date
    Input:
        - attribute: what we're counting (i.e. *, distinct(hvid), etc...)
        - start_date: beginning of date range
        - end_date: end of date range
    '''

    pass


def calculate_key_stats(df, earliest_date, start_date, end_date, \
                        provider_key_stats_conf):
    '''
    Calculate the key stats for a given provider
    Input:
        - df: a pyspark.sql.DataFrame
        - earliest_date: start of 24-month date range
        - start_date: beginning of date range
        - end_date: end of date range
    Output:
        - key_stats: a Dictionary of the key stats for 
                     patient, record, and row
    '''

    pass


