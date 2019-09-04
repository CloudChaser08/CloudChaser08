import datetime
from pyspark.sql.functions import col, trim

from ..models.results import GenericStatsResult

def _get_row_count(df, start_date, end_date, attribute, date_col, index_null_dates=False):
    '''
    Get the row count for the attribute between the start and end date
    Input:
        - df: a pyspark.sql.DataFrame
        - attribute: what we're counting (i.e. *, distinct(hvid), etc...)
        - start_date: beginning of date range
        - end_date: end of date range
        - date_col: the name of the column that conains the dates
    '''
    filter_by_date = ((col(date_col) >= start_date) & (col(date_col) <= end_date))
    if index_null_dates:
        filter_by_date = filter_by_date | (col(date_col).isNull() | (trim(col(date_col)) == ''))
    date_range_df = df.where(filter_by_date)
    if attribute == '*':
        count = date_range_df.count()
    else:
        count = date_range_df.select(attribute).distinct().count()
    return count


def calculate_key_stats(df, earliest_date, start_date, end_date, provider_conf):
    '''
    Calculate the key stats for a given provider
    Input:
        - df: a pyspark.sql.DataFrame
        - earliest_date: start of 24-month date range
        - start_date: beginning of date range
        - end_date: end of date range
        - provider_conf: Dict of the config
    Output:
        - key_stats: a Dictionary of the key stats for
                     patient, record, and row
    '''
    date_col = 'coalesced_date'
    index_all_dates = provider_conf.index_all_dates
    index_null_dates = provider_conf.index_null_dates
    patient_attribute = 'hvid'
    record_attribute = provider_conf.record_field or '*'
    row_attribute = '*'

    # If we are indexing all dates, then we want to ignore
    # the start_date and only use earliest_date
    start_date = earliest_date if index_all_dates else start_date

    total_24_month_patient = _get_row_count(
        df, start_date, end_date, patient_attribute, date_col, index_null_dates
    )
    total_24_month_record = _get_row_count(
        df, start_date, end_date, record_attribute, date_col, index_null_dates
    )
    if record_attribute == row_attribute:
        total_24_month_row = total_24_month_record
    else:
        total_24_month_row = _get_row_count(
            df, start_date, end_date, row_attribute, date_col, index_null_dates
        )

    if index_all_dates:
        # start_date == earliest_date, don't recalc
        total_patient = total_24_month_patient
        total_record = total_24_month_record
        total_row = total_24_month_row
    else:
        total_patient = _get_row_count(
            df, earliest_date, end_date, patient_attribute, date_col, index_null_dates
        )
        total_record = _get_row_count(
            df, earliest_date, end_date, record_attribute, date_col, index_null_dates
        )
        if record_attribute == row_attribute:
            total_row = total_record
        else:
            total_row = _get_row_count(
                df, earliest_date, end_date, row_attribute, date_col, index_null_dates
            )

    try:
        earliest_date_dt = datetime.datetime.strptime(earliest_date, "%Y-%m-%d")
        end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    except:
        earliest_date_dt = datetime.datetime.strptime(earliest_date, "%Y-%m")
        end_date_dt = datetime.datetime.strptime(end_date, "%Y-%m")

    days = (end_date_dt - earliest_date_dt).days

    key_stats = [
        GenericStatsResult(
            field='total_patient',
            value=int(total_patient)
        ),
        GenericStatsResult(
            field='total_24_month_patient',
            value=int(total_24_month_patient)
        ),
        GenericStatsResult(
            field='daily_avg_patient',
            value=int(total_patient / days)
        ),
        GenericStatsResult(
            field='monthly_avg_patient',
            value=int(((total_patient / days) * 365) / 12)
        ),
        GenericStatsResult(
            field='yearly_avg_patient',
            value=int((total_patient / days) * 365)
        ),
        GenericStatsResult(
            field='total_row',
            value=int(total_row)
        ),
        GenericStatsResult(
            field='total_24_month_row',
            value=int(total_24_month_row)
        ),
        GenericStatsResult(
            field='daily_avg_row',
            value=int(total_row / days)
        ),
        GenericStatsResult(
            field='monthly_avg_row',
            value=int(((total_row / days) * 365) / 12)
        ),
        GenericStatsResult(
            field='yearly_avg_row',
            value=int((total_row / days) * 365)
        ),

        GenericStatsResult(
            field='total_record',
            value=int(total_record)
        ),
        GenericStatsResult(
            field='total_24_month_record',
            value=int(total_24_month_record)
        ),
        GenericStatsResult(
            field='daily_avg_record',
            value=int(total_record / days)
        ),
        GenericStatsResult(
            field='monthly_avg_record',
            value=int(((total_record / days) * 365) / 12)
        ),
        GenericStatsResult(
            field='yearly_avg_record',
            value=int((total_record / days) * 365)
        ),
    ]

    return key_stats
