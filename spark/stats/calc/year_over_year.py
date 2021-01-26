from datetime import datetime
from operator import and_
from pyspark.sql.functions import rank, year, col
from pyspark.sql.window import Window

from ..models.results import YearOverYearResult

PATIENT_IDENTIFIER = 'hvid'


def _parse_year(s):
    return datetime.strptime(s, "%Y-%m-%d").year


def calculate_year_over_year(df, earliest_date, end_date, provider_conf):
    """
    Calculate patient year over year on the data set
    Input:
        -df: a pyspark.sql.DataFrame
        -earliest_date: the very beginning of the date range
        -end_date: the end of the date range
        -provider_conf: a dict of the providers config
    Output:
        yoy_stats: a Dictionary of the stats
    """

    patient_identifier = PATIENT_IDENTIFIER
    date_field = 'coalesced_date'

    end_year = _parse_year(end_date)

    # --- YoY Logic ---
    # (1) Select and limit by inputted date range
    # (2) Only care about unique paitent visits per year
    # (3) Window on patient id and sort by visit year,
    #     then add index within each window
    # (4) If a rows rank + 1 subtracted from the end year
    #     does not equal the year for that row,
    #     then it is not sequential and we exclude it
    # (5) Get unique patient visits for each year
    window = Window.partitionBy(col(patient_identifier)).orderBy(col('year').desc())
    yoy_stats = df.select(col(patient_identifier), year(col(date_field)).alias('year'))         \
                  .where((col(date_field) >= earliest_date) & (col(date_field) <= end_date))    \
                  .drop_duplicates()                                                            \
                  .withColumn('rank', rank().over(window))                                      \
                  .where(end_year - col('rank') + 1 == col('year'))                             \
                  .groupby('year').count()                                                      \
                  .collect()

    return [
        YearOverYearResult(year=r.year, count=r['count'])
        for r in yoy_stats
    ]
