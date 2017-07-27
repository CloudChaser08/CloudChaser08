from pyspark.sql.functions import col, udf
from spark.helpers.udf.general_helpers import extract_date

def cap_event_date(pattern, min_date=None):
    """
    Caps the column event_date on the dataframe between
    the min_date and the max_date.

    The dataframe is assumed to have the following columns:
    - event_date: date of the event as a StringType
    - created:    the current day as StringType
    """
    cap_udf = udf(lambda date, max_date: extract_date(date, pattern, min_date, max_date))

    def cap(df):
        return df.withColumn('event_date', cap_udf(col('event_date'), col('created')))
    
    return cap

def map_whitelist(df):
    """
    Looks through the dataframe and maps any blacklisted terms
    to their appropriate value.

    The dataframe is assumed to have the following columns:
    - event_val: The name of the MindBody class
    - event_val_uom: The type of class evant_val is
    """
    def convert(name):
        #TODO: term mapping will be done here, return None for now
        #      until we recieve the list.
        return None

    map_udf = udf(lambda term: convert(term))

    return df.withColumn('event_val', map_udf(col('event_val')))          \
             .withColumn('event_val_uom', map_udf(col('event_val_uom')))

