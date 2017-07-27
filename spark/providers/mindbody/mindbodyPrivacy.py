from pyspark.sql.functions import col, udf
from spark.helpers.udf.general_helpers import extract_date
from datetime import datetime

def cap_event_date(df):
    """
    Caps the column event_date on the dataframe between
    the min_date and the max_date.

    The dataframe is assumed to have the following columns:
    - event_date: date of the event as a StringType
    - created:    the current day as StringType
    """
    pattern = '%m/%d/%Y'
    min_date_text = '01/01/2016'

    def extract_text_date(text, pattern, min_date_text):
        """
        Because the date fields are all StringTypes, we need to
        convert them to date objects before calling
        the extract_date function
        """
        try:
            min_date = datetime.strptime(min_date_text, pattern).date()
        except:
            min_date = None
        max_date = datetime.now().date()

        return extract_date(text, pattern, min_date, max_date)
        
    cap_udf = udf(lambda date_text: \
                    extract_text_date( \
                                      date_text, \
                                      pattern, \
                                      min_date_text)
    )

    return df.withColumn('event_date', cap_udf(col('event_date')))
    

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

