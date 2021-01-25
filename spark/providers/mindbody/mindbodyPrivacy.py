from pyspark.sql.functions import col, udf
from spark.helpers.udf.general_helpers import extract_date
from datetime import datetime


def map_whitelist(df):
    """
    Looks through the dataframe and maps any blacklisted terms
    to their appropriate value.

    The dataframe is assumed to have the following columns:
    - event_val: The name of the MindBody class
    - event_val_uom: The type of class evant_val is
    """
    def convert(name):
        # TODO: term mapping will be done here, return None for now
        #      until we recieve the list.
        return None

    map_udf = udf(lambda term: convert(term))

    return df.withColumn('event_val', map_udf(col('event_val')))          \
             .withColumn('event_val_uom', map_udf(col('event_val_uom')))

