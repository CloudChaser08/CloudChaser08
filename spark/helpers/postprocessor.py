# Generic, agnostic functions to be applied on a dataframe

from pyspark.sql.functions import col, lit, when, trim, monotonically_increasing_id
import functools
import time

def _apply_to_all_columns(f, df):
    return df.select(*map(f, df.columns))


def nullify(df, null_val=''):
    "Convert all columns matching null_val to null"
    def convert_to_null(column_name):
        return when(col(column_name) == null_val, lit(None)) \
            .otherwise(col(column_name)).alias(column_name)

    return _apply_to_all_columns(convert_to_null, df)


def trimmify(df):
    "Trim all string columns"
    def get_type(col_name):
        return str(filter(
            lambda f: f.name == col_name,
            df.schema.fields
        )[0].dataType)

    def trim_col(column_name):
        if get_type(column_name) == 'StringType':
            return trim(col(column_name)).alias(column_name)
        else:
            return col(column_name)

    return _apply_to_all_columns(trim_col, df)


def add_universal_columns(feed_id, vendor_id, filename):
    """
    Add columns to a dataframe that are universal across all
    healthverity datasets.

    The dataframe is assumed to have the following columns:
    - record_id: Auto-inc PK
    - created: Current date
    - data_feed: Marketplace feed ID
    - data_set: Source filename
    - data_vendor: Marketplace vendor ID

    """
    def add(df):
        return df.withColumn('record_id', monotonically_increasing_id())                   \
                 .withColumn('created', lit(time.strftime('%Y-%m-%d', time.localtime())))  \
                 .withColumn('data_set', lit(filename))                                    \
                 .withColumn('data_feed', lit(feed_id))                                    \
                 .withColumn('data_vendor', lit(vendor_id))
    return add


def compose(*functions):
    """
    Utility method for composing a series of functions.

    Lives here because functions in this module may often be applied
    in series on a dataframe.
    """
    return functools.reduce(
        lambda f, g: lambda x: g(f(x)),
        functions,
        lambda x: x
    )
