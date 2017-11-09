# Generic, agnostic functions to be applied on a dataframe

import spark.helpers.udf.general_helpers as gen_helpers
from pyspark.sql.functions import col, lit, when, trim, monotonically_increasing_id, udf, upper, coalesce
import functools
import logging
import time
import datetime

def _apply_to_all_columns(f, df):
    return df.select(*map(f, df.columns))


def nullify(df, null_vals=None, preprocess_func=lambda c: c):
    "Convert all columns matching any value in null_vals to null"
    if not null_vals:
        null_vals = [""]

    def convert_to_null(column_name):
        return when(udf(preprocess_func)(col(column_name)).isin(null_vals), lit(None)) \
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


def apply_date_cap(sqlc, date_col, max_cap, vdr_feed_id, domain_name, custom_min_cap=None, date_function=None):
    """
    Apply date cap defined for this provider in the ref_gen_ref table
    """

    if not custom_min_cap:
        try:
            min_cap_results = sqlc.sql("""
            SELECT gen_ref_1_dt
            FROM ref_gen_ref
            WHERE hvm_vdr_feed_id='{}' AND gen_ref_domn_nm = '{}'
            """.format(vdr_feed_id, domain_name)).collect()
        except:
            logging.error("Error occurred while loading min_cap data for hvm_vdr_feed_id='{}' AND gen_ref_domn_nm = '{}', "
                          + "check to make sure ref_gen_ref was loaded before calling this function.".format(
                              vdr_feed_id, domain_name
                          ))
            raise

        if not min_cap_results:
            logging.warning(
                "No min cap found at for hvm_vdr_feed_id='{}' and gen_ref_domn_nm='{}', min capping was not applied.".format(
                    vdr_feed_id, domain_name
                )
            )
            return lambda df: df
        else:
            min_cap = min_cap_results[0].gen_ref_1_dt

    else:
        min_cap = datetime.datetime.strptime(custom_min_cap, '%Y-%m-%d').date()

    if date_function:
        min_cap = date_function(min_cap)

    max_cap = datetime.datetime.strptime(max_cap, '%Y-%m-%d').date()

    def out(df):
        return df.withColumn(
            date_col, udf(gen_helpers.cap_date)(col(date_col), lit(min_cap), lit(max_cap))
        )

    return out


def apply_whitelist(sqlc, col_name, domain_name, comp_col_names=None):
    """
    Apply whitelist defined for this provider in the ref_gen_ref table.
    """
    if comp_col_names is None:
        comp_col_names = []

    try:
        values = [r.gen_ref_itm_nm for r in sqlc.sql("""
        SELECT gen_ref_itm_nm
        FROM ref_gen_ref
        WHERE whtlst_flg = 'Y' AND gen_ref_domn_nm = '{}'
        """.format(domain_name)).collect()]
    except:
        logging.error("Error occurred while loading whitelist results for domain_name = '{}', "
                      + "check to make sure ref_gen_ref was loaded before calling this function.".format(
                          domain_name
                      ))
        raise

    if not values:
        logging.warn("No whitelist specified for {}".format(domain_name))

    def out(df):
        df = df.withColumn(
            col_name, when(udf(gen_helpers.clean_up_freetext)(upper(col(col_name))).isin(values),
                           udf(gen_helpers.clean_up_freetext)(upper(col(col_name)))).otherwise(lit(None))
        )
        for comp_col_name in comp_col_names:
            df = df.withColumn(
                comp_col_name, when(udf(gen_helpers.clean_up_freetext)(upper(col(col_name))).isin(values),
                            col(comp_col_name)).otherwise(lit(None))
            )
        return df

    return out


def add_universal_columns(feed_id, vendor_id, filename, **alternate_column_names):
    """
    Add columns to a dataframe that are universal across all
    healthverity datasets. If filename is None, the input_file_name
    will be derived via the built in function. Cache the dataframe so the
    monotonically_increasing_id is not recalculated on every query

    The dataframe is assumed to have the following columns:
    - record_id: Auto-inc PK
    - created: Current date
    - data_feed: Marketplace feed ID
    - data_set: Source filename
    - data_vendor: Marketplace vendor ID

    """
    record_id = alternate_column_names.get('record_id', 'record_id')
    created = alternate_column_names.get('created', 'created')
    data_set = alternate_column_names.get('data_set', 'data_set')
    data_feed = alternate_column_names.get('data_feed', 'data_feed')
    data_vendor = alternate_column_names.get('data_vendor', 'data_vendor')

    def add(df):
        return df.withColumn(record_id, monotonically_increasing_id())                   \
                 .alias(record_id)                                                       \
                 .withColumn(created, lit(time.strftime('%Y-%m-%d', time.localtime())))  \
                 .alias(created)                                                         \
                 .withColumn(data_set, coalesce(lit(filename), col(data_set)))           \
                 .alias(data_set)                                                        \
                 .withColumn(data_feed, lit(feed_id))                                    \
                 .alias(data_feed)                                                       \
                 .withColumn(data_vendor, lit(vendor_id))                                \
                 .alias(data_vendor)                                                     \
                 .cache()
    return add


def get_gen_ref_date(sqlc, feed_id, domain_name):
    res = sqlc.sql(
        "SELECT gen_ref_1_dt FROM ref_gen_ref WHERE hvm_vdr_feed_id = {} AND gen_ref_domn_nm = '{}'".format(
            feed_id, domain_name
        )
    ).collect()

    if res:
        return res[0].gen_ref_1_dt
    else:
        return None


def deobfuscate_hvid(project_name, hvid_col='hvid', nullify_non_integers=False):
    """
    Generate a function that will deobfuscate the hvid column in the
    given dataframe

    `nullify_non_integers` should be set to True if the hvid column might
    contain invalid integers
    """
    if nullify_non_integers:
        # only deobfuscate valid integers
        column = when(
            udf(gen_helpers.is_int)(col(hvid_col)).cast('boolean'),
            col(hvid_col).cast('int')
        ).otherwise(lit(None))
    else:
        column = col(hvid_col).cast('int')

    def out(df):
        return df.withColumn(
            hvid_col,
            udf(gen_helpers.slightly_deobfuscate_hvid)(column, lit(project_name))
        )
    return out


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
