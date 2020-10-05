# Generic, agnostic functions to be applied on a dataframe

import spark.helpers.udf.general_helpers as gen_helpers
from pyspark.sql.types import StringType, DateType
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, lit, when, trim, monotonically_increasing_id, udf, \
    coalesce, input_file_name, upper, current_date
from pyspark.sql import DataFrame
import functools
import logging
import time
import datetime
from typing import List, Tuple


def _apply_to_all_columns(f, df):
    return df.select(*[f(c) for c in df.columns])


def nullify(df, null_vals=None, preprocess_func=None):
    """Convert all columns matching any value in null_vals to null"""
    if not null_vals:
        null_vals = {"", "NULL"}
    else:
        null_vals = set(null_vals)

    if "NULL" not in null_vals:
        null_vals.add("NULL")

    column_types = {f.name: str(f.dataType) for f in df.schema.fields}

    # While this code footprint can be reduced by having a default
    # preprocess_func, even a simple Python UDF adds significant serialization
    # and deserialization overhead
    if preprocess_func is None:
        def convert_to_null(column_name):
            if column_types[column_name] == 'StringType':
                return when(upper(col(column_name)).isin(null_vals), lit(None)) \
                    .otherwise(col(column_name)).alias(column_name)
            else:
                return col(column_name)
    else:
        do_preprocess = udf(preprocess_func)

        def convert_to_null(column_name):
            if column_types[column_name] == 'StringType':
                return when(upper(do_preprocess(col(column_name))).isin(null_vals), lit(None)) \
                    .otherwise(col(column_name)).alias(column_name)
            else:
                return col(column_name)

    return _apply_to_all_columns(convert_to_null, df)


def trimmify(df):
    "Trim all string columns"

    column_types = {f.name: str(f.dataType) for f in df.schema.fields}

    def trim_col(column_name):
        if column_types[column_name] == 'StringType':
            return trim(col(column_name)).alias(column_name)
        else:
            return col(column_name)

    return _apply_to_all_columns(trim_col, df)


def apply_date_cap(sqlc, date_col, max_cap, vdr_feed_id, domain_name, custom_min_cap=None,
                   date_function=None):
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
            logging.error((
                "Error occurred while loading min_cap data for hvm_vdr_feed_id='{}' AND "
                "gen_ref_domn_nm = '{}', "
                "check to make sure ref_gen_ref was loaded before calling this function.").format(
                vdr_feed_id, domain_name
            ))
            raise

        if not min_cap_results:
            logging.warning(
                "No min cap found at for hvm_vdr_feed_id='{}' and gen_ref_domn_nm='{}', "
                "min capping was not applied.".format(
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
            date_col,
            udf(gen_helpers.cap_date, DateType())(col(date_col), lit(min_cap), lit(max_cap))
        )

    return out


def default_clean_up_freetext_fn(x):
    if x:
        return gen_helpers.clean_up_freetext(x.upper())
    else:
        return None


def apply_whitelist(sqlc, col_name, domain_name, comp_col_names=None,
                    whitelist_col_name='gen_ref_itm_nm', clean_up_freetext_fn=None, feed_id=None):
    """
    Apply whitelist defined for this provider in the ref_gen_ref table.
    """

    # We never want these values to be None, but sometimes, for code simplicity
    # reasons, thir function is called with the keyword argument specified but
    # with a None value
    if comp_col_names is None:
        comp_col_names = []

    if whitelist_col_name is None:
        whitelist_col_name = 'gen_ref_itm_nm'

    if clean_up_freetext_fn is None:
        clean_up_freetext_fn = default_clean_up_freetext_fn

    SQL_TEMPLATE = """
        SELECT {}
        FROM ref_gen_ref
        WHERE whtlst_flg = 'Y' AND gen_ref_domn_nm = '{}'
    """

    if feed_id:
        SQL_TEMPLATE = SQL_TEMPLATE + " AND hvm_vdr_feed_id='{}'".format(feed_id)

    try:
        values = [r.asDict()[whitelist_col_name] for r in sqlc.sql(
            SQL_TEMPLATE.format(whitelist_col_name, domain_name)
        ).collect()]
    except:
        logging.error(("Error occurred while loading whitelist results for domain_name = '{}', "
                       "check to make sure ref_gen_ref was loaded before calling this "
                       "function.").format(
            domain_name
        ))
        raise

    if not values:
        logging.warn("No whitelist specified for {}".format(domain_name))

    def out(df):
        c = udf(clean_up_freetext_fn)(col(col_name))
        df = df.withColumn(
            col_name, when(c.isin(values), c).otherwise(lit(None))
        )
        for comp_col_name in comp_col_names:
            df = df.withColumn(
                comp_col_name, when(
                    c.isin(values),
                    col(comp_col_name)
                ).otherwise(lit(None))
            )
        return df

    return out


def add_null_column(col_name):
    """
    Add a column of null values with the name col_name to a dataframe
    """

    def out(df):
        return df.withColumn(col_name, lit(None).cast(StringType()))

    return out


def add_universal_columns(feed_id, vendor_id, filename,
                          model_version_number=None, **alternate_column_names):
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
    - model_version: the version of the associated common model used

    """
    record_id = alternate_column_names.get('record_id', 'record_id')
    created = alternate_column_names.get('created', 'created')
    data_set = alternate_column_names.get('data_set', 'data_set')
    data_feed = alternate_column_names.get('data_feed', 'data_feed')
    data_vendor = alternate_column_names.get('data_vendor', 'data_vendor')
    model_version = alternate_column_names.get('model_version', 'model_version')

    def add(df):
        return df.withColumn(record_id, monotonically_increasing_id()) \
            .alias(record_id) \
            .withColumn(created, current_date()) \
            .alias(created) \
            .withColumn(data_set, coalesce(lit(filename), col(data_set))) \
            .alias(data_set) \
            .withColumn(data_feed, lit(feed_id)) \
            .alias(data_feed) \
            .withColumn(data_vendor, lit(vendor_id)) \
            .alias(data_vendor) \
            .withColumn(model_version,
                        coalesce(lit(model_version_number), col(model_version))) \
            .alias(model_version)

    return add


def get_gen_ref_date(sqlc, feed_id, domain_name, get_as_string=False):
    select_query = "SELECT gen_ref_1_dt "
    if get_as_string:
        select_query = "SELECT date_format(gen_ref_1_dt, 'yyyy-MM-dd') as gen_ref_1_dt "

    query = select_query + "FROM ref_gen_ref WHERE hvm_vdr_feed_id = {} AND gen_ref_domn_nm = '{}'"
    res = sqlc.sql(query.format(feed_id, domain_name)).collect()

    if res:
        return res[0].gen_ref_1_dt
    else:
        return None


def coalesce_dates(sqlc, feed_id, fallback_date, *dates):
    '''
      - Note: *dates must be input in order of importance.
              i.e.
              get_min_date(sqlc,
                           feed_id,
                           fallback_date,
                           date_pick_me_first,
                           date_pick_me_second,
                           ...)
    '''
    for d in dates:
        date = get_gen_ref_date(
            sqlc,
            feed_id,
            d
        )
        if date is not None:
            return date

    return fallback_date


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


def add_input_filename(column_name, include_parent_dirs=False,
                       persisted_df_id='df_with_input_filename'):
    "Add the input file name to a dataframe, removing the split suffix"

    def out(df):
        return df.withColumn(
            column_name, input_file_name()
        ).cache_and_track(persisted_df_id).withColumn(
            column_name,
            udf(gen_helpers.remove_split_suffix)(col(column_name), lit(include_parent_dirs))
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


def parse_fixed_width_columns(df: DataFrame, columns: List[Tuple[str, int, int, str]]) -> DataFrame:
    """
    Parses fixed width rows given a spark dataframe and the column specification. Returns the parsed dataframe.
    
    Columns are specified with a list of tuples containing (column name, column start, column length, column type)
    """
    
    df_mas_cols = []
   
    for row in columns: 
        col_name = row[0].strip()
        col_start = int(row[1])
        col_length = int(row[2])
        col_type = row[3].strip()

        col_def = df.value.substr(col_start, col_length).cast(col_type).alias(col_name)

        df_mas_cols.append(col_def)

    res_df = compose(trimmify, nullify)(df.select(*df_mas_cols))

    return res_df