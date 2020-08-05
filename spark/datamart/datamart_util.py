from datetime import date, datetime, timedelta
from collections import OrderedDict
from dateutil.relativedelta import relativedelta
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import spark.datamart.covid19.context as context
import spark.common.utility.logger as logger

"""
Below modules used for Datamart common
    some modules are related to pyspark
"""


def get_list_of_months(start_ts, end_ts):
    """
    Collect List of months for given start and end timestamp
        input: timestamp format
        output: List format (month format is 'YYYY-MM')
    """
    collect_mths = OrderedDict(((start_ts + timedelta(_)).strftime("%Y-%m"), 0) for _ in
                               range((end_ts + relativedelta(months=1) - start_ts).days))
    return [item[0] for item in list(collect_mths)]


def get_list_of_months_v1(start_month, end_month):
    """
    Collect List of months for given start and end month
        input: timestamp format
        output: List format (month format is 'YYYY-MM')
    """
    collect_mths = []
    temp_month = start_month
    while True:
        collect_mths.append(temp_month)
        temp_month = (datetime.strptime(temp_month, '%Y-%m') + relativedelta(months=1)).strftime('%Y-%m')
        if datetime.strptime(temp_month, '%Y-%m') > datetime.strptime(end_month, '%Y-%m'):
            break
    return collect_mths


def df_union_all(*dfs):
    """
    Collection of dataframes and returned as single DF
        input: collection of dataframes
        output: Single DataFrame (unionALL)
    """
    return reduce(DataFrame.unionAll, dfs)


def has_table(spark, db, table):
    """
    This is spark module to check table is exist or not in given
    database.
        input: spark, database  name and table name
        output: True /False
    """
    table_status = False
    tbl_cnt = spark.sql("show tables in {}".format(db)).where(f.col("tableName").isin({table})).count()

    if tbl_cnt == 1:
        table_status = True

    return table_status


def get_external_table_location(spark, db, table):
    """
    This is spark module and return external table
    location. Collect table description with formatted
    and filter-out location column details,
        input: spark, database  name and table name
        output: table location path
    """
    location_df_list = spark.sql(
        "desc formatted {}.{}".format(db, table)).filter(
        f.col("col_name") == "Location").select(
        f.col("data_type")).collect()

    location = str([x.data_type for x in location_df_list][0])
    return location


def table_repair(spark, runner, db, table, is_partitioned=False):
    """
    This is spark module. This will apply msck repair(for partitioned
     tables) , refresh table and return with table exists status.
        1. this will check table is exist or not.
        2. apply msck repair (for partitioned table)
        3. apply refresh table

        input: spark, database-name, table-name
            and table partitioned status (default False)
        output: True / False
    """
    logger.log('            -table_repair {}.{}: started'.format(db, table))
    if has_table(spark, db, table):
        if is_partitioned:
            runner.run_spark_query('msck repair table {}.{}'.format(db, table))
        runner.run_spark_query('refresh {}.{}'.format(db, table))
    else:
        logger.log('            -table_repair: failed. table is missing or parameter is incorrect')
    logger.log('            -table_repair: completed')

    return has_table(spark, db, table)


def get_nbr_of_buckets(asset_name, part_provider):
    """
        This will return number of buckets(pre configured) for dataset
        re-partition. Currently configured for labtests asset_name

        input: asset_name, provider_name
        output: True / False
    """
    part_provider_lower = part_provider.lower()
    asset_name_lower = asset_name.lower()
    if asset_name_lower in ['labtests']:
        if part_provider_lower in context.LAB_BIG_PART_PROVIDER:
            nbr_of_buckets = 50
        elif part_provider_lower in context.LAB_MEDIUM_PART_PROVIDER:
            nbr_of_buckets = 5
        elif part_provider_lower in context.LAB_SMALL_PART_PROVIDER:
            nbr_of_buckets = 1
        else:
            nbr_of_buckets = context.LAB_NBR_OF_BUCKETS
    else:
        nbr_of_buckets = context.LAB_NBR_OF_BUCKETS

    return nbr_of_buckets
