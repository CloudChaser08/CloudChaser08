import argparse
import copy
import inspect

import spark.spark_setup as spark_setup
import spark.helpers.file_utils as file_utils
import spark.stats.processor as processor
from spark.stats.data_layout.reader import get_base_data_layout
from spark.stats.data_layout.sql_writer import create_runnable_sql_file


def _fill_in_stat_values(data_layout, spark, sqlContext, quarter, start_date, end_date):


    return data_layout


def generate_data_layout_version_sql(datafeed_id, version_name, quarter, start_date, end_date):
    """
    For a given feed_id and a time frame, create a runnable SQL file that
    inserts a complete data_layout version into a Marketplace DB.
    """

    # TODO remove local True
    spark, sqlContext = spark_setup.init(
        'Feed {} marketplace data layout'.format(datafeed_id), True
    )

    # Get the base data_layout, without top_values and fill_rate filled in
    data_layout = get_base_data_layout(datafeed_id)

    # Fill top_values and fill_rate for each field in data_layout
    data_layout = _fill_in_stat_values(
        data_layout, spark, sqlContext, quarter, start_date, end_date
    )

    # Create the runnable SQL file for adding this new version of data_layout
    create_runnable_sql_file(datafeed_id, version_name, quarter, data_layout)
