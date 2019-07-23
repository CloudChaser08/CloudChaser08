import argparse
import copy
import inspect

import spark.spark_setup as spark_setup
import spark.helpers.file_utils as file_utils
import spark.stats.data_layout.data_layout_reader as data_layout_reader
import spark.stats.stats_writer as stats_writer
import spark.stats.processor as processor


def create_data_layout_version_sql(feed_id, version_name, quarter, start_date, end_date):
    """
    For a given feed_id and a time frame, create a runnable SQL file that
    inserts a complete data_layout version into a Marketplace DB.
    """

    # Set up spark
    spark, sqlContext = spark_setup.init('Feed {} marketplace data layout'.format(feed_id), True)  # TODO remove local

    # Get the base data_layout, without top_values and fill_rate filled in
    data_layout = data_layout_reader.get_data_layout(feed_id)

    # Calculate top_values and fill_rate for each field in data_layout,
    # and add them to the data_layout.
    pass

    # Create the runnable SQL file for adding this new version of data_layout
    pass

    return "TODO"
