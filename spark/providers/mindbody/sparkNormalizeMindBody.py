#! /usr/bin/python
import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader

def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/mindbody/resources/input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/mindbody/resources/matching'
        )
    # NOTE: there is nothing acutally here atm... just following format
    #       that I found in other normalization scripts.
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/mindbody/out/{}/'.format(
            date_input.replace('-', '/')
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/mindbody/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3://salusv/incoming/mindbody/record_data'
        matching_path = 's3://salusv/matching/payload/mindbody'

    # Create the Event v02 table 
    # to store the results in
    runner.run_spark_script('../../common/event_common_model_v2.sql', [
        ['table_name', 'event_common_model', False]
    ])

    # Point Hive to the location of the transaction data
    # and describe its schema
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    # Normalize the transaction data into the
    # event common model using transaction
    # and matching payload data
    runner.run_spark_script('normalize.sql', [
        ['set', 'some_set_name', False],
        ['feed', 'some_feed_name', False],
        ['vendor', 'some_vendor_name', False]
    ])


def main(args):
    # Initialize Spark
    spark, sqlContext = init("MindBody")

    # Initialize the Spark Runner
    runner = Runner(sqlContext)

    # Run the normalization routine
    run(spark, runner, args.date, test=args.local_test, airflow_test=args.airflow_test)
    
    # Tell spark to shutdown
    spark.stop()

    # Determine where to put the output
    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/mindbody/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/mindbody/{}/'.format(time.strftime('%Y-%m-%d', time.localtime())

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--local_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
