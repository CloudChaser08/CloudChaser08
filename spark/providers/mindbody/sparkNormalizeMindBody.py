#! /usr/bin/python
import argparse
import time
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.events as event_priv
import mindbodyPrivacy as mindbody_priv

def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'record_data_' + date_obj.strftime('%Y%m%d')
    
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/mindbody/resources/input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/mindbody/resources/matching/'
        )
    # NOTE: there is nothing acutally here atm... just following format
    #       that I found in other normalization scripts.
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/mindbody/out/{}/'\
                        .format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/mindbody/payload/{}/'\
                        .format(date_input.replace('-', '/'))
    else:
        input_path = 's3://salusv/incoming/consumer/mindbody/{}/'\
                        .format(date_input.replace('-', '/'))
        matching_path = 's3://salusv/matching/payload/consumer/mindbody/{}/'\
                        .format(date_input.replace('-', '/'))


    # Load the matching payload
    payload_loader.load(runner, matching_path, ['claimid', 'hvJoinKey'])
    
    # Create the Event v03 table 
    # to store the results in
    runner.run_spark_script('../../common/event_common_model_v3.sql', [
        ['table_name', 'event_common_model', False],
        ['properties', '', False]
    ])

    # Point Hive to the location of the transaction data
    # and describe its schema
    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    # Remove leading and trailing whitespace from any strings
    postprocessor.trimmify(runner.sqlContext.sql('select * from transactional_mindbody'))\
                    .createTempView('transactional_mindbody')

    # Normalize the transaction data into the
    # event common model using transaction
    # and matching payload data
    runner.run_spark_script('normalize.sql')

    # Postprocessing 
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='38', vendor_id='133', filename=setid),
        mindbody_priv.cap_event_date,
        mindbody_priv.map_whitelist,
        event_priv.filter
    )(
        runner.sqlContext.sql('select * from event_common_model')
    ).createTempView('event_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
                spark, runner, 'consumer', 'event_common_model_v3.sql',
                'mindbody', 'event_common_model', 'event_date', date_input
            )


def main(args):
    # Initialize Spark
    spark, sqlContext = init("MindBody", local=args.local_test)

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
        output_path = 's3://salusv/warehouse/parquet/mindbody/{}/'.format(time.strftime('%Y-%m-%d', time.localtime()))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--local_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
