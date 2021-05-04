#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.events as event_priv
from spark.providers.mindbody import mindbodyPrivacy as mindbodyPriv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/mindbody/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/consumer/2017-08-02/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    date_obj = datetime.strptime(date_input, '%Y-%m-%d')
    date_path = '/'.join(date_input.split('-')[:2])

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
                        .format(date_path)
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/mindbody/payload/{}/'\
                        .format(date_path)
    else:
        input_path = 's3://salusv/incoming/consumer/mindbody/{}/'\
                        .format(date_path)
        matching_path = 's3://salusv/matching/payload/consumer/mindbody/{}/'\
                        .format(date_path)

    min_date = datetime.strptime('2014-01-01', '%Y-%m-%d')

    # Load the matching payload
    payload_loader.load(runner, matching_path, ['claimId', 'hvJoinKey'])

    # Create the Event v03 table
    # to store the results in
    runner.run_spark_script('../../common/event_common_model_v4.sql', [
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
    runner.run_spark_script('normalize.sql', [
        ['min_date', str(min_date)],
        ['max_date', str(date_obj)]
    ])

    # Postprocessing
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id='38', vendor_id='133', filename=setid),
        mindbodyPriv.map_whitelist,
        event_priv.filter
    )(
        runner.sqlContext.sql('select * from event_common_model')
    ).createTempView('event_common_model')

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'consumer', 'event_common_model_v4.sql',
            'mindbody', 'event_common_model', 'event_date', date_input
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='MindBody',
            data_type=DataType.CONSUMER,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # Initialize Spark
    spark, sql_context = init("MindBody")

    # Initialize the Spark Runner
    runner = Runner(sql_context)

    # Run the normalization routine
    run(spark, runner, args.date, airflow_test=args.airflow_test)

    # Tell spark to shutdown
    spark.stop()

    # Determine where to put the output
    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
