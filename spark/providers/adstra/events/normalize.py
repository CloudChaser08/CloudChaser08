import argparse
from datetime import datetime, date
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.event_common_model_v4 import schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.events as event_priv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger

FEED_ID = '214'
VENDOR_ID = '665'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/adstra/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/consumer/2017-08-02/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    setid = 'adstra'

    script_path = __file__

    transaction_path = ''
    if test:
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/adstra/events/resources/matching/'
        )
        ids_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/adstra/events/resources/ids/'
        )
    elif airflow_test:
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/adstra/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        ids_path = 's3://salusv/testing/dewey/airflow/e2e/adstra/ids/'
    else:
        matching_path = 's3://salusv/matching/payload/consumer/adstra/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        FEED_ID,
        None,
        'HVM_AVAILABLE_HISTORY_START_DATE'
    )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    payload_loader.load(runner, matching_path, ['claimId'])

    normalized_df = runner.run_spark_script(
        'normalize.sql',
        [['date_input', date_input]],
        return_output=True
    )

    events_df = postprocessor.compose(
        schema_enforcer.apply_schema_func(schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID,
            vendor_id=VENDOR_ID,
            filename=setid,
            model_version_number='11'
        ),
        postprocessor.nullify,
        event_priv.filter
    )(
        normalized_df
    )

    events_df.createOrReplaceTempView('event_common_model')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            date(1900, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIST_VALID_SERVICE_DATE'
        )

        file_utils.clean_up_output_hdfs('/staging/')
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'events', 'event_common_model_v4.sql',
            'adstra', 'event_common_model',
            'source_record_date', date_input,
            hvm_historical_date=datetime(hvm_historical.year,
                                         hvm_historical.month,
                                         hvm_historical.day)
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Adstra',
            data_type=DataType.CONSUMER,
            data_source_transaction_path=transaction_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sqlContext = init('Adstra')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)