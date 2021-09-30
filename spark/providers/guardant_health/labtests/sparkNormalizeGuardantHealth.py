"""
guardant health labtest normalize
"""
import argparse
import datetime
from spark.runner import Runner
from spark.spark_setup import init
from spark.providers.guardant_health.labtests.transaction_schemas import \
    schema as transaction_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.explode as explode
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.labtests as priv_labtests

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/guardant_health/labtests/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    FEED_ID = '58'
    VENDOR_ID = '249'

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/guardant_health/labtests/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/guardant_health/labtests/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/guardant_health/labtests/out/{}/'\
            .format(date_input.replace('-', '/'))
        matching_path = \
            's3://salusv/testing/dewey/airflow/e2e/guardant_health/labtests/payload/{}/'\
            .format(date_input.replace('-', '/'))
    else:
        input_path = 's3a://salusv/incoming/labtests/guardant_health/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/labtests/guardant_health/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    postprocessor.compose(
        postprocessor.add_input_filename('source_file_name'),
        postprocessor.trimmify, postprocessor.nullify
    )(
        runner.sqlContext.read.csv(input_path, schema=transaction_schema)
    ).createOrReplaceTempView('transactions')

    explode.generate_exploder_table(spark, 19, name='result_exploder')

    runner.run_spark_script('normalize.sql', return_output=True).createOrReplaceTempView(
        'normalized_labtests'
    )

    postprocessor.compose(
        postprocessor.add_universal_columns(
            feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=None, model_version_number='04'
        ),
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_service', date_input, FEED_ID, 'EARLIEST_VALID_SERVICE_DATE'
        ),
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_report', date_input, FEED_ID, 'EARLIEST_VALID_SERVICE_DATE'
        ),
        lambda df: priv_labtests.filter(df, additional_transformer=Transformer(
            diagnosis_code=[
                TransformFunction(lambda c: c, ['diagnosis_code'])
            ]
        ))
    )(
        runner.sqlContext.sql('select * from {}'.format('normalized_labtests'))
    ).createOrReplaceTempView('normalized_labtests')

    if not test:
        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            datetime.date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'labtests', 'lab_common_model_v4.sql', 'guardant_health',
            'normalized_labtests', 'date_service', date_input,
            hvm_historical_date=datetime.datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Guardant',
            data_type=DataType.LAB_TESTS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init("Guardant Health")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
