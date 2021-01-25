#! /usr/bin/python
import argparse
from datetime import datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.medicalclaims as medical_priv

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_rcm/medicalclaims/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3a://salusv/warehouse/parquet/medicalclaims/2017-02-24/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_rcm/medicalclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal_rcm/medicalclaims/resources/matching/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_rcm/medicalclaims/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal_rcm/medicalclaims/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/medicalclaims/cardinal_rcm/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/medicalclaims/cardinal_rcm/{}/'.format(
            date_input.replace('-', '/')
        )

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    hvm_available_history_date = \
        postprocessor.get_gen_ref_date(runner.sqlContext, "29", "HVM_AVAILABLE_HISTORY_START_DATE")
    earliest_valid_service_date = \
        postprocessor.get_gen_ref_date(runner.sqlContext, "29", "EARLIEST_VALID_SERVICE_DATE")
    hvm_historical_date = hvm_available_history_date if hvm_available_history_date else \
        earliest_valid_service_date if earliest_valid_service_date else datetime.date(1901, 1, 1)
    max_date = date_input

    runner.run_spark_script('../../../common/medicalclaims_common_model.sql', [
        ['table_name', 'medicalclaims_common_model', False],
        ['properties', '', False]
    ])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    explode.generate_exploder_table(spark, 4, 'svc_diag_exploder')
    explode.generate_exploder_table(spark, 8, 'claim_diag_exploder')

    # trim and remove nulls from raw input
    postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(
        runner.sqlContext.sql('select * from transactions')
    ).createTempView('transactions')

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    runner.run_spark_script('normalize_service_lines.sql')
    runner.run_spark_script('normalize_claims.sql')

    vendor_feed_id = '29'
    vendor_id = '42'

    postprocessor.compose(
        postprocessor.add_universal_columns(
            feed_id=vendor_feed_id,
            vendor_id=vendor_id,

            # TODO: this is incorrect - fix when we find out what
            # their filenames will be named
            filename='RCM_Claims_{}.open'.format(date_obj.strftime('%Y%m%d'))
        ),
        medical_priv.filter,
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_service', max_date, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE'),
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_service_end', max_date, vendor_feed_id, 'EARLIEST_VALID_SERVICE_DATE')
    )(
        runner.sqlContext.sql('select * from medicalclaims_common_model')
    ).createTempView('medicalclaims_common_model')

    explode.explode_medicalclaims_dates(runner)

    if not test:
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'medicalclaims', 'medicalclaims_common_model.sql', 'cardinal_rcm',
            'medicalclaims_common_model', 'date_service', date_input,
            hvm_historical_date=datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Cardinal_RCM',
            data_type=DataType.MEDICAL_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):

    # init spark
    spark, sql_context = init("Cardinal RCM")

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
    args = parser.parse_args()
    main(args)
