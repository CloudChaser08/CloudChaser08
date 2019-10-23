import argparse
import datetime
import subprocess

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.lab_common_model import schema_v7 as lab_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as explode
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.privacy.labtests as priv_labtests
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.providers.neogenomics.labv2.transactional_schemas as transactional_schemas

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '32'
VENDOR_ID = '78'
MODEL_VERSION = '07'
GENERIC_MINIMUM_DATE = datetime.date(1901, 1, 1)

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/labtests/2018-02-09/'

script_path = __file__


def run(spark, runner, date_input, test=False, end_to_end_test=False):

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/neogenomics/resources/input/*/*/*/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/neogenomics/resources/matching/*/*/*/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests_v2/out/*/*/*/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests_v2/payload/*/*/*/'
    else:
        input_path = 's3a://salusv/incoming/labtests/neogenomics/*/*/*/'
        matching_path = 's3a://salusv/matching/payload/labtests/neogenomics/*/*/*/'

    records_loader.load_and_clean_all_v2(runner, input_path, transactional_schemas, load_file_name=True)
    payload_loader.load(runner, matching_path, ['claimId', 'personId', 'patientId'], load_file_name=True)

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        FEED_ID,
        None,
        'EARLIEST_VALID_SERVICE_DATE',
        'HVM_AVAILABLE_HISTORY_START_DATE'
    )

    if min_date:
        min_date = min_date.isoformat()

    normalized_output = runner.run_all_spark_scripts([
        ['min_date', min_date]
    ])

    df = postprocessor.compose(
        lambda df: schema_enforcer.apply_schema(df, lab_schema),
        priv_labtests.filter,
        lambda df: schema_enforcer.apply_schema(df, lab_schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=None, model_version_number=MODEL_VERSION
        ),
        lambda df: schema_enforcer.apply_schema(df, lab_schema)
    )(normalized_output)

    if not test:
        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            GENERIC_MINIMUM_DATE,
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.unload(
            spark, runner, df, 'date_service', date_input, 'neogenomics',
            hvm_historical_date=datetime.datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )

    if not test and not end_to_end_test:
        logger.log_run_details(
            provider_name='Neogenomics',
            data_type=DataType.LAB_TESTS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sqlContext = init("Neogenomics")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, end_to_end_test=args.end_to_end_test)

    spark.stop()

    if args.end_to_end_test:
        output_path = OUTPUT_PATH_TEST
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    backup_path = output_path.replace('salusv', 'salusv/backup')

    subprocess.check_call([
        'aws', 's3', 'mv', '--recursive', '{}part_provider=neogenomics/'.format(output_path),
        '{}part_provider=neogenomics/'.format(backup_path)
    ])

    if args.end_to_end_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)

    subprocess.check_call(
        ['aws', 's3', 'rm', '--recursive', '{}part_provider=neogenomics/'.format(backup_path)]
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
