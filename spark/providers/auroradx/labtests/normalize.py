"""
aurora dx normalize
"""
import argparse
import datetime
import subprocess
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.lab_common_model import schema_v7 as lab_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.providers.auroradx.labtests.transactional_schemas as transactional_schemas

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '85'
script_path = __file__

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/auroradx/labtests/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'


def run(spark, runner, date_input, test=False, end_to_end_test=False):

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/auroradx/labtests/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/auroradx/labtests/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/auroradx/labtests/out/2018/11/14/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/auroradx/labtests/payload/2018/11/14/'
    else:
        # reprocess the full data set every time
        input_path = 's3a://salusv/incoming/labtests/auroradx/*/*/*/'
        matching_path = 's3a://salusv/matching/payload/labtests/auroradx/*/*/*/'

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

    records_loader.load_and_clean_all_v2(runner, input_path, transactional_schemas, load_file_name=True)
    payload_loader.load(runner, matching_path, ['claimId', 'personId', 'patientId', 'hvJoinKey']
                        , table_name='auroradx_payload', load_file_name=True)

    normalized_output = runner.run_all_spark_scripts([
        ['file_date', date_input, False]
    ])

    df = postprocessor.compose(
        lambda df: schema_enforcer.apply_schema(df, lab_schema, columns_to_keep=['part_provider', 'part_best_date'])
    )(normalized_output)

    if not test:
        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            datetime.date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        _columns = df.columns
        _columns.remove('part_provider')
        _columns.remove('part_best_date')

        normalized_records_unloader.unload(
            spark, runner, df, 'part_best_date', date_input, 'aurora_diagnostics',
            columns=_columns,
            hvm_historical_date=datetime.datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )
    else:
        df.collect()

    if not test and not end_to_end_test:
        logger.log_run_details(
            provider_name='Auroradx',
            data_type=DataType.LAB_TESTS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init("auroradx")

    # initialize runner
    runner = Runner(sql_context)

    if args.end_to_end_test:
        output_path = OUTPUT_PATH_TEST
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    run(spark, runner, args.date, end_to_end_test=args.end_to_end_test)

    spark.stop()

    # the full data set is reprocessed every time
    subprocess.check_output(['aws', 's3', 'rm', '--recursive',
                             output_path + 'part_provider=aurora_diagnostics'])

    if args.end_to_end_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
