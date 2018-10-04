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
import spark.helpers.privacy.labtests as priv_labtests
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.providers.auoradx.labtests.transactional_schemas as transactional_schemas


FEED_ID = '85'
VENDOR_ID = '335'
MODEL_VERSION_NUMBER = '07'

script_path = __file__


def run(spark, runner, date_input, test=False, end_to_end_test=False):

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/auroradx/labtests/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/auroradx/labtests/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/auroradx/labtests/out/2018/09/12/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/auroradx/labtests/payload/2018/09/12/'
    else: 
        input_path = 's3a://salusv/incoming/labtests/auroradx/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/labtests/auroradx/{}/'.format(
            date_input.replace('-', '/')
        )

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
    payload_loader.load(runner, matching_path, ['claimId', 'personId', 'patientId', 'hvJoinKey'], table_name='auroradx_payload', load_file_name=True)

    normalized_output = runner.run_all_spark_scripts([
        ['min_date', min_date]
    ])

    df = postprocessor.compose(
        lambda df: schema_enforcer.apply_schema(df, lab_schema),
        # priv_labtests.filter,
        postprocessor.add_universal_columns(
            feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=None, model_version_number=MODEL_VERSION_NUMBER
        ),
        priv_labtests.filter,
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_service', date_input, FEED_ID, "EARLIEST_VALID_SERVICE_DATE", min_date
        ), 
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_specimen', date_input, FEED_ID, "EARLIEST_VALID_SERVICE_DATE", min_date
        )
    )(normalized_output)

    if not test:
        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            datetime.date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.unload(
            spark, runner, df, 'date_service', date_input, 'aurora_diagnostics',
            hvm_historical_date=datetime.datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )
    else: 
        df.collect()


def main(args):
    # init
    spark, sqlContext = init("auroradx")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, end_to_end_test=args.end_to_end_test)

    spark.stop()

    if args.end_to_end_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/auroradx/labtests/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/labtests/2017-02-16/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)    