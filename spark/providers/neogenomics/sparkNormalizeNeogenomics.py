import argparse
import datetime
import subprocess

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.lab_common_model_v4 import schema as lab_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as explode
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.privacy.labtests as priv_labtests
import spark.providers.neogenomics.udf as neo_udf
import spark.providers.neogenomics.deduplicator as neo_deduplicator
from spark.providers.neogenomics import RESULTS_START_DATE

FEED_ID = '32'
VENDOR_ID = '78'

script_path = __file__


def run(spark, runner, date_input, test=False, airflow_test=False):

    runner.sqlContext.registerFunction(
        'clean_neogenomics_diag_list', neo_udf.clean_neogenomics_diag_list
    )

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/neogenomics/resources/input/{}/'.format(
                date_input.replace('-', '/')
            )
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/neogenomics/resources/matching/{}/'.format(
                date_input.replace('-', '/')
            )
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/labtests/neogenomics/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/labtests/neogenomics/{}/'.format(
            date_input.replace('-', '/')
        )

    # create helper table
    explode.generate_exploder_table(spark, 50, 'diagnosis_exploder')

    neo_deduplicator.load_matching_payloads(runner, matching_path)

    neo_deduplicator.load_and_deduplicate_transaction_table(runner, input_path, test=test)

    if date_input >= RESULTS_START_DATE:
        neo_deduplicator.load_and_deduplicate_transaction_table(runner, input_path, is_results=True, test=test)

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    normalized_output = runner.run_spark_script(
        'normalize.sql', [
            [
                'result_columns', ', r.result_value AS result, r.result_name AS result_name'
                if date_input >= RESULTS_START_DATE else '', False
            ], [
                'result_join', 'LEFT JOIN transactional_results r ON t.test_order_id = r.test_order_id'
                if date_input >= RESULTS_START_DATE else '', False
            ]
        ], return_output=True
    )

    postprocessor.compose(
        lambda df: schema_enforcer.apply_schema(df, lab_schema),
        postprocessor.add_universal_columns(
            feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=None, model_version_number='04'
        ),
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_service', date_input, FEED_ID, 'EARLIEST_VALID_SERVICE_DATE'
        ),
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_specimen', date_input, FEED_ID, 'EARLIEST_VALID_SERVICE_DATE'
        ),
        postprocessor.apply_date_cap(
            runner.sqlContext, 'date_report', date_input, FEED_ID, 'EARLIEST_VALID_SERVICE_DATE'
        ),
        priv_labtests.filter
    )(
        normalized_output
    ).createOrReplaceTempView('lab_common_model')

    if not test:
        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            datetime.date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )
        normalized_records_unloader.partition_and_rename(
            spark, runner, 'lab', 'lab_common_model_v4.sql', 'neogenomics',
            'lab_common_model', 'date_service', date_input, hvm_historical_date=datetime.datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            )
        )


def main(args):
    # init
    spark, sqlContext = init("Neogenomics")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/labtests/2018-02-09/'

    backup_path = output_path.replace('salusv', 'salusv/backup')

    subprocess.check_call([
        'aws', 's3', 'mv', '--recursive', '{}part_provider=neogenomics/'.format(output_path),
        '{}part_provider=neogenomics/'.format(backup_path)
    ])

    normalized_records_unloader.distcp(output_path)

    subprocess.check_call(
        ['aws', 's3', 'rm', '--recursive', '{}part_provider=neogenomics/'.format(backup_path)]
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
