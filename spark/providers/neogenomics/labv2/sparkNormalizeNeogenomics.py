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

FEED_ID = '32'
VENDOR_ID = '78'

script_path = __file__


def run(spark, runner, date_input, test=False, airflow_test=False):

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/neogenomics/resources/input/{}/'.format(
                date_input.replace('-', '/')
            )
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/neogenomics/resources/matching/{}/'.format(
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

    records_loader.load_and_clean_all_v2(runner, input_path, transactional_schemas)
    payload_loader.load(runner, matching_path, ['claimId', 'personId', 'patientId'])

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    runner.run_spark_script('normalize_1.sql', return_output=True).createOrReplaceTempView('t1')
    runner.run_spark_script('normalize_2.sql', return_output=True).createOrReplaceTempView('neogenomics_meta_dedup')
    runner.run_spark_script('normalize_3.sql', return_output=True).createOrReplaceTempView('t3')
    runner.run_spark_script('normalize_4.sql', return_output=True).createOrReplaceTempView('neogenomics_results_dedup')
    runner.run_spark_script('normalize_5.sql', return_output=True).createOrReplaceTempView('t5')
    runner.run_spark_script('normalize_6.sql', return_output=True).createOrReplaceTempView('neogenomics_payload_lag')
    normalized_output = runner.run_spark_script('normalize_7.sql', return_output=True)

    df = postprocessor.compose(
        lambda df: schema_enforcer.apply_schema(df, lab_schema),
        priv_labtests.filter
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
            spark, runner, df, 'date_service', date_input, 'neogenomics',
            hvm_historical_date=datetime.datetime(
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
    return

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
