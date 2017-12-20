from datetime import datetime, date
import argparse

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.explode as explode
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv

def run(spark, runner, date_input, test = False, airflow_test = False):
    setid = 'MHHealthVerity.Record.{}'.format(date_input.replace('-', ''))

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson_macrohelix/pharmacyclaims/resources/input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/mckesson_macrohelix/pharmacyclaims/resources/matching/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson_macrohelix/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson_macrohelix/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/mckesson_macrohelix/{}/transactions/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/mckesson_macrohelix/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    min_date = postprocessor.coalesce_dates(
                    runner.sqlContext,
                    '48',
                    None,
                    'HVM_AVAILABLE_HISTORY_START_DATE'
                )
    if min_date:
        min_date = min_date.isoformat()

    max_date = date_input

    runner.run_spark_script('../../../common/pharmacyclaims_common_model_v4.sql', [
        ['external', '', False],
        ['table_name', 'pharmacyclaims_common_model', False],
        ['properties', '', False],
        ['additional_columns', [], False]
    ])

    payload_loader.load(runner, matching_path, ['claimId', 'patientId', 'hvJoinKey'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path],
    ])

    postprocessor.compose(
        postprocessor.trimmify,
        postprocessor.nullify
    )(runner.sqlContext.sql('select * from mckesson_macrohelix_transactions')) \
    .createTempView('mckesson_macrohelix_transactions')

    explode.generate_exploder_table(spark, 24, 'exploder')

    runner.run_spark_script('normalize.sql')

    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id = '48', vendor_id = '86', filename = setid, model_version_number = '4'),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_service', max_date, '48', None, min_date),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createTempView('pharmacyclaims_common_model')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
                        runner.sqlContext,
                        '48',
                        date(1900, 1, 1),
                        'HVM_AVAILABLE_HISTORY_START_DATE',
                        'EARLIST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v4.sql',
            'mckesson_macro_helix', 'pharmacyclaims_common_model',
            'date_service', date_input,
            hvm_historical_date = datetime(hvm_historical.year,
                                           hvm_historical.month,
                                           hvm_historical.day)
        )


def main(args):
    spark, sqlContext = init('Mckesson_Macro_Helix')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test = args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/mckesson_macrohelix/spark-output/'
    else:
        # TODO: change to where v4 is located
        output_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/'

    normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
