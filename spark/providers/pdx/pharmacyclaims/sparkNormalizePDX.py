import argparse
import subprocess
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims_common_model import schema_v7 as schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.providers.pdx.pharmacyclaims.load_transactions as load_transactions

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '65'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/pdx/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/pharmacyclaims/2018-11-26/'


def run(spark, runner, date_input, custom_input_path=None, custom_matching_path=None, test=False,
        end_to_end_test=False):

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/pdx/pharmacyclaims/resources/input/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/pdx/pharmacyclaims/resources/matching/'
        )
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        spark.table('dw._pharmacyclaims_nb').createOrReplaceTempView('_pharmacyclaims_nb')
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/pdx/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/pdx/{}/'.format(
            date_input.replace('-', '/')
        )
        spark.table('dw._pharmacyclaims_nb').createOrReplaceTempView('_pharmacyclaims_nb')

    if custom_input_path:
        input_path = custom_input_path

    if custom_matching_path:
        matching_path = custom_matching_path

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)
    else:
        pass

    min_date = postprocessor.coalesce_dates(
        runner.sqlContext,
        '65',
        None,
        'EARLIEST_VALID_SERVICE_DATE'
    )

    if min_date:
        min_date = min_date.isoformat()

    load_transactions.load(runner, input_path)
    payload_loader.load(runner, matching_path, ['claimId', 'patientId', 'hvJoinKey'], table_name='pdx_payload')

    date_input_do = datetime.strptime(date_input, '%Y-%m-%d')
    vendor_file_date = date_input_do.strftime('%Y%m%d')
    vendor_file_date_format = date_input_do.strftime('%Y-%m-%d')

    normalized_output = runner.run_all_spark_scripts([
        ['VENDOR_FILE_DATE_STR', vendor_file_date, False],
        ['VENDOR_FILE_DATE_FMT', vendor_file_date_format, False],
    ])

    df = postprocessor.compose(
        lambda df: schema_enforcer.apply_schema(df, schema, columns_to_keep=['part_provider', 'part_best_date'])
    )(normalized_output)

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            FEED_ID,
            date(1900, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIST_VALID_SERVICE_DATE'
        )

        _columns = df.columns
        _columns.remove('part_provider')
        _columns.remove('part_best_date')

        normalized_records_unloader.unload(
            spark, runner, df, 'part_best_date', date_input, 'pdx',
            columns=_columns,
            hvm_historical_date=datetime(hvm_historical.year,
                                         hvm_historical.month,
                                         hvm_historical.day),
            substr_date_part=False
        )

    else:
        df.collect()

    if not test and not end_to_end_test:
        logger.log_run_details(
            provider_name='PDX',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sqlContext = init('PDX Normalization')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, custom_input_path=args.input_path,
        custom_matching_path=args.matching_path, end_to_end_test=args.end_to_end_test)

    spark.stop()

    if args.end_to_end_test:
        output_path = OUTPUT_PATH_TEST
        tmp_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/temp/'
    elif args.output_path:
        output_path = args.output_path
    else:
        output_path = OUTPUT_PATH_PRODUCTION
        tmp_path = 's3://salusv/backup/pdx/{}/'.format(args.date)

    current_year_month = args.date[:7] + '-01'
    prev_year_month = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m-01')

    date_part = 'part_provider=pdx/part_best_date={}/'
    subprocess.check_call(
        ['aws', 's3', 'mv', '--recursive', output_path + date_part.format(current_year_month), tmp_path + date_part.format(current_year_month)]
    )
    subprocess.check_call(
        ['aws', 's3', 'mv', '--recursive', output_path + date_part.format(prev_year_month), tmp_path + date_part.format(prev_year_month)]
    )

    if args.end_to_end_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--input_path', help='Overwrite default input path with this value')
    parser.add_argument('--matching_path', help='Overwrite default matching path with this value')
    parser.add_argument('--ouptut_path', help='Overwrite default output path with this value')
    args = parser.parse_args()
    main(args)
