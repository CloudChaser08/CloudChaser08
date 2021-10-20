import os
import argparse
import subprocess
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims import schemas 
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
# import spark.helpers.reject_reversal as rr
import spark.providers.pdx.pharmacyclaims.load_transactions as load_transactions

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger

schema_obj = schemas['schema_v7']
schema = schema_obj.schema_structure

OUTPUT_PATH_PRODUCTION = os.path.join('s3://salusv/warehouse/parquet/', schema_obj.output_directory)
OUTPUT_PATH_TRANSFORMED = os.path.join('s3://salusv/warehouse/transformed/', schema_obj.output_directory)
OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/pdx/spark-output/'
PART_PROVIDER = schema_obj.provider_partition_column
PART_BEST_DATE = schema_obj.date_partition_column
FEED_ID = '65'
PDX = 'pdx'


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
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/pdx/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/pdx/{}/'.format(
            date_input.replace('-', '/')
        )

    if custom_input_path:
        input_path = custom_input_path

    if custom_matching_path:
        matching_path = custom_matching_path

    s3_output_part_provider = os.path.join(OUTPUT_PATH_PRODUCTION, 'part_provider=pdx')

    if not test:
        logger.log('Loading external tables')
        external_table_loader.load_ref_gen_ref(runner.sqlContext)
        spark.read.parquet(s3_output_part_provider).createOrReplaceTempView(
            '_temp_pharmacyclaims_nb')

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

    df = schema_enforcer.apply_schema(normalized_output, schema, columns_to_keep=['part_provider', PART_BEST_DATE])

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
            spark, runner, df, PART_BEST_DATE, date_input, PDX,
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
            provider_name=PDX,
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sql_context = init('PDX Normalization')

    runner = Runner(sql_context)

    run(spark, runner, args.date, custom_input_path=args.input_path,
        custom_matching_path=args.matching_path, end_to_end_test=args.end_to_end_test)

    spark.stop()

    output_path = OUTPUT_PATH_PRODUCTION
    tmp_path = 's3://salusv/backup/pdx/{}/'.format(args.date)
    if args.end_to_end_test:
        output_path = OUTPUT_PATH_TEST
        tmp_path = 's3://salusv/testing/dewey/airflow/e2e/pdx/temp/'
    elif args.output_path:
        output_path = args.output_path

    current_year_month = args.date[:7] + '-01'
    prev_year_month = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime(
        '%Y-%m-01')

    date_part = 'part_provider=pdx/part_best_date={}/'
    subprocess.check_call(
        ['aws', 's3', 'mv', '--recursive', os.path.join(output_path, date_part.format(current_year_month)),
         os.path.join(tmp_path, date_part.format(current_year_month))]
    )
    subprocess.check_call(
        ['aws', 's3', 'mv', '--recursive', os.path.join(output_path, date_part.format(prev_year_month)),
         os.path.join(tmp_path, date_part.format(prev_year_month))]
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
    parser.add_argument('--output_path', help='Overwrite default output path with this value')
    args = parser.parse_known_args()[0]
    main(args)
