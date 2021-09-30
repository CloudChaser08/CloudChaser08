"""
8451  normalize
"""
import argparse

from spark.runner import Runner
from spark.spark_setup import init
from spark.common.event_common_model import schema_v10 as event_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.providers._8451.grocery.transactional_schemas as transactional_schemas
from spark.common.utility import logger
from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder


FEED_ID = '124'
VENDOR_ID = '337'
MODEL_VERSION = '10'
SCRIPT_PATH = __file__

OUTPUT_PATH_TESTING = 's3://salusv/testing/dewey/airflow/e2e/8451/grocery/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/consumer/2017-08-02/'


def run(spark, runner, date_input, test=False, end_to_end_test=False):

    if test:
        input_path = file_utils.get_abs_path(
            SCRIPT_PATH, '../../../test/providers/_8451/grocery/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            SCRIPT_PATH, '../../../test/providers/_8451/grocery/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/8451/grocery/out/2019/05/21/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/8451/grocery/payload/2019/05/21/'
    else:
        input_path = 's3a://salusv/incoming/consumer/8451_grocery/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/consumer/8451_grocery/{}/'.format(
            date_input.replace('-', '/')
        )

    records_loader.load_and_clean_all_v2(runner, input_path,
                                         transactional_schemas, load_file_name=True)
    payload_loader.load(runner, matching_path, load_file_name=True)

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    df = runner.run_all_spark_scripts([['VDR_FILE_DT', date_input, False]])

    output = schema_enforcer.apply_schema(df, event_schema,
                                          columns_to_keep=['part_provider', 'part_best_date'])

    if not test:
        _columns = df.columns
        _columns.remove('part_provider')
        _columns.remove('part_best_date')

        normalized_records_unloader.unload(
            spark, runner, output, 'part_best_date', date_input, '8451',
            substr_date_part=False, columns=_columns
        )

    if not test and not end_to_end_test:
        logger.log_run_details(
            provider_name='8451_Grocery_HVM',
            data_type=DataType.CONSUMER,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init("{} 8451 Grocery HVM".format(args.date))

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.date, end_to_end_test=args.end_to_end_test)

    spark.stop()

    if args.end_to_end_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TESTING)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)

        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
