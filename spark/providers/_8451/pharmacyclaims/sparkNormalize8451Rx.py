"""Normalization routine for 84.51 Rx"""
import argparse
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.pharmacyclaims_common_model import schema_v9 as pharmacyclaims_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.providers._8451.pharmacyclaims.transactional_schemas as transactional_schemas

FEED_ID = '86'
SCRIPT_PATH = __file__

def run(spark, runner, date_input, test=False, end_to_end_test=False):
    if test:
        input_path = file_utils.get_abs_path(
            SCRIPT_PATH, '../../../test/providers/_8451/pharmacyclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            SCRIPT_PATH, '../../../test/providers/_8451/pharmacyclaims/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/8451/pharmacyclaims/out/2019/02/26/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/8451/pharmacyclaims/payload/2019/02/26/'
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/8451/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/8451/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)
        external_table_loader.load_analytics_db_table(
            runner.sqlContext, 'dw', 'ref_geo_state', 'ref_geo_state'
        )
        spark.table('ref_geo_state').cache().createOrReplaceTempView('ref_geo_state')
        spark.table('ref_geo_state').count()

    records_loader.load_and_clean_all_v2(runner, input_path, transactional_schemas, load_file_name=True)
    payload_loader.load(runner, matching_path, extra_cols=['card_code'])

    output = postprocessor.compose(
        schema_enforcer.apply_schema_func(
            pharmacyclaims_schema,
            cols_to_keep=['part_provider', 'part_best_date']
        )
    )(
        runner.run_all_spark_scripts([
            ['VDR_FILE_DT', date_input, False]
        ])
    )

    if not test:
        _columns = output.columns
        _columns.remove('part_provider')
        _columns.remove('part_best_date')

        normalized_records_unloader.unload(
            spark, runner, output, 'part_best_date', date_input, '8451',
            columns=_columns, substr_date_part=False
        )
    else:
        output.collect()


def main(args):
    # init
    spark, sqlContext = init("{} 8451 Rx HVM".format(args.date))

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, end_to_end_test=args.end_to_end_test)

    spark.stop()

    if args.end_to_end_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/8451/pharmacyclaims/spark-output/'
    elif args.output_loc is not None:
        output_path = args.output_loc
    else:
        output_path = 's3://salusv/warehouse/parquet/pharmacyclaims/2018-11-26/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--output_loc', type=str)
    args = parser.parse_args()
    main(args)
