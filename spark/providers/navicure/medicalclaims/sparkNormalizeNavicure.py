"""Normalization routine for Navicure"""
import argparse
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.medicalclaims_common_model import schema_v8 as medicalclaims_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.providers.navicure.medicalclaims.transactional_schemas as transactional_schemas
import pyspark.sql.functions as F

FEED_ID = '24'
SCRIPT_PATH = __file__

def run(spark, runner, date_input, test=False, end_to_end_test=False):
    if test:
        input_path = file_utils.get_abs_path(
            SCRIPT_PATH, '../../../test/providers/navicure/medicalclaims/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            SCRIPT_PATH, '../../../test/providers/navicure/medicalclaims/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/navicure/medicalclaims/out/2019/02/26/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/navicure/medicalclaims/payload/2019/02/26/'
    else:
        input_path = 's3://salusv/incoming/medicalclaims/navicure/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/medicalclaims/navicure/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)
        external_table_loader.load_analytics_db_table(
            runner.sqlContext, 'dw', 'date_explode_indices', 'date_explode_indices'
        )
        spark.table('date_explode_indices').cache().createOrReplaceTempView('date_explode_indices')
        spark.table('date_explode_indices').count()

    records_loader.load_and_clean_all_v2(runner, input_path, transactional_schemas, load_file_name=True)
    payload_loader.load(runner, matching_path, table_name='matching_payload', load_file_name=True)

    runner.run_all_spark_scripts([
        ['VDR_FILE_DT', date_input, False]
    ])

    for t in ['navicure_dedup_txn', 'navicure_dedup_pay', 'navicure_norm01_lines', 'navicure_norm02_claims']:
        spark.table(t).persist().createOrReplaceTempView(t)
        spark.table(t).count()

    target_tables = [
        'navicure_norm03_final_01',
        'navicure_norm03_final_02',
        'navicure_norm03_final_03',
        'navicure_norm03_final_04'
    ]

    df = reduce(lambda df1, df2: df1.union(df2), [
        schema_enforcer.apply_schema(
            spark.table(t),
            medicalclaims_schema,
            columns_to_keep=['part_provider', 'part_best_date']
        ) for t in target_tables])

    output = postprocessor.compose(
        lambda df: df.withColumn('record_id', F.monotonically_increasing_id()),
        schema_enforcer.apply_schema_func(
            medicalclaims_schema,
            cols_to_keep=['part_provider', 'part_best_date']
        )
    )

    if not test:
        _columns = output.columns
        _columns.remove('part_provider')
        _columns.remove('part_best_date')

        normalized_records_unloader.unload(
            spark, runner, output, 'part_best_date', date_input, 'navicure',
            columns=_columns, substr_date_part=False
        )
    else:
        output.collect()


def main(args):
    # init
    spark, sqlContext = init("navicure")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, end_to_end_test=args.end_to_end_test)

    spark.stop()

    if args.end_to_end_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/navicure/medicalclaims/spark-output/'
    elif args.output_loc is not None:
        output_path = args.output_loc
    else:
        output_path = 's3://salusv/warehouse/parquet/medicalclaims/2018-06-06/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--output_loc', type=str)
    args = parser.parse_args()
    main(args)    
