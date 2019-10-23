from datetime import datetime, date
import argparse

import pyspark.sql.functions as func

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.pharmacyclaims as pharm_priv
import spark.helpers.records_loader as records_loader
import spark.common.pharmacyclaims_common_model_v6 as pharmacyclaims_common_model
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.udf.general_helpers as gen_helpers
import spark.providers.genoa.pharmacyclaims.transactional_schemas as transactional_schemas

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/genoa/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/pharmacyclaims/2018-02-05/'


def run(spark, runner, date_input, test = False, airflow_test = False):
    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/genoa/pharmacyclaims/resources/input/*/*/*/'
        )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/genoa/pharmacyclaims/resources/matching/'
        )
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/genoa/out/*/*/*/transactions/'
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/genoa/payload/*/*/*/'
    else:
        input_path = 's3://salusv/incoming/pharmacyclaims/genoa/*/*/*/'
        matching_path = 's3://salusv/matching/payload/pharmacyclaims/genoa/*/*/*/'

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    max_date = date_input

    runner.run_spark_script('../../../common/zip3_to_state.sql')

    # Load in the matching payload
    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    records_loader.load_and_clean_all_v2(runner, input_path, transactional_schemas, load_file_name=True)

    # Genoa sent historical data (before 2017-11-01) in one schema, and then
    # added 3 new columns later on. We load everything as though it's in the
    # new schema, so the historical data columns need to be adjusted
    historical = spark.table('genoa_rx_raw').where(func.regexp_extract('input_file_name', '(..../../..)/[^/]*$', 1) < '2017/11/01')
    not_historical = spark.table('genoa_rx_raw').where(func.regexp_extract('input_file_name', '(..../../..)/[^/]*$', 1) >= '2017/11/01')

    old_col_name = historical.columns[-5]
    new_col_name = historical.columns[-2]
    historical_adjusted = (
        historical.withColumnRenamed(old_col_name, 'tmp')
                  .withColumnRenamed(historical.columns[-2], old_col_name)
                  .withColumnRenamed('tmp', new_col_name)
                  .select(*not_historical.columns) 
    )

    get_set_id = func.udf(lambda x: gen_helpers.remove_split_suffix(x).replace('Genoa_',''))

    # On 2017-11-01, Genoa gave us a restatement of all the data from
    # 2015-05-31 on. If any batches before 2017-11-01 contain data from the
    # restated time period, remove it
    historical_adjusted = historical_adjusted.where(historical_adjusted['date_of_service'] < '2015-05-31')

    (
        historical_adjusted.union(not_historical)
                           .withColumn('input_file_name', get_set_id('input_file_name'))
                           .createOrReplaceTempView('genoa_rx_raw')
    )

    # Genoa sends 90 days of data in every batch, resulting in a lot of duplciates. Rather than
    # trying to remove duplicates from individual batch, deduplicate all the Genoa data
    raw_table = spark.table('genoa_rx_raw')
    grouping_cols = raw_table.columns
    grouping_cols.remove("hv_join_key")
    grouping_cols.remove("input_file_name")

    final_columns = [func.col(c) for c in grouping_cols]
    final_columns.append(func.max("hv_join_key").alias("hv_join_key"))
    # Use the minimal input file name so that claim_id - data_set pairs will not change when we
    # normalize new batches that also contain the same claims
    final_columns.append(func.min("input_file_name").alias("input_file_name"))

    # Deduplicate by every column except hv_join_key and input_file_name since those are unique to
    # a batch
    (
        raw_table.groupBy(*grouping_cols)
                 .agg(*final_columns)
                 .createOrReplaceTempView("genoa_rx_raw")
    )

    norm_pharmacy = runner.run_spark_script('mapping.sql', return_output=True)
    schema_enforcer.apply_schema(norm_pharmacy, pharmacyclaims_common_model.schema) \
        .createOrReplaceTempView('pharmacyclaims_common_model')

    # Apply clean up and privacy filtering
    postprocessor.compose(
        postprocessor.nullify,
        postprocessor.add_universal_columns(feed_id = '21', vendor_id = '20', filename = None, model_version_number = '06'),
        postprocessor.apply_date_cap(runner.sqlContext, 'date_service', max_date, '21', 'EARLIEST_VALID_SERVICE_DATE'),
        pharm_priv.filter
    )(
        runner.sqlContext.sql('select * from pharmacyclaims_common_model')
    ).createOrReplaceTempView('pharmacyclaims_common_model')

    if not test:
        hvm_historical = postprocessor.coalesce_dates(
            runner.sqlContext,
            '21',
            date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.partition_and_rename(
            spark, runner, 'pharmacyclaims', 'pharmacyclaims_common_model_v6.sql',
            'genoa', 'pharmacyclaims_common_model',
            'date_service', date_input,
            hvm_historical_date = datetime(hvm_historical.year,
                                           hvm_historical.month,
                                           hvm_historical.day)
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Genoa',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    spark, sqlContext = init('Genoa')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test = args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
    else:
        output_path = OUTPUT_PATH_PRODUCTION

    backup_path = output_path.replace('salusv', 'salusv/backup')

    subprocess.check_call(
        ['aws', 's3', 'rm', '--recursive', '{}part_provider=genoa/'.format(backup_path)]
    )

    subprocess.check_call([
        'aws', 's3', 'mv', '--recursive', '{}part_provider=genoa/'.format(output_path),
        '{}part_provider=genoa/'.format(backup_path)
    ])

    if args.airflow_test:
        normalized_records_unloader.distcp(output_path)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(output_path)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
