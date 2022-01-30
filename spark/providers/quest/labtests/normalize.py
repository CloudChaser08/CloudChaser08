"""
quest normalize
"""
import argparse
import pyspark.sql.functions as FN

from datetime import timedelta, datetime
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.postprocessor as postprocessor
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
import spark.providers.quest.labtests.transactional_schemas_v1a as transactions_v1a
import spark.providers.quest.labtests.transactional_schemas_v1 as transactions_v1
import spark.providers.quest.labtests.transactional_schemas_v2 as transactions_v2
import spark.providers.quest.labtests.transactional_schemas_v3 as transactions_v3
from spark.common.utility import logger

CUTOFF_DATE_V1 = datetime.strptime('2016-08-31', '%Y-%m-%d')
CUTOFF_DATE_V2 = datetime.strptime('2017-01-15', '%Y-%m-%d')


def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    logger.log(" -quest-labtests: normalization ")

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'quest'
    output_table_names_to_schemas = {
        'quest_labtest_norm_final': labtest_schemas['schema_v4'],
    }
    provider_partition_name = 'quest'

    # ------------------------ Common for all providers -----------------------
    is_schema_v1 = datetime.strptime(date_input, '%Y-%m-%d') < CUTOFF_DATE_V1
    is_schema_v2 = CUTOFF_DATE_V1 <= datetime.strptime(date_input, '%Y-%m-%d') < CUTOFF_DATE_V2

    if is_schema_v1:
        logger.log('Historic Load schema')
        source_table_schemas = transactions_v1
    elif is_schema_v2:
        logger.log('Load without NPI')
        source_table_schemas = transactions_v2
    else:
        logger.log('Current Load schema with NPI')
        source_table_schemas = transactions_v3

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        test=test,
        unload_partition_count=5,
        vdr_feed_id=18,
        load_date_explode=False,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    script_path = __file__
    if test:
        driver.input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/quest/resources/input/year/month/day/'
        ) + '/'
        driver.matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/quest/resources/matching/year/month/day/'
        ) + '/'
    elif airflow_test:
        driver.input_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/out/{}/'.format(
            date_input.replace('-', '/')
        )
        driver.matching_path = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/payload/{}/'.format(
            date_input.replace('-', '/')
        )

    driver.load()
    driver.get_ref_gen_ref_values()

    matching_payload_df = driver.spark.table('matching_payload')
    if is_schema_v1:
        matching_payload_df = postprocessor.add_null_column('hvJoinKey')(matching_payload_df)
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    if is_schema_v1:
        txn_df = driver.spark.table('txn').distinct()
        txn_df.select('accn_id', 'dosid', 'local_order_code', 'standard_order_code',
                      'order_name', 'loinc_code', 'local_result_code', 'result_name'
                      ).createOrReplaceTempView('transactions_trunk')

        txn_df.select('accn_id', 'dosid', 'local_order_code', 'standard_order_code',
                      'order_name', 'loinc_code', 'local_result_code', 'result_name'
                      ).createOrReplaceTempView('transactions_trunk')

        txn_df.select('accn_id', 'date_of_service', 'dosid', 'lab_id', 'date_collected',
            # FN.lit(None).cast(ArrayType(StringType())).alias('patient_first_name'),
            # FN.lit(None).cast(ArrayType(StringType())).alias('patient_middle_name'),
            # FN.lit(None).cast(ArrayType(StringType())).alias('patient_last_name'),
            # FN.lit(None).cast(ArrayType(StringType())).alias('address1'),
            # FN.lit(None).cast(ArrayType(StringType())).alias('address2'),
            # FN.lit(None).cast(ArrayType(StringType())).alias('city'),
            FN.lit(None).cast(ArrayType(StringType())).alias('state'),
            FN.lit(None).cast(ArrayType(StringType())).alias('zip_code'),
            # FN.lit(None).cast(ArrayType(StringType())).alias('date_of_birth'),
            # FN.lit(None).cast(ArrayType(StringType())).alias('patient_age'),
            FN.lit(None).cast(ArrayType(StringType())).alias('gender'),
            'diagnosis_code', 'icd_codeset_ind',
            FN.lit(None).cast(ArrayType(StringType())).alias('acct_zip'),
            FN.lit(None).cast(ArrayType(StringType())).alias('npi'),
            FN.lit(None).cast(ArrayType(StringType())).alias('hv_join_key')
                     ).createOrReplaceTempView('transactions_trunk')
    else:
        driver.spark.table('trunk').distinct().createOrReplaceTempView('transactions_trunk')

        addon_df = driver.spark.table('addon').distinct()
        if is_schema_v2:
            addon_df = postprocessor.add_null_column('acct_zip')(addon_df)
            addon_df = postprocessor.add_null_column('npi')(addon_df)
        addon_df.createOrReplaceTempView('transactions_addon')

    provider_addon_tbl = 'transactions_provider_addon'
    augmented_mp_tbl = 'augmented_with_prov_attrs_mp'
    # provider matching information is stored in <matching_path>/provider_addon/<y>/<m>/<d>/

    try:
        prov_matching_path = '/'.join(driver.matching_path.split('/')[:-4]) + '/provider_addon/' \
                             + '/'.join(driver.matching_path.split('/')[-4:])
        payload_loader.load(driver.runner, prov_matching_path, ['claimId'], table_name=augmented_mp_tbl)
    except:
        driver.spark.table('matching_payload').createOrReplaceTempView(augmented_mp_tbl)

    # provider addon information will be at the month level
    try:
        prov_addon_path = '/'.join(driver.input_path.split('/')[:-2]) + '/provider_addon/'
        records_loader.load_and_clean_all_v2(
            driver.runner, prov_addon_path, transactions_v1a, load_file_name=True, spark_context=driver.spark)
    except:
        p_sql = """select cast(null as string) as accn_id, cast(null as string) as dosid, 
        cast(null as string) as lab_code, cast(null as string) as acct_zip, 
        cast(null as string) as npi 
        """
        driver.spark.sql(p_sql).createOrReplaceTempView(provider_addon_tbl)

    driver.transform()
    if not test:
        driver.save_to_disk()
        driver.stop_spark()
        driver.log_run()
        driver.copy_to_output_path()
    logger.log("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    run(args.date,  args.end_to_end_test)
