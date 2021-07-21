import argparse
import subprocess
from functools import reduce
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
import spark.providers.quest_rinse.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtests_schemas
import spark.common.utility.logger as logger
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.postprocessor as postprocessor


result_comments_hist_path = "s3://salusv/incoming/labtests/quest_rinse/order_result_comments_hist2/"

if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'quest_rinse'
    output_table_names_to_schemas = {
        'labtest_quest_rinse_final': labtests_schemas['schema_v9']
    }
    provider_partition_name = 'quest_rinse'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=187,
        use_ref_gen_values=True,
        unload_partition_count=20,
        output_to_transform_path=True
    )

    # init
    conf_parameters = {
        'spark.default.parallelism': 600,
        'spark.sql.shuffle.partitions': 600,
        'spark.executor.memoryOverhead': 10240,
        'spark.driver.memoryOverhead': 10240,
        'spark.sql.autoBroadcastJoinThreshold': 26214400
    }

    driver.init_spark_context(conf_parameters=conf_parameters)

    # ------------------------ Load Reference Tables -----------------------
    logger.log('Loading external table: ref_geo_state')
    external_table_loader.load_analytics_db_table(
        driver.sql_context, 'dw', 'ref_geo_state', 'ref_geo_state'
    )
    driver.spark.table('ref_geo_state').cache_and_track('ref_geo_state').createOrReplaceTempView(
        'ref_geo_state')
    driver.spark.table('ref_geo_state').count()

    logger.log('Loading temp table: result_comments_hist')
    driver.spark.read.parquet(
        result_comments_hist_path).createOrReplaceTempView("result_comments_hist")

    driver.load(cache_tables=False)

    accn_distinct = driver.spark.table('order_result').select('accn_id').distinct()

    # filter down the results comments history table by removing rows that are definitely
    # irrelevant
    driver.spark.table("result_comments_hist") \
        .join(F.broadcast(accn_distinct), 'accn_id') \
        .createOrReplaceTempView("result_comments_hist")

    try:
        subprocess.check_call(['aws', 's3', 'ls', driver.input_path + 'result_comments/'])
        driver.spark.read.parquet(driver.input_path + 'result_comments/') \
            .createOrReplaceTempView("result_comments")

    except:
        logger.log('...result_comments_hist read from hist')
        driver.spark.table('result_comments_hist') \
            .join(F.broadcast(accn_distinct), 'accn_id') \
            .createOrReplaceTempView("result_comments")

    logger.log(' -trimmify-nullify matchig_payload and transactions data')
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    for tbl in ['transactions', 'diagnosis', 'result_comments', 'result_comments_hist']:
        cleaned_df = postprocessor.nullify(
            postprocessor.trimmify(driver.spark.table(tbl)), ['NULL', 'Null', 'null', 'N/A', ''])
        if tbl == "diagnosis":
            cleaned_df2 = cleaned_df \
                .withColumn('date_of_service', col('date_of_service').cast(StringType())) \
                .withColumn('lab_code', col('lab_code').cast(StringType())) \
                .withColumn('unique_accession_id', col('unique_accession_id').cast(StringType())) \
                .withColumn('acct_number', col('acct_number').cast(StringType())) \
                .withColumn('s_icd_codeset_ind', col('s_icd_codeset_ind').cast(StringType())) \
                .withColumn('s_diag_code', col('s_diag_code').cast(StringType())) \
                .withColumn('diag_code', col('diag_code').cast(StringType())) \
                .withColumn('icd_codeset_ind', col('icd_codeset_ind').cast(StringType())) \
                .withColumn('dos_yyyymm', col('dos_yyyymm').cast(StringType()))
        elif tbl == "result_comments":
            cleaned_df2 = cleaned_df.withColumn(
                'unique_accession_id', col('unique_accession_id').cast(StringType())) \
                .withColumn('dos_yyyymm', col('dos_yyyymm').cast(StringType())) \
                .withColumn('lab_code', col('lab_code').cast(StringType())) \
                .withColumn('res_comm_text', col('res_comm_text').cast(StringType()))
        else:
            cleaned_df2 = cleaned_df
        cleaned_df2.cache().createOrReplaceTempView(tbl)

    df = postprocessor.nullify(postprocessor.trimmify(driver.spark.table('order_result')),
                               ['NULL', 'Null', 'null', 'N/A', ''])

    columns_to_cast_to_str = [
        'date_of_service', 'lab_code', 'unique_accession_id', 'accession_number',
        'requisition_number', 'cmdm_spclty_cd', 'acct_name', 'market_type', 'cmdm_licstate',
        'cmdm_licnum', 'billing_client_number', 'acct_address_1', 'acct_address_2', 'acct_city',
        'acct_state', 'acct_zip', 'fasting_ind', 'date_of_collection', 'date_final_report',
        'date_order_entry', 'informed_consent_flag', 'long_description', 'legal_entity',
        'specimen_type', 'ordering_site_code', 'canceled_accn_ind', 'pm_ethnic_grp',
        'relationship_to_insured', 'copy_to_clns', 'non_physician_name', 'non_physician_id',
        'phy_name', 'phy_first_name', 'phy_middle_name', 'phy_last_name', 'upin',
        'client_specialty', 'suffix', 'degree', 'npi', 'local_profile_code',
        'standard_profile_code', 'profile_name', 'standard_order_code', 'order_name',
        'local_result_code', 'idw_analyte_code', 'loinc_code', 'result_name', 'ref_range_alpha',
        'units', 'qls_service_code', 'reportable_results_ind', 'perf_lab_code', 'abnormal_ind',
        'cpt_code', 'result_type', 'result_value', 'amended_report_ind', 'alpha_normal_flag',
        'date_reported', 'instrument_id', 'result_release_date', 'enterprise_ntc_code',
        'idw_local_order_code', 'derived_profile_code', 'qtim_as_ordered_code', 'qtim_profile_ind',
        'dos_yyyymm', 'sister_lab', 'bill_type_cd', 'idw_report_change_status', 'company',
        'active_ind', 'bill_code', 'insurance_billing_type', 'policy_number', 'medicaid_number',
        'medicare_number', 'group_number', 'qbs_payor_cd', 'lab_name', 'lab_lis_type',
        'confidential_order_ind', 'daard_client_flag'
    ]
    df2 = (reduce(
        lambda memo_df, col_name: memo_df.withColumn(col_name, col(col_name).cast(StringType())),
        columns_to_cast_to_str,
        df
    ))

    partition_count = int(driver.spark.sparkContext.getConf()
                          .get('spark.sql.shuffle.partitions'))
    df3 = df2.repartition(partition_count, 'unique_accession_id')
    df3 = df3.cache_and_track('order_result')
    df3.createOrReplaceTempView('order_result')

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
