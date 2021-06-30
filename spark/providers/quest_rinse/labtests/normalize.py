import argparse
import subprocess
import spark.providers.quest_rinse.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtests_schemas
import spark.common.utility.logger as logger
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.postprocessor as postprocessor
from pyspark.sql.functions import col
from pyspark.sql.types import *

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
    driver.spark.table('ref_geo_state').createOrReplaceTempView('ref_geo_state')

    logger.log('Loading temp table: result_comments_hist')
    driver.spark.read.parquet(
        result_comments_hist_path).createOrReplaceTempView("result_comments_hist")

    try:
        subprocess.check_call(['aws', 's3', 'ls', driver.input_path + 'result_comments/'])
        driver.spark.read.parquet(
            driver.input_path + 'result_comments/').createOrReplaceTempView("result_comments")
    except:
        logger.log('...result_comments_hist read from hist')
        driver.spark.table('result_comments_hist').createOrReplaceTempView("result_comments")

    driver.load(cache_tables=False)

    logger.log(' -trimmify-nullify matchig_payload and transactions data')
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    for tbl in ['transactions', 'diagnosis', 'result_comments', 'result_comments_hist']:
        cleaned_df = postprocessor.nullify(
            postprocessor.trimmify(driver.spark.table(tbl)), ['NULL', 'Null', 'null', 'N/A', ''])
        if tbl == "diagnosis":
            cleaned_df2 = cleaned_df\
                .withColumn('date_of_service', cleaned_df['date_of_service'].cast(StringType())) \
                .withColumn('lab_code', cleaned_df['lab_code'].cast(StringType())) \
                .withColumn('unique_accession_id', cleaned_df['unique_accession_id'].cast(StringType())) \
                .withColumn('acct_number', cleaned_df['acct_number'].cast(StringType())) \
                .withColumn('s_icd_codeset_ind', cleaned_df['s_icd_codeset_ind'].cast(StringType())) \
                .withColumn('s_diag_code', cleaned_df['s_diag_code'].cast(StringType())) \
                .withColumn('diag_code', cleaned_df['diag_code'].cast(StringType())) \
                .withColumn('icd_codeset_ind', cleaned_df['icd_codeset_ind'].cast(StringType())) \
                .withColumn('dos_yyyymm', cleaned_df['dos_yyyymm'].cast(StringType()))
        elif tbl == "result_comments":
            cleaned_df2 = cleaned_df.withColumn(
                'unique_accession_id', cleaned_df['unique_accession_id'].cast(StringType())) \
                .withColumn('dos_yyyymm', cleaned_df['dos_yyyymm'].cast(StringType())) \
                .withColumn('lab_code', cleaned_df['lab_code'].cast(StringType())) \
                .withColumn('res_comm_text', cleaned_df['res_comm_text'].cast(StringType()))
        else:
            cleaned_df2 = cleaned_df
        cleaned_df2.cache().createOrReplaceTempView(tbl)

    df = postprocessor.nullify(postprocessor.trimmify(
        driver.spark.table('order_result')), ['NULL', 'Null', 'null', 'N/A', ''])
    df2 = df.withColumn('date_of_service', df['date_of_service'].cast(StringType())) \
        .withColumn('lab_code', df['lab_code'].cast(StringType())) \
        .withColumn('unique_accession_id', df['unique_accession_id'].cast(StringType())) \
        .withColumn('accession_number', df['accession_number'].cast(StringType())) \
        .withColumn('requisition_number', df['requisition_number'].cast(StringType())) \
        .withColumn('cmdm_spclty_cd', df['cmdm_spclty_cd'].cast(StringType())) \
        .withColumn('acct_name', df['acct_name'].cast(StringType())) \
        .withColumn('market_type', df['market_type'].cast(StringType())) \
        .withColumn('cmdm_licstate', df['cmdm_licstate'].cast(StringType())) \
        .withColumn('cmdm_licnum', df['cmdm_licnum'].cast(StringType())) \
        .withColumn('billing_client_number', df['billing_client_number'].cast(StringType())) \
        .withColumn('acct_address_1', df['acct_address_1'].cast(StringType())) \
        .withColumn('acct_address_2', df['acct_address_2'].cast(StringType())) \
        .withColumn('acct_city', df['acct_city'].cast(StringType())) \
        .withColumn('acct_state', df['acct_state'].cast(StringType())) \
        .withColumn('acct_zip', df['acct_zip'].cast(StringType())) \
        .withColumn('fasting_ind', df['fasting_ind'].cast(StringType())) \
        .withColumn('date_of_collection', df['date_of_collection'].cast(StringType())) \
        .withColumn('date_final_report', df['date_final_report'].cast(StringType())) \
        .withColumn('date_order_entry', df['date_order_entry'].cast(StringType())) \
        .withColumn('informed_consent_flag', df['informed_consent_flag'].cast(StringType())) \
        .withColumn('long_description', df['long_description'].cast(StringType())) \
        .withColumn('legal_entity', df['legal_entity'].cast(StringType())) \
        .withColumn('specimen_type', df['specimen_type'].cast(StringType())) \
        .withColumn('ordering_site_code', df['ordering_site_code'].cast(StringType())) \
        .withColumn('canceled_accn_ind', df['canceled_accn_ind'].cast(StringType())) \
        .withColumn('pm_ethnic_grp', df['pm_ethnic_grp'].cast(StringType())) \
        .withColumn('relationship_to_insured', df['relationship_to_insured'].cast(StringType())) \
        .withColumn('copy_to_clns', df['copy_to_clns'].cast(StringType())) \
        .withColumn('non_physician_name', df['non_physician_name'].cast(StringType())) \
        .withColumn('non_physician_id', df['non_physician_id'].cast(StringType())) \
        .withColumn('phy_name', df['phy_name'].cast(StringType())) \
        .withColumn('phy_first_name', df['phy_first_name'].cast(StringType())) \
        .withColumn('phy_middle_name', df['phy_middle_name'].cast(StringType())) \
        .withColumn('phy_last_name', df['phy_last_name'].cast(StringType())) \
        .withColumn('upin', df['upin'].cast(StringType())) \
        .withColumn('client_specialty', df['client_specialty'].cast(StringType())) \
        .withColumn('suffix', df['suffix'].cast(StringType())) \
        .withColumn('degree', df['degree'].cast(StringType())) \
        .withColumn('npi', df['npi'].cast(StringType())) \
        .withColumn('local_profile_code', df['local_profile_code'].cast(StringType())) \
        .withColumn('standard_profile_code', df['standard_profile_code'].cast(StringType())) \
        .withColumn('profile_name', df['profile_name'].cast(StringType())) \
        .withColumn('standard_order_code', df['standard_order_code'].cast(StringType())) \
        .withColumn('order_name', df['order_name'].cast(StringType())) \
        .withColumn('local_result_code', df['local_result_code'].cast(StringType())) \
        .withColumn('idw_analyte_code', df['idw_analyte_code'].cast(StringType())) \
        .withColumn('loinc_code', df['loinc_code'].cast(StringType())) \
        .withColumn('result_name', df['result_name'].cast(StringType())) \
        .withColumn('ref_range_alpha', df['ref_range_alpha'].cast(StringType())) \
        .withColumn('units', df['units'].cast(StringType())) \
        .withColumn('qls_service_code', df['qls_service_code'].cast(StringType())) \
        .withColumn('reportable_results_ind', df['reportable_results_ind'].cast(StringType())) \
        .withColumn('perf_lab_code', df['perf_lab_code'].cast(StringType())) \
        .withColumn('abnormal_ind', df['abnormal_ind'].cast(StringType())) \
        .withColumn('cpt_code', df['cpt_code'].cast(StringType())) \
        .withColumn('result_type', df['result_type'].cast(StringType())) \
        .withColumn('result_value', df['result_value'].cast(StringType())) \
        .withColumn('amended_report_ind', df['amended_report_ind'].cast(StringType())) \
        .withColumn('alpha_normal_flag', df['alpha_normal_flag'].cast(StringType())) \
        .withColumn('date_reported', df['date_reported'].cast(StringType())) \
        .withColumn('instrument_id', df['instrument_id'].cast(StringType())) \
        .withColumn('result_release_date', df['result_release_date'].cast(StringType())) \
        .withColumn('enterprise_ntc_code', df['enterprise_ntc_code'].cast(StringType())) \
        .withColumn('idw_local_order_code', df['idw_local_order_code'].cast(StringType())) \
        .withColumn('derived_profile_code', df['derived_profile_code'].cast(StringType())) \
        .withColumn('qtim_as_ordered_code', df['qtim_as_ordered_code'].cast(StringType())) \
        .withColumn('qtim_profile_ind', df['qtim_profile_ind'].cast(StringType())) \
        .withColumn('dos_yyyymm', df['dos_yyyymm'].cast(StringType())) \
        .withColumn('sister_lab', df['sister_lab'].cast(StringType())) \
        .withColumn('bill_type_cd', df['bill_type_cd'].cast(StringType())) \
        .withColumn('idw_report_change_status', df['idw_report_change_status'].cast(StringType())) \
        .withColumn('company', df['company'].cast(StringType())) \
        .withColumn('active_ind', df['active_ind'].cast(StringType())) \
        .withColumn('bill_code', df['bill_code'].cast(StringType())) \
        .withColumn('insurance_billing_type', df['insurance_billing_type'].cast(StringType())) \
        .withColumn('policy_number', df['policy_number'].cast(StringType())) \
        .withColumn('medicaid_number', df['medicaid_number'].cast(StringType())) \
        .withColumn('medicare_number', df['medicare_number'].cast(StringType())) \
        .withColumn('group_number', df['group_number'].cast(StringType())) \
        .withColumn('qbs_payor_cd', df['qbs_payor_cd'].cast(StringType())) \
        .withColumn('lab_name', df['lab_name'].cast(StringType())) \
        .withColumn('lab_lis_type', df['lab_lis_type'].cast(StringType())) \
        .withColumn('confidential_order_ind', df['confidential_order_ind'].cast(StringType())) \
        .withColumn('daard_client_flag', df['daard_client_flag'].cast(StringType()))
    df = df2.repartition(int(
        driver.spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'unique_accession_id')
    df = df.cache_and_track('order_result')
    df.createOrReplaceTempView('order_result')
    # df.count()

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
