"""
pregnancy_registry medicalclaims normalize
"""
import os
import argparse
import spark.providers.inovalon.medicalclaims.transactional_schemas_v4 as transactional_schemas_v4
from spark.common.datamart.registry.medicalclaims.medicalclaims_registry_model import schemas as claims_schemas
from spark.common.datamart.registry.fact.registry_race_common_model import schemas as race_schemas
from spark.common.marketplace_driver import MarketplaceDriver
import spark.common.utility.logger as logger
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.datamart.registry.registry_util as reg_util

trans_tbl = 'clm'
mp_tbl = 'matching_payload'
registry_tbl = '_mom_cohort'

tmp_loc = '/tmp/dx/'


if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inovalon'
    output_table_names_to_schemas = {
        'inv_norm_60_dx_cf_final': claims_schemas['schema_v1'],
        'inv_norm_60_dx_race_final': race_schemas['schema_v1']
    }
    provider_partition_name = provider_name
    # ------------------------ Common for all providers -----------------------

    source_table_schema = transactional_schemas_v4

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schema,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=10,
        vdr_feed_id=176,
        use_ref_gen_values=True,
        output_to_datamart_path=True,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 4096,
        'spark.driver.memoryOverhead': 4096
    }
    hdfs_utils.clean_up_output_hdfs(tmp_loc)
    driver.init_spark_context(conf_parameters=conf_parameters)

    b_payloads = True
    if date_input == '2022-02-07':
        driver.spark.read.parquet(driver.matching_path).createOrReplaceTempView(mp_tbl)
        b_payloads = False
    driver.load(payloads=b_payloads)

    partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))

    # registry and matching payload processing
    registry_df, filtered_mp_df = reg_util.get_mom_cohort_filtered_mp(driver.spark, mp_tbl)

    # trans processing
    logger.log('    -Filtering registry hvid from {} table'.format(trans_tbl))
    trans_df = driver.spark.table(trans_tbl)
    filtered_trans_df = trans_df.join(filtered_mp_df, trans_df['memberuid'] == filtered_mp_df['claimid'], 'inner')\
        .select(*[trans_df[c] for c in trans_df.columns]) \
        .repartition(partitions)
    filtered_trans_df.createOrReplaceTempView(trans_tbl)

    # dim processing
    for ref_tbl, ref_clmn, prtn_cnt in [('ccd', 'claimuid', 100), ('mbr', 'memberuid', 2)]:
        ref_loc = tmp_loc + ref_tbl + '/'
        logger.log('    -Filtering {} table'.format(ref_tbl))
        ref_df = driver.spark.table(ref_tbl)
        ref_df.join(driver.spark.table(trans_tbl), [ref_clmn]).select(*[ref_df[c] for c in ref_df.columns])\
            .distinct().repartition(prtn_cnt)\
            .write.parquet(ref_loc, compression='gzip', mode='overwrite')
        driver.spark.read.parquet(ref_loc).repartition(prtn_cnt).createOrReplaceTempView(ref_tbl)

    registry_df.createOrReplaceTempView(registry_tbl)
    filtered_mp_df.createOrReplaceTempView(mp_tbl)

    orig_provider_directory_path = driver.provider_directory_path
    driver.provider_directory_path = orig_provider_directory_path + 'run_first/'
    driver.transform()

    # inv_norm_40_dx_cf processing
    out_tbl = 'inv_norm_40_dx_cf'
    logger.log('    -Writing Interim table {}'.format(out_tbl))
    driver.spark.table(out_tbl).repartition(200)\
        .write.parquet(tmp_loc + out_tbl + '/', compression='gzip', mode='overwrite')
    logger.log('    -Reading Interim table {}'.format(out_tbl))
    driver.spark.read.parquet(tmp_loc + out_tbl + '/').repartition(200).createOrReplaceTempView(out_tbl)

    # final processing
    driver.provider_directory_path = orig_provider_directory_path
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    hdfs_utils.clean_up_output_hdfs(tmp_loc)
    logger.log('all done')
