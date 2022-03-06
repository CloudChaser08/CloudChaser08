"""
inovalon mom registry enrollments normalize
"""
import os
import argparse
import spark.providers.inovalon.enrollmentrecords.transactional_schemas as source_table_schemas
from spark.common.datamart.registry.enrollmentrecords.enrollmentrecords_registry_model import schemas as enr_schemas
from spark.common.marketplace_driver import MarketplaceDriver
import spark.common.utility.logger as logger
import spark.helpers.hdfs_utils as hdfs_utils
import spark.datamart.registry.registry_util as reg_util

tmp_loc = '/tmp/enr/'


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
        'inv_enr_norm_02_final': enr_schemas['schema_v1']
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=1,
        vdr_feed_id=179,
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
    driver.load()
    partitions = int(driver.spark.conf.get('spark.sql.shuffle.partitions'))

    # registry and matching payload processing
    mp_tbl = 'matching_payload'
    registry_tbl = '_temp_mom_cohort'
    registry_df, filtered_mp_df = reg_util.get_mom_cohort_filtered_mp(driver.spark, provider_partition_name, mp_tbl)

    trans_tbl = 'enr'
    trns_clmns = [
        'memberuid', 'effectivedate', 'terminationdate', 'createddate', 'productcode',  'groupplantypecode',
        'acaindicator', 'acaissuerstatecode', 'acaonexchangeindicator', 'acaactuarialvalue', 'payergroupcode',
        'payertypecode', 'input_file_name', 'medicalindicator', 'rxindicator'
    ]

    logger.log('    -Filtering registry hvid from {} table'.format(trans_tbl))
    trans_df = driver.spark.table(trans_tbl)
    trans_df.join(filtered_mp_df, trans_df['memberuid'] == filtered_mp_df['claimid'], 'inner').\
        select(*[trans_df[c] for c in trans_df.columns if c in trns_clmns])\
        .distinct().cache_and_track(trans_df)\
        .repartition(partitions, 'memberuid').createOrReplaceTempView(trans_tbl)

    for ref_tbl, ref_clmn in [('mbr', 'memberuid')]:
        ref_loc = tmp_loc + ref_tbl + '/'
        logger.log('    -Filtering {} table'.format(ref_tbl))
        ref_df = driver.spark.table(ref_tbl)
        ref_df.join(driver.spark.table(trans_tbl), [ref_clmn]).select(*[ref_df[c] for c in ref_df.columns])\
            .distinct().repartition(2)\
            .write.parquet(ref_loc, compression='gzip', mode='overwrite')
        driver.spark.read.parquet(ref_loc).repartition(1).createOrReplaceTempView(ref_tbl)

    registry_df.createOrReplaceTempView(registry_tbl)
    filtered_mp_df.createOrReplaceTempView(mp_tbl)

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    hdfs_utils.clean_up_output_hdfs(tmp_loc)
    logger.log('all done')
