"""
inovalon pharmacyclaims normalize
"""
import os
import argparse
import subprocess
from dateutil.relativedelta import relativedelta
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v5 as transactional_schemas_v5
from spark.common.datamart.registry.pharmacyclaims.pharmacyclaims_registry_model import schemas as rx_schemas
from spark.common.marketplace_driver import MarketplaceDriver
import spark.common.utility.logger as logger
import spark.helpers.hdfs_utils as hdfs_utils
import spark.datamart.registry.registry_util as reg_util

tmp_loc = '/tmp/rx/'


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
    rx_versioned_schema = rx_schemas['schema_v1']
    output_table_names_to_schemas = {
        'ps20_rx_rxc_05_norm_final': rx_versioned_schema
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    source_table_schema = transactional_schemas_v5

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schema,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=2,
        vdr_feed_id=177,
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

    # logger.log('Loading external tables')
    # rx_hist_path = os.path.join(driver.output_path, rx_versioned_schema.output_directory, 'part_provider=inovalon/')
    # driver.spark.read.parquet(rx_hist_path).createOrReplaceTempView('_temp_pharmacyclaims_hist')

    # registry and matching payload processing
    mp_tbl = 'matching_payload'
    registry_tbl = '_temp_mom_cohort'
    registry_df, filtered_mp_df = reg_util.get_mom_cohort_filtered_mp(driver.spark, provider_partition_name, mp_tbl)

    trans_tbl = 'rxc'
    trns_clmns = [
        'rxclaimuid', 'memberuid', 'provideruid', 'claimstatuscode', 'filldate', 'ndc11code', 'supplydayscount',
        'dispensedquantity', 'billedamount', 'allowedamount', 'copayamount', 'paidamount', 'costamount',
        'prescribingnpi', 'dispensingnpi', 'sourcemodifieddate', 'createddate', 'input_file_name']

    logger.log('    -Filtering registry hvid from {} table'.format(trans_tbl))
    trans_df = driver.spark.table(trans_tbl)
    trans_df.join(filtered_mp_df, trans_df['memberuid'] == filtered_mp_df['claimid'], 'inner').\
        select(*[trans_df[c] for c in trans_df.columns if c in trns_clmns])\
        .repartition(partitions, 'memberuid').createOrReplaceTempView(trans_tbl)

    logger.log('    -Filtering rxcw and rxcc tables')
    rxcw_df = driver.spark.table('rxcw').select('rxclaimuid', 'rxfilluid').distinct()
    rxcc_df = driver.spark.table('rxcc').select('rxfilluid', 'unadjustedprice').distinct()
    rxcw_df.join(
        driver.spark.table(trans_tbl), ['rxclaimuid']).select(rxcw_df.rxclaimuid, rxcw_df.rxfilluid)\
        .join(rxcc_df, ['rxfilluid']).select(rxcw_df.rxclaimuid, rxcc_df.unadjustedprice)\
        .distinct().repartition(3)\
        .write.parquet(tmp_loc + 'rxcw_rxcc/', compression='gzip', mode='overwrite')

    driver.spark.read.parquet(tmp_loc + 'rxcw_rxcc/').repartition(1).createOrReplaceTempView('rxcw_rxcc')

    for ref_tbl, ref_clmn in [('mbr', 'memberuid'), ('prv', 'provideruid'), ('psp', 'provideruid')]:
        logger.log('    -Filtering {} table'.format(ref_tbl))
        ref_loc = tmp_loc + ref_tbl + '/'
        ref_df = driver.spark.table(ref_tbl)
        ref_df.join(driver.spark.table(trans_tbl), [ref_clmn]).select(*[ref_df[c] for c in ref_df.columns])\
            .distinct().repartition(2)\
            .write.parquet(ref_loc, compression='gzip', mode='overwrite')
        driver.spark.read.parquet(ref_loc).repartition(2).createOrReplaceTempView(ref_tbl)

    registry_df.createOrReplaceTempView(registry_tbl)
    filtered_mp_df.createOrReplaceTempView(mp_tbl)

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    #
    # logger.log('Backup historical data')
    # if end_to_end_test:
    #     tmp_path = 's3://salusv/testing/dewey/airflow/e2e/inovalon/pharmacyclaims/backup/part_provider=inovalon/'
    # else:
    #     tmp_path = 's3://salusv/backup/registry/inovalon/pharmacyclaims/{}/part_provider=inovalon/'.format(date_input)
    # date_part = 'part_best_date={}/'
    #
    # current_year_month = date_input[:7] + '-01'
    # one_month_prior = (driver.date_input - relativedelta(months=1)).strftime('%Y-%m-01')
    # two_months_prior = (driver.date_input - relativedelta(months=2)).strftime('%Y-%m-01')
    #
    # for month in [current_year_month, one_month_prior, two_months_prior]:
    #     subprocess.check_call(
    #         ['aws', 's3', 'mv', '--recursive',
    #          rx_hist_path + date_part.format(month), tmp_path + date_part.format(month)]
    #     )

    driver.copy_to_output_path()
    hdfs_utils.clean_up_output_hdfs(tmp_loc)
    logger.log('Done')
