import argparse
import time
import subprocess
import spark.common.utility.logger as logger

from datetime import timedelta, datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.datamart.helix_genomics_cdc.transactional_schemas as source_table_schema
import spark.helpers.normalized_records_unloader as normalized_records_unloader

helix_stg_loc = '/staging/helix_extract/'
helix_provider_stg_loc = '/staging/helix_provider_extract/part_provider={}/'
helic_provider_extract_loc = '/staging/helix_provider_extract/'
s3_helix_cdc_loc = 's3://salusv/warehouse/datahub/cdc_genomics/overlap/helix/'
s3_helix_ops_loc = "s3://salusv/warehouse/datahub/prodops/cdc_genomics_overlap/helix_hv004689/ops_dt={date_input}/"
helix_cdc_stg_loc = '/staging/{}/'

part_provider_list = [
    'aurora_diagnostics',
    'bioreference',
    'labcorp_covid',
    'luminate',
    'neogenomics',
    'ovation',
    'quest',
    'quest_rinse']

labtests_loc = "s3://salusv/warehouse/parquet/labtests/2017-02-16/part_provider={}/"

if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_known_args()[0]
    date_input = args.date

    # init
    spark, sql_context = init("Helix CDC Refresh- {}". format(date_input.replace('/','')))

    # initialize runner
    runner = Runner(sql_context)
    transaction_path = 's3://salusv/incoming/census/helix/hv004689/'
    payload_path = 's3://salusv/matching/payload/census/helix/hv004689/'

    # list all batch locations
    transaction_batches = [ path for path in s3_utils.list_folders(transaction_path, full_path=True)]
    payload_batches = [ path for path in s3_utils.list_folders(payload_path, full_path=True)]

    logger.log('Loading the source data')
    logger.log(' -loading: transactions')
    records_loader.load_and_clean_all_v2(runner, transaction_batches, source_table_schema,
                                        load_file_name=True,
                                        spark_context=spark)
    df_trans = spark.table('txn')
    logger.log(' -Loading: payload')
    df_pay = payload_loader.load(runner, payload_batches, cache=True, return_output=True)
    logger.log('........extract process started')

    temp_df = df_trans.join(df_pay, ['hvjoinkey']).select(
        df_trans.claimid, df_trans.hvjoinkey, df_pay.hvid).distinct().repartition(2).write.parquet(
        helix_stg_loc, compression='gzip', mode='overwrite')

    df_helix_stg = spark.read.parquet(helix_stg_loc)
    df_cnt = df_helix_stg.count()
    logger.log('........extract process completed. count: {}'.format(df_cnt))

    logger.log('........crosswalk process started')
    overlap_select_sql = ''
    select_stmnt = """, case when {provider}.hvid is not null then 'Y' else null end as {provider} \n\t"""
    overlap_filter_sql_2018 = ''
    overlap_filter_sql_all = ''
    filter_stmnt = """ 
    left outer join helix_provider_stg {provider} 
        on {provider}.part_provider='{provider}' and stg.hvid = {provider}.hvid {filter} """
    additional_data_clmn = ''

    for part_provider in part_provider_list:
        logger.log(" - runnning {}".format(part_provider))
        df_prov = spark.read.parquet(labtests_loc.format(part_provider))
        logger.log("...write {}".format(helix_provider_stg_loc.format(part_provider)))
        df_helix_stg.join(df_prov, ['hvid']).select(
            df_helix_stg.hvid, df_prov.part_best_date).distinct().repartition(1).write.parquet(
            helix_provider_stg_loc.format(part_provider), compression='gzip', mode='overwrite')
        additional_data_clmn = additional_data_clmn + """,ols.{provider}""".format(provider=part_provider)
        overlap_select_sql = overlap_select_sql + select_stmnt.format(provider=part_provider)

        filter = " and {provider}.part_best_date>='2018-01-01'".format(provider=part_provider)
        overlap_filter_sql_2018 = overlap_filter_sql_2018 + filter_stmnt.format(provider=part_provider, filter=filter)
        overlap_filter_sql_all = overlap_filter_sql_all + filter_stmnt.format(provider=part_provider, filter='')

        logger.log("completed {}".format(part_provider))
        
    logger.log('........crosswalk process completed')

    df_helix_stg.createOrReplaceTempView('helix_stg')
    spark.read.parquet(helic_provider_extract_loc).createOrReplaceTempView('helix_provider_stg')
    additional_data_clmn = additional_data_clmn.strip(',')

    overlap_sql = """SELECT DISTINCT ols.claimid, ols.hvid, 
        {additional_data} FROM (  
        SELECT  
            stg.* 
        {select_sql}
        FROM helix_stg stg
            {filter_sql}
        ) ols
    """
    overlap_sql_2018 = overlap_sql.format(
        additional_data=additional_data_clmn, select_sql=overlap_select_sql, filter_sql=overlap_filter_sql_2018)

    overlap_sql_all = overlap_sql.format(
        additional_data=additional_data_clmn, select_sql=overlap_select_sql, filter_sql=overlap_filter_sql_all)

    logger.log('writing helix_hvid_overlap_after_2018')
    logger.log(overlap_sql_2018)
    spark.sql(overlap_sql_2018).repartition(1).write.parquet(
        helix_cdc_stg_loc.format('helix_hvid_overlap_after_2018'), compression='gzip', mode='overwrite')

    logger.log('writing helix_hvid_overlap_all_years')
    logger.log(overlap_sql_all)
    spark.sql(overlap_sql_all).repartition(1).write.parquet(
        helix_cdc_stg_loc.format('helix_hvid_overlap_all_years'), compression='gzip', mode='overwrite')
    logger.log('transfer to prodops')

    for tbl in ['helix_hvid_overlap_all_years', 'helix_hvid_overlap_after_2018']:
        logger.log('transfer to prodops {}'.format(tbl))
        prod_ops_loc = s3_helix_ops_loc.format(date_input) + '{}/'.format(tbl)
        subprocess.check_call(['aws', 's3', 'rm', '--recursive', prod_ops_loc])
        normalized_records_unloader.distcp(prod_ops_loc, helix_cdc_stg_loc.format(tbl))

    hdfs_utils.clean_up_output_hdfs('/staging/')

    logger.log('transfer to cdc production')
    subprocess.check_call(['aws', 's3', 'rm', '--recursive', s3_helix_cdc_loc])
    normalized_records_unloader.distcp(s3_helix_ops_loc.format(date_input), s3_helix_cdc_loc)

    logger.log('Done')
    spark.stop()
