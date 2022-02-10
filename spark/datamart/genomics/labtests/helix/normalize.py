"""
 CDC Genomics delivery - Helix Overlap
"""
import argparse
import time
import subprocess
import spark.common.utility.logger as logger
from pyspark.sql.types import StringType

from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.datamart.genomics.labtests.helix.transactional_schemas as source_table_schema
import spark.helpers.normalized_records_unloader as normalized_records_unloader

# HDFS output locations
stg_loc = '/staging/helix_extract/'
provider_extract_loc = '/staging/helix_provider_extract/'
cdc_stg_loc = '/staging/{}/'

# S3 output locations
s3_cdc_loc = 's3://salusv/warehouse/datahub/cdc_genomics/overlap/{}/'
s3_ops_loc = "s3://salusv/warehouse/datahub/prodops/cdc_genomics_overlap/helix_hv004689/ops_dt={date_input}/"

table_list = ['helix_hvid_overlap_all_years']

# Labtests warehouse data location
labtests_loc = "s3://salusv/warehouse/parquet/labtests/2017-02-16/part_provider={}/"

# Transaction and payload location
transaction_path = 's3://salusv/incoming/census/helix/hv004689/'
payload_path = 's3://salusv/matching/payload/census/helix/hv004689/'

# Labtests providers for crosswalk
part_provider_list = [
    'aurora_diagnostics',
    'bioreference',
    'labcorp_covid',
    'luminate',
    'neogenomics',
    'ovation',
    'quest',
    'quest_rinse']


def get_part_file_path(list_cmd, directory):
    for row in subprocess.check_output(list_cmd + [directory]).decode().split('\n'):
        file_path = row.split(' ')[-1].replace('//', '/')
        prefix = (directory + 'part-00000')
        if file_path.startswith(prefix):
            return file_path

if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_known_args()[0]
    date_input = args.date
    logger.log("Helix CDC Refresh- {}".format(date_input))

    # init
    spark, sql_context = init("Helix CDC Refresh- {}".format(date_input.replace('/', '')))

    # initialize runner
    runner = Runner(sql_context)

    # list all batch locations
    transaction_batches = [path for path in s3_utils.list_folders(transaction_path, full_path=True)]
    payload_batches = [path for path in s3_utils.list_folders(payload_path, full_path=True)]

    logger.log('Loading the source data')

    logger.log(' -loading: transactions')
    records_loader.load_and_clean_all_v2(
        runner, transaction_batches, source_table_schema, load_file_name=True, spark_context=spark)
    df_trans = spark.table('txn')

    logger.log(' -Loading: payload')
    df_pay = payload_loader.load(runner, payload_batches, cache=True, return_output=True)
    logger.log('........extract process started')

    # Joining transaction and payload data, and storing result
    df_trans.join(df_pay, ['hvjoinkey']).select(
        df_trans.claimid, df_trans.hvjoinkey, df_pay.hvid).distinct().repartition(2).write.parquet(
        stg_loc, compression='gzip', mode='overwrite')

    # Loading stored data
    df_stg = spark.read.parquet(stg_loc)
    df_cnt = df_stg.count()
    logger.log('........extract process completed. count: {}'.format(df_cnt))

    logger.log('........crosswalk process started')
    # Dynamic SQL for processing
    select_sql = ''
    select_stmnt = """, case when {provider}.hvid is not null then 'Y' else null end as {provider} \n\t"""
    join_sql = ''
    join_stmnt = """ 
    LEFT OUTER JOIN cdc_provider_stg_tbl {provider} 
        on {provider}.part_provider='{provider}' and stg.hvid = {provider}.hvid"""
    additional_data_clmn = ''

    for part_provider in part_provider_list:
        logger.log(" - runnning {}".format(part_provider))

        # Load labtests data from warehouse
        df_prov = spark.read.parquet(labtests_loc.format(part_provider))
        logger.log("...write " + provider_extract_loc + 'part_provider={}/'.format(part_provider))

        # Crosswalk and storing result
        df_stg.join(df_prov, ['hvid']).select(df_stg.claimid, df_stg.hvid, df_prov.part_best_date)\
            .withColumn("part_best_date", df_prov["part_best_date"].cast(StringType()))\
            .distinct().repartition(1).write.parquet(
            provider_extract_loc + 'part_provider={}/'.format(part_provider), compression='gzip', mode='overwrite')

        additional_data_clmn = additional_data_clmn + """,ols.{provider}""".format(provider=part_provider)
        select_sql = select_sql + select_stmnt.format(provider=part_provider)
        join_sql = join_sql + join_stmnt.format(provider=part_provider, filter='')

        logger.log("completed {}".format(part_provider))
        
    logger.log('........crosswalk process completed')

    df_stg.createOrReplaceTempView('cdc_stg_tbl')

    # Loading all crosswalk results
    spark.read.parquet(provider_extract_loc).createOrReplaceTempView('cdc_provider_stg_tbl')
    additional_data_clmn = 'COALESCE(' + additional_data_clmn.strip(',') + ', NULL) AS additional_data'

    overlap_sql = """SELECT DISTINCT ols.claimid as identifier, ols.hvid, 
        {additional_data} FROM (  
        SELECT  
            stg.* 
        {select_sql}
        FROM cdc_stg_tbl stg
            {join_sql}
        ) ols
    """

    for tbl in table_list:
        logger.log('writing {}'.format(tbl))
        v_sql = overlap_sql.format(additional_data=additional_data_clmn, select_sql=select_sql, join_sql=join_sql)
        logger.log(v_sql)
        spark.sql(v_sql).repartition(1).write.parquet(cdc_stg_loc.format(tbl), compression='gzip', mode='overwrite')

    output_path = cdc_stg_loc.format('send_to_helix') + '{}/'.format(date_input.replace('/', ''))
    spark.read.parquet(cdc_stg_loc.format('helix_hvid_overlap_all_years'))\
        .select('identifier').distinct()\
        .repartition(1)\
        .write.csv(output_path, sep='|', compression='gzip', mode='append')
    spark.stop()

    list_cmd = ['hadoop', 'fs', '-ls']
    move_cmd = ['hadoop', 'fs', '-mv']
    fn = get_part_file_path(list_cmd, output_path)
    cmd = move_cmd + [fn, output_path + 'hv_cdc_helix_identifiers_{}.psv.gz'.format(date_input.replace('/', '_'))]
    subprocess.check_call(cmd)

    logger.log('transfer to ops')
    for tbl in table_list:
        prod_ops_loc = s3_ops_loc.format(date_input=date_input.replace('/', '-')) + '{}/'.format(tbl)
        cdc_ops_loc = s3_cdc_loc.format('helix') + '{}/'.format(tbl)

        logger.log('transfer to prodops {}'.format(tbl))
        subprocess.check_call(['aws', 's3', 'rm', '--recursive', prod_ops_loc])
        normalized_records_unloader.distcp(prod_ops_loc, src=cdc_stg_loc.format(tbl))

        logger.log('transfer to cdcops {}'.format(tbl))
        subprocess.check_call(['aws', 's3', 'rm', '--recursive', cdc_ops_loc])
        normalized_records_unloader.distcp(cdc_ops_loc, src=prod_ops_loc, deleteOnSuccess=False)

    logger.log('transfer to hv-out {}'.format('send_to_helix'))
    subprocess.check_call(['aws', 's3', 'rm', '--recursive', s3_cdc_loc.format('send_to_helix')])
    normalized_records_unloader.distcp(
        s3_cdc_loc.format('send_to_helix'), src=cdc_stg_loc.format('send_to_helix'), deleteOnSuccess=False)

    hdfs_utils.clean_up_output_hdfs('/staging/')
    logger.log('Done')
