"""
 CDC Genomics delivery - Quest Overlap
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
import spark.helpers.normalized_records_unloader as normalized_records_unloader

# Provider
base_part_provider = 'quest_rinse'

base_extract_sql = """SELECT DISTINCT
        substring_index(vendor_record_id,'~',1) as accession_id, hvid
    FROM {base_part_provider_tbl}
    WHERE lab_id = '11'
        AND test_ordered_name like '%SARS%'
        AND result_id in ('86030478','86030485','86030486','86029812','86029813','86029814')
        AND hvid is not null
    """

# HDFS output locations
stg_loc = '/staging/quest_extract/'
provider_extract_loc = '/staging/quest_provider_extract/'
cdc_stg_loc = '/staging/{}/'

# S3 output locations
s3_cdc_loc = 's3://salusv/warehouse/datahub/cdc_genomics/overlap/quest/'
s3_ops_loc = "s3://salusv/warehouse/datahub/prodops/cdc_genomics_overlap/quest/ops_dt={date_input}/"

table_list = ['quest_hvid_overlap_all_years']

# Labtests warehouse data location
labtests_loc = "s3://salusv/warehouse/parquet/labtests/2017-02-16/part_provider={}/"


# Labtests providers for crosswalk
part_provider_list = [
    'aurora_diagnostics',
    'bioreference',
    'labcorp_covid',
    'luminate',
    'neogenomics',
    'ovation'
    ]

if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_known_args()[0]
    date_input = args.date
    logger.log("Quest CDC Refresh- {}". format(date_input))

    # init
    spark, sql_context = init("Quest CDC Refresh- {}". format(date_input.replace('/', '')))

    # initialize runner
    runner = Runner(sql_context)

    # extract from base
    spark.read.parquet(labtests_loc.format(base_part_provider)).createOrReplaceTempView('labtests_base_provider_tbl')

    spark.sql(base_extract_sql.format(base_part_provider_tbl='labtests_base_provider_tbl')).\
        repartition(2).write.parquet(stg_loc, compression='gzip', mode='overwrite')

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
        df_stg.join(df_prov, ['hvid']).select(df_stg.accession_id, df_stg.hvid, df_prov.part_best_date)\
            .withColumn("part_best_date", df_prov["part_best_date"].cast(StringType()))\
            .distinct().repartition(1).write.parquet(
            provider_extract_loc + 'part_provider={}/'.format(part_provider), compression='gzip', mode='overwrite')

        additional_data_clmn = additional_data_clmn + """,ols.{provider}""".format(provider=part_provider)
        select_sql = select_sql + select_stmnt.format(provider=part_provider)
        join_sql = join_sql + join_stmnt.format(provider=part_provider)

        logger.log("completed {}".format(part_provider))
        
    logger.log('........crosswalk process completed')

    df_stg.createOrReplaceTempView('cdc_stg_tbl')

    # Loading all crosswalk results
    spark.read.parquet(provider_extract_loc).createOrReplaceTempView('cdc_provider_stg_tbl')
    additional_data_clmn = 'COALESCE(' + additional_data_clmn.strip(',') + ', NULL) AS additional_data'

    overlap_sql = """SELECT DISTINCT ols.accession_id, ols.hvid, 
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

    spark.stop()

    logger.log('transfer to ops')
    for tbl in table_list:
        prod_ops_loc = s3_ops_loc.format(date_input=date_input.replace('/', '-')) + '{}/'.format(tbl)
        cdc_ops_loc = s3_cdc_loc + '{}/'.format(tbl)

        logger.log('transfer to prodops {}'.format(tbl))
        subprocess.check_call(['aws', 's3', 'rm', '--recursive', prod_ops_loc])
        normalized_records_unloader.distcp(prod_ops_loc, src=cdc_stg_loc.format(tbl))

        logger.log('transfer to cdcops {}'.format(tbl))
        subprocess.check_call(['aws', 's3', 'rm', '--recursive', cdc_ops_loc])
        normalized_records_unloader.distcp(cdc_ops_loc, src=prod_ops_loc, deleteOnSuccess=False)

    hdfs_utils.clean_up_output_hdfs('/staging/')
    logger.log('Done')
